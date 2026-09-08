"""Small pooled PostgreSQL store. Every ownership mutation uses a fencing token."""
import hashlib
import json
from pathlib import Path
import time


def query_hash(query):
    # Source/search-shape version participates; query relevance is never document identity.
    normalized = ' '.join(query.casefold().split())
    return hashlib.sha256(json.dumps([normalized, 'libgen-search-v1'], ensure_ascii=False).encode()).hexdigest()


class CoordinationUnavailable(RuntimeError):
    pass


class Database:
    def __init__(self, url, *, pool_size=4):
        from psycopg_pool import ConnectionPool
        from psycopg.rows import dict_row
        # Never interpolate or log a DSN, including errors from pool startup.
        try:
            self.pool = ConnectionPool(url, min_size=1, max_size=pool_size, timeout=10,
                kwargs={'row_factory': dict_row, 'connect_timeout': 8,
                        'options': '-c statement_timeout=10000 -c lock_timeout=5000'}, open=False)
            self.pool.open(wait=True, timeout=10)
        except Exception:
            if hasattr(self, 'pool'):
                self.pool.close()
            raise CoordinationUnavailable('Cannot connect to the shared PostgreSQL database') from None

    def initialize(self):
        with self.pool.connection() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(76280908)')
            conn.execute(Path(__file__).with_name('distributed_schema.sql').read_text())

    def close(self):
        self.pool.close()

    def register_run(self, run_id, manifest, instance, shard):
        from psycopg.types.json import Jsonb
        qh = query_hash(manifest['query'])
        manifest = {**manifest, 'query': ' '.join(manifest['query'].casefold().split())}
        with self.pool.connection() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', (qh,))
            previous = conn.execute('SELECT manifest FROM pipeline_runs WHERE query_hash=%s', (qh,)).fetchall()
            if any(row['manifest']['max_pages'] != manifest['max_pages'] or row['manifest']['shard_count'] != manifest['shard_count'] for row in previous):
                raise ValueError('Query already has a different page/shard configuration in this database')
            conn.execute('INSERT INTO pipeline_runs(run_id,query_hash,manifest) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                         (run_id, qh, Jsonb(manifest)))
            row = conn.execute('SELECT manifest FROM pipeline_runs WHERE run_id=%s FOR UPDATE', (run_id,)).fetchone()
            if row['manifest'] != manifest:
                raise ValueError('Distributed RUN_ID configuration mismatch: query, pages, target, shards or storage differ')
            conn.execute('INSERT INTO pipeline_instances(instance_id,run_id,shard_id) VALUES(%s,%s,%s)', (instance,run_id,shard))
            for page in range(1, manifest['max_pages'] + 1):
                conn.execute('INSERT INTO search_pages(query_hash,page_number,assigned_shard) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING',
                             (qh,page,(page-1)%manifest['shard_count']))
        return qh

    def upsert(self, candidate, qh, stage1='APPROVED', version='stage1-v1'):
        from psycopg.types.json import Jsonb
        with self.pool.connection() as conn:
            conn.execute('''INSERT INTO documents(document_id,canonical_title,source_url,source_metadata,filename)
                VALUES(%s,%s,%s,%s,%s) ON CONFLICT(document_id) DO UPDATE SET
                canonical_title=CASE WHEN documents.canonical_title='' THEN excluded.canonical_title ELSE documents.canonical_title END,
                source_metadata=documents.source_metadata || excluded.source_metadata,
                filename=CASE WHEN excluded.source_metadata ? 'mirrors' THEN excluded.filename ELSE documents.filename END''',
                (candidate['document_id'],candidate.get('title',''),candidate.get('source_url',''),Jsonb(candidate),candidate['filename']))
            conn.execute('''INSERT INTO query_document_results(query_hash,document_id,prompt_version,stage1_status)
                VALUES(%s,%s,%s,%s) ON CONFLICT(query_hash,document_id) DO UPDATE
                SET stage1_status=excluded.stage1_status,prompt_version=excluded.prompt_version''',
                (qh,candidate['document_id'],version,stage1))

    def document(self, document):
        with self.pool.connection() as conn:
            return conn.execute('SELECT * FROM documents WHERE document_id=%s', (document,)).fetchone()

    def claim(self, document, instance, run_id, qh, seconds, max_attempts=5):
        # Run row serializes target admission; document UPDATE serializes all
        # competing runtimes, including owners from different queries/runs.
        with self.pool.connection() as conn:
            run = conn.execute('SELECT manifest FROM pipeline_runs WHERE run_id=%s FOR UPDATE', (run_id,)).fetchone()
            accepted = conn.execute("SELECT count(*) n FROM query_document_results WHERE query_hash=%s AND final_query_status='APPROVED'", (qh,)).fetchone()['n']
            if accepted >= run['manifest']['target_documents']:
                return None
            row = conn.execute('''UPDATE documents SET claimed_by=%s,claim_token=claim_token+1,
                lease_expires_at=clock_timestamp() + %s * interval '1 second',download_status='CLAIMED',
                document_attempt=document_attempt+1,updated_at=clock_timestamp()
                WHERE document_id=%s AND download_status NOT IN ('COMPLETED','PERMANENTLY_FAILED')
                AND document_attempt < %s AND not_before <= clock_timestamp()
                AND (claimed_by IS NULL OR lease_expires_at < clock_timestamp()) RETURNING *''',
                (instance,seconds,document,max_attempts)).fetchone()
            return row

    def renew(self, document, instance, token, seconds, size):
        with self.pool.connection() as conn:
            return conn.execute('''UPDATE documents SET lease_expires_at=clock_timestamp()+%s*interval '1 second',
                downloaded_bytes=%s,download_status='DOWNLOADING',updated_at=clock_timestamp()
                WHERE document_id=%s AND claimed_by=%s AND claim_token=%s
                AND lease_expires_at > clock_timestamp() RETURNING document_id''',
                (seconds,size,document,instance,token)).fetchone() is not None

    def finish(self, document, instance, token, *, key=None, size=0, sha=None, error='', until=0, permanent=False, partial=False, refund=False, route='', rate_limited=False):
        status = 'COMPLETED' if key else ('PERMANENTLY_FAILED' if permanent else ('RATE_LIMIT_WAIT' if rate_limited else ('FAST_HANDOFF_READY' if partial else 'RECOVERY_READY')))
        with self.pool.connection() as conn:
            return conn.execute('''UPDATE documents SET download_status=%s, storage_location=COALESCE(%s,storage_location),
                final_size=COALESCE(%s,final_size),sha256=COALESCE(%s,sha256),downloaded_bytes=%s,last_error=%s,
                not_before=to_timestamp(%s),consecutive_failures=CASE WHEN %s THEN 0 ELSE consecutive_failures+1 END,
                fast_handoff_count=fast_handoff_count+%s,document_attempt=GREATEST(0,document_attempt-%s),last_route=%s,
                claimed_by=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
                WHERE document_id=%s AND claimed_by=%s AND claim_token=%s
                AND lease_expires_at > clock_timestamp() RETURNING document_id''',
                (status,key,size if key else None,sha,size,error[:500],until,bool(key),int(partial),int(refund),route,document,instance,token)).fetchone() is not None

    def invalidate_missing(self, document, key):
        with self.pool.connection() as conn:
            conn.execute("UPDATE documents SET download_status='RECOVERY_READY',storage_location=NULL WHERE document_id=%s AND storage_location=%s AND claimed_by IS NULL", (document,key))

    def recovery(self, qh, limit=50):
        with self.pool.connection() as conn:
            conn.execute("UPDATE documents SET download_status='PERMANENTLY_FAILED',claimed_by=NULL,lease_expires_at=NULL WHERE document_attempt>=5 AND download_status<>'COMPLETED' AND (claimed_by IS NULL OR lease_expires_at<clock_timestamp())")
            return conn.execute('''SELECT d.* FROM documents d JOIN query_document_results q USING(document_id)
                WHERE q.query_hash=%s AND q.stage1_status='APPROVED' AND d.download_status NOT IN ('COMPLETED','PERMANENTLY_FAILED')
                AND d.not_before<=clock_timestamp() AND (d.claimed_by IS NULL OR d.lease_expires_at<clock_timestamp())
                ORDER BY d.updated_at LIMIT %s''',(qh,limit)).fetchall()

    def progress(self,document,instance,token,size):
        with self.pool.connection() as conn:
            return conn.execute('''UPDATE documents SET downloaded_bytes=%s,updated_at=clock_timestamp()
                WHERE document_id=%s AND claimed_by=%s AND claim_token=%s AND lease_expires_at>clock_timestamp()
                RETURNING document_id''',(size,document,instance,token)).fetchone() is not None

    def claim_page(self, qh, page, shard, instance, seconds, takeover=False):
        with self.pool.connection() as conn:
            return conn.execute('''UPDATE search_pages SET claimed_by=%s,claim_token=claim_token+1,status='RUNNING',
                started_at=clock_timestamp(),lease_expires_at=clock_timestamp()+%s*interval '1 second'
                WHERE query_hash=%s AND page_number=%s AND status<>'COMPLETED'
                AND (assigned_shard=%s OR (%s AND lease_expires_at<clock_timestamp()))
                AND (claimed_by IS NULL OR lease_expires_at<clock_timestamp()) RETURNING claim_token''',
                (instance,seconds,qh,page,shard,takeover)).fetchone()

    def page_done(self, qh,page,instance,token,found,error=''):
        with self.pool.connection() as conn:
            conn.execute('''UPDATE search_pages SET status=%s,completed_at=clock_timestamp(),documents_found=%s,
                last_error=%s,claimed_by=NULL,lease_expires_at=NULL WHERE query_hash=%s AND page_number=%s
                AND claimed_by=%s AND claim_token=%s AND lease_expires_at>clock_timestamp()''',
                ('FAILED' if error else 'COMPLETED',found,error[:200],qh,page,instance,token))

    def heartbeat(self, instance, metrics, qh, seconds, validations=()):
        from psycopg.types.json import Jsonb
        with self.pool.connection() as conn:
            conn.execute('UPDATE pipeline_instances SET last_heartbeat=clock_timestamp(),metrics=%s WHERE instance_id=%s', (Jsonb(metrics),instance))
            conn.execute("UPDATE search_pages SET lease_expires_at=clock_timestamp()+%s*interval '1 second' WHERE claimed_by=%s AND lease_expires_at>clock_timestamp()", (seconds,instance))
            for query, document, token in validations:
                conn.execute("UPDATE query_document_results SET validation_lease=clock_timestamp()+%s*interval '1 second' WHERE query_hash=%s AND document_id=%s AND validation_owner=%s AND validation_token=%s AND validation_lease>clock_timestamp()", (seconds,query,document,instance,token))

    def release_validation(self, qh, document, instance, token):
        with self.pool.connection() as conn:
            conn.execute('''UPDATE query_document_results SET validation_owner=NULL,validation_lease=NULL
                WHERE query_hash=%s AND document_id=%s AND validation_owner=%s AND validation_token=%s''',
                (qh,document,instance,token))

    def stage1_cached(self, qh, document, version):
        with self.pool.connection() as conn:
            row = conn.execute('SELECT stage1_status FROM query_document_results WHERE query_hash=%s AND document_id=%s AND prompt_version=%s',(qh,document,version)).fetchone()
            return row['stage1_status'] if row else None

    def validation(self, qh, document, sha):
        with self.pool.connection() as conn:
            row = conn.execute("SELECT result FROM query_document_results WHERE query_hash=%s AND document_id=%s AND validation_sha=%s AND final_query_status IN ('APPROVED','REJECTED')",(qh,document,sha)).fetchone()
            return row['result'] if row else None

    def claim_validation(self, qh, document, instance, seconds):
        with self.pool.connection() as conn:
            return conn.execute('''UPDATE query_document_results SET validation_owner=%s,validation_token=validation_token+1,
                validation_lease=clock_timestamp()+%s*interval '1 second' WHERE query_hash=%s AND document_id=%s
                AND (validation_owner IS NULL OR validation_lease<clock_timestamp()) RETURNING validation_token''',
                (instance,seconds,qh,document)).fetchone()

    def save_validation(self,qh,document,sha,instance,token,result):
        from psycopg.types.json import Jsonb
        with self.pool.connection() as conn:
            row=conn.execute('''UPDATE query_document_results SET result=%s,validation_sha=%s,stage2_query_relevant=%s,
                final_query_status=%s,validation_owner=NULL,validation_lease=NULL
                WHERE query_hash=%s AND document_id=%s AND validation_owner=%s AND validation_token=%s
                AND validation_lease>clock_timestamp() RETURNING document_id''',
                (Jsonb(result),sha,result.get('query_relevant'),result.get('status','PENDING'),qh,document,instance,token)).fetchone()
            if row and isinstance(result.get('ashrae_identity'),bool):
                conn.execute('UPDATE documents SET ashrae_identity=%s,identity_sha=%s,identity_result=%s WHERE document_id=%s AND sha256=%s',
                    (result['ashrae_identity'],sha,Jsonb(result),document,sha))
            return bool(row)

    def defer_host(self,host,until):
        with self.pool.connection() as conn:
            conn.execute('''INSERT INTO source_limits(host,not_before) VALUES(%s,to_timestamp(%s))
                ON CONFLICT(host) DO UPDATE SET not_before=GREATEST(source_limits.not_before,excluded.not_before)''',(host,until))

    def host_not_before(self,host):
        with self.pool.connection() as conn:
            row=conn.execute("SELECT EXTRACT(EPOCH FROM not_before) t FROM source_limits WHERE host=%s AND not_before>clock_timestamp()",(host,)).fetchone()
            return float(row['t']) if row else 0

    def reserve_start(self,host,spacing):
        with self.pool.connection() as conn:
            conn.execute('INSERT INTO source_limits(host) VALUES(%s) ON CONFLICT DO NOTHING',(host,))
            row=conn.execute('''UPDATE source_limits SET next_start=clock_timestamp()+%s*interval '1 second'
                WHERE host=%s AND next_start<=clock_timestamp() AND not_before<=clock_timestamp() RETURNING host''',(spacing,host)).fetchone()
            return bool(row)

    def counters(self,run_id,qh):
        with self.pool.connection() as conn:
            row=conn.execute('''SELECT count(*) FILTER(WHERE d.download_status='COMPLETED') completed,
                count(*) FILTER(WHERE d.claimed_by IS NOT NULL AND d.lease_expires_at>clock_timestamp()) active_claims,
                count(*) FILTER(WHERE d.download_status='FAST_HANDOFF_READY') fast_handoff_ready,
                count(*) FILTER(WHERE d.download_status IN ('RECOVERY_READY','RATE_LIMIT_WAIT')) recovery_ready,
                count(*) discovered,count(*) FILTER(WHERE q.final_query_status='APPROVED') accepted
                FROM documents d JOIN query_document_results q USING(document_id) WHERE q.query_hash=%s''',(qh,)).fetchone()
            row['pages_completed']=conn.execute("SELECT count(*) n FROM search_pages WHERE query_hash=%s AND status='COMPLETED'",(qh,)).fetchone()['n']
            rows=conn.execute("SELECT metrics FROM pipeline_instances WHERE run_id=%s AND last_heartbeat>clock_timestamp()-interval '60 seconds'",(run_id,)).fetchall()
            row['global_goodput_mib_s']=sum(float(r['metrics'].get('goodput_mib_s',0)) for r in rows)
            return row
