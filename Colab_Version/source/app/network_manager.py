"""Small, stateful network layer for resilient document retrieval."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
import random
import threading
import time
from typing import Any
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from email.utils import parsedate_to_datetime

from curl_cffi import requests as c_requests
from app.download_progress import METRICS


def route_key(url):
    """Keep document selectors; ignore fragments and explicit tracking/cache busters."""
    parsed = urlparse(url)
    query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
             if not key.lower().startswith('utm_') and key.lower() not in
             {'_', 'cache_bust', 'cachebuster', 'timestamp', 'ts'}]
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, '',
                       urlencode(sorted(query)), ''))


class RequestDeferred(Exception):
    """Scheduler control flow, never a network timeout or failed attempt."""
    def __init__(self, url, not_before, retry_class='DEFERRED_ROUTE', reason='Route cooldown'):
        super().__init__(reason)
        self.url, self.not_before, self.retry_class = url, not_before, retry_class

    def outcome(self, candidate, part_size=0):
        return dict(success=False, deferred=True, permanent=False, candidate=candidate,
                    attempt=int(candidate.get('_document_attempt',1)), retry_class=self.retry_class,
                    reason=str(self), not_before=self.not_before, retry_after_until=self.not_before,
                    part_size=part_size, downloaded_bytes=part_size, route=route_key(self.url))


@dataclass(frozen=True)
class NetworkSettings:
    proxy_enabled: bool = False
    max_workers: int = 5
    retry_limit: int = 5
    retry_window_seconds: int = 1800
    cooldown_enabled: bool = True
    new_download_min_seconds: float = 2.0
    new_download_max_seconds: float = 6.0
    max_request_retries_per_attempt: int = 2
    max_stall_seconds: int = 60
    max_document_attempts: int = 5
    recovery_round_delay_seconds: float = 5.0
    download_queue_maxsize: int = 50
    validation_workers: int = 2
    stage2_queue_maxsize: int = 20
    defer_download_retries: bool = False
    scheduler_admission: bool = False
    max_fast_handoffs_per_attempt: int = 2
    fast_handoff_delay_seconds: float = 1.0


class NetworkManager:
    """Owns reusable worker sessions, optional proxy rotation, and UI-safe events."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.settings = self._read_settings(project_root / "config" / "settings.yaml")
        self.proxies = self._read_proxies(project_root / "config" / "proxies.txt")
        self.events: Queue[dict[str, Any]] = Queue()
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._sessions: dict[int, Any] = {}
        self._proxy_health = defaultdict(lambda: {"successful_requests": 0, "failed_requests": 0, "last_used": 0.0, "disabled_until": 0.0})
        self._gateway_health = defaultdict(lambda: {
            "gateway_url": "", "success_count": 0, "failure_count": 0,
            "last_failure": None, "score": 0,
        })
        self._last_new_download = 0.0
        self.route_waits = {}
        self.route_failures = {}
        self.host_rate_limits = {}
        self.error_counts = defaultdict(int)
        self.context = threading.local()
        self.telemetry = None
        self.distributed = None

    @staticmethod
    def _read_settings(path: Path) -> NetworkSettings:
        values: dict[str, str] = {}
        if path.exists():
            section = ""
            for raw in path.read_text(encoding="utf-8").splitlines():
                uncommented = raw.split("#", 1)[0]
                line = uncommented.strip()
                if not line or ":" not in line:
                    continue
                key, value = (part.strip() for part in line.split(":", 1))
                if not value and not raw[:len(raw) - len(raw.lstrip())]:
                    section = key
                    continue
                if section:
                    values[f"{section}.{key}"] = value
        def boolean(name: str, default: bool) -> bool:
            return values.get(name, str(default)).lower() in {"true", "1", "yes"}
        def number(name: str, default: float) -> float:
            try:
                return float(values.get(name, default))
            except ValueError:
                return default
        return NetworkSettings(
            proxy_enabled=boolean("network.proxy_enabled", False),
            max_workers=max(1, min(5, int(number("network.max_workers", 5)))),
            retry_limit=max(1, int(number("network.retry_limit", 5))),
            retry_window_seconds=max(60, int(number("network.retry_window_seconds", 1800))),
            cooldown_enabled=boolean("network.cooldown_enabled", True),
            new_download_min_seconds=max(0, number("network.new_download_min_seconds", 2)),
            new_download_max_seconds=max(0, number("network.new_download_max_seconds", 6)),
            max_request_retries_per_attempt=max(1, int(number("network.max_request_retries_per_attempt", 2))),
            max_stall_seconds=max(1, int(number("network.max_stall_seconds", 60))),
            max_document_attempts=max(1, int(number("network.max_document_attempts", 5))),
            recovery_round_delay_seconds=max(0, number("network.recovery_round_delay_seconds", 5)),
            download_queue_maxsize=max(1, int(number("pipeline.download_queue_maxsize", 50))),
            validation_workers=max(1, int(number("validation.max_workers", 2))),
            stage2_queue_maxsize=max(1, int(number("pipeline.stage2_queue_maxsize", 20))),
        )

    @staticmethod
    def _read_proxies(path: Path) -> list[str]:
        if not path.exists():
            return []
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.lstrip().startswith("#")]

    def begin_download(self, worker: int, filename: str) -> str | None:
        if self.settings.scheduler_admission:
            with self._lock:
                return self._select_proxy()
        with self._lock:
            delay = max(0.0, self._last_new_download + random.uniform(self.settings.new_download_min_seconds, self.settings.new_download_max_seconds) - time.monotonic())
            self._last_new_download = time.monotonic() + delay
            proxy = self._select_proxy()
        if delay:
            started = time.perf_counter()
            time.sleep(delay)
            METRICS.add(pacing_wait_seconds=time.perf_counter() - started)
        self.emit(
            "status", worker, filename,
            f"Network Status: Provider: curl_cffi · Worker: {worker}/{self.settings.max_workers} · Proxy: {proxy or 'disabled'}",
            level="DEBUG", category="network",
        )
        return proxy

    def _select_proxy(self) -> str | None:
        if not self.settings.proxy_enabled or not self.proxies:
            return None
        now = time.monotonic()
        candidates = [proxy for proxy in self.proxies if self._proxy_health[proxy]["disabled_until"] <= now]
        if not candidates:
            return None
        proxy = min(candidates, key=lambda item: self._proxy_health[item]["last_used"])
        self._proxy_health[proxy]["last_used"] = now
        return proxy

    def session(self):
        key = threading.get_ident()
        with self._lock:
            if key not in self._sessions:
                self._sessions[key] = c_requests.Session()
            return self._sessions[key]

    @staticmethod
    def request_options(proxy: str | None) -> dict[str, str]:
        return {"proxy": proxy} if proxy else {}

    def result(self, proxy: str | None, success: bool) -> None:
        if not proxy:
            return
        with self._lock:
            health = self._proxy_health[proxy]
            health["successful_requests" if success else "failed_requests"] += 1
            if not success and health["failed_requests"] >= self.settings.retry_limit:
                health["disabled_until"] = time.monotonic() + 300

    def gateway_result(self, gateway_url: str, status_code: int | None = None, *, completed: bool = False, error=None) -> None:
        """Persist a small, per-run health score for binary download gateways."""
        if getattr(self.context, 'deferred', False) and not completed:
            return  # Admission denial is not another failed HTTP request.
        with self._lock:
            key = route_key(gateway_url)
            health = self._gateway_health[gateway_url]
            health["gateway_url"] = gateway_url
            if completed:
                health["success_count"] += 1
                health["score"] += 10 if status_code == 206 else 5 if status_code == 200 else 0
                health['consecutive_failures'] = 0
                self.route_failures[key] = 0
                self.route_waits.pop(key, None)
                return

            category = ('HTTP_' + str(status_code)) if status_code else (
                'connection_reset' if 'reset' in str(error).lower() else
                'timeout' if isinstance(error, TimeoutError) or 'timeout' in type(error).__name__.lower() or 'timed out' in str(error).lower() else 'connection_error')
            self.error_counts[category] += 1
            if self.telemetry:
                self.telemetry.error(category)

            health["failure_count"] += 1
            health["last_failure"] = time.time()
            health["score"] += {
                500: -20, 502: -20, 503: -20, 504: -30,
            }.get(status_code, 0)
            if status_code in (None, 500, 502, 503, 504):
                health['consecutive_failures'] = health.get('consecutive_failures', 0) + 1
                self.route_failures[key] = self.route_failures.get(key, 0) + 1
                if self.route_failures[key] >= 3:
                    until = time.time() + min(300, 30 * self.route_failures[key])
                    self.route_waits[key] = max(self.route_waits.get(key, 0), until)
                    self.context.not_before = max(getattr(self.context, 'not_before', 0), until)

    def rate_limit(self, url, response):
        value = response.headers.get('Retry-After', '')
        try:
            seconds = max(0, float(value))
        except (TypeError, ValueError):
            try:
                seconds = max(0, parsedate_to_datetime(value).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                seconds = 60  # No usable deadline: conservative scheduler delay.
        host = urlparse(url).netloc.lower()
        until = time.time() + seconds
        with self._lock:
            self.host_rate_limits[host] = max(self.host_rate_limits.get(host, 0), until)
            self.error_counts['HTTP_429'] += 1
            if self.telemetry:
                self.telemetry.error('HTTP_429')
        self.context.not_before = max(getattr(self.context, 'not_before', 0), until)
        self.context.rate_limited = True
        if self.distributed:
            self.distributed.db.defer_host(host, until)
        return seconds

    def before_request(self, url, worker=0):
        self.context.last_route = url
        until, kind = self.request_deadline(url)
        if self.distributed:
            self.distributed.ensure_owner()
            shared_until = self.distributed.db.host_not_before(urlparse(url).netloc.lower())
            if shared_until > until:
                until, kind = shared_until, 'DEFERRED_RATE_LIMIT'
        if until > time.time():
            self.context.deferred = True
            self.context.not_before = max(getattr(self.context, 'not_before', 0), until)
            raise RequestDeferred(url, until, kind)
        if self.telemetry:
            self.telemetry.state(worker, 'CONNECTING')

    def request_deadline(self, url):
        with self._lock:
            route_until = self.route_waits.get(route_key(url), 0)
            host_until = self.host_rate_limits.get(urlparse(url).netloc.lower(), 0)
        return (host_until, 'DEFERRED_RATE_LIMIT') if host_until >= route_until else (route_until, 'DEFERRED_ROUTE')

    def diagnostics(self):
        with self._lock:
            now = time.time()
            waiting = {key: value for key, value in self.route_waits.items() if value > now}
            hosts = {key: value for key, value in self.host_rate_limits.items() if value > now}
            return dict(route_failures=dict(self.route_failures), routes_deferred=len(waiting),
                        earliest_route_eligibility=min(waiting.values(),default=None),
                        host_rate_limit_waits=hosts, error_counts=dict(self.error_counts))

    def check_owner(self):
        if self.distributed:
            self.distributed.ensure_owner()

    def gateway_score(self, gateway_url: str) -> int:
        with self._lock:
            return int(self._gateway_health[gateway_url]["score"])

    def cooldown(self, attempt: int) -> None:
        if self.settings.scheduler_admission:
            raise RuntimeError('Scheduler timing belongs to FairDownloadQueue')
        if self.settings.cooldown_enabled:
            started = time.perf_counter()
            time.sleep(min(5 * (2 ** max(0, attempt - 1)), 40))
            METRICS.add(retry_wait_seconds=time.perf_counter() - started)

    def emit(self, kind: str, worker: int, filename: str, message: str = "", **extra: Any) -> None:
        self.events.put({
            "kind": kind, "worker": worker, "filename": filename, "message": message,
            "timestamp": time.time(), **extra,
        })

    def close(self) -> None:
        with self._lock:
            sessions, self._sessions = list(self._sessions.values()), {}
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass
