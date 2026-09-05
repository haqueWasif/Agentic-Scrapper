import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

from crewai import Agent, Task, LLM
from app.agents import storage


class StorageTests(unittest.TestCase):
    def test_corrupt_database_and_concurrent_initialization_need_no_sqlite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_db = root / 'latest_kickoff_task_outputs.db'
            old_db.write_bytes(b'corrupt legacy database')
            with patch.object(storage, 'STORAGE_DIRECTORY', root), patch.dict('os.environ'), patch(
                'sqlite3.connect', side_effect=AssertionError('SQLite must not be opened')
            ):
                def initialize(_):
                    storage.prepare_storage()
                    agent = Agent(role='Evaluator', goal='Evaluate HVAC', backstory='Reviewer',
                                  llm=LLM(model='openrouter/openrouter/free', api_key='test'), memory=False)
                    task = Task(description='Review', expected_output='[]', agent=agent)
                    crew = storage.StatelessCrew(agents=[agent], tasks=[task], memory=False)
                    crew._task_output_handler.reset()
                    crew._task_output_handler.update(0, {})
                    self.assertFalse(crew.memory)
                    return crew._task_output_handler
                with ThreadPoolExecutor(max_workers=3) as pool:
                    handlers = list(pool.map(initialize, range(3)))
                self.assertEqual(len({id(handler) for handler in handlers}), 3)
            self.assertEqual(old_db.read_bytes(), b'corrupt legacy database')

    def test_unwritable_storage_is_reported(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(storage, 'STORAGE_DIRECTORY', Path(directory)), patch(
            'tempfile.TemporaryFile', side_effect=PermissionError('read-only')
        ):
            with self.assertRaisesRegex(RuntimeError, 'not writable'):
                storage.prepare_storage()


if __name__ == '__main__':
    unittest.main()
