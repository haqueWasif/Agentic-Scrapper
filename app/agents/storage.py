"""CrewAI storage policy for stateless page evaluation."""

import os
from pathlib import Path
import tempfile

from crewai import Crew
from pydantic import PrivateAttr


STORAGE_DIRECTORY = Path(__file__).resolve().parents[2] / "runtime" / "crewai"


def prepare_storage() -> Path:
    """Check real write access before Crew initialization, without opening old DBs."""
    STORAGE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryFile(dir=STORAGE_DIRECTORY) as probe:
            probe.write(b"crewai storage check")
            probe.flush()
            os.fsync(probe.fileno())
    except OSError as exc:
        raise RuntimeError(f"CrewAI runtime storage is not writable: {STORAGE_DIRECTORY}") from exc
    # Absolute paths are honored by the installed crewai_core/appdirs path resolver.
    # This also confines optional future storage to the runtime directory.
    os.environ["CREWAI_STORAGE_DIR"] = str(STORAGE_DIRECTORY)
    return STORAGE_DIRECTORY


class DisabledTaskOutputStorage:
    """No historical replay/audit storage; Crew still returns its normal output.

    No SQLite connection is created. Existing corrupt or locked databases are
    therefore harmless and are left intact rather than deleting another run's data.
    Each Crew gets its own handler, with no mutable state shared between reruns.
    """

    def load(self):
        return []

    def reset(self):
        pass

    def update(self, task_index, log):
        pass

    def add(self, *args, **kwargs):
        pass


class StatelessCrew(Crew):
    # CrewAI has no public switch for kickoff-output persistence. Override the
    # factory before Pydantic initializes it, rather than replacing it afterward
    # (which is too late to prevent the SQLite I/O failure).
    _task_output_handler: DisabledTaskOutputStorage = PrivateAttr(
        default_factory=DisabledTaskOutputStorage
    )
