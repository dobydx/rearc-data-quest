"""Where ingest keeps its state and audit: a protocol, plus an in-memory store for tests."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from quest.ingest.models import FileState, FileVersion, IngestRun, RunFile
from quest.ingest.planner import PipelineUpdateInfo


class OpsStore(Protocol):
    """Reads and writes of the ``quest_ops`` tables that ingest needs."""

    def ensure_tables(self) -> None:
        """Create the ops tables if they do not exist."""

    def start_run(self, run: IngestRun) -> None:
        """Insert the run row with status ``running``."""

    def finish_run(self, run: IngestRun) -> None:
        """Update the run row with its final status and counts."""

    def file_states(self, source: str) -> dict[str, FileState]:
        """Return the current state of every tracked file of ``source``, keyed by file name."""
        ...

    def known_shas(self, source: str, file_name: str) -> set[str]:
        """Return every SHA-256 ever landed for one file."""
        ...

    def served_shas(self, source: str, file_name: str) -> set[str]:
        """Return every SHA-256 a committed pointer flip ever made current for one file."""
        ...

    def insert_version(self, version: FileVersion) -> None:
        """Insert a file version; a second insert of the same key does nothing."""

    def record_run_files(self, rows: Sequence[RunFile]) -> None:
        """Record what this run did with each file."""

    def commit_states(self, states: Sequence[FileState]) -> None:
        """Upsert every state in one atomic write."""

    def silver_lineage(self) -> set[tuple[str, str]] | None:
        """Return the ``(file_name, sha256)`` pairs silver serves, or None if silver is absent."""
        ...

    def last_pipeline_update(self) -> PipelineUpdateInfo | None:
        """Return the last pipeline update, or None if there has been none."""
        ...


@dataclass
class InMemoryOpsStore:
    """Dict-backed ``OpsStore`` for unit tests; set the two ``*_value`` fields to steer the gate."""

    runs: dict[str, IngestRun] = field(default_factory=dict)
    versions: dict[tuple[str, str, str], FileVersion] = field(default_factory=dict)
    run_files: dict[tuple[str, str, str], RunFile] = field(default_factory=dict)
    states: dict[tuple[str, str], FileState] = field(default_factory=dict)
    silver_lineage_value: set[tuple[str, str]] | None = None
    last_update_value: PipelineUpdateInfo | None = None

    def ensure_tables(self) -> None:
        """Nothing to create in memory."""

    def start_run(self, run: IngestRun) -> None:
        """Store the run row."""
        self.runs[run.run_id] = run

    def finish_run(self, run: IngestRun) -> None:
        """Replace the run row."""
        self.runs[run.run_id] = run

    def file_states(self, source: str) -> dict[str, FileState]:
        """Return the states of ``source`` keyed by file name."""
        return {name: s for (src, name), s in self.states.items() if src == source}

    def known_shas(self, source: str, file_name: str) -> set[str]:
        """Return every SHA-256 stored for one file."""
        return {sha for (src, name, sha) in self.versions if (src, name) == (source, file_name)}

    def served_shas(self, source: str, file_name: str) -> set[str]:
        """Return ``sha256_after`` of every flipped run-file row of one file."""
        return {
            rf.sha256_after
            for rf in self.run_files.values()
            if (rf.source, rf.file_name) == (source, file_name)
            and rf.pointer_flipped
            and rf.sha256_after is not None
        }

    def insert_version(self, version: FileVersion) -> None:
        """Keep the first row per key, as the Delta MERGE does."""
        self.versions.setdefault((version.source, version.file_name, version.sha256), version)

    def record_run_files(self, rows: Sequence[RunFile]) -> None:
        """Store each row under its primary key."""
        for row in rows:
            self.run_files[(row.run_id, row.source, row.file_name)] = row

    def commit_states(self, states: Sequence[FileState]) -> None:
        """Upsert every state."""
        for s in states:
            self.states[(s.source, s.file_name)] = s

    def silver_lineage(self) -> set[tuple[str, str]] | None:
        """Return the configured lineage."""
        return self.silver_lineage_value

    def last_pipeline_update(self) -> PipelineUpdateInfo | None:
        """Return the configured last update."""
        return self.last_update_value
