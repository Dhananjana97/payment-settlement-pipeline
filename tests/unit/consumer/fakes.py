"""In-memory EventStream and LedgerStore for ConsumerApp tests."""
from __future__ import annotations

from collections import defaultdict

from sqlalchemy.exc import OperationalError

from services.consumer.ports import Outcome, Record


class InMemoryStream:
    """Hands out scripted records in poll-sized chunks; records commits; `stop_after_polls` ends the run."""

    def __init__(self, records: list[Record], chunk: int | None = None, parts: list[int] | None = None):
        self._records = list(records)
        self._chunk = chunk
        self.commits: list[tuple[int, int]] = []
        self.polls = 0
        self.closed = False
        self._parts = parts if parts is not None else sorted({r.partition for r in records})

    def poll(self, max_records: int, timeout_s: float) -> list[Record]:
        self.polls += 1
        n = min(max_records, self._chunk or max_records)
        out, self._records = self._records[:n], self._records[n:]
        return out

    def commit(self, partition: int, next_offset: int) -> None:
        self.commits.append((partition, next_offset))

    def assignment(self) -> list[int]:
        return list(self._parts)

    def close(self) -> None:
        self.closed = True

    @property
    def drained(self) -> bool:
        return not self._records


class FakeStore:
    """Applies by recording. `fail_times`: raise OperationalError on the first N apply_batch calls."""

    def __init__(self, fail_times: int = 0, fence: int = 7, status_for: dict[str, str] | None = None):
        self.fail_times = fail_times
        self.fence = fence
        self.status_for = status_for or {}
        self.applied: list[tuple[int, list[Record], int]] = []
        self.calls = 0

    def apply_batch(self, partition: int, records: list[Record], fence_epoch: int) -> list[Outcome]:
        self.calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise OperationalError("connection lost", None, Exception("boom"))
        self.applied.append((partition, list(records), fence_epoch))
        return [Outcome(r.partition, r.offset, self.status_for.get(r.key, "applied"), r.key, r.offset + 1)
                for r in records]

    def fence_epoch(self) -> int:
        return self.fence

    def applied_offsets(self) -> dict[int, list[int]]:
        out = defaultdict(list)
        for p, recs, _ in self.applied:
            out[p].extend(r.offset for r in recs)
        return dict(out)
