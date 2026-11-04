"""ExecutorApp: claims payouts and drives each one through the payout state machine (STEP5-PLAN §3–§4).

Threads, not asyncio (C §3.1): one claim thread fills a small queue, N workers each process one
payout at a time. The state machine (`domain/payout_fsm.py`) decides; this module only does I/O in
the order that makes a crash at any instruction safe:

    renew lease → rate slot → token → COMMIT write-ahead intent (key, pending_send_at) → POST
    → record attempt → COMMIT the decision

Every row write is a compare-and-set on (payout_id, lease_owner, status, attempt): an executor that
lost its lease can't overwrite a newer decision and never calls the bank for that row again.
"""
from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field, fields as dc_fields, replace
from typing import Any, Callable, TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError

from services.common import metrics
from services.common.clock import Clock, SystemClock
from services.common.log import BoundLogger
from services.executor.domain import payout_fsm as fsm
from services.executor.ports import (BankGateway, BankPayout, BankResponse, ExecutorRegistry, PayoutRepository,
                                     PayoutSnapshot, RateLimiter)

T = TypeVar("T")

_SNAPSHOT_FIELDS = {f.name for f in dc_fields(PayoutSnapshot)}
TERMINAL = ("paid", "failed", "rejected", "stuck")
# fields of a Send decision that belong to the send itself (fsm.before_send); not persisted if we can't send
_GATE_LOST = object()     # sentinel from _gate_send: lease lost
_INTENT_FIELDS = {"status", "last_sent_at", "pending_send_at", "attempt", "idempotency_key", "key_first_sent_at"}

# process_one() results that are not a payout status
LOST = "lost"            # CAS or lease renewal failed: someone else owns the row now
RELEASED = "released"    # waiting (Retry-After, search delay, no rate slot); lease released, next_attempt_at set


@dataclass
class ExecutorConfig:
    holder: str
    threads: int = 16
    lease_s: float = 45.0
    claim_batch: int = 8
    per_account: int = 2
    idle_poll_s: float = 0.25
    heartbeat_s: float = 3.0
    metrics_refresh_s: float = 5.0
    retry_min_s: float = 0.2
    retry_max_s: float = 5.0
    max_inline_wait_s: float = 2.0       # waits up to this are slept on the lease; longer ones are released (D26)
    limiter_timeout_s: float = 2.0
    no_slot_backoff_s: float = 0.5
    token_backoff_s: float = 1.0
    search_timeout_s: float = 5.0
    max_steps: int = 40                  # safety valve per process_one call
    fsm: Any = field(default_factory=lambda: fsm.FsmConfig())


class _Stopping(Exception):
    pass


class ExecutorApp:
    def __init__(self, *, cfg: ExecutorConfig, repo: PayoutRepository, bank: BankGateway, limiter: RateLimiter,
                 tokens: Any, registry: ExecutorRegistry, log: BoundLogger, clock: Clock | None = None):
        """`tokens` is a `TokenManager` (domain/token_manager.py): `current() -> str`,
        `on_unauthorized(token) -> str`."""
        self.cfg = cfg
        self.repo, self.bank, self.limiter, self.tokens, self.registry = repo, bank, limiter, tokens, registry
        self.log = log
        self.clock = clock or SystemClock()
        self.owner = cfg.holder
        self._stop = threading.Event()
        self._queue: queue.Queue[PayoutSnapshot] = queue.Queue(maxsize=max(1, cfg.threads))
        self._threads: list[threading.Thread] = []
        self._busy = 0
        self._busy_lock = threading.Lock()
        self._last_db_ok = 0.0
        self._last_token_ok = 0.0
        self._last_token_error: str | None = None

    # ===================================================================== lifecycle
    def start(self) -> None:
        spec: list[tuple[str, Callable[[], None]]] = [("claim", self._claim_loop),
                                                      ("heartbeat", self._heartbeat_loop),
                                                      ("metrics", self._metrics_loop)]
        spec += [(f"worker-{i}", self._worker_loop) for i in range(self.cfg.threads)]
        for name, target in spec:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)
        self.log.info("executor started", threads=self.cfg.threads, holder=self.owner)

    def request_stop(self) -> None:
        self._stop.set()

    def join(self, timeout_s: float = 10.0) -> None:
        deadline = time.monotonic() + timeout_s
        for t in self._threads:
            t.join(max(0.0, deadline - time.monotonic()))
        self._release_unstarted()
        self.log.info("executor stopped")

    def run_forever(self) -> None:
        self.start()
        self._stop.wait()
        self.join()

    def readiness(self) -> tuple[bool, dict]:
        now = time.time()
        db_ok = now - self._last_db_ok < max(15.0, 3 * self.cfg.metrics_refresh_s)
        token_ok = now - self._last_token_ok < 120.0
        if not token_ok:
            try:
                self.tokens.current()
                self._last_token_ok = time.time()
                token_ok = True
            except Exception as e:  # bank down / maintenance: not ready to send payouts
                self._last_token_error = repr(e)
        detail = {"postgres": db_ok, "token": token_ok, "threads_busy": self._busy, "queue": self._queue.qsize(),
                  "limiter_mode": self._safe(self.limiter.mode, "unknown"), "token_error": self._last_token_error}
        return db_ok and token_ok, detail

    # ===================================================================== loops
    def _claim_loop(self) -> None:
        while not self._stop.is_set():
            room = self._queue.maxsize - self._queue.qsize()
            if room <= 0:
                self._stop.wait(self.cfg.idle_poll_s)
                continue
            try:
                rows = self._db(lambda: self.repo.claim(self.owner, self.cfg.lease_s,
                                                        min(room, self.cfg.claim_batch), self.cfg.per_account))
            except _Stopping:
                return
            for r in rows:
                self._queue.put(r)
            if not rows:
                self._stop.wait(self.cfg.idle_poll_s)

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                p = self._queue.get(timeout=self.cfg.idle_poll_s)
            except queue.Empty:
                continue
            with self._busy_lock:
                self._busy += 1
                metrics.executor_threads_busy.set(self._busy)
            try:
                self.process_one(p)
            except _Stopping:
                return
            except Exception as e:  # a bug must not kill the thread; the lease expires and the row is retried
                self.log.error("payout processing crashed", payout_id=p.payout_id, error=repr(e), exc_info=True)
            finally:
                with self._busy_lock:
                    self._busy -= 1
                    metrics.executor_threads_busy.set(self._busy)

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.registry.heartbeat(self.owner)
            except Exception as e:
                self.log.warning("heartbeat failed", error=repr(e))
            self._stop.wait(self.cfg.heartbeat_s)

    def _metrics_loop(self) -> None:
        while not self._stop.is_set():
            try:
                counts = self.repo.counts_by_status()
                self._last_db_ok = time.time()
                for st in ("accepted", "claimed", "in_flight", "ambiguous", "paid", "failed", "rejected", "stuck"):
                    metrics.payouts_by_status.labels(status=st).set(counts.get(st, 0))
                metrics.limiter_fallback.set(1 if self._safe(self.limiter.mode, "redis") == "local" else 0)
            except Exception as e:
                self.log.warning("status refresh failed", error=repr(e))
            self._stop.wait(self.cfg.metrics_refresh_s)

    def _release_unstarted(self) -> None:
        """SIGTERM: rows claimed but never started go back now (status untouched, D26/C §3.4)."""
        while True:
            try:
                p = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                self.repo.release(p.payout_id, self.owner, self.clock.now())
            except Exception:
                pass  # the lease expires on its own

    # ===================================================================== one payout
    def process_one(self, p: PayoutSnapshot) -> str:
        """Drive one claimed payout until it is terminal, released, or lost. Returns the final
        status, RELEASED or LOST. Deterministic with fakes and a FakeClock."""
        log = self.log.bind(payout_id=p.payout_id, attempt=p.attempt, key=p.idempotency_key)
        if not self._db(lambda: self.repo.renew(p.payout_id, self.owner, self.cfg.lease_s)):
            log.warning("lease lost before start")
            return LOST
        d = fsm.next_step(p, self.clock.now(), self.cfg.fsm)
        for _ in range(self.cfg.max_steps):
            a = d.action
            if isinstance(a, fsm.Send):
                # gate first (lease, rate slot, token); only then commit the write-ahead intent the
                # decision carries (status in_flight, key, pending_send_at), then POST (D24)
                token = self._gate_send(p, log)
                if token is None:
                    return self._blocked(p, d, a, log)
                if token is _GATE_LOST:
                    return LOST
                ok, p = self._apply(p, d, log)
                if not ok:
                    log.warning("lost the row while writing intent; not sending")
                    return LOST
                p, d = self._post(p, a, token, log)
                log = log.bind(attempt=p.attempt, key=p.idempotency_key)
                continue

            ok, p = self._apply(p, d, log)
            if not ok:
                return LOST
            if isinstance(a, fsm.Done):
                return p.status
            if isinstance(a, fsm.Wait):
                dt = a.until - self.clock.now()
                if dt > self.cfg.max_inline_wait_s:
                    self._db(lambda: self.repo.release(p.payout_id, self.owner, a.until))
                    log.info("waiting", reason=a.reason, wait_s=round(dt, 2))
                    return RELEASED
                if dt > 0:
                    self.clock.sleep(dt)
                d = fsm.next_step(p, self.clock.now(), self.cfg.fsm)
                continue
            if isinstance(a, fsm.Search):
                nxt = self._search(p, log)
                if isinstance(nxt, str):
                    return nxt
                p, d = nxt
                continue
            raise RuntimeError(f"unknown action {a!r}")
        log.error("step budget exhausted; releasing", steps=self.cfg.max_steps)
        self._db(lambda: self.repo.release(p.payout_id, self.owner, self.clock.now() + self.cfg.retry_max_s))
        return RELEASED

    # --------------------------------------------------------------------- send
    def _gate_send(self, p: PayoutSnapshot, log: BoundLogger):
        """Returns the token to use, None if we must back off (no slot / no token), or LOST."""
        if not self._db(lambda: self.repo.renew(p.payout_id, self.owner, self.cfg.lease_s)):
            log.warning("lease lost before send")
            return _GATE_LOST
        if not self._acquire("payout"):
            self._block_backoff = self.cfg.no_slot_backoff_s
            return None
        try:
            token = self.tokens.current()
            self._last_token_ok = time.time()
            return token
        except Exception as e:
            log.warning("no bank token", error=repr(e))
            self._last_token_error = repr(e)
            self._block_backoff = self.cfg.token_backoff_s
            return None

    def _blocked(self, p: PayoutSnapshot, d: Any, a: Any, log: BoundLogger) -> str:
        """Can't send right now. Keep what the decision learned (e.g. key_uncertain, retries,
        search_attempts) but not the send intent, then release with a short backoff."""
        keep = {k: v for k, v in (d.fields or {}).items() if k not in _INTENT_FIELDS}
        if a.new_key:
            keep.pop("key_uncertain", None)   # belongs to the key we did not mint
        if keep:
            ok, p = self._persist(p, keep, log)
            if not ok:
                return LOST
        backoff = getattr(self, "_block_backoff", self.cfg.no_slot_backoff_s)
        self._db(lambda: self.repo.release(p.payout_id, self.owner, self.clock.now() + backoff))
        return RELEASED

    def _post(self, p: PayoutSnapshot, a: Any, token: str, log: BoundLogger) -> tuple[PayoutSnapshot, Any]:
        key = p.idempotency_key or a.key
        if a.new_key and p.attempt > 1:
            log.warning("sending under a new key after a confirmed-empty search", key=key, attempt=p.attempt)
        t0 = time.monotonic()
        try:
            r = self.bank.create_payout(token, key, p.payout_id, p.bank_account, p.amount_minor, p.currency,
                                        self.cfg.fsm.http_timeout_s)
        except Exception as e:   # adapter bug: the request may have gone out, so the outcome is unknown
            log.error("bank client raised", error=repr(e))
            r = BankResponse(status=0, sent=True, error=f"exception:{type(e).__name__}")
        metrics.bank_request_seconds.labels(endpoint="payout").observe(time.monotonic() - t0)

        if r.status == 401:
            try:
                self.tokens.on_unauthorized(token)
            except Exception as e:
                log.warning("token refresh failed", error=repr(e))
        err = r.error if r.status else (r.error or ("conn_error" if not r.sent else "timeout"))
        self._db(lambda: self.repo.record_attempt(p.payout_id, p.attempt, key, r.status or None, err))
        d = fsm.on_response(p, r, self.clock.now(), self.cfg.fsm)
        metrics.bank_responses_total.labels(klass=d.klass or "unknown").inc()
        log.info("bank response", http_status=r.status, sent=r.sent, error=r.error, replayed=r.replayed,
                 klass=d.klass, next_status=d.status)
        return p, d

    # --------------------------------------------------------------------- search
    def _search(self, p: PayoutSnapshot, log: BoundLogger) -> Any:
        if not self._acquire("search"):
            self._db(lambda: self.repo.release(p.payout_id, self.owner, self.clock.now() + self.cfg.no_slot_backoff_s))
            return RELEASED
        try:
            token = self.tokens.current()
        except Exception as e:
            log.warning("no bank token for search", error=repr(e))
            self._db(lambda: self.repo.release(p.payout_id, self.owner, self.clock.now() + self.cfg.token_backoff_s))
            return RELEASED
        t0 = time.monotonic()
        try:
            res = self.bank.search(token, p.payout_id, self.cfg.search_timeout_s)
        except Exception as e:
            log.error("bank search raised", error=repr(e))
            res = BankResponse(status=0, sent=True, error=f"exception:{type(e).__name__}")
        metrics.bank_request_seconds.labels(endpoint="search").observe(time.monotonic() - t0)
        rows: list[BankPayout] | None
        if isinstance(res, BankResponse):
            if res.status == 401:
                try:
                    self.tokens.on_unauthorized(token)
                except Exception:
                    pass
            rows = None
            log.info("search failed", http_status=res.status, error=res.error)
        else:
            rows = list(res)
            if len(rows) > 1:
                metrics.bank_duplicate_reference_total.inc()
                log.error("P1: bank executed this reference more than once", executions=len(rows),
                          bank_payout_ids=[r.payout_id for r in rows])
        d = fsm.on_search(p, rows if rows is not None else res, self.clock.now(), self.cfg.fsm)
        metrics.bank_responses_total.labels(klass=f"search_{d.klass or 'unknown'}").inc()
        log.info("search result", rows=None if rows is None else len(rows), klass=d.klass, next_status=d.status)
        return p, d

    # --------------------------------------------------------------------- persistence
    def _apply(self, p: PayoutSnapshot, d: Any, log: BoundLogger) -> tuple[bool, PayoutSnapshot]:
        fields_ = dict(d.fields or {})
        if d.status != p.status:
            fields_["status"] = d.status
        if not fields_:
            return True, p
        ok, newp = self._persist(p, fields_, log)
        if ok and d.status != p.status:
            if d.klass == "duplicate_reference":
                log.error("P1: duplicate bank payouts for one reference", klass=d.klass)
            if d.status == "stuck":
                metrics.payouts_stuck_total.inc()
                log.error("payout stuck: needs a human", klass=d.klass, reason=fields_.get("reason"),
                          last_error=fields_.get("last_error"))
        return ok, newp

    def _persist(self, p: PayoutSnapshot, fields_: dict, log: BoundLogger) -> tuple[bool, PayoutSnapshot]:
        new_status = fields_.get("status", p.status)
        ok = self._db(lambda: self.repo.transition(p.payout_id, self.owner, p.status, p.attempt, fields_))
        if not ok:
            log.warning("CAS lost: row changed under us", expect_status=p.status, expect_attempt=p.attempt)
            return False, p
        if new_status != p.status:
            metrics.payout_transitions_total.labels(from_status=p.status, to_status=new_status).inc()
            # one line per state change, so a payout's history can be read in Loki/Grafana
            # (Trace dashboard: `| payout_id="..."`); the log is bound with payout_id/attempt/key
            log.info("payout transition", from_status=p.status, to_status=new_status,
                     attempt=fields_.get("attempt", p.attempt),
                     idempotency_key=fields_.get("idempotency_key", p.idempotency_key))
        return True, _merge(p, fields_)

    # --------------------------------------------------------------------- helpers
    def _acquire(self, bucket: str) -> bool:
        t0 = time.monotonic()
        try:
            got = self.limiter.acquire(bucket, self.cfg.limiter_timeout_s)
        except Exception as e:
            self.log.warning("limiter error", bucket=bucket, error=repr(e))
            got = False
        metrics.limiter_wait_seconds.labels(bucket=bucket).observe(time.monotonic() - t0)
        return got

    def _db(self, fn: Callable[[], T]) -> T:
        """Retry DB errors with backoff (D20): a Postgres restart must not kill the executor."""
        delay = self.cfg.retry_min_s
        while True:
            try:
                out = fn()
                self._last_db_ok = time.time()
                return out
            except (OperationalError, DBAPIError) as e:
                if self._stop.is_set():
                    raise _Stopping() from e
                self.log.warning("database error, retrying", error=type(e).__name__, delay_s=delay)
                time.sleep(delay)
                delay = min(delay * 2, self.cfg.retry_max_s)

    @staticmethod
    def _safe(fn: Callable[[], T], default: T) -> T:
        try:
            return fn()
        except Exception:
            return default


def _merge(p: PayoutSnapshot, fields_: dict) -> PayoutSnapshot:
    upd = {k: v for k, v in fields_.items() if k in _SNAPSHOT_FIELDS}
    return replace(p, **upd) if upd else p
