"""Shared Prometheus metrics. Every service imports the ones it updates; `/metrics` renders them all.

Names answer the A6 operator questions:
  which channels are behind       -> reader_channel_lag, consumer_partition_lag
  who holds the session, epoch    -> reader_is_leader, reader_session_epoch
  resend and gap rates            -> reader_gaps_total, reader_resends_total
  payouts by state incl. ambiguous-> payouts_by_status
  bank error classes              -> bank_responses_total{klass}
"""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry(auto_describe=True)

# --- reader -------------------------------------------------------------------------
reader_is_leader = Gauge("reader_is_leader", "1 if this replica holds the gateway session", registry=REGISTRY)
reader_session_epoch = Gauge("reader_session_epoch", "session_epoch of the held session", registry=REGISTRY)
reader_session_since = Gauge("reader_session_since_seconds", "unix time the current session was established", registry=REGISTRY)
reader_frames_total = Counter("reader_frames_total", "frames received from the gateway", ["type"], registry=REGISTRY)
reader_gaps_total = Counter("reader_gaps_total", "sequence gaps detected", registry=REGISTRY)
reader_resends_total = Counter("reader_resends_total", "RESEND requests sent", ["reason"], registry=REGISTRY)
reader_duplicates_total = Counter("reader_duplicates_total", "duplicate frames dropped", registry=REGISTRY)
reader_reconnects_total = Counter("reader_reconnects_total", "gateway reconnects", ["reason"], registry=REGISTRY)
reader_channel_lag = Gauge("reader_channel_lag", "frames received but not yet acked by the broker", ["channel"], registry=REGISTRY)
reader_credit_outstanding = Gauge("reader_credit_outstanding", "credit granted but not yet consumed", ["channel"], registry=REGISTRY)

# --- consumer -----------------------------------------------------------------------
consumer_events_total = Counter("consumer_events_total", "events seen by outcome", ["outcome"], registry=REGISTRY)
consumer_partition_lag = Gauge("consumer_partition_lag", "broker high-watermark minus committed offset", ["partition"], registry=REGISTRY)
consumer_apply_seconds = Histogram("consumer_apply_seconds", "apply transaction latency", registry=REGISTRY,
                                   buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
consumer_batch_size = Histogram("consumer_batch_size", "records per apply batch (per partition)", registry=REGISTRY,
                                buckets=(1, 5, 10, 25, 50, 100, 200, 500))
consumer_batches_total = Counter("consumer_batches_total", "apply batches by result", ["result"], registry=REGISTRY)
consumer_stale_epoch_total = Counter("consumer_stale_epoch_total", "messages rejected for a stale session_epoch", registry=REGISTRY)
consumer_out_of_order_total = Counter("consumer_out_of_order_total", "messages parked because seq != last_seq+1", registry=REGISTRY)

# --- payout executor ----------------------------------------------------------------
payouts_by_status = Gauge("payouts_by_status", "payout rows by status", ["status"], registry=REGISTRY)
payout_transitions_total = Counter("payout_transitions_total", "state transitions", ["from_status", "to_status"], registry=REGISTRY)
bank_responses_total = Counter("bank_responses_total", "bank responses by class", ["klass"], registry=REGISTRY)
bank_request_seconds = Histogram("bank_request_seconds", "bank request latency", ["endpoint"], registry=REGISTRY,
                                 buckets=(0.1, 0.25, 0.5, 1, 1.5, 2, 5, 10, 30))
limiter_wait_seconds = Histogram("limiter_wait_seconds", "time spent waiting for a rate-limit slot", ["bucket"], registry=REGISTRY)
limiter_fallback = Gauge("limiter_fallback", "1 when the Redis limiter is unavailable and the local share is in use", registry=REGISTRY)

# --- api ----------------------------------------------------------------------------
api_request_seconds = Histogram("api_request_seconds", "HTTP request latency", ["route", "status"], registry=REGISTRY,
                                buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1))


def render() -> bytes:
    return generate_latest(REGISTRY)
