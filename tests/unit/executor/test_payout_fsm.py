"""The payout state machine against every row of the outcome matrix (docs/step5/A-fsm.md §1, §6)."""
import random
from dataclasses import replace

import pytest

from services.executor.domain.payout_fsm import (Done, FsmConfig, Search, Send, Wait, before_send, can_reuse_key,
                                                 key_for, next_step, on_response, on_search)
from services.executor.ports import BankPayout, BankResponse, PayoutSnapshot

CFG = FsmConfig()            # safe age 10 s, timeout 5 s, search delay 10 s, max attempts 5
T0 = 1_000_000.0
PID = "M-0042:1187"


def snap(**kw) -> PayoutSnapshot:
    base = dict(payout_id=PID, merchant_id="M-0042", amount_minor=125_000, currency="USD",
                bank_account="ACCT-1", status="accepted", attempt=0, idempotency_key=None,
                key_first_sent_at=None, last_sent_at=None, pending_send_at=None, key_uncertain=False,
                search_attempts=0, retries=0, lease_owner="me")
    base.update(kw)
    return PayoutSnapshot(**base)


def apply(p: PayoutSnapshot, fields: dict) -> PayoutSnapshot:
    """Persist decision fields onto a snapshot, as the repository would."""
    known = {k: v for k, v in fields.items() if k in PayoutSnapshot.__dataclass_fields__}
    return replace(p, **known)


def sent(at: float = T0, **kw) -> PayoutSnapshot:
    """A row right after the first write-ahead at `at` (the snapshot on_response receives)."""
    d = next_step(snap(status="claimed"), at, CFG)
    return replace(apply(snap(status="claimed"), d.fields), **kw)


def resp(status, error=None, *, sent_=True, replayed=False, retry_after=None, payout_id=None):
    return BankResponse(status=status, sent=sent_, error=error, payout_id=payout_id, replayed=replayed,
                        retry_after_s=retry_after)


def row(**kw) -> BankPayout:
    base = dict(payout_id="BP-1", reference=PID, account="ACCT-1", amount_minor=125_000, currency="USD",
                status="executed", committed_at=T0)
    base.update(kw)
    return BankPayout(**base)


# --- first send / write-ahead --------------------------------------------------------------
def test_first_send_writes_intent_before_post():
    d = next_step(snap(status="claimed"), T0, CFG)
    assert d.action == Send(key=f"{PID}#a1", new_key=True)
    assert d.status == "in_flight"
    assert d.fields == {"status": "in_flight", "last_sent_at": T0, "pending_send_at": T0, "attempt": 1,
                        "idempotency_key": f"{PID}#a1", "key_first_sent_at": T0, "key_uncertain": False}


# 1
def test_201_fresh_is_paid_and_clears_pending():
    d = on_response(sent(), resp(201, payout_id="BP-9"), T0 + 1, CFG)
    assert d.status == "paid" and isinstance(d.action, Done) and d.klass == "paid"
    assert d.fields["bank_payout_id"] == "BP-9" and d.fields["pending_send_at"] is None


# 2
def test_201_replay_after_502_is_paid():
    p = sent()
    d = on_response(p, resp(502, "bad_gateway"), T0 + 1, CFG)
    p2 = apply(p, d.fields)
    d2 = on_response(p2, resp(201, payout_id="BP-9", replayed=True), T0 + 2, CFG)
    assert d2.status == "paid" and d2.klass == "replayed" and d2.fields["bank_payout_id"] == "BP-9"


# 3
@pytest.mark.parametrize("replayed", [False, True])
def test_422_account_closed_is_failed_forever(replayed):
    d = on_response(sent(), resp(422, "account_closed", replayed=replayed), T0 + 1, CFG)
    assert d.status == "failed" and d.fields["reason"] == "account_closed" and isinstance(d.action, Done)


# 4
def test_401_resends_same_key_not_uncertain():
    p = sent()
    d = on_response(p, resp(401, "invalid_token"), T0 + 1, CFG)
    assert d.action == Send(key=p.idempotency_key, new_key=False) and d.klass == "unauthorized"
    assert d.fields["pending_send_at"] == T0 + 1               # re-armed write-ahead
    assert d.fields.get("key_uncertain", False) is False


# 5
def test_409_too_many_inflight_waits_same_key():
    d = on_response(sent(), resp(409, "too_many_inflight"), T0 + 1, CFG)
    assert d.status == "in_flight" and d.action == Wait(T0 + 2, "too_many_inflight")
    assert "key_uncertain" not in d.fields and d.fields["next_attempt_at"] == T0 + 2


# 6
def test_409_request_in_progress_marks_key_uncertain():
    d = on_response(sent(), resp(409, "request_in_progress"), T0 + 1, CFG)
    assert d.action == Wait(T0 + 2, "in_progress") and d.fields["key_uncertain"] is True


# 7
def test_429_waits_retry_after():
    d = on_response(sent(), resp(429, "rate_limited", retry_after=1), T0 + 1, CFG)
    assert d.action == Wait(T0 + 2, "rate_limited") and d.status == "in_flight"


# 8
def test_503_maintenance_waits_then_resends_same_key_at_any_age():
    p = sent()
    d = on_response(p, resp(503, "maintenance", retry_after=30), T0 + 1, CFG)
    assert d.action == Wait(T0 + 31, "maintenance") and "key_uncertain" not in d.fields
    p = apply(p, d.fields)
    d2 = next_step(p, T0 + 150, CFG)                            # key 150 s old, never uncertain
    assert d2.action == Send(key=p.idempotency_key, new_key=False)


# 9
def test_503_upstream_unavailable_backs_off_same_key():
    p = sent()
    d = on_response(p, resp(503, "upstream_unavailable"), T0 + 1, CFG)
    assert isinstance(d.action, Wait) and d.status == "in_flight" and d.klass == "upstream_unavailable"
    assert next_step(apply(p, d.fields), T0 + 5, CFG).action == Send(p.idempotency_key, False)


# 10
def test_502_young_key_resends_same_key_and_marks_uncertain():
    p = sent()
    d = on_response(p, resp(502, "bad_gateway"), T0 + 2, CFG)
    assert d.action == Send(key=p.idempotency_key, new_key=False) and d.fields["key_uncertain"] is True


# 11
def test_502_old_key_goes_ambiguous_and_waits_for_search():
    p = sent()
    d = on_response(p, resp(502, "bad_gateway"), T0 + 10, CFG)
    # last_sent_at = T0, so the search is due at T0 + 10 == now -> search right away
    assert d.status == "ambiguous" and d.action == Search() and d.fields["key_uncertain"] is True


def test_502_old_key_before_search_due_waits():
    p = replace(sent(), last_sent_at=T0 + 8)                    # a recent resend under the old key
    d = on_response(p, resp(502, "bad_gateway"), T0 + 12, CFG)
    assert d.status == "ambiguous" and d.action == Wait(T0 + 18, "search_lag")


# 12
def test_timeout_is_treated_like_502():
    p = sent()
    d = on_response(p, resp(0, None), T0 + 5, CFG)
    assert d.action == Send(p.idempotency_key, False) and d.fields["key_uncertain"] is True
    assert d.klass == "unknown_resend"


# 13
def test_connect_error_retries_same_key_not_uncertain():
    p = sent()
    d = on_response(p, resp(0, "connect_refused", sent_=False), T0 + 1, CFG)
    assert isinstance(d.action, Wait) and d.klass == "connect_error" and "key_uncertain" not in d.fields
    assert next_step(apply(p, d.fields), T0 + 60, CFG).action == Send(p.idempotency_key, False)


# 14
@pytest.mark.parametrize("err", ["invalid_request", "idempotency_key_reused"])
def test_our_bugs_are_stuck(err):
    d = on_response(sent(), resp(422, err), T0 + 1, CFG)
    assert d.status == "stuck" and d.fields["reason"] == err and isinstance(d.action, Done)


# 15
def test_timeout_then_maintenance_longer_than_ttl_never_sends():
    p = sent()
    d = on_response(p, resp(0), T0 + 5, CFG)                   # unknown, young -> resend same key
    p = apply(p, d.fields)
    assert isinstance(d.action, Send)
    t = T0 + 5
    for _ in range(5):                                         # 150 s of maintenance
        d = on_response(p, resp(503, "maintenance", retry_after=30), t, CFG)
        p = apply(p, d.fields)
        t += 30
        d = next_step(p, t, CFG)
        assert not isinstance(d.action, Send), "an uncertain, aged key must never be resent"
        p = apply(p, d.fields)
        if d.status == "ambiguous":
            break
    assert p.status == "ambiguous" and p.key_uncertain is True


# 16
def test_failed_search_stays_ambiguous_and_backs_off():
    p = replace(sent(), status="ambiguous", key_uncertain=True, pending_send_at=None)
    for failure in (None, resp(503, "maintenance", retry_after=30), resp(429, "rate_limited")):
        d = on_search(p, failure, T0 + 20, CFG)
        assert d.status == "ambiguous" and isinstance(d.action, Wait)
        assert d.fields["search_attempts"] == 1
    assert on_search(p, resp(503, "maintenance", retry_after=30), T0 + 20, CFG).action.until == T0 + 50


# 17
def test_search_found_is_paid():
    p = replace(sent(), status="ambiguous", key_uncertain=True)
    d = on_search(p, [row(payout_id="BP-7")], T0 + 20, CFG)
    assert d.status == "paid" and d.fields["bank_payout_id"] == "BP-7" and d.klass == "search_found"


# 18
def test_search_two_rows_is_paid_and_flagged():
    p = replace(sent(), status="ambiguous", key_uncertain=True)
    d = on_search(p, [row(payout_id="BP-7"), row(payout_id="BP-8")], T0 + 20, CFG)
    assert d.status == "paid" and d.klass == "duplicate_reference"


# 19
@pytest.mark.parametrize("bad", [dict(amount_minor=1), dict(account="ACCT-X"), dict(currency="EUR")])
def test_search_mismatch_is_stuck(bad):
    p = replace(sent(), status="ambiguous", key_uncertain=True)
    d = on_search(p, [row(**bad)], T0 + 20, CFG)
    assert d.status == "stuck" and d.fields["reason"] == "search_mismatch"


# 20
def test_empty_search_too_early_waits_then_new_key():
    p = replace(sent(), status="ambiguous", key_uncertain=True, pending_send_at=None, last_sent_at=T0 + 5)
    early = on_search(p, [], T0 + 12, CFG)
    assert early.action == Wait(T0 + 15, "search_lag") and early.status == "ambiguous"
    d = on_search(p, [], T0 + 15, CFG)
    assert d.action == Send(key=f"{PID}#a2", new_key=True) and d.klass == "new_key_after_search"
    assert d.fields["attempt"] == 2 and d.fields["key_uncertain"] is False
    assert d.fields["key_first_sent_at"] == T0 + 15 and d.fields["pending_send_at"] == T0 + 15


def test_rows_for_other_references_are_ignored():
    p = replace(sent(), status="ambiguous", key_uncertain=True)
    d = on_search(p, [row(reference="M-0042:9999")], T0 + 20, CFG)
    assert isinstance(d.action, Send) and d.action.new_key


# 21
def test_key_age_boundary():
    p = replace(sent(), key_uncertain=True, pending_send_at=None)
    assert can_reuse_key(p, T0 + CFG.key_safe_age_s - 1e-6, CFG) is True
    assert can_reuse_key(p, T0 + CFG.key_safe_age_s, CFG) is False
    assert next_step(p, T0 + CFG.key_safe_age_s, CFG).status == "ambiguous"


# 22
def test_attempts_exhausted_is_stuck():
    p = replace(sent(), status="ambiguous", key_uncertain=True, attempt=CFG.max_attempts)
    d = on_search(p, [], T0 + 30, CFG)
    assert d.status == "stuck" and d.fields["reason"] == "attempts_exhausted"


# 23
def test_recovery_with_pending_send_at():
    p = sent()                                                  # crashed right after the write-ahead
    young = next_step(p, T0 + 3, CFG)
    assert young.action == Send(p.idempotency_key, False) and young.fields["key_uncertain"] is True
    old = next_step(p, T0 + 12, CFG)
    assert old.status == "ambiguous" and old.action == Search() and old.fields["key_uncertain"] is True


# 24
def test_key_length_fits():
    long_id = "M-60000:1234567890"                              # 18 chars
    assert len(key_for(long_id, 99)) <= 64
    with pytest.raises(ValueError):
        key_for("X" * 70, 1)


# 25
def test_before_send_new_vs_same_key():
    p = sent()
    same = before_send(p, T0 + 3, new_key=False)
    assert "key_first_sent_at" not in same and "attempt" not in same and same["pending_send_at"] == T0 + 3
    new = before_send(p, T0 + 3, new_key=True)
    assert new["attempt"] == 2 and new["key_first_sent_at"] == T0 + 3 and new["key_uncertain"] is False


# 26
def test_property_new_key_only_after_successful_empty_search():
    responses = [resp(201, payout_id="BP"), resp(201, replayed=True, payout_id="BP"), resp(401, "invalid_token"),
                 resp(409, "too_many_inflight"), resp(409, "request_in_progress"), resp(429, "rate_limited"),
                 resp(503, "maintenance", retry_after=30), resp(503, "upstream_unavailable"),
                 resp(502, "bad_gateway"), resp(0), resp(0, sent_=False), resp(504)]
    searches = [None, resp(503, "maintenance", retry_after=30), [], [row()]]
    rng = random.Random(7)
    for trial in range(2000):
        p, t = snap(status="claimed"), T0
        d = next_step(p, t, CFG)
        last_search_empty_ok = False
        for _ in range(40):
            if isinstance(d.action, Send):
                if d.action.new_key and p.attempt > 0:
                    assert last_search_empty_ok, f"trial {trial}: new key without an empty search"
                last_search_empty_ok = False
                p = apply(p, d.fields)
                t += rng.uniform(0.1, 6)
                d = on_response(p, rng.choice(responses), t, CFG)
            elif isinstance(d.action, Search):
                p = apply(p, d.fields)
                result = rng.choice(searches)
                d = on_search(p, result, t, CFG)
                last_search_empty_ok = result == [] and isinstance(d.action, Send)
            elif isinstance(d.action, Wait):
                p = apply(p, d.fields)
                t = max(t, d.action.until) + rng.uniform(0, 40)
                d = next_step(p, t, CFG)
            else:
                break


# --- config -------------------------------------------------------------------------------
def test_from_env_caps_safe_age_by_bank_ttl(monkeypatch):
    monkeypatch.delenv("PAYOUT_KEY_SAFE_AGE_S", raising=False)
    monkeypatch.setenv("BANK_IDEMPOTENCY_TTL_S", "12")
    assert FsmConfig.from_env().key_safe_age_s == 6.0
    monkeypatch.setenv("BANK_IDEMPOTENCY_TTL_S", "120")
    assert FsmConfig.from_env().key_safe_age_s == 10.0
    monkeypatch.setenv("PAYOUT_MAX_ATTEMPTS", "3")
    assert FsmConfig.from_env().max_attempts == 3


def test_terminal_rows_are_done():
    for s in ("paid", "failed", "rejected", "stuck"):
        assert isinstance(next_step(snap(status=s, idempotency_key="k", attempt=1), T0, CFG).action, Done)
