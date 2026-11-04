"""The §4 ledger rules, as a pure function. Exact mirror of harness/ledger_events.py::apply_event
(L165-215), which ASSIGNMENT.md §4 says wins over the prose.

    result = apply(book, event, lookup)

`book` is never mutated; `result.book_after` is a copy with the deltas applied. `result.effects`
are executed by the store, in order, inside the batch transaction. `result.outcome` uses the same
strings as apply_event ("applied", "rejected:<reason>", "ignored").

One deliberate divergence from the reference: apply_event raises KeyError/TypeError on a missing
or malformed field (the harness never produces such an event). Here that yields
"rejected:malformed_event" with no effects, so a poison message consumes its sequence number and
is counted instead of killing the consumer (STEP3-PLAN §2.6).
"""
from __future__ import annotations

from services.consumer.domain.effects import (
    PAYOUT_ACCEPTED,
    PAYOUT_REJECTED,
    AddPlatformFee,
    AddRefunded,
    AdjustBalance,
    CloseAuth,
    Effect,
    Lookup,
    MerchantBook,
    OpenAuth,
    RecordPayout,
    RuleResult,
    SetConfig,
    UpsertCapture,
)

APPLIED = "applied"
IGNORED = "ignored"
MALFORMED = "rejected:malformed_event"

KNOWN_TYPES = ("MERCHANT_CONFIG", "AUTH", "AUTH_EXPIRY", "CAPTURE", "REFUND", "CHARGEBACK", "PAYOUT_REQUEST")


class _Malformed(Exception):
    pass


def _int(ev: dict, key: str) -> int:
    v = ev.get(key)
    if isinstance(v, bool) or not isinstance(v, int):
        raise _Malformed(key)
    return v


def _str(ev: dict, key: str) -> str:
    v = ev.get(key)
    if not isinstance(v, str):
        raise _Malformed(key)
    return v


def _done(outcome: str, book: MerchantBook, *effects: Effect) -> RuleResult:
    return RuleResult(outcome=outcome, effects=tuple(effects), book_after=book)


def apply(book: MerchantBook, event: dict, lookup: Lookup) -> RuleResult:
    after = book.copy()
    typ = event.get("type")
    if typ not in KNOWN_TYPES:
        return _done(IGNORED, after)                                  # apply_event L215
    try:
        return _apply_known(after, typ, event, lookup)
    except _Malformed:
        return _done(MALFORMED, book.copy())


def _apply_known(after: MerchantBook, typ: str, ev: dict, lookup: Lookup) -> RuleResult:
    eid = _str(ev, "event_id")

    if typ == "MERCHANT_CONFIG":                                      # L169-174
        reserve = _int(ev, "reserve_minor")
        account = _str(ev, "bank_account")
        new_currency: str | None = None
        if not after.currency:
            new_currency = _str(ev, "currency")
            after.currency = new_currency
        after.reserve_minor = reserve
        after.bank_account = account
        return _done(APPLIED, after, SetConfig(new_currency, reserve, account))

    if typ == "AUTH":                                                 # L175-178
        amount = _int(ev, "amount_minor")
        after.held_minor += amount
        return _done(APPLIED, after, OpenAuth(eid, amount), AdjustBalance(held_delta=amount))

    if typ == "AUTH_EXPIRY":                                          # L179-185
        a = lookup.auth(_str(ev, "ref"))
        if a is None or not a.is_open:
            return _done("rejected:auth_not_open", after)
        after.held_minor -= a.amount_minor
        return _done(APPLIED, after, CloseAuth(a.event_id), AdjustBalance(held_delta=-a.amount_minor))

    if typ == "CAPTURE":                                              # L186-197
        amount = _int(ev, "amount_minor")
        fee = _int(ev, "fee_minor")
        a = lookup.auth(_str(ev, "ref"))
        if a is None or not a.is_open:
            return _done("rejected:auth_not_open", after)
        if amount > a.amount_minor:
            return _done("rejected:capture_exceeds_auth", after)
        after.held_minor -= a.amount_minor                            # whole hold released, even partial
        after.settled_minor += amount - fee
        return _done(
            APPLIED, after,
            CloseAuth(a.event_id),
            AdjustBalance(settled_delta=amount - fee, held_delta=-a.amount_minor),
            AddPlatformFee(after.currency, fee),                      # book's currency, may be ""
            UpsertCapture(eid, amount),
        )

    if typ == "REFUND":                                               # L198-205
        amount = _int(ev, "amount_minor")
        c = lookup.capture(_str(ev, "ref"))
        if c is None:
            return _done("rejected:capture_not_found", after)
        if c.refunded_minor + amount > c.amount_minor:
            return _done("rejected:refund_exceeds_capture", after)
        after.settled_minor -= amount
        return _done(APPLIED, after, AddRefunded(c.event_id, amount), AdjustBalance(settled_delta=-amount))

    if typ == "CHARGEBACK":                                           # L206-211 (no once-only check)
        amount = _int(ev, "amount_minor")
        fee = _int(ev, "fee_minor")
        c = lookup.capture(_str(ev, "ref"))
        if c is None:
            return _done("rejected:capture_not_found", after)
        after.settled_minor -= amount + fee
        return _done(
            APPLIED, after,
            AdjustBalance(settled_delta=-(amount + fee)),
            AddPlatformFee(after.currency, fee),
        )

    # PAYOUT_REQUEST                                                  # L212-214
    amount = _int(ev, "amount_minor")
    if amount <= 0 or after.available_minor < amount:
        return _done(
            "rejected:insufficient_funds", after,
            RecordPayout(eid, amount, PAYOUT_REJECTED, "insufficient_funds", after.bank_account, after.currency),
        )
    after.settled_minor -= amount
    return _done(
        APPLIED, after,
        AdjustBalance(settled_delta=-amount),
        RecordPayout(eid, amount, PAYOUT_ACCEPTED, None, after.bank_account, after.currency),
    )
