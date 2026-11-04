"""One test per row of STEP3-PLAN §1 (apply_event L165-215), plus purity and malformed input."""
import pytest

from services.consumer.domain.effects import (
    AddPlatformFee, AddRefunded, AdjustBalance, AuthRow, CaptureRow, CloseAuth, MerchantBook, OpenAuth,
    RecordPayout, SetConfig, UpsertCapture,
)
from services.consumer.domain.rules import apply

M = "M-0001"


class DictLookup:
    def __init__(self, auths=None, captures=None):
        self.auths = auths or {}
        self.captures = captures or {}

    def auth(self, event_id):
        return self.auths.get(event_id)

    def capture(self, event_id):
        return self.captures.get(event_id)


def ev(typ, seq=1, **f):
    return {"event_id": f"{M}:{seq}", "merchant_id": M, "type": typ, "event_time": "2026-11-01T00:00:00.000Z",
            "currency": "USD", **f}


def book(**kw):
    return MerchantBook(M, **kw)


# --- MERCHANT_CONFIG ------------------------------------------------------------------
def test_first_config_sets_currency_reserve_account():
    r = apply(book(), ev("MERCHANT_CONFIG", reserve_minor=5000, bank_account="ACCT-1"), DictLookup())
    assert r.outcome == "applied"
    assert r.effects == (SetConfig("USD", 5000, "ACCT-1"),)
    assert r.book_after == book(currency="USD", reserve_minor=5000, bank_account="ACCT-1")


def test_later_config_keeps_currency_changes_reserve():
    b = book(currency="EUR", reserve_minor=5000, bank_account="ACCT-1", settled_minor=10)
    r = apply(b, ev("MERCHANT_CONFIG", reserve_minor=25000, bank_account="ACCT-1"), DictLookup())
    assert r.effects == (SetConfig(None, 25000, "ACCT-1"),)
    assert r.book_after.currency == "EUR" and r.book_after.reserve_minor == 25000


# --- AUTH -------------------------------------------------------------------------------
def test_auth_opens_and_holds():
    r = apply(book(currency="USD"), ev("AUTH", seq=2, amount_minor=700), DictLookup())
    assert r.outcome == "applied"
    assert r.effects == (OpenAuth(f"{M}:2", 700), AdjustBalance(held_delta=700))
    assert r.book_after.held_minor == 700 and r.book_after.settled_minor == 0


# --- AUTH_EXPIRY ------------------------------------------------------------------------
def test_auth_expiry_closes_open_auth():
    lk = DictLookup(auths={f"{M}:2": AuthRow(f"{M}:2", 700, True)})
    r = apply(book(held_minor=700), ev("AUTH_EXPIRY", seq=3, ref=f"{M}:2"), lk)
    assert r.outcome == "applied"
    assert r.effects == (CloseAuth(f"{M}:2"), AdjustBalance(held_delta=-700))
    assert r.book_after.held_minor == 0


@pytest.mark.parametrize("auths", [{}, {f"{M}:2": AuthRow(f"{M}:2", 700, False)}])
def test_auth_expiry_rejected_when_missing_or_closed(auths):
    r = apply(book(held_minor=700), ev("AUTH_EXPIRY", seq=3, ref=f"{M}:2"), DictLookup(auths=auths))
    assert r.outcome == "rejected:auth_not_open"
    assert r.effects == ()
    assert r.book_after == book(held_minor=700)


# --- CAPTURE ----------------------------------------------------------------------------
def test_full_capture():
    lk = DictLookup(auths={f"{M}:2": AuthRow(f"{M}:2", 1000, True)})
    r = apply(book(currency="USD", held_minor=1000), ev("CAPTURE", seq=3, ref=f"{M}:2", amount_minor=1000, fee_minor=59), lk)
    assert r.outcome == "applied"
    assert r.effects == (CloseAuth(f"{M}:2"), AdjustBalance(settled_delta=941, held_delta=-1000),
                         AddPlatformFee("USD", 59), UpsertCapture(f"{M}:3", 1000))
    assert (r.book_after.settled_minor, r.book_after.held_minor) == (941, 0)


def test_partial_capture_releases_whole_hold():
    lk = DictLookup(auths={f"{M}:2": AuthRow(f"{M}:2", 1000, True)})
    r = apply(book(currency="USD", held_minor=1000), ev("CAPTURE", seq=3, ref=f"{M}:2", amount_minor=400, fee_minor=42), lk)
    assert r.effects[1] == AdjustBalance(settled_delta=358, held_delta=-1000)
    assert (r.book_after.settled_minor, r.book_after.held_minor) == (358, 0)


def test_capture_fee_goes_under_empty_currency_when_no_config_yet():
    lk = DictLookup(auths={f"{M}:2": AuthRow(f"{M}:2", 1000, True)})
    r = apply(book(held_minor=1000), ev("CAPTURE", seq=3, ref=f"{M}:2", amount_minor=1000, fee_minor=10), lk)
    assert AddPlatformFee("", 10) in r.effects


@pytest.mark.parametrize("auths", [{}, {f"{M}:2": AuthRow(f"{M}:2", 1000, False)}])
def test_capture_rejected_auth_not_open(auths):
    r = apply(book(), ev("CAPTURE", seq=3, ref=f"{M}:2", amount_minor=100, fee_minor=1), DictLookup(auths=auths))
    assert r.outcome == "rejected:auth_not_open" and r.effects == ()


def test_capture_rejected_exceeds_auth():
    lk = DictLookup(auths={f"{M}:2": AuthRow(f"{M}:2", 1000, True)})
    r = apply(book(held_minor=1000), ev("CAPTURE", seq=3, ref=f"{M}:2", amount_minor=1001, fee_minor=1), lk)
    assert r.outcome == "rejected:capture_exceeds_auth" and r.effects == ()
    assert r.book_after.held_minor == 1000


# --- REFUND -----------------------------------------------------------------------------
def test_refund_within_capture():
    lk = DictLookup(captures={f"{M}:3": CaptureRow(f"{M}:3", 1000, 300)})
    r = apply(book(settled_minor=941), ev("REFUND", seq=4, ref=f"{M}:3", amount_minor=700), lk)
    assert r.outcome == "applied"
    assert r.effects == (AddRefunded(f"{M}:3", 700), AdjustBalance(settled_delta=-700))
    assert r.book_after.settled_minor == 241


def test_refund_rejected_capture_not_found():
    r = apply(book(), ev("REFUND", seq=4, ref=f"{M}:3", amount_minor=1), DictLookup())
    assert r.outcome == "rejected:capture_not_found" and r.effects == ()


def test_refund_rejected_exceeds_capture():
    lk = DictLookup(captures={f"{M}:3": CaptureRow(f"{M}:3", 1000, 300)})
    r = apply(book(settled_minor=941), ev("REFUND", seq=4, ref=f"{M}:3", amount_minor=701), lk)
    assert r.outcome == "rejected:refund_exceeds_capture" and r.effects == ()
    assert r.book_after.settled_minor == 941


# --- CHARGEBACK -------------------------------------------------------------------------
def test_chargeback_can_go_negative_and_repeat():
    lk = DictLookup(captures={f"{M}:3": CaptureRow(f"{M}:3", 1000, 0)})
    b = book(currency="GBP", settled_minor=500)
    r1 = apply(b, ev("CHARGEBACK", seq=4, ref=f"{M}:3", amount_minor=1000, fee_minor=1500), lk)
    assert r1.outcome == "applied"
    assert r1.effects == (AdjustBalance(settled_delta=-2500), AddPlatformFee("GBP", 1500))
    assert r1.book_after.settled_minor == -2000
    r2 = apply(r1.book_after, ev("CHARGEBACK", seq=5, ref=f"{M}:3", amount_minor=1000, fee_minor=1500), lk)
    assert r2.outcome == "applied" and r2.book_after.settled_minor == -4500


def test_chargeback_rejected_capture_not_found():
    r = apply(book(), ev("CHARGEBACK", seq=4, ref=f"{M}:9", amount_minor=1, fee_minor=1), DictLookup())
    assert r.outcome == "rejected:capture_not_found" and r.effects == ()


# --- PAYOUT_REQUEST ---------------------------------------------------------------------
def test_payout_accepted_when_available_covers_it():
    b = book(currency="USD", reserve_minor=5000, bank_account="ACCT-1", settled_minor=20000)
    r = apply(b, ev("PAYOUT_REQUEST", seq=6, amount_minor=15000), DictLookup())
    assert r.outcome == "applied"
    assert r.effects == (AdjustBalance(settled_delta=-15000),
                         RecordPayout(f"{M}:6", 15000, "accepted", None, "ACCT-1", "USD"))
    assert r.book_after.settled_minor == 5000


def test_payout_rejected_when_reserve_blocks_it():
    b = book(currency="USD", reserve_minor=5000, bank_account="ACCT-1", settled_minor=20000)
    r = apply(b, ev("PAYOUT_REQUEST", seq=6, amount_minor=15001), DictLookup())
    assert r.outcome == "rejected:insufficient_funds"
    assert r.effects == (RecordPayout(f"{M}:6", 15001, "rejected", "insufficient_funds", "ACCT-1", "USD"),)
    assert r.book_after.settled_minor == 20000


@pytest.mark.parametrize("amount", [0, -5])
def test_payout_rejected_non_positive(amount):
    r = apply(book(settled_minor=100), ev("PAYOUT_REQUEST", seq=6, amount_minor=amount), DictLookup())
    assert r.outcome == "rejected:insufficient_funds"
    assert r.effects[0].status == "rejected"


# --- other ------------------------------------------------------------------------------
def test_unknown_type_ignored_no_effects():
    r = apply(book(settled_minor=7), ev("LOYALTY_ACCRUAL", seq=8, points=12), DictLookup())
    assert r.outcome == "ignored" and r.effects == () and r.book_after == book(settled_minor=7)


def test_input_book_never_mutated():
    b = book(currency="USD", held_minor=0)
    before = b.copy()
    apply(b, ev("AUTH", seq=2, amount_minor=700), DictLookup())
    assert b == before


@pytest.mark.parametrize("event", [
    ev("AUTH", seq=2),                                            # no amount
    ev("AUTH", seq=2, amount_minor="700"),                        # wrong type
    ev("AUTH", seq=2, amount_minor=True),                         # bool is not an amount
    ev("CAPTURE", seq=3, amount_minor=1, fee_minor=1),            # no ref
    ev("MERCHANT_CONFIG", seq=1, reserve_minor=0),                # no bank_account
    {"type": "PAYOUT_REQUEST", "amount_minor": 1},                # no event_id
])
def test_malformed_event_rejected_without_raising(event):
    r = apply(book(settled_minor=3), event, DictLookup())
    assert r.outcome == "rejected:malformed_event"
    assert r.effects == ()
    assert r.book_after == book(settled_minor=3)
