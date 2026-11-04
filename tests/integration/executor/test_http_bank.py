"""HttpBank against the real harness bank (compose, default chaos).

Only references prefixed `test-` are used. verify.py flags unknown references in the bank's ledger,
so the bank must be restarted (with a fresh DB) before a milestone run.
"""
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

from services.executor.adapters.http_bank import HttpBank
from services.executor.ports import BankResponse

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "harness"))
import ledger_events as le  # noqa: E402

BANK_URL = os.environ.get("TEST_BANK_URL", "http://localhost:7200")
SEED = int(os.environ.get("GATEWAY_SEED", "42"))
MERCHANTS = int(os.environ.get("GATEWAY_MERCHANTS", "200"))


@pytest.fixture(scope="module")
def bank():
    b = HttpBank(BANK_URL, "bo-settlement", "bo-bank-dev-secret", timeout_s=30)
    yield b
    b.close()


class TokenBox:
    """One live token per client (bank.py:164): any mint revokes the previous token, so every test
    reads the current one from here and a test that mints writes it back."""
    value: str = ""


def _mint(bank):
    for _ in range(20):
        t = bank.mint_token()
        if isinstance(t, tuple):
            TokenBox.value = t[0]
            return t[0]
        time.sleep(t)
    pytest.fail("could not mint a token")


@pytest.fixture
def token(bank):
    if not TokenBox.value:
        _mint(bank)
    return TokenBox.value


def ref():
    return f"test-{uuid.uuid4().hex[:12]}"


def open_account():
    closed = le.closed_accounts(SEED, MERCHANTS)
    for mid in le.merchant_ids(MERCHANTS):
        acct = le.bank_account(SEED, mid)
        if acct not in closed:
            return acct
    raise AssertionError("no open account")


def closed_account():
    return sorted(le.closed_accounts(SEED, MERCHANTS))[0]


def create_until_201(bank, token, account, attempts=40):
    """Chaos-tolerant: retries 502/503/429/409/timeouts with fresh keys until one plain 201."""
    for _ in range(attempts):
        key, r_ = f"{ref()}#a1", ref()
        resp = bank.create_payout(token, key, r_, account, 100, "USD", timeout_s=30)
        if resp.status == 201 and not resp.replayed:
            return key, r_, resp
        time.sleep(resp.retry_after_s or 0.3 if resp.status == 503 and resp.error == "maintenance" else 0.3)
        if resp.error == "maintenance":
            time.sleep(min(resp.retry_after_s or 5, 5))
    pytest.skip("bank never returned a clean 201 (maintenance window?)")


def test_second_mint_within_5s_is_rate_limited(bank):
    first = _mint(bank)
    r = bank.mint_token()                     # inside 5 s of the previous issuance (bank.py:161-163)
    assert isinstance(r, float) and r >= 1.0
    assert TokenBox.value == first


def test_create_then_same_key_replays_original_201(bank, token):
    key, r_, first = create_until_201(bank, token, open_account())
    assert first.payout_id and first.payout_id.startswith("BP-") and first.sent
    for _ in range(20):
        again = bank.create_payout(token, key, r_, open_account(), 100, "USD", timeout_s=30)
        if again.status == 429 or again.error == "maintenance":
            time.sleep(1)
            continue
        break
    assert again.status == 201 and again.replayed is True and again.payout_id == first.payout_id


def test_closed_account_is_422_and_replays_422(bank, token):
    acct = closed_account()
    for _ in range(30):
        key, r_ = f"{ref()}#a1", ref()
        first = bank.create_payout(token, key, r_, acct, 100, "USD", timeout_s=30)
        if first.status == 422:
            break
        time.sleep(min(first.retry_after_s or 0.5, 5))
    assert first.status == 422 and first.error == "account_closed" and first.replayed is False
    for _ in range(20):
        again = bank.create_payout(token, key, r_, acct, 100, "USD", timeout_s=30)
        if again.status in (429, 503):
            time.sleep(1)
            continue
        break
    assert again.status == 422 and again.error == "account_closed" and again.replayed is True


def test_search_finds_reference_after_index_lag(bank, token):
    _, r_, first = create_until_201(bank, token, open_account())
    time.sleep(3.5)                                   # search index lags commits by 3 s (bank.py:148)
    for _ in range(20):
        rows = bank.search(token, r_, timeout_s=10)
        if isinstance(rows, list):
            break
        time.sleep(min(rows.retry_after_s or 1, 5))
    assert isinstance(rows, list) and len(rows) == 1
    assert rows[0].payout_id == first.payout_id and rows[0].reference == r_ and rows[0].amount_minor == 100
    assert bank.search(token, ref(), timeout_s=10) in ([],) or isinstance(bank.search(token, ref(), 10), BankResponse)


def test_bad_token_is_401(bank):
    r = bank.create_payout("nope", f"{ref()}#a1", ref(), open_account(), 100, "USD", timeout_s=10)
    assert r.status in (401, 503)                      # 503 only inside a maintenance window (checked first)
    if r.status == 401:
        assert r.error == "invalid_token" and r.sent
    s = bank.search("nope", ref(), timeout_s=10)
    assert isinstance(s, BankResponse) and s.status in (401, 503)


def test_connect_error_is_not_sent():
    b = HttpBank("http://127.0.0.1:1", "x", "y", timeout_s=1)
    r = b.create_payout("t", "k", "test-x", "A", 1, "USD", timeout_s=1)
    assert r == BankResponse(status=0, sent=False, error="conn_error")
    assert b.mint_token() == 1.0
    b.close()


def test_read_timeout_is_sent_unknown(bank, token):
    # 1 ms read timeout: the request reaches the bank but the reply can't arrive in time
    r = bank.create_payout(token, f"{ref()}#a1", ref(), open_account(), 100, "USD", timeout_s=0.001)
    assert (r.status == 0 and r.sent is True) or r.status in (201, 429, 503)
