import pytest

from services.common.messages import LedgerMessage, MessageError, decode, encode

EV = {"event_id": "M-0001:7", "merchant_id": "M-0001", "type": "AUTH", "amount_minor": 100}


@pytest.mark.parametrize("m", [
    LedgerMessage("M-0001", 7, 7, "DATA", EV, 1793000001, 1700000000.5),
    LedgerMessage("M-0001", 8, 8, "NOTICE", None, 1793000001, 1700000000.5),
    LedgerMessage("M-0001", 9, 12, "GAP_FILL", None, 1793000002, 1700000001.0),
])
def test_round_trip(m):
    raw = encode(m)
    assert b"\n" not in raw and b" " not in raw.split(b'"event"')[0]
    assert decode(raw) == m


def test_event_id_property():
    assert LedgerMessage("M-0001", 7, 7, "DATA", EV, 1, 0.0).event_id == "M-0001:7"
    assert LedgerMessage("M-0001", 8, 8, "NOTICE", None, 1, 0.0).event_id is None


@pytest.mark.parametrize("kwargs", [
    dict(kind="BOGUS", event=None),
    dict(kind="DATA", event=None),
    dict(kind="NOTICE", event=EV),
    dict(kind="NOTICE", event=None, end_seq=9),       # end_seq != seq
    dict(kind="GAP_FILL", event=None, end_seq=6),     # end_seq < seq
])
def test_invariants(kwargs):
    base = dict(channel="M-0001", seq=7, end_seq=7, event=EV, session_epoch=1, published_at=0.0)
    with pytest.raises(MessageError):
        LedgerMessage(**{**base, **kwargs})


def test_decode_rejects_other_versions_and_garbage():
    raw = encode(LedgerMessage("M-0001", 7, 7, "DATA", EV, 1, 0.0)).replace(b'"v":1', b'"v":2')
    with pytest.raises(MessageError, match="version"):
        decode(raw)
    with pytest.raises(MessageError):
        decode(b"not json")
    with pytest.raises(MessageError):
        decode(b"[1,2]")
    with pytest.raises(MessageError):
        decode(b'{"v":1,"channel":"x"}')
