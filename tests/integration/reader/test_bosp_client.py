"""BospClient against the real harness gateway (compose, chaos on). Uses its own client_id."""
import asyncio
import uuid

import pytest

from services.reader.adapters.bosp_client import BospClient, LogonRejected

pytestmark = pytest.mark.integration

HOST, PORT, SECRET = "localhost", 7100, "bo-dev-secret"


def cid():
    return f"test-{uuid.uuid4().hex[:8]}"


async def test_logon_ack_and_first_data_frame():
    c = BospClient(HOST, PORT, cid(), SECRET)
    info = await c.connect({})
    try:
        assert info.session_epoch > 0 and info.heartbeat_ms > 0
        assert info.max_outstanding_credit == 10_000
        assert len(info.channels) >= 1 and info.channels[0].startswith("M-")
        ch = info.channels[0]
        await c.send({"type": "CREDIT", "channel": ch, "n": 5})
        got = None
        async with asyncio.timeout(20):
            async for f in c.frames():
                if f.type in ("DATA", "NOTICE", "GAP_FILL") and f.channel == ch:
                    got = f
                    break
                if f.type == "TEST_REQUEST":
                    await c.send({"type": "HEARTBEAT", "test_req_id": f.raw["test_req_id"]})
        assert got is not None and got.seq == 1
    finally:
        await c.close()


async def test_heartbeats_keep_session_alive():
    c = BospClient(HOST, PORT, cid(), SECRET)
    info = await c.connect({})
    hb = info.heartbeat_ms / 1000

    async def heartbeat():
        while True:
            await asyncio.sleep(hb / 2)
            await c.send({"type": "HEARTBEAT"})

    task = asyncio.create_task(heartbeat())
    try:
        logout = None
        try:
            async with asyncio.timeout(hb * 3.5):      # > server's 2.5*hb timeout
                async for f in c.frames():
                    if f.type == "LOGOUT":
                        logout = f.raw
                        break
        except TimeoutError:
            pass
        assert logout is None, f"server logged us out: {logout}"
    finally:
        task.cancel()
        await c.close()


async def test_bad_secret_rejected():
    c = BospClient(HOST, PORT, cid(), "wrong")
    with pytest.raises(LogonRejected) as e:
        await c.connect({})
    assert e.value.reason == "bad_credentials"


async def test_second_logon_supersedes_first():
    client_id = cid()
    first = BospClient(HOST, PORT, client_id, SECRET)
    await first.connect({})
    await asyncio.sleep(1.1)                     # logon throttle is 1/s per client_id (gw:373)
    second = BospClient(HOST, PORT, client_id, SECRET)
    info2 = await second.connect({})
    try:
        seen = None
        async with asyncio.timeout(5):
            async for f in first.frames():
                if f.type == "LOGOUT":
                    seen = f.raw
                    break
        assert seen == {"type": "LOGOUT", "reason": "superseded"}
        assert info2.session_epoch > 0
    finally:
        await first.close(logout=False)
        await second.close()


async def test_resume_skips_processed_frames():
    client_id = cid()
    c = BospClient(HOST, PORT, client_id, SECRET)
    info = await c.connect({})
    ch = info.channels[0]
    await c.close()
    await asyncio.sleep(1.1)
    c = BospClient(HOST, PORT, client_id, SECRET)
    await c.connect({ch: 3})
    try:
        await c.send({"type": "CREDIT", "channel": ch, "n": 3})
        first = None
        async with asyncio.timeout(20):
            async for f in c.frames():
                if f.channel == ch and f.type in ("DATA", "NOTICE", "GAP_FILL"):
                    first = f
                    break
                if f.type == "TEST_REQUEST":
                    await c.send({"type": "HEARTBEAT", "test_req_id": f.raw["test_req_id"]})
        assert first is not None and first.seq == 4
    finally:
        await c.close()
