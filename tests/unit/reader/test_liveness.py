from services.reader.domain.actions import PeerDead, SendHeartbeat
from services.reader.domain.liveness import Liveness


def test_heartbeat_when_idle_half_interval():
    l = Liveness(2000, now=0.0)
    assert l.tick(0.5) == []
    assert l.tick(1.0) == [SendHeartbeat()]
    l.on_sent(1.0)
    assert l.tick(1.5) == []
    assert l.tick(2.0) == [SendHeartbeat()]


def test_sending_anything_resets_timer():
    l = Liveness(2000, now=0.0)
    l.on_sent(0.9)                 # e.g. a CREDIT frame
    assert l.tick(1.5) == []
    assert l.tick(2.0) == [SendHeartbeat()]


def test_test_request_echoes_id():
    l = Liveness(2000, now=0.0)
    acts = l.on_recv(3.0, {"type": "TEST_REQUEST", "test_req_id": "T123"})
    assert acts == [SendHeartbeat(test_req_id="T123")]
    assert l.on_recv(3.0, {"type": "HEARTBEAT"}) == []
    assert l.on_recv(3.0, {"type": "DATA", "channel": "x", "seq": 1}) == []


def test_server_silence_is_dead_peer():
    l = Liveness(2000, now=0.0)
    l.on_recv(1.0, {"type": "HEARTBEAT"})
    assert l.tick(5.0) == [SendHeartbeat()]            # 4.0 s silent: not yet
    assert l.tick(5.01) == [PeerDead(silent_for_s=4.01)]


def test_dead_takes_priority_over_heartbeat():
    l = Liveness(1000, now=0.0)
    assert l.tick(2.5) == [PeerDead(silent_for_s=2.5)]
