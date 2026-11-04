import json
import logging

from services.common import log as logmod


def _capture(capsys, fn):
    logmod._CONFIGURED = False  # force a fresh stdout handler bound to capsys
    logmod.configure("DEBUG")
    fn()
    out = capsys.readouterr().out.strip().splitlines()
    return [json.loads(line) for line in out]


def test_correlation_fields_become_json_keys(capsys):
    def go():
        log = logmod.get_logger("reader", instance="h:1")
        log.info("session up", session_epoch=17, channel="M-0001", event_id="M-0001:5", payout_id="M-0001:9")

    (rec,) = _capture(capsys, go)
    assert rec["msg"] == "session up"
    assert rec["service"] == "reader"
    assert rec["instance"] == "h:1"
    assert rec["session_epoch"] == 17
    assert rec["channel"] == "M-0001"
    assert rec["event_id"] == "M-0001:5"
    assert rec["payout_id"] == "M-0001:9"
    assert rec["level"] == "INFO"
    assert rec["ts"].endswith("Z")


def test_bind_carries_fields(capsys):
    def go():
        base = logmod.get_logger("executor")
        bound = base.bind(payout_id="M-0002:3")
        bound.warning("retrying", attempt=2)

    (rec,) = _capture(capsys, go)
    assert rec["payout_id"] == "M-0002:3"
    assert rec["attempt"] == 2
    assert rec["level"] == "WARNING"


def test_one_line_per_record(capsys):
    def go():
        log = logmod.get_logger("consumer")
        log.info("a")
        log.info("b", nested={"x": [1, 2]})

    recs = _capture(capsys, go)
    assert [r["msg"] for r in recs] == ["a", "b"]
    assert recs[1]["nested"] == {"x": [1, 2]}


def test_exception_is_captured(capsys):
    def go():
        log = logmod.get_logger("api")
        try:
            raise ValueError("boom")
        except ValueError:
            log.error("failed", exc_info=True)

    (rec,) = _capture(capsys, go)
    assert "ValueError: boom" in rec["exc"]


def test_level_filter(capsys):
    def go():
        logging.getLogger().setLevel("WARNING")
        log = logmod.get_logger("x")
        log.info("hidden")
        log.warning("shown")

    recs = _capture(capsys, go)
    assert [r["msg"] for r in recs] == ["shown"]
