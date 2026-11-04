"""R1 + R2 — `session_reader.py:101–113`: every `poss_dup: true` frame is dropped, and a gap is "handled" by
requesting a resend and then publishing the out-of-order frame anyway, moving the checkpoint past the hole.

Gateway facts (`gateway.py:167, 226, 262`): `poss_dup = seq <= seen`, where `seen` is the highest seq ever
written to this `client_id`, kept across sessions. So after any reconnect, every frame between our checkpoint
and `seen` is `poss_dup: true` — and every resent frame is too. The flag means "you may have seen this", not
"you have". The reader must dedupe by sequence number and never trust the flag.
"""
from __future__ import annotations

from tests.incumbent.conftest import impls
from tests.incumbent.fakes import FakeWriter, data_frame


@impls("R1")
def test_resume_after_reconnect_keeps_frames_flagged_poss_dup(impl):
    """Checkpoint says 5. The gateway had written up to 10 before the disconnect (so seen=10). It resumes at
    6 and, because 6..10 <= seen, flags each of them poss_dup. They have never been published."""
    w = FakeWriter()
    reader = impl.Reader(channels=["m1"], resume={"m1": 5}, writer=w)

    for seq in range(6, 11):
        reader.on_frame(data_frame("m1", seq, poss_dup=True))

    assert [e["seq"] for e in reader.published] == [6, 7, 8, 9, 10]
    assert reader.checkpoint["m1"] == 10


@impls("R1/R2")
def test_gap_is_filled_by_the_resend_and_nothing_is_skipped(impl):
    """Frame 3 is dropped on the wire. 4 arrives → gap → RESEND from 3. The gateway rewinds (go-back-N,
    `gateway.py:330`) and sends 3 and 4 again, both poss_dup. Result must be 1,2,3,4 once each, in order."""
    w = FakeWriter()
    reader = impl.Reader(channels=["m1"], resume={}, writer=w)

    reader.on_frame(data_frame("m1", 1))
    reader.on_frame(data_frame("m1", 2))
    reader.on_frame(data_frame("m1", 4))  # 3 was dropped
    reader.on_frame(data_frame("m1", 3, poss_dup=True))  # resend
    reader.on_frame(data_frame("m1", 4, poss_dup=True))  # resend (go-back-N)

    assert w.resends() == [("m1", 3)]
    assert [e["seq"] for e in reader.published] == [1, 2, 3, 4], f"published {[e['seq'] for e in reader.published]}"
    assert reader.checkpoint["m1"] == 4


@impls("R1")
def test_gap_fill_flagged_poss_dup_advances_the_cursor(impl):
    """GAP_FILL is always poss_dup (`gateway.py:222`). Dropping it leaves `expected` stuck and the next DATA
    frame looks like a gap."""
    w = FakeWriter()
    reader = impl.Reader(channels=["m1"], resume={}, writer=w)

    reader.on_frame(data_frame("m1", 1))
    reader.on_frame({"type": "GAP_FILL", "channel": "m1", "seq": 2, "new_seq": 5, "poss_dup": True})
    reader.on_frame(data_frame("m1", 5))

    assert w.resends() == [], "spurious RESEND after a GAP_FILL"
    assert [e["seq"] for e in reader.published] == [1, 5]
    assert reader.expected["m1"] == 6
