"""Every row of the frame table in docs/STEP2-PLAN.md §4, plus credit accounting and D17."""
import pytest

from services.reader.domain.actions import Deliver, Dropped, GrantCredit, SendResend
from services.reader.domain.sequencer import Sequencer, SequencerConfig

CH = "M-0001"


def data(seq, ch=CH, poss_dup=False):
    return {"type": "DATA", "channel": ch, "seq": seq, "poss_dup": poss_dup,
            "event": {"event_id": f"{ch}:{seq}", "type": "AUTH"}}


def notice(seq, ch=CH):
    return {"type": "NOTICE", "channel": ch, "seq": seq, "poss_dup": False, "text": "x"}


def gap_fill(seq, new_seq, ch=CH):
    return {"type": "GAP_FILL", "channel": ch, "seq": seq, "new_seq": new_seq, "poss_dup": True}


def only(actions, kind):
    return [a for a in actions if isinstance(a, kind)]


@pytest.fixture
def seq():
    s = Sequencer(resume={CH: 0}, config=SequencerConfig(initial_credit=10, low_water=5, high_water=20))
    s.start_session([CH])
    return s


# --- ordering -----------------------------------------------------------------------
def test_in_order_delivers_and_advances(seq):
    for n in (1, 2, 3):
        acts = seq.on_frame(data(n), now=0)
        assert acts == [Deliver(CH, n, n, "DATA", data(n)["event"])]
    assert seq.channels[CH].expected == 4
    assert seq.stats.delivered == 3


@pytest.mark.parametrize("poss_dup", [True, False])
def test_duplicate_below_expected_dropped_whatever_poss_dup(seq, poss_dup):
    seq.on_frame(data(1), 0)
    seq.on_frame(data(2), 0)
    acts = seq.on_frame(data(1, poss_dup=poss_dup), 0)
    assert acts == [Dropped(CH, 1, "duplicate")]
    assert seq.channels[CH].expected == 3
    assert seq.stats.dropped_duplicate == 1


def test_gap_requests_one_resend_and_drops_ahead_frames(seq):
    seq.on_frame(data(1), 0)
    acts = seq.on_frame(data(4), now=10)          # 2,3 lost
    assert acts == [SendResend(CH, 2, 1), Dropped(CH, 4, "ahead_of_gap")]
    acts = seq.on_frame(data(5), now=10)
    assert acts == [Dropped(CH, 5, "ahead_of_gap")]     # no second RESEND
    assert seq.stats.gaps == 1
    assert seq.channels[CH].expected == 2


def test_gap_closes_when_expected_arrives(seq):
    seq.on_frame(data(1), 0)
    seq.on_frame(data(4), 0)
    assert seq.channels[CH].resend is not None
    acts = seq.on_frame(data(2, poss_dup=True), 1)    # resent frames carry poss_dup=True
    assert only(acts, Deliver) == [Deliver(CH, 2, 2, "DATA", data(2)["event"])]
    assert seq.channels[CH].resend is None
    assert seq.on_frame(data(3, poss_dup=True), 1) == [Deliver(CH, 3, 3, "DATA", data(3)["event"])]
    assert seq.on_frame(data(4, poss_dup=True), 1) == [Deliver(CH, 4, 4, "DATA", data(4)["event"])]


def test_lost_resend_is_re_requested_after_timeout(seq):
    seq.on_frame(data(1), 0)
    seq.on_frame(data(4), now=0)
    assert only(seq.tick(now=1.0), SendResend) == []
    assert only(seq.tick(now=2.0), SendResend) == [SendResend(CH, 2, 2)]
    assert only(seq.tick(now=3.0), SendResend) == []
    assert only(seq.tick(now=4.5), SendResend) == [SendResend(CH, 2, 3)]
    assert seq.stats.resends_timeout == 2


def test_resend_attempts_exhausted_is_fatal():
    s = Sequencer(resume={CH: 0}, config=SequencerConfig(resend_max_attempts=3, resend_timeout_s=1))
    s.start_session([CH])
    s.on_frame(data(1), 0)
    s.on_frame(data(3), 0)
    for t in (1, 2):
        s.tick(now=t)
    assert s.fatal is None
    s.tick(now=3)
    assert s.fatal and "resend" in s.fatal


# --- GAP_FILL / NOTICE --------------------------------------------------------------
def test_gap_fill_fully_duplicate_dropped(seq):
    for n in (1, 2, 3, 4, 5):
        seq.on_frame(data(n), 0)
    assert seq.on_frame(gap_fill(2, 4), 0) == [Dropped(CH, 2, "duplicate")]
    assert seq.channels[CH].expected == 6


def test_gap_fill_overlapping_delivers_from_expected(seq):
    seq.on_frame(data(1), 0)
    seq.on_frame(data(2), 0)                       # expected = 3
    acts = seq.on_frame(gap_fill(2, 6), 0)         # covers 2..5
    assert acts == [Deliver(CH, 3, 5, "GAP_FILL", None)]
    assert seq.channels[CH].expected == 6


def test_gap_fill_exact(seq):
    seq.on_frame(data(1), 0)
    assert seq.on_frame(gap_fill(2, 4), 0) == [Deliver(CH, 2, 3, "GAP_FILL", None)]
    assert seq.channels[CH].expected == 4


def test_gap_fill_ahead_is_a_gap(seq):
    seq.on_frame(data(1), 0)
    acts = seq.on_frame(gap_fill(5, 7), now=0)
    assert acts == [SendResend(CH, 2, 1), Dropped(CH, 5, "ahead_of_gap")]


def test_gap_fill_closes_pending_resend(seq):
    seq.on_frame(data(1), 0)
    seq.on_frame(data(6), 0)                      # gap at 2
    acts = seq.on_frame(gap_fill(2, 6), 1)        # 2..5 were notices
    assert acts == [Deliver(CH, 2, 5, "GAP_FILL", None)]
    assert seq.channels[CH].resend is None
    assert seq.on_frame(data(6, poss_dup=True), 1) == [Deliver(CH, 6, 6, "DATA", data(6)["event"])]


def test_notice_advances_like_data(seq):
    seq.on_frame(data(1), 0)
    assert seq.on_frame(notice(2), 0) == [Deliver(CH, 2, 2, "NOTICE", None)]
    assert seq.channels[CH].expected == 3


# --- resume / channels --------------------------------------------------------------
def test_resume_sets_expected_and_acked():
    s = Sequencer(resume={CH: 1532})
    s.start_session([CH])
    assert s.channels[CH].expected == 1533
    assert s.acked_map() == {CH: 1532}
    assert s.on_frame(data(1532), 0) == [Dropped(CH, 1532, "duplicate")]
    assert only(s.on_frame(data(1533), 0), Deliver)


def test_unknown_channel_dropped(seq):
    assert seq.on_frame(data(1, ch="M-9999"), 0) == [Dropped("M-9999", 1, "unknown_channel")]


def test_channels_from_logon_ack_are_added():
    s = Sequencer(resume={})
    acts = s.start_session(["M-0001", "M-0002"])
    assert {a.channel for a in only(acts, GrantCredit)} == {"M-0001", "M-0002"}
    assert s.channels["M-0002"].expected == 1


def test_malformed_frames_dropped(seq):
    assert seq.on_frame({"type": "DATA", "channel": CH, "seq": "x"}, 0) == [Dropped(CH, "x", "malformed")]
    assert seq.on_frame({"type": "DATA", "channel": CH, "seq": 1}, 0) == [Dropped(CH, 1, "malformed")]  # no event
    assert seq.on_frame({"type": "GAP_FILL", "channel": CH, "seq": 1, "new_seq": 1}, 0) == [Dropped(CH, 1, "malformed")]
    assert seq.on_frame({"type": "HEARTBEAT"}, 0) == []


# --- credit -------------------------------------------------------------------------
def test_start_session_grants_initial_credit(seq):
    s = seq.channels[CH]
    assert s.granted == 10 and s.received == 0 and s.outstanding == 10


def test_received_counts_duplicates_and_gap_fill(seq):
    seq.on_frame(data(1), 0)
    seq.on_frame(data(1), 0)          # dup consumed credit on the server (gw:267)
    seq.on_frame(gap_fill(2, 3), 0)
    assert seq.channels[CH].received == 3
    assert seq.channels[CH].outstanding == 7


def test_top_up_when_below_low_water(seq):
    for n in range(1, 7):             # outstanding 10 -> 4 (< low_water 5)
        seq.on_frame(data(n), 0)
        seq.on_acked(CH, n)
    acts = only(seq.tick(0), GrantCredit)
    assert acts == [GrantCredit(CH, 16)]          # high_water 20 - outstanding 4
    assert seq.channels[CH].outstanding == 20


def test_no_top_up_above_low_water(seq):
    for n in range(1, 3):
        seq.on_frame(data(n), 0)
        seq.on_acked(CH, n)
    assert only(seq.tick(0), GrantCredit) == []


def test_never_exceeds_max_outstanding():
    s = Sequencer(resume={CH: 0}, config=SequencerConfig(max_outstanding=12, initial_credit=10, low_water=11, high_water=12))
    acts = s.start_session([CH])
    assert acts == [GrantCredit(CH, 10)]
    acts = only(s.tick(0), GrantCredit)
    assert acts == [GrantCredit(CH, 2)]
    assert s.channels[CH].outstanding == 12
    assert only(s.tick(0), GrantCredit) == []


def test_no_top_up_when_unacked_at_cap():
    s = Sequencer(resume={CH: 0}, config=SequencerConfig(initial_credit=10, low_water=5, high_water=20, unacked_cap_per_channel=3))
    s.start_session([CH])
    for n in range(1, 7):
        s.on_frame(data(n), 0)        # unacked 6 >= cap 3, outstanding 4 < low_water
    assert only(s.tick(0), GrantCredit) == []
    for n in range(1, 5):
        s.on_acked(CH, n)             # unacked 2
    assert only(s.tick(0), GrantCredit) == [GrantCredit(CH, 16)]


def test_total_unacked_cap_blocks_all_channels():
    s = Sequencer(resume={}, config=SequencerConfig(initial_credit=10, low_water=5, high_water=20, unacked_cap_total=2))
    s.start_session(["A", "B"])
    for n in range(1, 7):
        s.on_frame(data(n, ch="A"), 0)
    for n in range(1, 7):
        s.on_frame(data(n, ch="B"), 0)
    assert only(s.tick(0), GrantCredit) == []


def test_credit_floor_during_resend_at_unacked_cap():
    """D17: channel at its unacked cap, open gap, zero outstanding -> grant 64 so the resend can arrive."""
    s = Sequencer(resume={CH: 0}, config=SequencerConfig(initial_credit=4, low_water=2, high_water=6, unacked_cap_per_channel=2))
    s.start_session([CH])
    s.on_frame(data(1), 0)
    s.on_frame(data(2), 0)                        # unacked 2 == cap
    s.on_frame(data(5), 0)                        # gap at 3; received 3, outstanding 1
    s.on_frame(data(6), 0)                        # outstanding 0
    acts = only(s.tick(0), GrantCredit)
    assert acts == [GrantCredit(CH, 64)]
    assert s.channels[CH].outstanding == 64


def test_credit_floor_respects_hard_cap():
    s = Sequencer(resume={CH: 0}, config=SequencerConfig(max_outstanding=30, initial_credit=2, low_water=1, high_water=2, unacked_cap_per_channel=1))
    s.start_session([CH])
    s.on_frame(data(1), 0)
    s.on_frame(data(3), 0)                        # gap, outstanding 0, unacked 1 == cap
    assert only(s.tick(0), GrantCredit) == [GrantCredit(CH, 30)]


def test_new_session_resets_credit_keeps_position(seq):
    for n in (1, 2, 3):
        seq.on_frame(data(n), 0)
    seq.on_acked(CH, 3)
    acts = seq.start_session([CH])
    s = seq.channels[CH]
    assert acts == [GrantCredit(CH, 10)]
    assert (s.granted, s.received, s.unacked) == (10, 0, 0)
    assert s.expected == 4 and s.acked_seq == 3
    assert seq.total_unacked == 0


# --- publisher feedback -------------------------------------------------------------
def test_ack_advances_acked_seq_and_resume_map(seq):
    seq.on_frame(data(1), 0)
    seq.on_frame(gap_fill(2, 5), 0)              # one message covering 2..4
    assert seq.channels[CH].unacked == 2
    seq.on_acked(CH, 1)
    seq.on_acked(CH, 4)
    assert seq.channels[CH].unacked == 0
    assert seq.acked_map() == {CH: 4}


def test_publish_failure_is_fatal(seq):
    seq.on_frame(data(1), 0)
    seq.on_publish_failed(CH, 1, "broker down")
    assert seq.fatal and "publish failed" in seq.fatal


def test_config_validation():
    with pytest.raises(ValueError):
        SequencerConfig(low_water=10, high_water=5)
    with pytest.raises(ValueError):
        SequencerConfig(initial_credit=20_000)
