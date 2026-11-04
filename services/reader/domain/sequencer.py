"""Per-channel sequencing state machine and credit policy (pure; D10, D13, D14, D17).

Facts from harness/gateway.py this relies on:
- credit is consumed only by frames fully written to us, duplicates included (gw:252-268), so
  `outstanding = granted - received` is exact;
- RESEND rewinds the server cursor (gw:330): after a gap, everything from `from_seq` comes again;
- the server sends at most 64 frames per channel per batch (gw:248);
- `poss_dup` is a hint with no information we need (gw:226); only seq numbers are compared.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from services.reader.domain.actions import Action, Deliver, Dropped, GrantCredit, SendResend

CHANNEL_FRAMES = ("DATA", "NOTICE", "GAP_FILL")
SERVER_BATCH = 64  # gw:248


@dataclass
class ResendState:
    from_seq: int
    requested_at: float
    attempts: int


@dataclass
class ChannelState:
    expected: int
    granted: int = 0
    received: int = 0
    unacked: int = 0
    acked_seq: int = 0
    resend: ResendState | None = None

    @property
    def outstanding(self) -> int:
        return self.granted - self.received


@dataclass
class SequencerStats:
    delivered: int = 0
    dropped_duplicate: int = 0
    dropped_ahead: int = 0
    dropped_unknown: int = 0
    gaps: int = 0
    resends_timeout: int = 0


@dataclass
class SequencerConfig:
    max_outstanding: int = 10_000
    initial_credit: int = 500
    low_water: int = 250
    high_water: int = 1000
    unacked_cap_per_channel: int = 2000
    unacked_cap_total: int = 50_000
    resend_timeout_s: float = 2.0
    resend_max_attempts: int = 30
    resend_floor_credit: int = SERVER_BATCH

    def __post_init__(self) -> None:
        if not (0 < self.low_water < self.high_water <= self.max_outstanding):
            raise ValueError("need 0 < low_water < high_water <= max_outstanding")
        if self.initial_credit > self.max_outstanding:
            raise ValueError("initial_credit exceeds max_outstanding")


class Sequencer:
    def __init__(self, *, resume: dict[str, int], config: SequencerConfig | None = None):
        self.cfg = config or SequencerConfig()
        self.channels: dict[str, ChannelState] = {
            ch: ChannelState(expected=int(last) + 1, acked_seq=int(last)) for ch, last in resume.items()
        }
        self.stats = SequencerStats()
        self.total_unacked = 0
        self._fatal: str | None = None

    # -- lifecycle ----------------------------------------------------------------------
    @property
    def fatal(self) -> str | None:
        return self._fatal

    def start_session(self, channels: list[str]) -> list[Action]:
        """Credit never survives a session (gw:166): reset accounting, keep positions."""
        self._fatal = None
        self.total_unacked = 0
        for ch in channels:
            self.channels.setdefault(ch, ChannelState(expected=1))
        actions: list[Action] = []
        for ch, s in self.channels.items():
            s.granted = s.received = s.unacked = 0
            s.resend = None
            if ch in channels:
                actions.append(self._grant(ch, s, self.cfg.initial_credit))
        return actions

    def acked_map(self) -> dict[str, int]:
        """Highest acked seq per channel: the resume fallback when the checkpoint can't be read."""
        return {ch: s.acked_seq for ch, s in self.channels.items()}

    # -- frames -------------------------------------------------------------------------
    def on_frame(self, frame: dict, now: float) -> list[Action]:
        t = frame.get("type")
        ch = frame.get("channel")
        seq = frame.get("seq")
        if t not in CHANNEL_FRAMES:
            return []
        s = self.channels.get(ch)
        if s is None:
            self.stats.dropped_unknown += 1
            return [Dropped(ch, seq, "unknown_channel")]
        if not isinstance(seq, int) or seq < 1:
            self.stats.dropped_unknown += 1
            return [Dropped(ch, seq, "malformed")]
        s.received += 1  # credit consumed on the server side (gw:261,267)

        if t == "GAP_FILL":
            new_seq = frame.get("new_seq")
            if not isinstance(new_seq, int) or new_seq <= seq:
                self.stats.dropped_unknown += 1
                return [Dropped(ch, seq, "malformed")]
            if new_seq <= s.expected:
                return self._duplicate(ch, seq)
            if seq > s.expected:
                return self._gap(ch, s, seq, now)
            # seq <= expected < new_seq: the range covers our expected position
            deliver = Deliver(ch, s.expected, new_seq - 1, "GAP_FILL", None)
            s.expected = new_seq
            return self._delivered(s, deliver)

        if seq < s.expected:
            return self._duplicate(ch, seq)
        if seq > s.expected:
            return self._gap(ch, s, seq, now)
        deliver = Deliver(ch, seq, seq, t, frame.get("event") if t == "DATA" else None)
        if t == "DATA" and not isinstance(deliver.event, dict):
            self.stats.dropped_unknown += 1
            return [Dropped(ch, seq, "malformed")]
        s.expected = seq + 1
        return self._delivered(s, deliver)

    def _delivered(self, s: ChannelState, deliver: Deliver) -> list[Action]:
        s.unacked += 1
        self.total_unacked += 1
        s.resend = None
        self.stats.delivered += 1
        return [deliver]

    def _duplicate(self, ch: str, seq: int) -> list[Action]:
        self.stats.dropped_duplicate += 1
        return [Dropped(ch, seq, "duplicate")]

    def _gap(self, ch: str, s: ChannelState, seq: int, now: float) -> list[Action]:
        self.stats.dropped_ahead += 1
        actions: list[Action] = [Dropped(ch, seq, "ahead_of_gap")]
        if s.resend is None:
            self.stats.gaps += 1
            s.resend = ResendState(from_seq=s.expected, requested_at=now, attempts=1)
            actions.insert(0, SendResend(ch, s.expected, 1))
        return actions

    # -- publisher feedback -------------------------------------------------------------
    def on_acked(self, channel: str, end_seq: int) -> None:
        s = self.channels.get(channel)
        if s is None:
            return
        if s.unacked > 0:
            s.unacked -= 1
            self.total_unacked -= 1
        if end_seq > s.acked_seq:
            s.acked_seq = end_seq

    def on_publish_failed(self, channel: str, end_seq: int, error: str) -> None:
        self._fatal = f"publish failed for {channel}:{end_seq}: {error}"

    # -- timers -------------------------------------------------------------------------
    def tick(self, now: float) -> list[Action]:
        actions: list[Action] = []
        for ch, s in self.channels.items():
            r = s.resend
            if r is not None and now - r.requested_at >= self.cfg.resend_timeout_s:
                if r.attempts >= self.cfg.resend_max_attempts:
                    self._fatal = f"resend for {ch} from {s.expected} not answered after {r.attempts} attempts"
                    return actions
                r.attempts += 1
                r.from_seq = s.expected
                r.requested_at = now
                self.stats.resends_timeout += 1
                actions.append(SendResend(ch, s.expected, r.attempts))

            out = s.outstanding
            if out < self.cfg.low_water and s.unacked < self.cfg.unacked_cap_per_channel \
                    and self.total_unacked < self.cfg.unacked_cap_total:
                n = self.cfg.high_water - out
                actions.append(self._grant(ch, s, n))
            elif r is not None and out <= 0:
                # D17: a channel at its unacked cap with an open gap must still be able to receive
                # the resend; ahead-of-gap drops consumed its credit (gw:261).
                actions.append(self._grant(ch, s, self.cfg.resend_floor_credit))
        return actions

    def _grant(self, ch: str, s: ChannelState, n: int) -> GrantCredit:
        room = self.cfg.max_outstanding - s.outstanding
        n = max(0, min(n, room))
        s.granted += n
        return GrantCredit(ch, n)
