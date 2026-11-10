from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Optional

# cmd is None for the no-op a new leader appends, otherwise (client, seq, op)
Entry = namedtuple("Entry", "term cmd")


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class VoteReply:
    term: int
    voter: str
    granted: bool


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader: str
    prev_index: int
    prev_term: int
    entries: tuple
    leader_commit: int


@dataclass(frozen=True)
class AppendReply:
    term: int
    follower: str
    success: bool
    match_index: int = 0
    # fast backtracking hints (the optimisation from section 5.3 of the paper)
    conflict_index: int = 0
    conflict_term: Optional[int] = None


@dataclass(frozen=True)
class ClientRequest:
    client: str
    seq: int
    op: tuple


@dataclass(frozen=True)
class ClientReply:
    seq: int
    ok: bool
    result: Any = None
    leader_hint: Optional[str] = None
