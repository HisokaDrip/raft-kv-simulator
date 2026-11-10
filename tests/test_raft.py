import pytest

from raftsim import Cluster, LEADER
from raftsim.messages import VoteReply
from raftsim.node import RaftNode
from raftsim.chaos import run_chaos, verify_appends


@pytest.mark.parametrize("seed", range(10))
def test_elects_exactly_one_leader(seed):
    c = Cluster(n=5, seed=seed)
    leader = c.wait_for_leader()
    assert leader is not None
    c.run(2000)  # a stable cluster shouldn't churn
    leaders = [n for n in c.nodes.values() if n.role == LEADER]
    assert len(leaders) == 1
    assert leaders[0].term == leader.term


def test_single_node_cluster_works():
    c = Cluster(n=1)
    cl = c.client()
    out = []
    cl.submit(("put", "k", "v"), out.append)
    c.wait_idle()
    assert out == ["v"]


def test_writes_replicate_everywhere():
    c = Cluster(n=5, seed=3)
    c.wait_for_leader()
    cl = c.client()
    results = []
    for i in range(20):
        cl.submit(("append", "log", str(i % 10)), results.append)
    cl.submit(("get", "log"), results.append)
    assert c.wait_idle()
    c.run(300)
    assert results[-1] == "0123456789" * 2
    assert all(n.kv.data == {"log": "0123456789" * 2} for n in c.nodes.values())


def test_leader_crash_fails_over():
    c = Cluster(n=5, seed=7)
    old = c.wait_for_leader()
    cl = c.client()
    cl.submit(("put", "a", "1"))
    c.wait_idle()
    c.crash(old.id)
    new = c.wait_for_leader()
    assert new.id != old.id and new.term > old.term
    got = []
    cl.submit(("get", "a"), got.append)
    assert c.wait_idle()
    assert got == ["1"]


def test_minority_leader_cannot_commit_and_gets_overwritten():
    c = Cluster(n=5, seed=11)
    old = c.wait_for_leader()
    others = [i for i in c.ids if i != old.id]
    c.partition([old.id, others[0]], others[1:])
    commit_before = old.commit_index

    # sneak a write into the stranded leader directly
    stranded = c.client(timeout=10_000)
    stranded.guess = old.id
    stranded.submit(("put", "k", "lost"))
    c.run(1500)
    assert old.commit_index == commit_before
    assert stranded.inflight is not None

    new = c.leader()
    assert new is not None and new.id in others[1:] and new.term > old.term
    writer = c.client()
    writer.guess = new.id
    writer.submit(("put", "k", "kept"))
    c.run(1000)
    assert writer.idle

    c.heal()
    c.run(1500)
    assert old.role != LEADER or old.term > new.term
    c.wait_idle()
    c.run(500)
    # the stranded write eventually commits after its retry, but never *instead of* "kept"
    assert all(n.kv.data["k"] in ("kept", "lost") for n in c.nodes.values())
    assert len({n.kv.data["k"] for n in c.nodes.values()}) == 1
    c.checker.check_log_matching(c.nodes.values())
    assert c.checker.violations == []


def test_lagging_follower_catches_up_after_restart():
    c = Cluster(n=3, seed=5)
    leader = c.wait_for_leader()
    lagger = next(i for i in c.ids if i != leader.id)
    c.crash(lagger)
    cl = c.client()
    for i in range(300):
        cl.submit(("put", f"k{i}", i))
    assert c.wait_idle(20_000)
    c.restart(lagger)
    c.run(2000)
    lag = c.nodes[lagger]
    assert lag.last_applied == c.leader().commit_index
    assert lag.kv.data == c.leader().kv.data


@pytest.mark.parametrize("n", [3, 5, 7])
@pytest.mark.parametrize("seed", range(6))
def test_chaos_keeps_safety_and_exactly_once(n, seed):
    cluster, acked, final, drained = run_chaos(n=n, seed=seed, duration=8000, drop=0.05)
    assert drained
    assert cluster.converged()
    assert verify_appends(acked, final) == []
    assert sum(len(v) for v in acked.values()) > 20


class ForgetfulVoter(RaftNode):
    """Grants votes without the up-to-date log check (5.4.1). This is a real bug."""

    def _on_request_vote(self, m):
        if m.term > self.term:
            self._step_down(m.term)
        grant = m.term == self.term and self.voted_for in (None, m.candidate)
        if grant:
            self.voted_for = m.candidate
            self._arm_election_timer()
        self._send(m.candidate, VoteReply(self.term, self.id, grant))


def test_checker_catches_a_broken_vote_rule():
    caught = 0
    for seed in range(5):
        cluster, acked, final, _ = run_chaos(n=5, seed=seed, duration=8000,
                                             node_cls=ForgetfulVoter, strict=False)
        if cluster.checker.violations or verify_appends(acked, final):
            caught += 1
    assert caught >= 3


def test_runs_are_deterministic():
    a = run_chaos(n=5, seed=42, duration=5000)
    b = run_chaos(n=5, seed=42, duration=5000)
    assert a[0].timeline == b[0].timeline
    assert a[2] == b[2]
