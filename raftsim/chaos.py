"""Nemesis + workload: break things at random while clients hammer the cluster.

The workload is the interesting part. Every client appends unique tokens to
one shared key. If Raft (or my dedup) is wrong you see it immediately in the
final string: a token missing, a token twice, or a client's tokens out of order.
"""
from collections import defaultdict

from .cluster import Cluster, Config


def nemesis_step(cluster, rng):
    dead = [n.id for n in cluster.nodes.values() if not n.alive]
    roll = rng.random()
    if roll < 0.25 and len(dead) < len(cluster.ids) - 1:
        # bias towards killing the leader, that's where the bugs live
        leader = cluster.leader()
        victim = leader.id if leader and rng.random() < 0.6 else rng.choice(
            [n.id for n in cluster.alive()])
        cluster.crash(victim)
    elif roll < 0.45 and dead:
        cluster.restart(rng.choice(dead))
    elif roll < 0.7:
        ids = cluster.ids[:]
        rng.shuffle(ids)
        cut = rng.randint(1, len(ids) - 1)
        cluster.partition(ids[:cut], ids[cut:])
    else:
        if cluster.net.groups is not None:
            cluster.heal()


def run_chaos(n=5, seed=0, duration=20_000, clients=3, drop=0.02, ops_per_client=None,
              node_cls=None, strict=True):
    extra = {"node_cls": node_cls} if node_cls else {}
    cluster = Cluster(n=n, seed=seed, config=Config(drop=drop), strict=strict, **extra)
    rng = cluster.sim.rng
    acked = defaultdict(list)

    def feed(c, i=0):
        if ops_per_client is not None and i >= ops_per_client:
            return
        token = f"{c.id}.{i};"
        def done(_):
            acked[c.id].append(token)
            if cluster.sim.now < duration:
                feed(c, i + 1)
        c.submit(("append", "x", token), done)

    for _ in range(clients):
        feed(cluster.client())

    while cluster.sim.now < duration:
        cluster.run(rng.randint(300, 1200))
        nemesis_step(cluster, rng)

    # let it recover: heal, bring everyone back, drain, then settle
    cluster.heal()
    for nid, node in cluster.nodes.items():
        if not node.alive:
            cluster.restart(nid)
    drained = cluster.wait_idle(15_000)
    cluster.sim.run_while(lambda: not cluster.converged(), 5_000)
    cluster.checker.check_log_matching(cluster.nodes.values())

    final = cluster.leader().kv.data.get("x", "") if cluster.leader() else ""
    return cluster, dict(acked), final, drained


def verify_appends(acked, final):
    """Every acked token exactly once, and each client's tokens in submission order."""
    tokens = [t + ";" for t in final.split(";") if t]
    problems = []
    if len(tokens) != len(set(tokens)):
        problems.append("duplicate tokens in final value")
    for cid, mine in acked.items():
        seen = [t for t in tokens if t.startswith(cid + ".")]
        if seen != mine:
            problems.append(f"{cid}: expected {len(mine)} tokens in order, found {len(seen)}")
    return problems
