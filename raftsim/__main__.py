import argparse
import statistics
import time

from .chaos import run_chaos, verify_appends
from .node import LEADER


def main(argv=None):
    ap = argparse.ArgumentParser(prog="raftsim", description="Raft under a chaos monkey")
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--seconds", type=float, default=30, help="simulated seconds of chaos")
    ap.add_argument("--clients", type=int, default=3)
    ap.add_argument("--drop", type=float, default=0.02, help="packet loss rate")
    ap.add_argument("--quiet", action="store_true", help="skip the timeline")
    args = ap.parse_args(argv)

    t0 = time.perf_counter()
    cluster, acked, final, drained = run_chaos(
        n=args.nodes, seed=args.seed, duration=int(args.seconds * 1000),
        clients=args.clients, drop=args.drop)
    wall = time.perf_counter() - t0

    if not args.quiet:
        for at, text in cluster.timeline:
            print(f"{at / 1000:8.3f}s  {text}")
        print()

    lat = sorted(l for c in cluster.clients for _, _, l in c.done)
    problems = verify_appends(acked, final) + cluster.checker.violations
    if not drained:
        problems.append("clients never drained after healing")
    leader = cluster.leader()
    elections = len(cluster.checker.leaders)
    q = statistics.quantiles(lat, n=100) if len(lat) > 1 else [0] * 99

    print(f"simulated {cluster.sim.now / 1000:.1f}s in {wall:.2f}s wall clock, seed {args.seed}")
    print(f"leaders elected: {elections}, final term {max(n.term for n in cluster.nodes.values())}, "
          f"final leader {leader.id if leader else '-'}")
    print(f"committed log entries: {leader.commit_index if leader else 0}")
    print(f"client ops acked: {len(lat)}  (retries {sum(c.retries for c in cluster.clients)})")
    print(f"latency ms  p50 {q[49]:.0f}  p90 {q[89]:.0f}  p99 {q[98]:.0f}  max {lat[-1] if lat else 0}")
    net = cluster.net
    print(f"messages: {net.sent} sent, {net.dropped} dropped ({net.dropped / max(net.sent, 1):.1%})")
    states = {n.id: (n.role[0].upper() if n.role == LEADER else '.') + str(n.last_applied)
              for n in cluster.nodes.values()}
    print("applied index per node:", "  ".join(f"{k}={v}" for k, v in states.items()))
    if problems:
        print("\nFAILED:")
        for p in problems:
            print("  -", p)
        return 1
    print("safety checks: ok (one leader per term, logs match, applied entries agree)")
    print("exactly-once appends: ok (every acked token exactly once, per-client order kept)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
