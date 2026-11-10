# raft-kv-simulator

Raft consensus in pure Python (no dependencies), running a replicated key-value
store on a fake network that I can crash, partition and drop packets on at will.

I wanted to actually understand Raft instead of nodding along to the paper, and
the only way I know to do that is to write it and then try very hard to break it.
So the whole thing runs on a deterministic discrete-event simulator: one thread,
a fake clock, one seed. A 30-second chaos run with ~18k messages finishes in
about a tenth of a second, and when something goes wrong you can replay the
exact same interleaving.

What's in it:

- leader election with randomised timeouts and the up-to-date-log vote rule
- log replication with the fast-backtracking conflict hints (so a node that
  missed 300 entries catches up in a couple of round trips, not 300)
- the commit-only-your-own-term rule from figure 8, plus the no-op a new
  leader appends to get old entries committed
- crash/restart with "durable" term, vote and log; the state machine is rebuilt
  by replaying the log
- a KV store with client sessions, so a retried request is applied exactly once
- clients that don't know the leader: they guess, follow redirects and retry
- a safety checker hooked into every node that asserts, live:
  one leader per term, no two nodes apply different entries at the same index,
  every new leader already holds every applied entry, and log matching

## Running it

```
python -m raftsim                       # 5 nodes, 30s of chaos, seed 1
python -m raftsim --nodes 7 --drop 0.1 --seconds 60 --seed 3 --quiet
python -m pytest -q
```

No requirements beyond Python 3.10+ (pytest for the tests).

## What a run looks like

The nemesis wakes up every 0.3-1.2s and crashes a node (usually the leader),
restarts one, splits the network in two, or heals it. Meanwhile three clients
keep appending unique tokens like `c2.17;` to the same key. At the end
everything is healed and restarted, and the final string is checked: every
acknowledged token has to be there exactly once, and each client's tokens have
to be in the order it sent them. Lost writes, double-applied retries and
reordering all show up there immediately.

```
$ python -m raftsim --seed 7 --seconds 30
   0.198s  n5 elected leader for term 1
   1.772s  partition n2,n3,n5 | n1,n4
   2.082s  n5 crashed
   2.894s  n5 restarted
   2.950s  n3 elected leader for term 5
   3.982s  n3 crashed
   4.415s  n3 restarted
   4.742s  n5 elected leader for term 9
   4.821s  network healed
   5.074s  n5 elected leader for term 17
   5.361s  n5 crashed
   5.649s  n3 elected leader for term 18
   6.637s  n3 crashed
   6.836s  n1 elected leader for term 19
     ...

simulated 30.3s in 0.13s wall clock, seed 7
leaders elected: 12, final term 90, final leader n1
committed log entries: 669
client ops acked: 654  (retries 245)
latency ms  p50 57  p90 78  p99 2559  max 5287
messages: 17894 sent, 1479 dropped (8.3%)
applied index per node: n1=L669  n2=.669  n3=.669  n4=.669  n5=.669
safety checks: ok (one leader per term, logs match, applied entries agree)
exactly-once appends: ok (every acked token exactly once, per-client order kept)
```

Latency is ~57ms at p50 (a couple of network hops) and a few seconds at p99,
which is the time clients spend stuck behind a partition or a dead leader.

## Does the checker actually catch anything?

There's a test for that. `ForgetfulVoter` in `tests/test_raft.py` drops the
"candidate's log must be at least as up-to-date as mine" check from
RequestVote, which is a one-line, very plausible bug. Under chaos it gets
flagged on 9 out of 10 seeds, typically within a few seconds:

```
n4 became leader of term 6 missing applied entry 204
```

The correct implementation came out clean on 900 chaos runs (300 seeds x 3, 5
and 7 nodes, 5% packet loss) before I pushed.

## Layout

```
raftsim/sim.py      event loop + network (delay, loss, partitions)
raftsim/node.py     the Raft node, ~300 lines, follows the paper's figure 2
raftsim/kv.py       state machine with client sessions
raftsim/client.py   retrying client
raftsim/checker.py  safety invariants
raftsim/chaos.py    nemesis, workload and the final verification
```

## What I'd do next

- **pre-vote.** Look at the final term in the 7-node run: 197. A node stuck on
  the small side of a partition keeps bumping its term, and when it rejoins it
  forces the healthy leader to step down. Pre-vote (section 9.6 of Diego
  Ongaro's thesis) fixes that and should cut the long tail on latency.
- log compaction / snapshots, right now the log grows forever
- membership changes (joint consensus is the scary part)
- a real linearizability checker (Knossos/Porcupine style) over the full
  client history instead of the append-token trick
