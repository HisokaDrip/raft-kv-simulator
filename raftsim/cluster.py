from dataclasses import dataclass

from .checker import SafetyChecker
from .client import Client
from .node import LEADER, RaftNode
from .sim import Network, Simulator


@dataclass
class Config:
    election_timeout: tuple = (150, 300)
    heartbeat: int = 50
    max_batch: int = 64
    delay: tuple = (5, 25)
    drop: float = 0.0


class Cluster:
    def __init__(self, n=5, seed=0, config=None, strict=True, node_cls=RaftNode):
        self.cfg = config or Config()
        self.sim = Simulator(seed)
        self.net = Network(self.sim, self.cfg.delay, self.cfg.drop)
        self.checker = SafetyChecker(strict=strict)
        self.ids = [f"n{i + 1}" for i in range(n)]
        self.nodes = {
            nid: node_cls(nid, self.ids, self.sim, self.net, self.cfg, self.checker)
            for nid in self.ids
        }
        self.clients = []
        self.timeline = []
        self.checker.listeners.append(
            lambda kind, node: self.note(f"{node.id} elected leader for term {node.term}")
        )

    def note(self, text):
        self.timeline.append((self.sim.now, text))

    def client(self, timeout=400):
        c = Client(f"c{len(self.clients) + 1}", self.sim, self.net, self.ids, timeout)
        self.clients.append(c)
        return c

    def alive(self):
        return [n for n in self.nodes.values() if n.alive]

    def leader(self):
        leaders = [n for n in self.alive() if n.role == LEADER]
        return max(leaders, key=lambda n: n.term) if leaders else None

    def run(self, ms):
        self.sim.run_for(ms)

    def wait_for_leader(self, limit_ms=3000):
        self.sim.run_while(lambda: self.leader() is None, limit_ms)
        return self.leader()

    def wait_idle(self, limit_ms=10000):
        return self.sim.run_while(lambda: not all(c.idle for c in self.clients), limit_ms)

    def crash(self, nid):
        self.nodes[nid].crash()
        self.note(f"{nid} crashed")

    def restart(self, nid):
        self.nodes[nid].restart()
        self.note(f"{nid} restarted")

    def partition(self, *groups):
        self.net.partition(*groups)
        self.note("partition " + " | ".join(",".join(g) for g in groups))

    def heal(self):
        self.net.heal()
        self.note("network healed")

    def converged(self):
        live = self.alive()
        top = max(n.commit_index for n in live)
        return all(n.last_applied == top for n in live) and \
            all(n.kv.data == live[0].kv.data for n in live)
