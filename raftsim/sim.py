"""Deterministic discrete-event simulator and a lossy, partitionable network.

Everything runs on one thread with a fake clock, so a seed fully determines
a run. That's the whole trick: when the chaos test finds a bug you can
replay the exact same interleaving and stare at it.
"""
import heapq
import random


class Simulator:
    def __init__(self, seed=0):
        self.now = 0
        self.rng = random.Random(seed)
        self._queue = []
        self._seq = 0  # tie-breaker so same-time events stay FIFO

    def at(self, when, fn):
        self._seq += 1
        heapq.heappush(self._queue, (when, self._seq, fn))

    def after(self, delay, fn):
        self.at(self.now + delay, fn)

    def run_until(self, t):
        while self._queue and self._queue[0][0] <= t:
            when, _, fn = heapq.heappop(self._queue)
            self.now = when
            fn()
        self.now = max(self.now, t)

    def run_for(self, ms):
        self.run_until(self.now + ms)

    def run_while(self, keep_going, limit_ms, step=10):
        """Advance in small steps while keep_going() holds. Returns True if it stopped on its own."""
        deadline = self.now + limit_ms
        while keep_going():
            if self.now >= deadline:
                return False
            self.run_for(step)
        return True


class Network:
    def __init__(self, sim, delay=(5, 25), drop=0.0):
        self.sim = sim
        self.delay = delay
        self.drop = drop
        self.endpoints = {}
        self.groups = None  # eid -> partition id, None means fully connected
        self.sent = self.dropped = self.delivered = 0

    def register(self, eid, handler):
        self.endpoints[eid] = handler

    def can_talk(self, a, b):
        if self.groups is None:
            return True
        ga, gb = self.groups.get(a), self.groups.get(b)
        # anything not mentioned in the partition (clients) can reach everyone
        return ga is None or gb is None or ga == gb

    def send(self, src, dst, msg):
        self.sent += 1
        if not self.can_talk(src, dst) or self.sim.rng.random() < self.drop:
            self.dropped += 1
            return
        delay = self.sim.rng.randint(*self.delay)

        def deliver():
            # a partition can show up while the packet is in flight
            if not self.can_talk(src, dst):
                self.dropped += 1
                return
            self.delivered += 1
            self.endpoints[dst](msg)

        self.sim.after(delay, deliver)

    def partition(self, *groups):
        self.groups = {eid: i for i, g in enumerate(groups) for eid in g}

    def heal(self):
        self.groups = None
