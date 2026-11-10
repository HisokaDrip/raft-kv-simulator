from collections import deque

from .messages import ClientReply, ClientRequest


class Client:
    """A client with one request in flight at a time, retrying until it sticks.

    It doesn't know who the leader is; it guesses, follows redirects, and
    times out onto a random server when the guess goes quiet.
    """

    def __init__(self, cid, sim, net, servers, timeout=400):
        self.id = cid
        self.sim, self.net = sim, net
        self.servers = list(servers)
        self.timeout = timeout
        self.guess = self.servers[0]
        self.seq = 0
        self.attempt = 0
        self.inflight = None   # (seq, op, callback, started_at)
        self.queue = deque()
        self.done = []         # (op, result, latency_ms)
        self.retries = 0
        net.register(cid, self.receive)

    @property
    def idle(self):
        return self.inflight is None and not self.queue

    def submit(self, op, callback=None):
        self.queue.append((op, callback))
        if self.inflight is None:
            self._next()

    def _next(self):
        if not self.queue:
            return
        op, cb = self.queue.popleft()
        self.seq += 1
        self.inflight = (self.seq, op, cb, self.sim.now)
        self._send()

    def _send(self):
        seq, op = self.inflight[0], self.inflight[1]
        self.attempt += 1
        attempt = self.attempt
        self.net.send(self.id, self.guess, ClientRequest(self.id, seq, op))
        self.sim.after(self.timeout, lambda: self._maybe_retry(seq, attempt, rotate=True))

    def _maybe_retry(self, seq, attempt, rotate):
        if self.inflight is None or self.inflight[0] != seq or attempt != self.attempt:
            return
        if rotate and len(self.servers) > 1:
            self.guess = self.sim.rng.choice([s for s in self.servers if s != self.guess])
        self.retries += 1
        self._send()

    def receive(self, m: ClientReply):
        if self.inflight is None or m.seq != self.inflight[0]:
            return  # late reply for something we already finished
        if not m.ok:
            attempt = self.attempt
            if m.leader_hint and m.leader_hint != self.guess:
                self.guess = m.leader_hint
                self.sim.after(1, lambda: self._maybe_retry(m.seq, attempt, rotate=False))
            else:
                # nobody knows who's in charge yet, back off a little
                self.sim.after(50, lambda: self._maybe_retry(m.seq, attempt, rotate=True))
            return
        seq, op, cb, started = self.inflight
        self.guess = m.leader_hint or self.guess
        self.inflight = None
        self.done.append((op, m.result, self.sim.now - started))
        if cb:
            cb(m.result)
        self._next()
