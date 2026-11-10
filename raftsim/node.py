from .kv import KVStore
from .messages import (
    AppendEntries, AppendReply, ClientReply, ClientRequest, Entry,
    RequestVote, VoteReply,
)

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class RaftNode:
    def __init__(self, nid, cluster_ids, sim, net, cfg, observer=None):
        self.id = nid
        self.peers = [p for p in cluster_ids if p != nid]
        self.quorum = len(cluster_ids) // 2 + 1
        self.sim, self.net, self.cfg = sim, net, cfg
        self.observer = observer

        # "persistent" state - survives crash() / restart()
        self.term = 0
        self.voted_for = None
        self.log = [Entry(0, None)]  # sentinel so the log is 1-indexed like the paper

        self.alive = True
        self._epoch = 0          # bumped on crash so stale timers die quietly
        self._election_token = 0
        self._reset_volatile()
        net.register(nid, self.receive)
        self._arm_election_timer()

    def _reset_volatile(self):
        self.role = FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.kv = KVStore()
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self.waiting = set()  # (client, seq) we owe a reply to

    @property
    def last_index(self):
        return len(self.log) - 1

    @property
    def last_term(self):
        return self.log[-1].term

    def __repr__(self):
        return f"<{self.id} {self.role} t={self.term} log={self.last_index} commit={self.commit_index}>"

    # ---- lifecycle -------------------------------------------------------

    def crash(self):
        self.alive = False
        self._epoch += 1

    def restart(self):
        self.alive = True
        self._epoch += 1
        # state machine is rebuilt by re-applying the log once commit_index catches up
        self._reset_volatile()
        self._arm_election_timer()

    def _later(self, delay, fn):
        epoch = self._epoch

        def fire():
            if self.alive and self._epoch == epoch:
                fn()

        self.sim.after(delay, fire)

    def _send(self, dst, msg):
        self.net.send(self.id, dst, msg)

    # ---- timers ----------------------------------------------------------

    def _arm_election_timer(self):
        self._election_token += 1
        token = self._election_token
        timeout = self.sim.rng.randint(*self.cfg.election_timeout)

        def check():
            if token == self._election_token and self.role != LEADER:
                self._start_election()

        self._later(timeout, check)

    def _heartbeat(self, term):
        if self.role != LEADER or self.term != term:
            return
        for p in self.peers:
            self._replicate(p)
        self._later(self.cfg.heartbeat, lambda: self._heartbeat(term))

    # ---- roles -----------------------------------------------------------

    def _start_election(self):
        self.term += 1
        self.role = CANDIDATE
        self.voted_for = self.id
        self.votes = {self.id}
        self.leader_id = None
        self._arm_election_timer()
        if len(self.votes) >= self.quorum:  # single-node cluster
            self._become_leader()
            return
        msg = RequestVote(self.term, self.id, self.last_index, self.last_term)
        for p in self.peers:
            self._send(p, msg)

    def _become_leader(self):
        self.role = LEADER
        self.leader_id = self.id
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # A leader can't count replicas of old-term entries towards commit
        # (figure 8), so it commits a no-op of its own term to drag them along.
        self.log.append(Entry(self.term, None))
        if self.observer:
            self.observer.on_leader(self)
        self._heartbeat(self.term)
        self._advance_commit()

    def _step_down(self, term):
        if term > self.term:
            self.term = term
            self.voted_for = None
        was_follower = self.role == FOLLOWER
        self.role = FOLLOWER
        self.votes = set()
        self.waiting.clear()  # clients will time out and retry against the new leader
        if not was_follower:
            self._arm_election_timer()

    # ---- message handling ------------------------------------------------

    def receive(self, msg):
        if not self.alive:
            return
        handler = {
            RequestVote: self._on_request_vote,
            VoteReply: self._on_vote_reply,
            AppendEntries: self._on_append,
            AppendReply: self._on_append_reply,
            ClientRequest: self._on_client,
        }[type(msg)]
        handler(msg)

    def _on_request_vote(self, m):
        if m.term > self.term:
            self._step_down(m.term)
        up_to_date = (m.last_log_term, m.last_log_index) >= (self.last_term, self.last_index)
        grant = (
            m.term == self.term
            and self.voted_for in (None, m.candidate)
            and up_to_date
        )
        if grant:
            self.voted_for = m.candidate
            self._arm_election_timer()
        self._send(m.candidate, VoteReply(self.term, self.id, grant))

    def _on_vote_reply(self, m):
        if m.term > self.term:
            self._step_down(m.term)
            return
        if self.role != CANDIDATE or m.term != self.term or not m.granted:
            return
        self.votes.add(m.voter)
        if len(self.votes) >= self.quorum:
            self._become_leader()

    def _on_append(self, m):
        if m.term < self.term:
            self._send(m.leader, AppendReply(self.term, self.id, False))
            return
        if m.term > self.term or self.role != FOLLOWER:
            self._step_down(m.term)
        self.leader_id = m.leader
        self._arm_election_timer()

        if m.prev_index > self.last_index:
            self._send(m.leader, AppendReply(self.term, self.id, False,
                                             conflict_index=self.last_index + 1))
            return
        if self.log[m.prev_index].term != m.prev_term:
            bad_term = self.log[m.prev_index].term
            first = m.prev_index
            while first > 1 and self.log[first - 1].term == bad_term:
                first -= 1
            self._send(m.leader, AppendReply(self.term, self.id, False,
                                             conflict_index=first, conflict_term=bad_term))
            return

        # Only truncate on an actual conflict. A delayed, shorter AppendEntries
        # must not chop off entries a newer one already gave us.
        idx = m.prev_index
        for entry in m.entries:
            idx += 1
            if idx <= self.last_index:
                if self.log[idx].term == entry.term:
                    continue
                del self.log[idx:]
            self.log.append(entry)

        match = m.prev_index + len(m.entries)
        new_commit = min(m.leader_commit, match)
        if new_commit > self.commit_index:
            self.commit_index = new_commit
            self._apply()
        self._send(m.leader, AppendReply(self.term, self.id, True, match_index=match))

    def _on_append_reply(self, m):
        if m.term > self.term:
            self._step_down(m.term)
            return
        if self.role != LEADER or m.term != self.term:
            return
        p = m.follower
        if m.success:
            if m.match_index > self.match_index[p]:
                self.match_index[p] = m.match_index
                self.next_index[p] = m.match_index + 1
                self._advance_commit()
            if self.next_index[p] <= self.last_index:
                self._replicate(p)
            return

        ni = m.conflict_index
        if m.conflict_term is not None:
            # if we have entries from that term, jump just past our last one
            for i in range(self.last_index, 0, -1):
                t = self.log[i].term
                if t == m.conflict_term:
                    ni = i + 1
                    break
                if t < m.conflict_term:
                    break
        self.next_index[p] = max(self.match_index[p] + 1, min(ni, self.last_index + 1))
        self._replicate(p)

    def _on_client(self, m):
        if self.role != LEADER:
            self._send(m.client, ClientReply(m.seq, False, leader_hint=self.leader_id))
            return
        last = self.kv.sessions.get(m.client)
        if last is not None and last[0] == m.seq:
            self._send(m.client, ClientReply(m.seq, True, last[1], self.id))
            return
        key = (m.client, m.seq)
        if key in self.waiting:
            return
        self.waiting.add(key)
        self.log.append(Entry(self.term, (m.client, m.seq, m.op)))
        for p in self.peers:
            self._replicate(p)
        self._advance_commit()

    # ---- replication -----------------------------------------------------

    def _replicate(self, peer):
        ni = self.next_index[peer]
        prev = ni - 1
        entries = tuple(self.log[ni:ni + self.cfg.max_batch])
        self._send(peer, AppendEntries(self.term, self.id, prev, self.log[prev].term,
                                       entries, self.commit_index))

    def _advance_commit(self):
        for n in range(self.last_index, self.commit_index, -1):
            if self.log[n].term != self.term:
                break  # terms only go down from here
            count = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
            if count >= self.quorum:
                self.commit_index = n
                self._apply()
                break

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            result = self.kv.apply(entry.cmd)
            if self.observer:
                self.observer.on_apply(self, self.last_applied, entry)
            if entry.cmd is not None and self.role == LEADER:
                key = entry.cmd[:2]
                if key in self.waiting:
                    self.waiting.discard(key)
                    self._send(key[0], ClientReply(key[1], True, result, self.id))
