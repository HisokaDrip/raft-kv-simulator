class SafetyViolation(AssertionError):
    pass


class SafetyChecker:
    """Watches the cluster and checks the Raft safety properties (figure 3) as it runs.

    - election safety: at most one leader per term
    - state machine safety: no two nodes ever apply different entries at the same index
    - leader completeness: a new leader already holds every entry applied anywhere
    - log matching: checked on demand, it's quadratic-ish
    """

    def __init__(self, strict=True):
        self.strict = strict
        self.leaders = {}   # term -> node id
        self.applied = {}   # index -> entry
        self.violations = []
        self.listeners = []

    def _fail(self, msg):
        self.violations.append(msg)
        if self.strict:
            raise SafetyViolation(msg)

    def on_leader(self, node):
        prev = self.leaders.setdefault(node.term, node.id)
        if prev != node.id:
            self._fail(f"two leaders in term {node.term}: {prev} and {node.id}")
        for idx, entry in self.applied.items():
            if idx > node.last_index or node.log[idx] != entry:
                self._fail(f"{node.id} became leader of term {node.term} missing applied entry {idx}")
                break
        for fn in self.listeners:
            fn("leader", node)

    def on_apply(self, node, index, entry):
        prev = self.applied.setdefault(index, entry)
        if prev != entry:
            self._fail(f"{node.id} applied {entry} at {index}, someone else applied {prev}")

    def check_log_matching(self, nodes):
        nodes = list(nodes)
        for i, a in enumerate(nodes):
            for b in nodes[i + 1:]:
                top = min(a.last_index, b.last_index)
                # find the highest index where the terms agree; everything below must be identical
                while top > 0 and a.log[top].term != b.log[top].term:
                    top -= 1
                if a.log[:top + 1] != b.log[:top + 1]:
                    self._fail(f"log matching broken between {a.id} and {b.id} below {top}")
