class KVStore:
    """The replicated state machine. Tiny on purpose; Raft doesn't care what it is.

    Sessions give exactly-once semantics: a client retrying a request after a
    leader change will often get it appended twice, and the second copy must
    be a no-op that returns the original answer.
    """

    def __init__(self):
        self.data = {}
        self.sessions = {}  # client -> (last seq applied, its result)

    def apply(self, cmd):
        if cmd is None:
            return None
        client, seq, op = cmd
        last = self.sessions.get(client)
        if last is not None and seq <= last[0]:
            return last[1] if seq == last[0] else None
        result = self._execute(op)
        self.sessions[client] = (seq, result)
        return result

    def _execute(self, op):
        kind, key = op[0], op[1]
        if kind == "get":
            return self.data.get(key)
        if kind == "put":
            self.data[key] = op[2]
            return op[2]
        if kind == "append":
            self.data[key] = self.data.get(key, "") + op[2]
            return self.data[key]
        if kind == "delete":
            return self.data.pop(key, None)
        raise ValueError(f"unknown op {kind!r}")
