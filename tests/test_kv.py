from raftsim.kv import KVStore


def test_basic_ops():
    kv = KVStore()
    assert kv.apply(("c1", 1, ("put", "a", "1"))) == "1"
    assert kv.apply(("c1", 2, ("append", "a", "2"))) == "12"
    assert kv.apply(("c1", 3, ("get", "a"))) == "12"
    assert kv.apply(("c1", 4, ("delete", "a"))) == "12"
    assert kv.apply(("c1", 5, ("get", "a"))) is None


def test_duplicate_request_is_applied_once():
    kv = KVStore()
    kv.apply(("c1", 1, ("append", "x", "a")))
    # same request shows up again after a leader change
    assert kv.apply(("c1", 1, ("append", "x", "a"))) == "a"
    assert kv.data["x"] == "a"


def test_noop_entries_are_ignored():
    kv = KVStore()
    assert kv.apply(None) is None
    assert kv.data == {}
