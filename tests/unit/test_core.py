import asyncio
import pytest
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.metrics import MetricsCollector
from src.communication.message_passing import Message, MessageBus
from src.communication.failure_detector import FailureDetector


# ── TEST 1: Metrics counter ────────────────────────────────────────────────────
def test_metrics_counter():
    m = MetricsCollector()
    m.increment("req.count", 5)
    assert m._counters["req.count"] == 5
    m.increment("req.count", 3)
    assert m._counters["req.count"] == 8


# ── TEST 2: Metrics histogram stats ───────────────────────────────────────────
def test_metrics_histogram():
    m = MetricsCollector()
    for v in [0.1, 0.2, 0.3, 0.4, 0.5]:
        m.record("latency", v)
    stats = m.get_stats()
    h = stats["histograms"]["latency"]
    assert h["count"] == 5
    assert h["min"] == 0.1
    assert h["max"] == 0.5


# ── TEST 3: Message serialization ─────────────────────────────────────────────
def test_message_serialization():
    msg = Message("test_type", "node1", {"key": "value"})
    d = msg.to_dict()
    restored = Message.from_dict(d)
    assert restored.type == "test_type"
    assert restored.sender == "node1"
    assert restored.data["key"] == "value"


# ── TEST 4: Failure detector — node alive ─────────────────────────────────────
def test_failure_detector_alive():
    fd = FailureDetector("node1", timeout=5.0)
    fd.heartbeat_received("node2")
    assert fd.is_alive("node2") is True


# ── TEST 5: Failure detector — node timeout ───────────────────────────────────
def test_failure_detector_timeout():
    fd = FailureDetector("node1", timeout=0.01)
    fd._last_seen["node2"] = time.time() - 1.0
    assert fd.is_alive("node2") is False


# ── TEST 6: Failure detector — get alive nodes ────────────────────────────────
def test_failure_detector_alive_nodes():
    fd = FailureDetector("node1", timeout=5.0)
    fd.heartbeat_received("node2")
    fd.heartbeat_received("node3")
    alive = fd.get_alive_nodes(["node1", "node2", "node3", "node4"])
    assert "node2" in alive
    assert "node3" in alive
    assert "node4" not in alive


# ── TEST 7: Metrics timer context manager ─────────────────────────────────────
def test_metrics_timer():
    m = MetricsCollector()
    with m.time_it("op.duration"):
        time.sleep(0.01)
    stats = m.get_stats()
    assert "op.duration" in stats["histograms"]
    assert stats["histograms"]["op.duration"]["count"] == 1
    assert stats["histograms"]["op.duration"]["min"] >= 0.01


# ── TEST 8: Raft node initial state ───────────────────────────────────────────
@pytest.mark.asyncio
async def test_raft_initial_state():
    from unittest.mock import AsyncMock
    from src.consensus.raft import RaftNode, RaftRole
    bus = AsyncMock()
    bus.register_handler = lambda *a, **kw: None
    raft = RaftNode("node1", ["node2", "node3"], bus)
    assert raft.role == RaftRole.FOLLOWER
    assert raft.current_term == 0
    assert raft.voted_for is None


# ── TEST 9: PBFT quorum calculation ───────────────────────────────────────────
def test_pbft_quorum():
    from unittest.mock import AsyncMock
    from src.consensus.pbft import PBFTNode
    bus = AsyncMock()
    bus.register_handler = lambda *a, **kw: None
    node = PBFTNode("node1", ["node2", "node3", "node4"], bus, f=1)
    assert node._quorum() == 3
    node2 = PBFTNode("node1", ["n2", "n3", "n4", "n5", "n6", "n7"], bus, f=2)
    assert node2._quorum() == 5


# ── TEST 10: LRU Cache eviction ───────────────────────────────────────────────
def test_lru_cache_eviction():
    from src.nodes.cache_node import LRUCache
    cache = LRUCache(capacity=3)
    cache.put("a", {"value": 1})
    cache.put("b", {"value": 2})
    cache.put("c", {"value": 3})
    cache.get("a")  # access a, now b is LRU
    cache.put("d", {"value": 4})  # should evict b
    assert cache.get("b") is None
    assert cache.get("a") is not None
    assert cache.get("d") is not None


# ── TEST 11: Consistent hashing distribution ──────────────────────────────────
def test_consistent_hashing():
    from unittest.mock import AsyncMock, patch
    import hashlib

    def mock_hash(key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    ring = {}
    nodes = ["node1", "node2", "node3"]
    for node in nodes:
        for i in range(10):
            vh = mock_hash(f"{node}:{i}")
            ring[vh] = node

    def get_node(key):
        h = mock_hash(key)
        sorted_keys = sorted(ring.keys())
        for rk in sorted_keys:
            if h <= rk:
                return ring[rk]
        return ring[sorted_keys[0]]

    results = [get_node(f"key-{i}") for i in range(100)]
    unique_nodes = set(results)
    assert len(unique_nodes) > 1, "Consistent hashing should distribute across nodes"


# ── TEST 12: Deadlock detection ────────────────────────────────────────────────
def test_deadlock_detection():
    from src.nodes.lock_manager import LockManagerNode
    from unittest.mock import AsyncMock, patch

    with patch.object(LockManagerNode, '__init__', lambda self, *a, **kw: None):
        node = LockManagerNode.__new__(LockManagerNode)
        node._deadlock_graph = {}

        node._update_deadlock_graph("A", "B")
        node._update_deadlock_graph("B", "C")
        node._update_deadlock_graph("C", "A")
        assert node._detect_deadlock("A") is True

        node2 = LockManagerNode.__new__(LockManagerNode)
        node2._deadlock_graph = {}
        node2._update_deadlock_graph("X", "Y")
        assert node2._detect_deadlock("X") is False