import asyncio
import logging
import time
from collections import defaultdict

logger = logging.getLogger(__name__)


class FailureDetector:
    """
    Heartbeat-based failure detector.
    Node dianggap gagal jika tidak ada heartbeat dalam threshold tertentu.
    """

    def __init__(self, node_id: str, timeout: float = 5.0):
        self.node_id = node_id
        self.timeout = timeout
        self._last_seen: dict[str, float] = {}
        self._suspected: set[str] = set()
        self._running = False
        self._callbacks: list = []

    def register_failure_callback(self, cb):
        self._callbacks.append(cb)

    def heartbeat_received(self, from_node: str):
        self._last_seen[from_node] = time.time()
        if from_node in self._suspected:
            self._suspected.discard(from_node)
            logger.info(f"[{self.node_id}] Node {from_node} recovered")

    def is_alive(self, node_id: str) -> bool:
        if node_id not in self._last_seen:
            return False
        return (time.time() - self._last_seen[node_id]) < self.timeout

    def get_alive_nodes(self, all_nodes: list[str]) -> list[str]:
        return [n for n in all_nodes if self.is_alive(n) or n == self.node_id]

    async def start(self):
        self._running = True
        asyncio.create_task(self._check_loop())

    async def stop(self):
        self._running = False

    async def _check_loop(self):
        while self._running:
            now = time.time()
            for node_id, last in list(self._last_seen.items()):
                if now - last > self.timeout and node_id not in self._suspected:
                    self._suspected.add(node_id)
                    logger.warning(f"[{self.node_id}] Suspected failure: {node_id}")
                    for cb in self._callbacks:
                        try:
                            await cb(node_id)
                        except Exception as e:
                            logger.error(f"Failure callback error: {e}")
            await asyncio.sleep(1.0)