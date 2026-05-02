import asyncio
import logging
import time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.nodes.base_node import BaseNode
from src.consensus.raft import RaftNode
from src.communication.message_passing import Message
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class LockManagerNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, peers: list[str]):
        super().__init__(node_id, host, port)
        self.raft = RaftNode(
            node_id=node_id,
            peers=peers,
            bus=self.bus,
        )
        self._deadlock_graph: dict[str, set] = {}
        self._setup_lock_routes()

    def _setup_lock_routes(self):
        @self.app.post("/lock/acquire")
        async def acquire_lock(request: Request):
            data = await request.json()
            key = data.get("key")
            holder = data.get("holder")
            lock_type = data.get("type", "exclusive")

            if not key or not holder:
                return JSONResponse({"error": "key and holder required"}, status_code=400)

            with metrics.time_it("lock.acquire_latency"):
                if self.raft.role.value != "leader":
                    return JSONResponse({
                        "granted": False,
                        "reason": "not_leader",
                        "leader": self.raft.leader_id
                    })

                if key in self.raft._locks:
                    existing = self.raft._locks[key]
                    if existing["type"] == "exclusive" or lock_type == "exclusive":
                        self._update_deadlock_graph(holder, existing["holder"])
                        if self._detect_deadlock(holder):
                            metrics.increment("lock.deadlocks_detected")
                            return JSONResponse({
                                "granted": False,
                                "reason": "deadlock_detected",
                            })
                        return JSONResponse({
                            "granted": False,
                            "reason": "lock_held",
                            "holder": existing["holder"]
                        })

                success = await self.raft.propose({
                    "type": "acquire_lock",
                    "key": key,
                    "holder": holder,
                    "lock_type": lock_type,
                })
                metrics.increment("lock.acquired" if success else "lock.acquire_failed")
                return JSONResponse({"granted": success, "key": key, "holder": holder})

        @self.app.post("/lock/release")
        async def release_lock(request: Request):
            data = await request.json()
            key = data.get("key")
            holder = data.get("holder")

            if self.raft.role.value != "leader":
                return JSONResponse({"error": "not_leader"}, status_code=400)

            lock = self.raft._locks.get(key)
            if not lock:
                return JSONResponse({"released": False, "reason": "not_held"})
            if lock["holder"] != holder:
                return JSONResponse({"released": False, "reason": "not_owner"})

            await self.raft.propose({"type": "release_lock", "key": key})
            self._clear_deadlock_graph(holder)
            metrics.increment("lock.released")
            return JSONResponse({"released": True, "key": key})

        @self.app.get("/lock/status")
        async def lock_status():
            return {
                "raft_state": self.raft.get_state(),
                "deadlock_graph": {k: list(v) for k, v in self._deadlock_graph.items()},
            }

    def _update_deadlock_graph(self, waiter: str, holder: str):
        if waiter not in self._deadlock_graph:
            self._deadlock_graph[waiter] = set()
        self._deadlock_graph[waiter].add(holder)

    def _clear_deadlock_graph(self, node: str):
        self._deadlock_graph.pop(node, None)
        for waiters in self._deadlock_graph.values():
            waiters.discard(node)

    def _detect_deadlock(self, start: str) -> bool:
        visited = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node in visited:
                return True
            visited.add(node)
            stack.extend(self._deadlock_graph.get(node, []))
        return False

    async def start(self):
        await super().start()
        await self.raft.start()
        logger.info(f"[{self.node_id}] LockManager started")