import asyncio
import logging
import time
from collections import OrderedDict
from enum import Enum
from fastapi import Request
from fastapi.responses import JSONResponse

from src.nodes.base_node import BaseNode
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class MESIState(Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: OrderedDict[str, dict] = OrderedDict()

    def get(self, key: str) -> dict | None:
        if key not in self._cache:
            return None
        self._cache.move_to_end(key)
        return self._cache[key]

    def put(self, key: str, value: dict):
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = value
        if len(self._cache) > self.capacity:
            evicted = self._cache.popitem(last=False)
            metrics.increment("cache.evictions")
            logger.debug(f"Evicted: {evicted[0]}")

    def invalidate(self, key: str):
        self._cache.pop(key, None)

    def __len__(self):
        return len(self._cache)


class CacheNode(BaseNode):
    """
    Cache dengan protokol MESI (Modified, Exclusive, Shared, Invalid).
    Mendukung cache invalidation dan update propagation antar node.
    """

    def __init__(self, node_id: str, host: str, port: int,
                 cache_size: int = 1000):
        super().__init__(node_id, host, port)
        self._cache = LRUCache(cache_size)
        self._mesi_states: dict[str, MESIState] = {}
        self._setup_cache_routes()
        self.bus.register_handler("cache_invalidate", self._handle_invalidate)
        self.bus.register_handler("cache_update", self._handle_update)
        self.bus.register_handler("cache_fetch", self._handle_fetch)

    def _get_state(self, key: str) -> MESIState:
        return self._mesi_states.get(key, MESIState.INVALID)

    def _set_state(self, key: str, state: MESIState):
        self._mesi_states[key] = state

    def _setup_cache_routes(self):
        @self.app.get("/cache/{key}")
        async def cache_get(key: str):
            with metrics.time_it("cache.read_latency"):
                state = self._get_state(key)
                if state == MESIState.INVALID:
                    metrics.increment("cache.miss")
                    return JSONResponse({"hit": False, "key": key, "state": state.value})

                entry = self._cache.get(key)
                if entry is None:
                    self._set_state(key, MESIState.INVALID)
                    metrics.increment("cache.miss")
                    return JSONResponse({"hit": False, "key": key})

                metrics.increment("cache.hit")
                return JSONResponse({
                    "hit": True,
                    "key": key,
                    "value": entry["value"],
                    "state": state.value,
                    "cached_at": entry.get("cached_at"),
                })

        @self.app.put("/cache/{key}")
        async def cache_put(key: str, request: Request):
            data = await request.json()
            value = data.get("value")
            if value is None:
                return JSONResponse({"error": "value required"}, status_code=400)

            with metrics.time_it("cache.write_latency"):
                self._cache.put(key, {"value": value, "cached_at": time.time()})
                old_state = self._get_state(key)

                if old_state == MESIState.INVALID:
                    self._set_state(key, MESIState.EXCLUSIVE)
                else:
                    self._set_state(key, MESIState.MODIFIED)

                # Broadcast invalidate ke peer nodes
                from src.communication.message_passing import Message
                inv_msg = Message("cache_invalidate", self.node_id, {
                    "key": key,
                    "invalidated_by": self.node_id,
                })
                asyncio.create_task(self.bus.broadcast(self.peers, inv_msg))

            metrics.increment("cache.writes")
            return JSONResponse({
                "stored": True,
                "key": key,
                "state": self._get_state(key).value,
            })

        @self.app.delete("/cache/{key}")
        async def cache_delete(key: str):
            self._cache.invalidate(key)
            self._set_state(key, MESIState.INVALID)
            from src.communication.message_passing import Message
            inv_msg = Message("cache_invalidate", self.node_id, {
                "key": key,
                "invalidated_by": self.node_id,
            })
            asyncio.create_task(self.bus.broadcast(self.peers, inv_msg))
            return JSONResponse({"deleted": True, "key": key})

        @self.app.get("/cache/stats/summary")
        async def cache_stats():
            hit = metrics._counters.get("cache.hit", 0)
            miss = metrics._counters.get("cache.miss", 0)
            total = hit + miss
            return {
                "node_id": self.node_id,
                "size": len(self._cache),
                "hit_count": hit,
                "miss_count": miss,
                "hit_rate": round(hit / total, 4) if total > 0 else 0,
                "mesi_states": {k: v.value for k, v in self._mesi_states.items()},
            }

    async def _handle_invalidate(self, msg):
        key = msg.data["key"]
        self._cache.invalidate(key)
        self._set_state(key, MESIState.INVALID)
        metrics.increment("cache.invalidations_received")
        logger.info(f"[{self.node_id}] Cache invalidated: {key}")
        return {"ok": True}

    async def _handle_update(self, msg):
        key = msg.data["key"]
        value = msg.data["value"]
        self._cache.put(key, {"value": value, "cached_at": time.time()})
        self._set_state(key, MESIState.SHARED)
        return {"ok": True}

    async def _handle_fetch(self, msg):
        key = msg.data["key"]
        entry = self._cache.get(key)
        return {"found": entry is not None, "value": entry}

    async def start(self):
        await super().start()
        logger.info(f"[{self.node_id}] CacheNode started (MESI protocol)")