import asyncio
import hashlib
import json
import logging
import time
import uuid
import redis.asyncio as aioredis
from fastapi import Request
from fastapi.responses import JSONResponse

from src.nodes.base_node import BaseNode
from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class QueueNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, virtual_nodes: int = 150):
        super().__init__(node_id, host, port)
        self.virtual_nodes = virtual_nodes
        self._ring: dict[int, str] = {}
        self._nodes: list[str] = []
        self._redis: aioredis.Redis = None
        self._setup_queue_routes()

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node_to_ring(self, node_id: str):
        self._nodes.append(node_id)
        for i in range(self.virtual_nodes):
            vh = self._hash(f"{node_id}:{i}")
            self._ring[vh] = node_id
        logger.info(f"[{self.node_id}] Added {node_id} to consistent hash ring")

    def get_node_for_key(self, key: str) -> str:
        if not self._ring:
            return self.node_id
        h = self._hash(key)
        sorted_keys = sorted(self._ring.keys())
        for rk in sorted_keys:
            if h <= rk:
                return self._ring[rk]
        return self._ring[sorted_keys[0]]

    def _queue_key(self, topic: str) -> str:
        return f"queue:{topic}"

    def _dlq_key(self, topic: str) -> str:
        return f"dlq:{topic}"

    def _setup_queue_routes(self):
        @self.app.post("/queue/publish")
        async def publish(request: Request):
            data = await request.json()
            topic = data.get("topic")
            message = data.get("message")
            if not topic or message is None:
                return JSONResponse({"error": "topic and message required"}, status_code=400)

            msg_id = str(uuid.uuid4())
            payload = json.dumps({
                "id": msg_id,
                "topic": topic,
                "message": message,
                "producer": data.get("producer", "unknown"),
                "timestamp": time.time(),
                "attempts": 0,
            })

            responsible_node = self.get_node_for_key(topic)
            qkey = self._queue_key(topic)
            await self._redis.lpush(qkey, payload)

            metrics.increment("queue.published")
            logger.info(f"[{self.node_id}] Published to {topic}, node={responsible_node}")
            return JSONResponse({"published": True, "msg_id": msg_id, "topic": topic})

        @self.app.post("/queue/consume")
        async def consume(request: Request):
            data = await request.json()
            topic = data.get("topic")
            consumer_id = data.get("consumer_id", "unknown")
            if not topic:
                return JSONResponse({"error": "topic required"}, status_code=400)

            qkey = self._queue_key(topic)
            raw = await self._redis.rpop(qkey)
            if not raw:
                return JSONResponse({"message": None, "topic": topic})

            with metrics.time_it("queue.consume_latency"):
                msg = json.loads(raw)
                msg["attempts"] += 1
                msg["consumer"] = consumer_id
                msg["consumed_at"] = time.time()

            metrics.increment("queue.consumed")
            return JSONResponse({"message": msg, "topic": topic})

        @self.app.get("/queue/length")
        async def queue_length(topic: str):
            qkey = self._queue_key(topic)
            length = await self._redis.llen(qkey)
            return {"topic": topic, "length": length}

        @self.app.get("/queue/topics")
        async def list_topics():
            keys = await self._redis.keys("queue:*")
            topics = [k.decode().replace("queue:", "") for k in keys]
            return {"topics": topics}

        @self.app.post("/queue/deadletter")
        async def move_to_dlq(request: Request):
            data = await request.json()
            topic = data.get("topic")
            message = data.get("message")
            if not topic or not message:
                return JSONResponse({"error": "topic and message required"}, status_code=400)

            dlq_key = self._dlq_key(topic)
            await self._redis.lpush(dlq_key, json.dumps(message))
            metrics.increment("queue.dlq_messages")
            return JSONResponse({"moved_to_dlq": True})

    async def start(self):
        await super().start()
        self._redis = aioredis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=False,
        )
        self.add_node_to_ring(self.node_id)
        logger.info(f"[{self.node_id}] QueueNode started")