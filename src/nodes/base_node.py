import asyncio
import logging
import time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

from src.communication.message_passing import MessageBus, Message
from src.communication.failure_detector import FailureDetector
from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class BaseNode:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers: list[str] = []
        self.start_time = time.time()

        self.bus = MessageBus(node_id)
        self.failure_detector = FailureDetector(node_id)

        self.app = FastAPI(title=f"Node {node_id}")
        self._setup_routes()

    def _setup_routes(self):
        @self.app.post("/message")
        async def receive_message(request: Request):
            data = await request.json()
            result = await self.bus.handle_incoming(data)
            return JSONResponse(content={"ok": True, "result": result})

        @self.app.get("/health")
        async def health():
            return {
                "node_id": self.node_id,
                "status": "ok",
                "uptime": round(time.time() - self.start_time, 2),
            }

        @self.app.get("/metrics")
        async def get_metrics():
            return metrics.get_stats()

        @self.app.post("/heartbeat")
        async def heartbeat(request: Request):
            data = await request.json()
            from_node = data.get("from", data.get("sender", "unknown"))
            self.failure_detector.heartbeat_received(from_node)
            return {"ok": True}

    def set_peers(self, peer_urls: list[str]):
        self.peers = peer_urls

    async def send_heartbeats(self):
        while True:
            for peer in self.peers:
                try:
                    async with __import__("httpx").AsyncClient(timeout=1.0) as client:
                        await client.post(
                            f"{peer}/heartbeat",
                            json={"from": self.node_id}
                        )
                except Exception:
                    pass
            await asyncio.sleep(1.0)

    async def start(self):
        await self.bus.start()
        await self.failure_detector.start()
        asyncio.create_task(self.send_heartbeats())
        logger.info(f"[{self.node_id}] BaseNode started on {self.host}:{self.port}")

    async def stop(self):
        await self.bus.stop()
        await self.failure_detector.stop()