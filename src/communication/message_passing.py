import asyncio
import json
import logging
import time
from typing import Any
import aiohttp

logger = logging.getLogger(__name__)


class Message:
    def __init__(self, msg_type: str, sender: str, data: dict, msg_id: str = None):
        self.type = msg_type
        self.sender = sender
        self.data = data
        self.msg_id = msg_id or f"{sender}-{time.time_ns()}"
        self.timestamp = time.time()

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "sender": self.sender,
            "data": self.data,
            "msg_id": self.msg_id,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        m = cls(d["type"], d["sender"], d["data"], d.get("msg_id"))
        m.timestamp = d.get("timestamp", time.time())
        return m


class MessageBus:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._session: aiohttp.ClientSession = None
        self._handlers: dict[str, list] = {}
        self._timeout = aiohttp.ClientTimeout(total=2.0)

    async def start(self):
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        logger.info(f"[{self.node_id}] MessageBus started")

    async def stop(self):
        if self._session:
            await self._session.close()

    def register_handler(self, msg_type: str, handler):
        if msg_type not in self._handlers:
            self._handlers[msg_type] = []
        self._handlers[msg_type].append(handler)

    async def handle_incoming(self, message_dict: dict) -> Any:
        msg = Message.from_dict(message_dict)
        handlers = self._handlers.get(msg.type, [])
        results = []
        for handler in handlers:
            try:
                result = await handler(msg)
                results.append(result)
            except Exception as e:
                logger.error(f"Handler error for {msg.type}: {e}")
        return results[0] if len(results) == 1 else results

    async def send(self, target_url: str, message: Message) -> dict | None:
        if not self._session:
            return None
        try:
            async with self._session.post(
                f"{target_url}/message",
                json=message.to_dict(),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
        except asyncio.TimeoutError:
            logger.warning(f"[{self.node_id}] Timeout sending to {target_url}")
        except Exception as e:
            logger.debug(f"[{self.node_id}] Send error to {target_url}: {e}")
        return None

    async def broadcast(self, peers: list[str], message: Message) -> list:
        tasks = [self.send(url, message) for url in peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if r is not None and not isinstance(r, Exception)]