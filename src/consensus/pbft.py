import asyncio
import hashlib
import json
import logging
import time
from enum import Enum
from dataclasses import dataclass, field
from fastapi import Request
from fastapi.responses import JSONResponse

from src.communication.message_passing import MessageBus, Message
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class PBFTPhase(Enum):
    IDLE = "idle"
    PRE_PREPARE = "pre-prepare"
    PREPARE = "prepare"
    COMMIT = "commit"
    REPLY = "reply"


@dataclass
class PBFTRequest:
    client_id: str
    operation: dict
    timestamp: float
    digest: str = ""

    def __post_init__(self):
        content = json.dumps(
            {"op": self.operation, "ts": self.timestamp}, sort_keys=True
        )
        self.digest = hashlib.sha256(content.encode()).hexdigest()[:16]


class PBFTNode:
    """
    Practical Byzantine Fault Tolerance (PBFT) Implementation.
    Toleransi hingga f = (n-1)/3 faulty nodes.
    Phases: Pre-prepare -> Prepare -> Commit -> Reply
    """

    def __init__(self, node_id: str, peers: list[str],
                 bus: MessageBus, f: int = 1):
        self.node_id = node_id
        self.peers = peers  # full URLs: ["http://pbft-node2:8032", ...]
        self.bus = bus
        self.f = f
        self.n = len(peers) + 1
        self.is_primary = False
        self.view = 0
        self.sequence = 0
        self.primary_id: str | None = None
        self.primary_url: str | None = None

        # Message logs
        self._pre_prepare_log: dict[int, dict] = {}
        self._prepare_log: dict[int, set] = {}
        self._commit_log: dict[int, set] = {}
        self._executed: set[int] = set()
        self._replies: dict[str, dict] = {}

        # Register handlers
        self.bus.register_handler("pbft_request", self._handle_client_request)
        self.bus.register_handler("pbft_pre_prepare", self._handle_pre_prepare)
        self.bus.register_handler("pbft_prepare", self._handle_prepare)
        self.bus.register_handler("pbft_commit", self._handle_commit)
        self.bus.register_handler("pbft_view_change", self._handle_view_change)
        self.bus.register_handler("pbft_reply", self._handle_reply)

    def set_primary(self, is_primary: bool, primary_id: str):
        self.is_primary = is_primary
        self.primary_id = primary_id
        # Cari URL primary dari peers
        for url in self.peers:
            host = url.split("//")[-1].split(":")[0]
            if host == primary_id:
                self.primary_url = url
                break
        if is_primary:
            self.primary_url = self._self_url()
        logger.info(
            f"[{self.node_id}] PBFT primary={is_primary}, "
            f"primary_id={primary_id}, primary_url={self.primary_url}"
        )

    def _self_url(self) -> str:
        from src.utils.config import config
        return f"http://{self.node_id}:{config.NODE_PORT}"

    def _quorum(self) -> int:
        return 2 * self.f + 1

    async def submit_request(self, client_id: str, operation: dict) -> dict:
        req = PBFTRequest(
            client_id=client_id,
            operation=operation,
            timestamp=time.time()
        )
        msg = Message("pbft_request", self.node_id, {
            "client_id": client_id,
            "operation": operation,
            "timestamp": req.timestamp,
            "digest": req.digest,
            "reply_url": self._self_url(),
        })

        if self.is_primary:
            await self._handle_client_request(msg)
        else:
            target = self.primary_url or (self.peers[0] if self.peers else None)
            if target:
                await self.bus.send(target, msg)

        # Tunggu reply masuk ke _replies
        for _ in range(100):
            if req.digest in self._replies:
                return self._replies[req.digest]
            await asyncio.sleep(0.1)
        return {"status": "timeout", "digest": req.digest}

    async def _handle_client_request(self, msg: Message) -> dict:
        if not self.is_primary:
            target = self.primary_url
            if target:
                await self.bus.send(target, msg)
            return {"forwarded": True}

        self.sequence += 1
        seq = self.sequence
        data = msg.data

        self._pre_prepare_log[seq] = {
            "view": self.view,
            "seq": seq,
            "digest": data["digest"],
            "operation": data["operation"],
            "client_id": data.get("client_id", ""),
            "reply_url": data.get("reply_url", ""),
        }
        # Tambahkan diri sendiri ke prepare log
        self._prepare_log.setdefault(seq, set())
        self._prepare_log[seq].add(self.node_id)

        pp_msg = Message("pbft_pre_prepare", self.node_id, {
            "view": self.view,
            "seq": seq,
            "digest": data["digest"],
            "operation": data["operation"],
            "client_id": data.get("client_id", ""),
            "reply_url": data.get("reply_url", ""),
            "primary_url": self._self_url(),
        })
        await self.bus.broadcast(self.peers, pp_msg)
        metrics.increment("pbft.pre_prepare_sent")
        logger.info(
            f"[{self.node_id}] PBFT PRE-PREPARE seq={seq} "
            f"digest={data['digest']}"
        )

        # Cek apakah sudah quorum prepare (primary sudah 1 vote)
        if len(self._prepare_log[seq]) >= self._quorum():
            await self._enter_commit(seq, data["digest"])

        return {"seq": seq, "digest": data["digest"]}

    async def _handle_pre_prepare(self, msg: Message) -> dict:
        data = msg.data
        seq = data["seq"]
        view = data["view"]

        if view != self.view:
            return {"accepted": False, "reason": "wrong_view"}
        if seq in self._pre_prepare_log:
            return {"accepted": False, "reason": "duplicate"}

        self._pre_prepare_log[seq] = data
        self._prepare_log.setdefault(seq, set())
        self._prepare_log[seq].add(self.node_id)

        # Broadcast prepare ke semua peer + primary
        all_targets = list(self.peers)
        primary_url = data.get("primary_url")
        if primary_url and primary_url not in all_targets:
            all_targets.append(primary_url)

        prepare_msg = Message("pbft_prepare", self.node_id, {
            "view": self.view,
            "seq": seq,
            "digest": data["digest"],
            "node_id": self.node_id,
            "sender_url": self._self_url(),
        })
        await self.bus.broadcast(all_targets, prepare_msg)
        metrics.increment("pbft.prepare_sent")
        logger.info(f"[{self.node_id}] PBFT PREPARE seq={seq}")
        return {"accepted": True}

    async def _handle_prepare(self, msg: Message) -> dict:
        data = msg.data
        seq = data["seq"]

        self._prepare_log.setdefault(seq, set())
        self._prepare_log[seq].add(data["node_id"])

        logger.info(
            f"[{self.node_id}] PREPARE count={len(self._prepare_log[seq])} "
            f"quorum={self._quorum()} seq={seq}"
        )

        if len(self._prepare_log[seq]) >= self._quorum():
            pp = self._pre_prepare_log.get(seq, {})
            await self._enter_commit(seq, pp.get("digest", ""))
        return {"ok": True}

    async def _enter_commit(self, seq: int, digest: str):
        # Hindari double commit
        if seq in self._commit_log and self.node_id in self._commit_log[seq]:
            return

        self._commit_log.setdefault(seq, set())
        self._commit_log[seq].add(self.node_id)

        # Broadcast commit ke semua peer
        commit_msg = Message("pbft_commit", self.node_id, {
            "view": self.view,
            "seq": seq,
            "digest": digest,
            "node_id": self.node_id,
            "sender_url": self._self_url(),
        })
        await self.bus.broadcast(self.peers, commit_msg)
        metrics.increment("pbft.commit_sent")
        logger.info(f"[{self.node_id}] PBFT COMMIT seq={seq}")

        # Cek apakah sudah bisa execute
        await self._try_execute(seq)

    async def _handle_commit(self, msg: Message) -> dict:
        data = msg.data
        seq = data["seq"]

        self._commit_log.setdefault(seq, set())
        self._commit_log[seq].add(data["node_id"])

        logger.info(
            f"[{self.node_id}] COMMIT count={len(self._commit_log[seq])} "
            f"quorum={self._quorum()} seq={seq}"
        )

        await self._try_execute(seq)
        return {"ok": True}

    async def _try_execute(self, seq: int):
        if (len(self._commit_log.get(seq, set())) >= self._quorum() and
                seq not in self._executed):
            self._executed.add(seq)
            pp = self._pre_prepare_log.get(seq, {})
            operation = pp.get("operation", {})
            result = await self._execute_operation(operation)

            digest = pp.get("digest", "")
            reply_url = pp.get("reply_url", "")

            self._replies[digest] = {
                "status": "ok",
                "result": result,
                "seq": seq,
            }

            # Kirim reply ke requester jika ada reply_url
            if reply_url:
                reply_msg = Message("pbft_reply", self.node_id, {
                    "digest": digest,
                    "status": "ok",
                    "result": result,
                    "seq": seq,
                })
                asyncio.create_task(self.bus.send(reply_url, reply_msg))

            metrics.increment("pbft.committed")
            logger.info(f"[{self.node_id}] PBFT EXECUTED seq={seq}")

    async def _handle_reply(self, msg: Message) -> dict:
        data = msg.data
        digest = data.get("digest", "")
        if digest and digest not in self._replies:
            self._replies[digest] = {
                "status": data.get("status", "ok"),
                "result": data.get("result", {}),
                "seq": data.get("seq", 0),
            }
        return {"ok": True}

    async def _execute_operation(self, operation: dict) -> dict:
        op_type = operation.get("type", "unknown")
        logger.info(
            f"[{self.node_id}] Executing PBFT operation: {op_type}"
        )
        return {"executed": True, "type": op_type, "ts": time.time()}

    async def _handle_view_change(self, msg: Message) -> dict:
        data = msg.data
        new_view = data.get("new_view", self.view + 1)
        if new_view > self.view:
            self.view = new_view
            logger.warning(f"[{self.node_id}] View change to {new_view}")
        return {"ok": True}

    def get_state(self) -> dict:
        return {
            "node_id": self.node_id,
            "is_primary": self.is_primary,
            "primary_id": self.primary_id,
            "view": self.view,
            "sequence": self.sequence,
            "f": self.f,
            "n": self.n,
            "quorum": self._quorum(),
            "executed_count": len(self._executed),
            "peers": self.peers,
        }


class PBFTServiceNode:

    def __init__(self, node_id: str, host: str, port: int,
                 peers: list[str], f: int = 1,
                 is_primary: bool = False,
                 is_byzantine: bool = False):
        from src.nodes.base_node import BaseNode
        # Buat base node secara komposisi
        self._base = BaseNode(node_id, host, port)
        self.node_id = node_id
        self.host = host
        self.port = port
        self.app = self._base.app
        self.is_byzantine = is_byzantine

        self.pbft = PBFTNode(
            node_id=node_id,
            peers=peers,
            bus=self._base.bus,
            f=f,
        )

        if is_primary:
            self.pbft.set_primary(True, node_id)

        self._setup_pbft_routes()

    def _setup_pbft_routes(self):

        @self.app.post("/pbft/request")
        async def submit_request(request: Request):
            """Submit operasi ke PBFT cluster."""
            data = await request.json()
            client_id = data.get("client_id", "anonymous")
            operation = data.get("operation", {})

            if not operation:
                return JSONResponse(
                    {"error": "operation required"}, status_code=400
                )

            if self.is_byzantine:
                logger.warning(
                    f"[{self.node_id}] BYZANTINE: Returning fake result!"
                )
                metrics.increment("pbft.byzantine_faults")
                return JSONResponse({
                    "status": "ok",
                    "result": {
                        "executed": True,
                        "byzantine": True,
                        "fake_data": "malicious_response"
                    },
                    "node": self.node_id,
                    "warning": "This node is Byzantine (malicious)!"
                })

            with metrics.time_it("pbft.request_latency"):
                result = await self.pbft.submit_request(client_id, operation)

            metrics.increment("pbft.requests_submitted")
            return JSONResponse({
                "status": result.get("status", "unknown"),
                "result": result,
                "node": self.node_id,
                "is_primary": self.pbft.is_primary,
            })

        @self.app.get("/pbft/state")
        async def get_state():
            """Lihat state PBFT node saat ini."""
            return {
                "pbft_state": self.pbft.get_state(),
                "is_byzantine": self.is_byzantine,
                "node_id": self.node_id,
            }

        @self.app.post("/pbft/set-primary")
        async def set_primary(request: Request):
            """Set node ini sebagai primary PBFT."""
            data = await request.json()
            is_primary = data.get("is_primary", False)
            primary_id = data.get("primary_id", self.node_id)
            self.pbft.set_primary(is_primary, primary_id)
            return {
                "ok": True,
                "is_primary": is_primary,
                "primary_id": primary_id
            }

        @self.app.post("/pbft/simulate-byzantine")
        async def toggle_byzantine(request: Request):
            """Toggle mode Byzantine untuk simulasi faulty node."""
            data = await request.json()
            self.is_byzantine = data.get("byzantine", True)
            logger.warning(
                f"[{self.node_id}] Byzantine mode: {self.is_byzantine}"
            )
            metrics.increment("pbft.byzantine_toggle")
            return {
                "ok": True,
                "node_id": self.node_id,
                "is_byzantine": self.is_byzantine,
                "warning": (
                    "Node is now behaving maliciously!"
                    if self.is_byzantine else "Node is honest again"
                )
            }

        @self.app.get("/pbft/metrics")
        async def pbft_metrics():
            """Metrics khusus PBFT."""
            state = self.pbft.get_state()
            return {
                "node_id": self.node_id,
                "is_primary": self.pbft.is_primary,
                "is_byzantine": self.is_byzantine,
                "view": state["view"],
                "sequence": state["sequence"],
                "executed_count": state["executed_count"],
                "quorum_required": state["quorum"],
                "f_tolerated": state["f"],
                "total_nodes": state["n"],
                "performance": metrics.get_stats(),
            }

    async def start(self):
        await self._base.start()
        logger.info(
            f"[{self.node_id}] PBFTServiceNode started "
            f"(primary={self.pbft.is_primary}, "
            f"byzantine={self.is_byzantine})"
        )