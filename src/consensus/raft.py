import asyncio
import logging
import random
import time
from enum import Enum
from dataclasses import dataclass, field

from src.communication.message_passing import MessageBus, Message
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class RaftRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: dict
    timestamp: float = field(default_factory=time.time)


class RaftNode:
    def __init__(self, node_id: str, peers: list[str], bus: MessageBus,
                 election_timeout_min: int = 150, election_timeout_max: int = 300,
                 heartbeat_interval: int = 50):
        self.node_id = node_id
        self.peers = peers  
        self.bus = bus

        # Persistent state
        self.current_term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        # Volatile state
        self.commit_index = -1
        self.last_applied = -1
        self.role = RaftRole.FOLLOWER
        self.leader_id: str | None = None
        self.votes_received: set[str] = set()

        # Leader volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # Locks
        self._locks: dict[str, dict] = {}

        # Timing
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        self._election_deadline = self._new_election_deadline()
        self._running = False

        # Register handlers
        self.bus.register_handler("request_vote", self._handle_request_vote)
        self.bus.register_handler("request_vote_response", self._handle_vote_response)
        self.bus.register_handler("append_entries", self._handle_append_entries)
        self.bus.register_handler("append_entries_response", self._handle_append_entries_response)
        self.bus.register_handler("lock_request", self._handle_lock_request)
        self.bus.register_handler("lock_release", self._handle_lock_release)

    def _new_election_deadline(self) -> float:
        timeout_ms = random.randint(self.election_timeout_min, self.election_timeout_max)
        return time.time() + timeout_ms / 1000.0

    def _last_log_index(self) -> int:
        return len(self.log) - 1

    def _last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _peer_url_for_node(self, node_id: str) -> str | None:
        """Cari URL peer berdasarkan node_id (nama host)."""
        for url in self.peers:
            # url format: http://lock-node2:8002
            host = url.split("//")[-1].split(":")[0]
            if host == node_id:
                return url
        return None

    async def start(self):
        self._running = True
        asyncio.create_task(self._main_loop())
        logger.info(f"[{self.node_id}] Raft started as {self.role.value}, peers={self.peers}")

    async def stop(self):
        self._running = False

    async def _main_loop(self):
        while self._running:
            now = time.time()
            if self.role == RaftRole.LEADER:
                await self._send_heartbeats()
                await asyncio.sleep(self.heartbeat_interval / 1000.0)
            else:
                if now >= self._election_deadline:
                    await self._start_election()
                await asyncio.sleep(0.01)

    async def _start_election(self):
        self.current_term += 1
        self.role = RaftRole.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self._election_deadline = self._new_election_deadline()

        logger.info(f"[{self.node_id}] Starting election term={self.current_term} peers={self.peers}")
        metrics.increment("raft.elections_started")

        msg = Message("request_vote", self.node_id, {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "candidate_url": self._self_url(),
            "last_log_index": self._last_log_index(),
            "last_log_term": self._last_log_term(),
        })
        await self.bus.broadcast(self.peers, msg)

    def _self_url(self) -> str:
        """URL diri sendiri berdasarkan port dari config."""
        from src.utils.config import config
        return f"http://{self.node_id}:{config.NODE_PORT}"

    async def _send_heartbeats(self):
        for peer in self.peers:
            prev_log_index = self._last_log_index()
            prev_log_term = self._last_log_term()
            entries = []

            next_idx = self.next_index.get(peer, len(self.log))
            if next_idx <= len(self.log) - 1:
                entries = [
                    {"term": e.term, "index": e.index, "command": e.command}
                    for e in self.log[next_idx:]
                ]
                prev_log_index = next_idx - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0

            msg = Message("append_entries", self.node_id, {
                "term": self.current_term,
                "leader_id": self.node_id,
                "leader_url": self._self_url(),
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self.commit_index,
            })
            asyncio.create_task(self.bus.send(peer, msg))

    async def _handle_request_vote(self, msg: Message) -> dict:
        data = msg.data
        term = data["term"]
        candidate = data["candidate_id"]
        # Ambil URL kandidat dari pesan
        candidate_url = data.get("candidate_url") or self._peer_url_for_node(candidate)
        last_log_index = data["last_log_index"]
        last_log_term = data["last_log_term"]

        if term > self.current_term:
            self.current_term = term
            self.role = RaftRole.FOLLOWER
            self.voted_for = None

        vote_granted = False
        if (term >= self.current_term and
                (self.voted_for is None or self.voted_for == candidate) and
                (last_log_term > self._last_log_term() or
                 (last_log_term == self._last_log_term() and
                  last_log_index >= self._last_log_index()))):
            self.voted_for = candidate
            vote_granted = True
            self._election_deadline = self._new_election_deadline()

        logger.info(
            f"[{self.node_id}] Vote for {candidate}: granted={vote_granted} "
            f"term={term} candidate_url={candidate_url}"
        )

        if candidate_url:
            response = Message("request_vote_response", self.node_id, {
                "term": self.current_term,
                "vote_granted": vote_granted,
                "voter_id": self.node_id,
            })
            asyncio.create_task(self.bus.send(candidate_url, response))

        return {"vote_granted": vote_granted}

    async def _handle_vote_response(self, msg: Message) -> dict:
        data = msg.data
        if data["term"] > self.current_term:
            self.current_term = data["term"]
            self.role = RaftRole.FOLLOWER
            return {}

        if self.role != RaftRole.CANDIDATE:
            return {}

        if data.get("vote_granted"):
            self.votes_received.add(data["voter_id"])
            quorum = (len(self.peers) + 1) // 2 + 1
            logger.info(
                f"[{self.node_id}] Votes: {len(self.votes_received)}/{quorum} needed"
            )
            if len(self.votes_received) >= quorum:
                await self._become_leader()
        return {}

    async def _become_leader(self):
        self.role = RaftRole.LEADER
        self.leader_id = self.node_id
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = -1

        logger.info(f"[{self.node_id}] *** BECAME LEADER *** term={self.current_term}")
        metrics.increment("raft.leader_elections_won")

        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command={"type": "noop"}
        )
        self.log.append(entry)

    async def _handle_append_entries(self, msg: Message) -> dict:
        data = msg.data
        term = data["term"]
        leader_url = data.get("leader_url") or self._peer_url_for_node(data["leader_id"])

        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        self.current_term = term
        self.role = RaftRole.FOLLOWER
        self.leader_id = data["leader_id"]
        self._election_deadline = self._new_election_deadline()

        prev_log_index = data["prev_log_index"]
        prev_log_term = data["prev_log_term"]

        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                return {"term": self.current_term, "success": False}
            if self.log[prev_log_index].term != prev_log_term:
                self.log = self.log[:prev_log_index]
                return {"term": self.current_term, "success": False}

        for entry_dict in data.get("entries", []):
            entry = LogEntry(
                term=entry_dict["term"],
                index=entry_dict["index"],
                command=entry_dict["command"]
            )
            if entry.index < len(self.log):
                if self.log[entry.index].term != entry.term:
                    self.log = self.log[:entry.index]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        leader_commit = data.get("leader_commit", -1)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            await self._apply_committed_entries()

        # Kirim response ke leader URL
        if leader_url:
            response = Message("append_entries_response", self.node_id, {
                "term": self.current_term,
                "success": True,
                "node_id": self.node_id,
            })
            asyncio.create_task(self.bus.send(leader_url, response))

        return {"term": self.current_term, "success": True, "node_id": self.node_id}

    async def _handle_append_entries_response(self, msg: Message) -> dict:
        data = msg.data
        peer_url = self._peer_url_for_node(msg.sender)
        if not peer_url:
            return {}

        if data.get("success"):
            self.next_index[peer_url] = len(self.log)
            self.match_index[peer_url] = len(self.log) - 1

            for n in range(self.commit_index + 1, len(self.log)):
                if self.log[n].term == self.current_term:
                    replicated = sum(
                        1 for p in self.peers
                        if self.match_index.get(p, -1) >= n
                    ) + 1
                    quorum = (len(self.peers) + 1) // 2 + 1
                    if replicated >= quorum:
                        self.commit_index = n
                        await self._apply_committed_entries()
        else:
            self.next_index[peer_url] = max(0, self.next_index.get(peer_url, 0) - 1)
        return {}

    async def _apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            await self._apply_command(entry.command)

    async def _apply_command(self, command: dict):
        cmd_type = command.get("type")
        if cmd_type == "acquire_lock":
            key = command["key"]
            holder = command["holder"]
            lock_type = command.get("lock_type", "exclusive")
            self._locks[key] = {
                "holder": holder,
                "type": lock_type,
                "term": self.current_term
            }
            logger.info(f"[{self.node_id}] Lock applied: {key} -> {holder}")
        elif cmd_type == "release_lock":
            key = command["key"]
            self._locks.pop(key, None)
            logger.info(f"[{self.node_id}] Lock released: {key}")

    async def propose(self, command: dict) -> bool:
        if self.role != RaftRole.LEADER:
            return False
        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command
        )
        self.log.append(entry)
        metrics.increment("raft.proposals")
        return True

    async def _handle_lock_request(self, msg: Message) -> dict:
        data = msg.data
        key = data["key"]
        holder = data["holder"]
        lock_type = data.get("lock_type", "exclusive")

        if key in self._locks:
            existing = self._locks[key]
            if existing["type"] == "exclusive" or lock_type == "exclusive":
                return {
                    "granted": False,
                    "reason": "lock_held",
                    "holder": existing["holder"]
                }

        success = await self.propose({
            "type": "acquire_lock",
            "key": key,
            "holder": holder,
            "lock_type": lock_type,
        })
        metrics.increment("raft.lock_requests")
        return {"granted": success, "leader": self.node_id}

    async def _handle_lock_release(self, msg: Message) -> dict:
        key = msg.data["key"]
        await self.propose({"type": "release_lock", "key": key})
        return {"released": True}

    def get_state(self) -> dict:
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "peers": self.peers,
            "locks": self._locks,
        }