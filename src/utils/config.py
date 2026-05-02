import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    NODE_ID: str = os.getenv("NODE_ID", "node1")
    NODE_HOST: str = os.getenv("NODE_HOST", "0.0.0.0")
    NODE_PORT: int = int(os.getenv("NODE_PORT", "8001"))

    CLUSTER_NODES: list[str] = os.getenv(
        "CLUSTER_NODES", "node1:8001,node2:8002,node3:8003"
    ).split(",")

    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))

    RAFT_ELECTION_TIMEOUT_MIN: int = int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "150"))
    RAFT_ELECTION_TIMEOUT_MAX: int = int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "300"))
    RAFT_HEARTBEAT_INTERVAL: int = int(os.getenv("RAFT_HEARTBEAT_INTERVAL", "50"))

    PBFT_NODES: list[str] = os.getenv(
        "PBFT_NODES", "node1:8001,node2:8002,node3:8003,node4:8004"
    ).split(",")
    PBFT_F: int = int(os.getenv("PBFT_F", "1"))

    CACHE_SIZE: int = int(os.getenv("CACHE_SIZE", "1000"))
    CACHE_POLICY: str = os.getenv("CACHE_POLICY", "LRU")

    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9090"))


config = Config()