import asyncio
import logging
import os
import sys

import uvicorn

from src.utils.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

NODE_TYPE = os.getenv("NODE_TYPE", "lock")
IS_PRIMARY = os.getenv("IS_PRIMARY", "false").lower() == "true"
IS_BYZANTINE = os.getenv("IS_BYZANTINE", "false").lower() == "true"


def build_peer_urls(cluster_nodes: list[str], self_id: str) -> list[str]:
    urls = []
    for node in cluster_nodes:
        parts = node.split(":")
        nid = parts[0].strip()
        port = parts[1].strip() if len(parts) > 1 else "8001"
        if nid != self_id:
            urls.append(f"http://{nid}:{port}")
    return urls


async def main():
    peer_urls = build_peer_urls(config.CLUSTER_NODES, config.NODE_ID)
    logger.info(
        f"Starting node {config.NODE_ID} type={NODE_TYPE} peers={peer_urls}"
    )

    if NODE_TYPE == "lock":
        from src.nodes.lock_manager import LockManagerNode
        node = LockManagerNode(
            node_id=config.NODE_ID,
            host=config.NODE_HOST,
            port=config.NODE_PORT,
            peers=peer_urls,
        )

    elif NODE_TYPE == "queue":
        from src.nodes.queue_node import QueueNode
        node = QueueNode(
            node_id=config.NODE_ID,
            host=config.NODE_HOST,
            port=config.NODE_PORT,
        )
        node.set_peers(peer_urls)

    elif NODE_TYPE == "cache":
        from src.nodes.cache_node import CacheNode
        node = CacheNode(
            node_id=config.NODE_ID,
            host=config.NODE_HOST,
            port=config.NODE_PORT,
        )
        node.set_peers(peer_urls)

    elif NODE_TYPE == "pbft":
        from src.consensus.pbft import PBFTServiceNode
        node = PBFTServiceNode(
            node_id=config.NODE_ID,
            host=config.NODE_HOST,
            port=config.NODE_PORT,
            peers=peer_urls,
            f=config.PBFT_F,
            is_primary=IS_PRIMARY,
            is_byzantine=IS_BYZANTINE,
        )

    else:
        logger.error(f"Unknown NODE_TYPE: {NODE_TYPE}")
        sys.exit(1)

    await node.start()

    if NODE_TYPE == "pbft" and IS_PRIMARY:
        node.pbft.set_primary(True, config.NODE_ID)
        logger.info(f"[{config.NODE_ID}] Set as PBFT primary")

    config_uvi = uvicorn.Config(
        app=node.app,
        host=config.NODE_HOST,
        port=config.NODE_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config_uvi)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())