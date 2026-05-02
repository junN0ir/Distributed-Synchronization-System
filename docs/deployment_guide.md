# Deployment Guide

## Prerequisites
- Docker Desktop (Windows/Linux/Mac)
- Docker Compose v2+
- Python 3.11+ (untuk menjalankan tests lokal)

## Quick Start

### 1. Clone repository
```bash
git clone https://github.com/USERNAME/distributed-sync-system.git
cd distributed-sync-system
```

### 2. Build dan jalankan semua service
```bash
docker-compose -f docker/docker-compose.yml up --build
```

### 3. Verifikasi semua node healthy
```bash
# Linux/Mac
curl http://localhost:8001/health
curl http://localhost:8011/health
curl http://localhost:8021/health

# Windows PowerShell
Invoke-RestMethod http://localhost:8001/health
Invoke-RestMethod http://localhost:8011/health
Invoke-RestMethod http://localhost:8021/health
```

### 4. Cek Raft leader terpilih
```bash
Invoke-RestMethod http://localhost:8001/lock/status
Invoke-RestMethod http://localhost:8002/lock/status
Invoke-RestMethod http://localhost:8003/lock/status
```

## Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| NODE_ID | node1 | ID unik node |
| NODE_HOST | 0.0.0.0 | Bind address |
| NODE_PORT | 8001 | Port node |
| NODE_TYPE | lock | Tipe node: lock/queue/cache |
| CLUSTER_NODES | - | Daftar node dalam cluster |
| REDIS_HOST | redis | Hostname Redis |
| REDIS_PORT | 6379 | Port Redis |
| RAFT_ELECTION_TIMEOUT_MIN | 150 | Min election timeout (ms) |
| RAFT_ELECTION_TIMEOUT_MAX | 300 | Max election timeout (ms) |
| RAFT_HEARTBEAT_INTERVAL | 50 | Heartbeat interval (ms) |
| CACHE_SIZE | 1000 | Max cache entries per node |

## Scaling Nodes

Tambah node baru di docker-compose.yml:
```yaml
lock-node4:
  build:
    context: ..
    dockerfile: docker/Dockerfile.node
  environment:
    - NODE_ID=lock-node4
    - NODE_PORT=8004
    - NODE_TYPE=lock
    - CLUSTER_NODES=lock-node1:8001,lock-node2:8002,lock-node3:8003,lock-node4:8004
  ports:
    - "8004:8004"
  networks:
    - sync-net
```

## Troubleshooting

| Masalah | Solusi |
|---------|--------|
| Node tidak bisa connect | Pastikan berada di network sync-net yang sama |
| Raft tidak memilih leader | Cek log dengan `docker logs lock-node1` |
| Redis connection error | Pastikan container redis healthy dulu |
| Port conflict | Ganti port mapping di docker-compose.yml |

## Menjalankan Tests

```bash
# Aktifkan virtual environment Python 3.11
venv311\Scripts\Activate.ps1  # Windows
source venv311/bin/activate   # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Jalankan tests
pytest tests/ -v
```