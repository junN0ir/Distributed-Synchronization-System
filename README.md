# Distributed Synchronization System
**Mata Kuliah:** Sistem Parallel dan Terdistribusi  
**Nama:** Junnior Marcellino Polla  
**NIM:** 11231034  

---

## Daftar Isi
1. [Gambaran Sistem](#gambaran-sistem)
2. [Arsitektur](#arsitektur)
3. [Core Requirements](#core-requirements)
   - [A. Distributed Lock Manager](#a-distributed-lock-manager)
   - [B. Distributed Queue System](#b-distributed-queue-system)
   - [C. Distributed Cache Coherence](#c-distributed-cache-coherence)
   - [D. Containerization](#d-containerization)
4. [Bonus: PBFT Advanced Consensus](#bonus-pbft-advanced-consensus)
5. [Cara Build dan Run](#cara-build-dan-run)
6. [Demo Commands](#demo-commands)
7. [Unit Tests](#unit-tests)
8. [Benchmark dan Performa](#benchmark-dan-performa)
9. [Struktur Folder](#struktur-folder)
10. [Troubleshooting](#troubleshooting)

---

## Gambaran Sistem

Sistem ini mengimplementasikan distributed synchronization yang terdiri dari empat subsistem utama yang berjalan secara independen dalam container Docker:

- **Distributed Lock Manager** — menggunakan algoritma Raft Consensus untuk memastikan hanya satu proses yang memegang lock pada satu waktu
- **Distributed Queue** — menggunakan Consistent Hashing dengan Redis sebagai persistent backend
- **Distributed Cache Coherence** — menggunakan protokol MESI untuk menjaga konsistensi cache antar node
- **PBFT Node (Bonus)** — mengimplementasikan Practical Byzantine Fault Tolerance untuk ketahanan terhadap malicious nodes

---

## Arsitektur

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Network (sync-net)                     │
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │ lock-node1  │   │ lock-node2  │   │ lock-node3  │           │
│  │ (Follower)  │◄──┤  (LEADER)   ├──►│ (Follower)  │           │
│  │ port: 8001  │   │ port: 8002  │   │ port: 8003  │           │
│  └─────────────┘   └─────────────┘   └─────────────┘           │
│         Raft Consensus — Leader Election + Log Replication       │
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐                             │
│  │ queue-node1 │   │ queue-node2 │                             │
│  │ (Consistent │◄──┤  Hashing)   │                             │
│  │ port: 8011  │   │ port: 8012  │                             │
│  └──────┬──────┘   └──────┬──────┘                             │
│         └────────┬─────────┘                                    │
│                  ▼                                               │
│       ┌─────────────────┐                                       │
│       │      Redis      │                                       │
│       │  port: 6379     │                                       │
│       └─────────────────┘                                       │
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │ cache-node1 │   │ cache-node2 │   │ cache-node3 │           │
│  │   (MESI)    │◄──┤   (MESI)    ├──►│   (MESI)    │           │
│  │ port: 8021  │   │ port: 8022  │   │ port: 8023  │           │
│  └─────────────┘   └─────────────┘   └─────────────┘           │
│                                                                  │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐  ┌──────┐ │
│  │ pbft-node1  │   │ pbft-node2  │   │ pbft-node3  │  │pbft-4│ │
│  │ (PRIMARY)   │◄──┤  (Replica)  ├──►│  (Replica)  │  │(Rep.)│ │
│  │ port: 8031  │   │ port: 8032  │   │ port: 8033  │  │ 8034 │ │
│  └─────────────┘   └─────────────┘   └─────────────┘  └──────┘ │
│              PBFT — Byzantine Fault Tolerance (f=1)              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Core Requirements

### A. Distributed Lock Manager

Distributed Lock Manager diimplementasikan menggunakan algoritma **Raft Consensus** yang menjamin konsistensi lock di seluruh node. Sistem menjalankan 3 node lock yang saling berkomunikasi — satu node terpilih sebagai **leader** melalui proses election, dan semua operasi lock harus melalui leader.

**Fitur yang diimplementasikan:**
- Raft leader election dengan randomized timeout (150–300ms)
- Log replication ke semua follower sebelum commit
- Shared lock — multiple reader boleh memegang bersamaan
- Exclusive lock — hanya satu holder, memblokir semua request lain
- Deadlock detection menggunakan directed wait-for graph (cycle detection)
- Network partition handling — saat leader terputus, follower otomatis memulai election baru

**Port:** lock-node1: 8001 | lock-node2: 8002 | lock-node3: 8003

**Endpoints:**

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| POST | `/lock/acquire` | Acquire distributed lock (shared/exclusive) |
| POST | `/lock/release` | Release lock |
| GET | `/lock/status` | Status Raft (role, term, leader) dan daftar lock aktif |
| GET | `/health` | Health check node |
| GET | `/metrics` | Performance metrics |

---

### B. Distributed Queue System

Distributed Queue menggunakan **Consistent Hashing** dengan 150 virtual nodes per physical node untuk distribusi pesan yang merata. **Redis** digunakan sebagai persistent backend sehingga pesan tidak hilang saat node restart.

**Fitur yang diimplementasikan:**
- Consistent hashing ring — penambahan/penghapusan node tidak mengganggu distribusi
- Multiple producers dan consumers — bisa dari node manapun
- Message persistence di Redis — data aman saat container restart
- At-least-once delivery — producer dapat mengirim ulang jika tidak ada respons
- Dead Letter Queue (DLQ) — pesan bermasalah dipisahkan agar tidak memblokir antrian utama

**Port:** queue-node1: 8011 | queue-node2: 8012

**Endpoints:**

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| POST | `/queue/publish` | Publish message ke topic |
| POST | `/queue/consume` | Consume message dari topic |
| GET | `/queue/length?topic=` | Panjang antrian topic |
| GET | `/queue/topics` | Daftar semua topic aktif |
| POST | `/queue/deadletter` | Pindahkan pesan ke DLQ |

---

### C. Distributed Cache Coherence

Cache Coherence diimplementasikan menggunakan **protokol MESI** (Modified, Exclusive, Shared, Invalid). Setiap cache entry memiliki state MESI yang diperbarui secara otomatis saat terjadi operasi read/write di node manapun.

**State MESI:**
- **M (Modified)** — data dimodifikasi lokal, belum sinkron ke memory utama
- **E (Exclusive)** — hanya node ini yang punya copy, sinkron dengan memory
- **S (Shared)** — multiple nodes punya copy yang sama dan valid
- **I (Invalid)** — data tidak valid, harus fetch ulang saat dibutuhkan

**Fitur yang diimplementasikan:**
- MESI protocol — state transition otomatis saat read/write/invalidate
- Cache invalidation broadcast — saat satu node menulis, semua peer di-invalidate
- LRU (Least Recently Used) replacement policy — entry yang paling lama tidak diakses dibuang saat kapasitas penuh
- Performance monitoring — hit rate, miss rate, eviction count per node

**Port:** cache-node1: 8021 | cache-node2: 8022 | cache-node3: 8023

**Endpoints:**

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| GET | `/cache/{key}` | Read cache (cek MESI state) |
| PUT | `/cache/{key}` | Write cache + broadcast invalidate ke peer |
| DELETE | `/cache/{key}` | Hapus dan invalidate semua node |
| GET | `/cache/stats/summary` | Hit rate, miss rate, MESI states |

---

### D. Containerization

Seluruh komponen dikemas dalam Docker image menggunakan **python:3.11-slim** sebagai base image. Orchestration menggunakan **Docker Compose** dengan jaringan internal `sync-net`.

**Fitur yang diimplementasikan:**
- Dockerfile terpisah untuk semua node (`docker/Dockerfile.node`)
- Docker Compose dengan 13 service (Redis + 3 lock + 2 queue + 3 cache + 4 PBFT node)
- Non-root user (`appuser`) untuk keamanan container
- Health check di setiap service
- Named volume untuk Redis persistence
- Environment configuration via `.env` files
- Dynamic scaling — tambah node baru cukup dengan menambah service di docker-compose.yml

**Cara build:**
```powershell
docker-compose -f docker/docker-compose.yml up --build -d
```

---

## Bonus: PBFT Advanced Consensus

**PBFT (Practical Byzantine Fault Tolerance)** diimplementasikan sebagai bonus dengan 4 node (n=4, f=1), artinya sistem toleran terhadap maksimal 1 node Byzantine (malicious/faulty).

**Formula toleransi:** f = (n-1)/3, sehingga dengan n=4: f=1

**Tiga fase PBFT:**
1. **Pre-Prepare** — Primary menerima request client, broadcast ke semua replica
2. **Prepare** — Setiap replica memvalidasi dan broadcast Prepare ke semua node
3. **Commit** — Setelah quorum (2f+1=3) Prepare terkumpul, broadcast Commit dan eksekusi

**Fitur yang diimplementasikan:**
- Complete PBFT three-phase protocol (Pre-Prepare → Prepare → Commit)
- Byzantine node simulation — node dapat di-toggle menjadi malicious via API
- Quorum calculation otomatis berdasarkan n dan f
- View change handling saat primary gagal
- Sistem tetap berjalan normal meski 1 dari 4 node berperilaku jahat

**Port:** pbft-node1: 8031 | pbft-node2: 8032 | pbft-node3: 8033 | pbft-node4: 8034

**Endpoints:**

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| POST | `/pbft/request` | Submit operasi ke PBFT cluster |
| GET | `/pbft/state` | State PBFT node (view, sequence, executed) |
| POST | `/pbft/set-primary` | Set node sebagai primary |
| POST | `/pbft/simulate-byzantine` | Toggle mode Byzantine (malicious) |
| GET | `/pbft/metrics` | Metrics PBFT lengkap |

---

## Cara Build dan Run

### Prasyarat
- Docker Desktop (Windows/Linux/Mac)
- Docker Compose v2+
- Python 3.11+ dengan virtual environment

### 1. Aktifkan Virtual Environment

```powershell
# Windows
.venv\Scripts\Activate.ps1
```

### 2. Build dan Jalankan Semua Service

```powershell
# Dari root folder distributed-sync-system
docker-compose -f docker/docker-compose.yml up --build -d
```

### 3. Tunggu semua container healthy (±15 detik), lalu verifikasi

```powershell
docker ps --format "table {{.Names}}`t{{.Status}}"
```

### 4. Health check semua node

```powershell
Write-Host "=== HEALTH CHECK SEMUA NODE ===" -ForegroundColor Cyan
Invoke-RestMethod http://localhost:8001/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8002/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8003/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8011/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8012/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8021/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8022/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8023/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8031/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8032/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8033/health | ConvertTo-Json
Invoke-RestMethod http://localhost:8034/health | ConvertTo-Json
```

### 5. Cek Raft leader terpilih

```powershell
Write-Host "=== RAFT STATUS ===" -ForegroundColor Cyan
Invoke-RestMethod http://localhost:8001/lock/status | ConvertTo-Json -Depth 3
Invoke-RestMethod http://localhost:8002/lock/status | ConvertTo-Json -Depth 3
Invoke-RestMethod http://localhost:8003/lock/status | ConvertTo-Json -Depth 3
# Salah satu node harus menunjukkan "role": "leader"
```

### 6. Setup PBFT Primary

```powershell
Invoke-RestMethod -Method POST -Uri "http://localhost:8031/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":true,"primary_id":"pbft-node1"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8032/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":false,"primary_id":"pbft-node1"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8033/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":false,"primary_id":"pbft-node1"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8034/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":false,"primary_id":"pbft-node1"}' | ConvertTo-Json
```

---

## Demo Commands

### Demo 1: Distributed Lock (Exclusive & Shared)

```powershell
# --- Acquire exclusive lock ---
Write-Host "=== DEMO 1: Acquire Exclusive Lock ===" -ForegroundColor Green
Invoke-RestMethod -Method POST -Uri "http://localhost:8002/lock/acquire" `
  -ContentType "application/json" `
  -Body '{"key":"resource-db","holder":"client-A","type":"exclusive"}' | ConvertTo-Json

# --- Client lain coba acquire lock yang sama (harus DITOLAK) ---
Write-Host "=== DEMO 2: Coba acquire lock yang sama (harus DITOLAK) ===" -ForegroundColor Yellow
Invoke-RestMethod -Method POST -Uri "http://localhost:8002/lock/acquire" `
  -ContentType "application/json" `
  -Body '{"key":"resource-db","holder":"client-B","type":"exclusive"}' | ConvertTo-Json

# --- Acquire shared lock (dua reader boleh bersamaan) ---
Write-Host "=== DEMO 3: Shared Lock ===" -ForegroundColor Green
Invoke-RestMethod -Method POST -Uri "http://localhost:8002/lock/acquire" `
  -ContentType "application/json" `
  -Body '{"key":"resource-read","holder":"reader-A","type":"shared"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8002/lock/acquire" `
  -ContentType "application/json" `
  -Body '{"key":"resource-read","holder":"reader-B","type":"shared"}' | ConvertTo-Json

# --- Release lock client-A ---
Write-Host "=== DEMO 4: Release lock client-A ===" -ForegroundColor Cyan
Invoke-RestMethod -Method POST -Uri "http://localhost:8002/lock/release" `
  -ContentType "application/json" `
  -Body '{"key":"resource-db","holder":"client-A"}' | ConvertTo-Json

# --- Client-B sekarang bisa acquire ---
Write-Host "=== DEMO 5: Client-B sekarang bisa acquire ===" -ForegroundColor Green
Invoke-RestMethod -Method POST -Uri "http://localhost:8002/lock/acquire" `
  -ContentType "application/json" `
  -Body '{"key":"resource-db","holder":"client-B","type":"exclusive"}' | ConvertTo-Json

# --- Bersihkan ---
Invoke-RestMethod -Method POST -Uri "http://localhost:8002/lock/release" `
  -ContentType "application/json" `
  -Body '{"key":"resource-db","holder":"client-B"}' | ConvertTo-Json
```

### Demo 2: Network Partition (Raft)



**Monitoring real-time:**
```powershell
while ($true) {
    $s1 = (Invoke-RestMethod http://localhost:8001/lock/status).raft_state.role
    $s2 = (Invoke-RestMethod http://localhost:8002/lock/status).raft_state.role
    $s3 = (Invoke-RestMethod http://localhost:8003/lock/status).raft_state.role
    Write-Host "$(Get-Date -Format 'HH:mm:ss') | node1=$s1 | node2=$s2 | node3=$s3"
    Start-Sleep 2
}
```

**Disconnect dan reconnect leader:**
```powershell
Write-Host "Memutuskan lock-node2 (leader) dari network..." -ForegroundColor Red
docker network disconnect docker_sync-net lock-node2

Start-Sleep 12

Write-Host "Menyambungkan kembali lock-node2..." -ForegroundColor Green
docker network connect docker_sync-net lock-node2
```


### Demo 3: Distributed Queue

```powershell
# --- Publish dari dua node berbeda ---
Write-Host "=== DEMO QUEUE: Publish ===" -ForegroundColor Cyan

Invoke-RestMethod -Method POST -Uri "http://localhost:8011/queue/publish" `
  -ContentType "application/json" `
  -Body '{"topic":"orders","message":{"item":"laptop","qty":1},"producer":"shop-service"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8011/queue/publish" `
  -ContentType "application/json" `
  -Body '{"topic":"orders","message":{"item":"phone","qty":2},"producer":"shop-service"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8012/queue/publish" `
  -ContentType "application/json" `
  -Body '{"topic":"logs","message":{"level":"ERROR","msg":"DB timeout"},"producer":"api-service"}' | ConvertTo-Json

# --- Cek panjang queue dan topics ---
Write-Host "=== Cek panjang queue ===" -ForegroundColor Yellow
Invoke-RestMethod "http://localhost:8011/queue/length?topic=orders" | ConvertTo-Json

Write-Host "=== List semua topics ===" -ForegroundColor Yellow
Invoke-RestMethod http://localhost:8011/queue/topics | ConvertTo-Json

# --- Consume message ---
Write-Host "=== Consume message ===" -ForegroundColor Green
Invoke-RestMethod -Method POST -Uri "http://localhost:8011/queue/consume" `
  -ContentType "application/json" `
  -Body '{"topic":"orders","consumer_id":"order-processor"}' | ConvertTo-Json

# --- Cek panjang queue setelah consume ---
Write-Host "=== Cek panjang queue setelah consume ===" -ForegroundColor Yellow
Invoke-RestMethod "http://localhost:8011/queue/length?topic=orders" | ConvertTo-Json
```

### Demo 4: Cache Coherence (MESI)

```powershell
Write-Host "=== DEMO CACHE MESI ===" -ForegroundColor Cyan

# --- Write ke cache-node1 ---
Write-Host "--- Write ke cache-node1 ---" -ForegroundColor Green
Invoke-RestMethod -Method PUT -Uri "http://localhost:8021/cache/user-1" `
  -ContentType "application/json" `
  -Body '{"value":{"name":"Junnior","role":"admin"}}' | ConvertTo-Json

# --- Read dari node1 (harus HIT, state M) ---
Write-Host "--- Read dari node1 (harus HIT) ---" -ForegroundColor Green
Invoke-RestMethod http://localhost:8021/cache/user-1 | ConvertTo-Json

# --- Read dari node2 (harus MISS, state I — sudah di-invalidate) ---
Write-Host "--- Read dari node2 (harus MISS) ---" -ForegroundColor Yellow
Invoke-RestMethod http://localhost:8022/cache/user-1 | ConvertTo-Json

# --- Read dari node3 (harus MISS juga) ---
Write-Host "--- Read dari node3 (harus MISS) ---" -ForegroundColor Yellow
Invoke-RestMethod http://localhost:8023/cache/user-1 | ConvertTo-Json

# --- Cache stats ---
Write-Host "--- Stats node1 ---" -ForegroundColor Cyan
Invoke-RestMethod http://localhost:8021/cache/stats/summary | ConvertTo-Json

Write-Host "--- Stats node2 ---" -ForegroundColor Cyan
Invoke-RestMethod http://localhost:8022/cache/stats/summary | ConvertTo-Json
```

### Demo 5: PBFT Byzantine Fault Tolerance (Bonus)

```powershell
Write-Host "=== SETUP PBFT PRIMARY ===" -ForegroundColor Cyan
Invoke-RestMethod -Method POST -Uri "http://localhost:8031/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":true,"primary_id":"pbft-node1"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8032/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":false,"primary_id":"pbft-node1"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8033/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":false,"primary_id":"pbft-node1"}' | ConvertTo-Json

Invoke-RestMethod -Method POST -Uri "http://localhost:8034/pbft/set-primary" `
  -ContentType "application/json" `
  -Body '{"is_primary":false,"primary_id":"pbft-node1"}' | ConvertTo-Json

# --- Submit request normal ---
Write-Host "=== Submit request NORMAL ===" -ForegroundColor Green
Invoke-RestMethod -Method POST -Uri "http://localhost:8031/pbft/request" `
  -ContentType "application/json" `
  -Body '{"client_id":"client-1","operation":{"type":"write","key":"config","value":"production"}}' | ConvertTo-Json

# --- Jadikan node4 Byzantine ---
Write-Host "=== Jadikan node4 BYZANTINE (malicious) ===" -ForegroundColor Red
Invoke-RestMethod -Method POST -Uri "http://localhost:8034/pbft/simulate-byzantine" `
  -ContentType "application/json" `
  -Body '{"byzantine":true}' | ConvertTo-Json

# --- Submit request saat ada Byzantine node (sistem tetap bekerja) ---
Write-Host "=== Submit request saat ada Byzantine node ===" -ForegroundColor Yellow
Invoke-RestMethod -Method POST -Uri "http://localhost:8031/pbft/request" `
  -ContentType "application/json" `
  -Body '{"client_id":"client-2","operation":{"type":"transfer","from":"A","to":"B","amount":500}}' | ConvertTo-Json

# --- Bandingkan state node jujur vs Byzantine ---
Write-Host "=== Cek state node1 (jujur) vs node4 (Byzantine) ===" -ForegroundColor Cyan
Invoke-RestMethod http://localhost:8031/pbft/state | ConvertTo-Json
Invoke-RestMethod http://localhost:8034/pbft/state | ConvertTo-Json

# --- Kembalikan node4 jujur ---
Write-Host "=== Kembalikan node4 jujur ===" -ForegroundColor Green
Invoke-RestMethod -Method POST -Uri "http://localhost:8034/pbft/simulate-byzantine" `
  -ContentType "application/json" `
  -Body '{"byzantine":false}' | ConvertTo-Json
```

---

## Unit Tests

Pengujian unit dilakukan menggunakan **pytest** untuk memverifikasi semua komponen inti sistem bekerja dengan benar secara terisolasi tanpa memerlukan container Docker berjalan.

### Jalankan Tests

```powershell
# Aktifkan virtual environment
.venv\Scripts\Activate.ps1

# Install dependencies jika belum
pip install -r requirements.txt

# Jalankan semua tests
pytest tests/ -v
```

### Cakupan 12 Test

| Test | Komponen | Yang Diuji |
|------|----------|------------|
| test_metrics_counter | Utils | Counter increment dan akumulasi nilai |
| test_metrics_histogram | Utils | Histogram stats: min, max, count |
| test_message_serialization | Communication | Serialisasi dan deserialisasi objek Message |
| test_failure_detector_alive | Communication | Node dianggap alive setelah menerima heartbeat |
| test_failure_detector_timeout | Communication | Node dianggap mati setelah melewati timeout |
| test_failure_detector_alive_nodes | Communication | Filter node yang masih hidup dari daftar |
| test_metrics_timer | Utils | Context manager time_it mengukur durasi |
| test_raft_initial_state | Consensus | State awal Raft: role=follower, term=0 |
| test_pbft_quorum | Consensus | Kalkulasi quorum PBFT: 2f+1 |
| test_lru_cache_eviction | Cache | LRU eviction saat kapasitas penuh |
| test_consistent_hashing | Queue | Distribusi key ke multiple nodes |
| test_deadlock_detection | Lock | Deteksi cycle di wait-for graph |

---

## Benchmark dan Performa

### Manual Benchmark

Jalankan script benchmark untuk mengukur throughput dan latency semua komponen:

```powershell
python benchmarks/load_test_scenarios.py
```

Script ini mengukur:
- **Queue:** throughput publish/consume (msg/detik), latency p50/p95/p99 — n=200
- **Cache:** latency read/write, hit rate, MESI invalidation — n=200
- **Lock:** success rate, latency acquire/release, contention test — n=50

### Load Testing dengan Locust

```powershell
# Jalankan Locust
locust -f benchmarks/load_test_scenarios.py

# Buka browser: http://localhost:8089
# Set: Number of users=30, Spawn rate=10, Host=(kosongkan), klik Start
```

> **Catatan penting:** Kosongkan field Host di UI Locust. Setiap user class sudah memiliki host masing-masing yang ditentukan dari `BASE_LOCK`, `BASE_QUEUE`, dan `BASE_CACHE` di dalam script.

Di UI Locust tersedia:
- Tab **Statistics** — tabel per endpoint: requests, failures, median, p95, p99, RPS
- Tab **Charts** — grafik RPS, Response Times, dan Number of Users secara real-time

---

## Struktur Folder

```text
distributed-sync-system/
├── src/
│   ├── __init__.py
│   ├── main.py                         # Entry point dan routing berdasarkan NODE_TYPE
│   ├── nodes/
│   │   ├── __init__.py
│   │   ├── base_node.py                # Base class seluruh node (FastAPI + heartbeat)
│   │   ├── lock_manager.py             # Distributed lock dan deadlock detection
│   │   ├── queue_node.py               # Distributed queue dengan consistent hashing
│   │   └── cache_node.py               # Cache coherence menggunakan MESI + LRU
│   ├── consensus/
│   │   ├── __init__.py
│   │   ├── raft.py                     # Raft leader election dan log replication
│   │   └── pbft.py                     # Protokol PBFT tiga fase + PBFTServiceNode
│   ├── communication/
│   │   ├── __init__.py
│   │   ├── message_passing.py          # MessageBus berbasis aiohttp
│   │   └── failure_detector.py         # Failure detector berbasis heartbeat
│   └── utils/
│       ├── __init__.py
│       ├── config.py                   # Konfigurasi environment
│       └── metrics.py                  # Metrics collector (counter, histogram)
├── tests/
│   └── unit/
│       └── test_core.py                # 12 unit test inti sistem
├── docker/
│   ├── Dockerfile.node                 # Docker image untuk seluruh node
│   └── docker-compose.yml              # Orkestrasi 13 layanan
├── docs/
│   ├── architecture.md                 # Dokumentasi arsitektur lengkap
│   ├── api_spec.yaml                   # Spesifikasi OpenAPI
│   └── deployment_guide.md             # Panduan deployment
├── benchmarks/
│   └── load_test_scenarios.py          # Benchmark manual + Locust load testing
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| NODE_ID | node1 | ID unik node dalam cluster |
| NODE_HOST | 0.0.0.0 | Bind address |
| NODE_PORT | 8001 | Port HTTP server |
| NODE_TYPE | lock | Tipe node: lock / queue / cache / pbft |
| IS_PRIMARY | false | Apakah node ini PBFT primary |
| IS_BYZANTINE | false | Simulasi Byzantine node |
| CLUSTER_NODES | — | Daftar node: `node1:port,node2:port` |
| REDIS_HOST | redis | Hostname Redis |
| REDIS_PORT | 6379 | Port Redis |
| REDIS_DB | 0 | Redis database index |
| RAFT_ELECTION_TIMEOUT_MIN | 150 | Min election timeout (ms) |
| RAFT_ELECTION_TIMEOUT_MAX | 300 | Max election timeout (ms) |
| RAFT_HEARTBEAT_INTERVAL | 50 | Heartbeat interval (ms) |
| PBFT_F | 1 | Max Byzantine nodes yang ditoleransi |
| CACHE_SIZE | 1000 | Kapasitas maksimum cache per node |

---

## Troubleshooting

| Masalah | Penyebab | Solusi |
|---------|----------|--------|
| Container exit setelah start | `src/__init__.py` tidak ada | Buat file kosong: `New-Item src\__init__.py -Force` |
| Raft tidak memilih leader | Peer URL tidak bisa dijangkau | Cek log: `docker logs lock-node1 --tail 30` |
| Redis connection error | Redis belum healthy | Tunggu hingga redis status `healthy` |
| PBFT tidak commit | Primary belum di-set | Jalankan perintah setup PBFT Primary di atas |
| Port conflict | Port sudah dipakai proses lain | Ganti port mapping di docker-compose.yml |
| Benchmark tidak ada output | Locust mengambil alih eksekusi | Pastikan menggunakan file `load_test_scenarios.py` versi terbaru |
| Locust failures tinggi | Acquire dan release task terpisah | Sudah diperbaiki — acquire+release dalam satu task |

### Melihat Log Container

```powershell
docker logs lock-node1 --tail 30
docker logs lock-node2 --tail 30
docker logs queue-node1 --tail 30
docker logs cache-node1 --tail 30
docker logs pbft-node1 --tail 30
```

### Stop Semua Container

```powershell
docker-compose -f docker/docker-compose.yml down
```

### Rebuild dari Awal (hapus semua cache dan volume)

```powershell
docker-compose -f docker/docker-compose.yml down -v
docker-compose -f docker/docker-compose.yml up --build -d
```

---

*Link Video Demo YouTube: [https://youtu.be/5RcBFFe24AU]*
