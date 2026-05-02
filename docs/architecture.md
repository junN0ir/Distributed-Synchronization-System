# Arsitektur Sistem Distributed Synchronization

## Gambaran Umum

Sistem terdiri dari tiga subsistem independen yang berjalan dalam container Docker dan saling terhubung melalui jaringan internal `sync-net`.

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

## Komponen

### 1. Distributed Lock Manager (Raft Consensus)

Mengimplementasikan algoritma Raft untuk distributed lock dengan tiga fase:
- **Leader Election**: Node dengan timeout paling cepat memulai election
- **Log Replication**: Leader mereplikasi log ke semua follower
- **Commit**: Entry di-commit setelah majority (quorum) mengkonfirmasi

**Shared vs Exclusive Lock:**
- Exclusive: hanya satu holder, blokir semua request lain
- Shared: multiple reader boleh memegang bersamaan, blokir exclusive

**Deadlock Detection**: Menggunakan directed graph (wait-for graph). Jika ditemukan cycle, request ditolak dengan status deadlock_detected.

### 2. Distributed Queue (Consistent Hashing)

Menggunakan consistent hashing dengan 150 virtual nodes per physical node untuk distribusi merata. Redis digunakan sebagai persistent backend sehingga data tidak hilang saat node restart.

**At-least-once delivery**: Publisher dapat mengirim ulang jika tidak ada ACK. Consumer bertanggung jawab melakukan idempotent processing.

### 3. Cache Coherence (MESI Protocol)

Empat state per cache entry:
- **M (Modified)**: Data dimodifikasi lokal, belum ditulis ke memory
- **E (Exclusive)**: Hanya node ini yang punya copy, sinkron dengan memory
- **S (Shared)**: Multiple nodes punya copy yang sama
- **I (Invalid)**: Data tidak valid, harus fetch ulang

Saat node menulis, semua peer di-broadcast pesan invalidate sehingga mereka mengubah state ke Invalid.

## Algoritma

### Raft Leader Election
1. Node mulai sebagai Follower
2. Jika tidak menerima heartbeat dalam election timeout (150-300ms random), jadi Candidate
3. Candidate increment term, vote untuk diri sendiri, broadcast RequestVote
4. Jika mendapat majority vote → jadi Leader
5. Leader kirim heartbeat setiap 50ms untuk mencegah election baru

### MESI State Transitions
- Read hit: state tetap (S atau E)
- Read miss: fetch data, set state E (exclusive jika hanya node ini)
- Write: set state M, broadcast Invalidate ke semua peer
- Receive Invalidate: set state I

## API Documentation

Lihat file `docs/api_spec.yaml` untuk OpenAPI specification lengkap.

## Deployment

Lihat file `docs/deployment_guide.md` untuk panduan deployment.