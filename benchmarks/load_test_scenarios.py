
import sys
import time
import json
import urllib.request
import urllib.error
import statistics

BASE_LOCK  = "http://localhost:8002"   # lock-node2 = leader
BASE_QUEUE = "http://localhost:8011"
BASE_CACHE = "http://localhost:8021"


def http_post(url: str, body: dict) -> tuple:
    start = time.perf_counter()
    try:
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            elapsed = time.perf_counter() - start
            return r.status, elapsed
    except Exception as e:
        return 0, time.perf_counter() - start


def http_put(url: str, body: dict) -> tuple:
    start = time.perf_counter()
    try:
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
            method="PUT"
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            elapsed = time.perf_counter() - start
            return r.status, elapsed
    except Exception as e:
        return 0, time.perf_counter() - start


def http_get(url: str) -> tuple:
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            elapsed = time.perf_counter() - start
            return r.status, elapsed
    except Exception as e:
        return 0, time.perf_counter() - start

def benchmark_queue(n: int = 200):
    print(f"\n{'='*55}")
    print(f"  BENCHMARK: Distributed Queue (n={n})")
    print(f"{'='*55}")

    # --- Publish ---
    pub_lats = []
    errors = 0
    start_total = time.time()
    for i in range(n):
        status, lat = http_post(f"{BASE_QUEUE}/queue/publish", {
            "topic": "bench-topic",
            "message": {"seq": i, "data": f"payload-{i}"},
            "producer": "benchmark"
        })
        pub_lats.append(lat)
        if status != 200:
            errors += 1

    elapsed = time.time() - start_total
    throughput = n / elapsed if elapsed > 0 else 0
    sorted_pub = sorted(pub_lats)

    print(f"\n  [Publish {n} messages]")
    print(f"  Total time  : {elapsed:.2f}s")
    print(f"  Throughput  : {throughput:.1f} msg/s")
    print(f"  Errors      : {errors}")
    print(f"  Latency p50 : {statistics.median(pub_lats)*1000:.1f} ms")
    print(f"  Latency p95 : {sorted_pub[int(n*0.95)]*1000:.1f} ms")
    print(f"  Latency p99 : {sorted_pub[int(n*0.99)]*1000:.1f} ms")

    # --- Consume ---
    con_lats = []
    errors_con = 0
    start_total = time.time()
    for i in range(n):
        status, lat = http_post(f"{BASE_QUEUE}/queue/consume", {
            "topic": "bench-topic",
            "consumer_id": "bench-consumer"
        })
        con_lats.append(lat)
        if status != 200:
            errors_con += 1

    elapsed = time.time() - start_total
    sorted_con = sorted(con_lats)

    print(f"\n  [Consume {n} messages]")
    print(f"  Total time  : {elapsed:.2f}s")
    print(f"  Throughput  : {n/elapsed:.1f} msg/s")
    print(f"  Errors      : {errors_con}")
    print(f"  Latency p50 : {statistics.median(con_lats)*1000:.1f} ms")
    print(f"  Latency p95 : {sorted_con[int(n*0.95)]*1000:.1f} ms")


def benchmark_cache(n: int = 200):
    print(f"\n{'='*55}")
    print(f"  BENCHMARK: Distributed Cache MESI (n={n})")
    print(f"{'='*55}")

    # --- Write ---
    write_lats = []
    errors_w = 0
    for i in range(n):
        status, lat = http_put(
            f"{BASE_CACHE}/cache/bench-key-{i % 50}",
            {"value": {"data": f"value-{i}", "ts": time.time()}}
        )
        write_lats.append(lat)
        if status != 200:
            errors_w += 1

    sorted_w = sorted(write_lats)
    print(f"\n  [Write {n} entries ke cache-node1]")
    print(f"  Errors      : {errors_w}")
    print(f"  Latency p50 : {statistics.median(write_lats)*1000:.1f} ms")
    print(f"  Latency p95 : {sorted_w[int(n*0.95)]*1000:.1f} ms")
    print(f"  Latency p99 : {sorted_w[int(n*0.99)]*1000:.1f} ms")

    read_lats = []
    hits = 0
    for i in range(n):
        status, lat = http_get(f"{BASE_CACHE}/cache/bench-key-{i % 50}")
        read_lats.append(lat)
        if status == 200:
            hits += 1

    sorted_r = sorted(read_lats)
    print(f"\n  [Read {n} entries dari cache-node1]")
    print(f"  Hit rate    : {hits/n*100:.1f}%")
    print(f"  Latency p50 : {statistics.median(read_lats)*1000:.1f} ms")
    print(f"  Latency p95 : {sorted_r[int(n*0.95)]*1000:.1f} ms")

    read_lats2 = []
    miss2 = 0
    for i in range(min(50, n)):
        status, lat = http_get(f"http://localhost:8022/cache/bench-key-{i % 50}")
        read_lats2.append(lat)

    print(f"\n  [Read {min(50,n)} entries dari cache-node2 (setelah invalidate)]")
    print(f"  Latency p50 : {statistics.median(read_lats2)*1000:.1f} ms")
    print(f"  (Node2 harus MISS karena node1 broadcast invalidate)")

    # --- Stats summary ---
    try:
        with urllib.request.urlopen(f"{BASE_CACHE}/cache/stats/summary", timeout=3) as r:
            stats = json.loads(r.read())
            print(f"\n  [Cache Stats Node1]")
            print(f"  Hit rate    : {stats.get('hit_rate', 0)*100:.1f}%")
            print(f"  Cache size  : {stats.get('size', 0)} entries")
    except Exception as e:
        print(f"  Stats error : {e}")


def benchmark_lock(n: int = 50):
    print(f"\n{'='*55}")
    print(f"  BENCHMARK: Distributed Lock Raft (n={n})")
    print(f"{'='*55}")

    acq_lats = []
    rel_lats = []
    success = 0
    errors = 0

    for i in range(n):
        # Acquire
        status, lat = http_post(f"{BASE_LOCK}/lock/acquire", {
            "key": f"bench-lock-{i}",
            "holder": f"client-{i}",
            "type": "exclusive"
        })
        acq_lats.append(lat)
        if status == 200:
            success += 1
        else:
            errors += 1

        # Release
        status2, lat2 = http_post(f"{BASE_LOCK}/lock/release", {
            "key": f"bench-lock-{i}",
            "holder": f"client-{i}"
        })
        rel_lats.append(lat2)

    sorted_acq = sorted(acq_lats)
    sorted_rel = sorted(rel_lats)

    print(f"\n  [Acquire {n} locks (exclusive)]")
    print(f"  Success     : {success}/{n} ({success/n*100:.1f}%)")
    print(f"  Errors      : {errors}")
    print(f"  Latency p50 : {statistics.median(acq_lats)*1000:.1f} ms")
    print(f"  Latency p95 : {sorted_acq[int(n*0.95)]*1000:.1f} ms")
    print(f"  Latency p99 : {sorted_acq[int(n*0.99)]*1000:.1f} ms")

    print(f"\n  [Release {n} locks]")
    print(f"  Latency p50 : {statistics.median(rel_lats)*1000:.1f} ms")
    print(f"  Latency p95 : {sorted_rel[int(n*0.95)]*1000:.1f} ms")

    # Test contention — dua client berebut lock yang sama
    print(f"\n  [Contention Test — 2 client berebut 1 lock]")
    http_post(f"{BASE_LOCK}/lock/acquire", {"key": "shared-res", "holder": "client-X", "type": "exclusive"})
    status3, _ = http_post(f"{BASE_LOCK}/lock/acquire", {"key": "shared-res", "holder": "client-Y", "type": "exclusive"})
    print(f"  Client-Y acquire saat lock dipegang client-X: status={status3} (harus 200, granted=false)")
    http_post(f"{BASE_LOCK}/lock/release", {"key": "shared-res", "holder": "client-X"})
    status4, _ = http_post(f"{BASE_LOCK}/lock/acquire", {"key": "shared-res", "holder": "client-Y", "type": "exclusive"})
    print(f"  Client-Y acquire setelah client-X release: status={status4} (harus 200, granted=true)")
    http_post(f"{BASE_LOCK}/lock/release", {"key": "shared-res", "holder": "client-Y"})


def check_connectivity():
    """Cek apakah semua node bisa diakses."""
    print("="*55)
    print("  CEK KONEKSI KE SEMUA NODE")
    print("="*55)
    endpoints = [
        ("Lock Node 1 (follower)", "http://localhost:8001/health"),
        ("Lock Node 2 (leader)",   "http://localhost:8002/health"),
        ("Lock Node 3 (follower)", "http://localhost:8003/health"),
        ("Queue Node 1",           "http://localhost:8011/health"),
        ("Queue Node 2",           "http://localhost:8012/health"),
        ("Cache Node 1",           "http://localhost:8021/health"),
        ("Cache Node 2",           "http://localhost:8022/health"),
        ("Cache Node 3",           "http://localhost:8023/health"),
    ]
    all_ok = True
    for name, url in endpoints:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                data = json.loads(r.read())
                print(f"  ✓ {name}: {data.get('status','ok')} (uptime={data.get('uptime',0):.0f}s)")
        except Exception as e:
            print(f"  ✗ {name}: GAGAL — {e}")
            all_ok = False
    return all_ok

_running_via_locust = "locust" in sys.argv[0].lower() or any("locust" in a for a in sys.argv)

if _running_via_locust:
    try:
        from locust import HttpUser, task, between

        class LockUser(HttpUser):
            wait_time = between(0.1, 0.5)
            host = BASE_LOCK  # sudah mengarah ke port 8002 (leader)

            @task(3)
            def acquire_and_release(self):
                """Acquire lalu langsung release — hindari 400 not_leader."""
                key = f"resource-{id(self) % 10}"
                holder = f"user-{id(self)}"
                
                # Acquire
                r = self.client.post("/lock/acquire", json={
                    "key": key,
                    "holder": holder,
                    "type": "exclusive"
                })
                
                # Hanya release jika acquire berhasil (granted=true)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("granted"):
                        self.client.post("/lock/release", json={
                            "key": key,
                            "holder": holder,
                        })

            @task(1)
            def check_status(self):
                self.client.get("/lock/status")

        class QueueUser(HttpUser):
            wait_time = between(0.05, 0.2)
            host = BASE_QUEUE

            @task(4)
            def publish(self):
                self.client.post("/queue/publish", json={
                    "topic": f"topic-{id(self) % 5}",
                    "message": {"data": f"msg-{id(self)}"},
                    "producer": f"producer-{id(self)}",
                })

            @task(3)
            def consume(self):
                self.client.post("/queue/consume", json={
                    "topic": f"topic-{id(self) % 5}",
                    "consumer_id": f"consumer-{id(self)}",
                })

        class CacheUser(HttpUser):
            wait_time = between(0.05, 0.1)
            host = BASE_CACHE

            @task(5)
            def cache_read(self):
                self.client.get(f"/cache/key-{id(self) % 20}")

            @task(2)
            def cache_write(self):
                self.client.put(
                    f"/cache/key-{id(self) % 20}",
                    json={"value": f"value-{id(self)}"}
                )

    except ImportError:
        pass

if __name__ == "__main__":
    print("\n" + "="*55)
    print("  Distributed Sync System — Manual Benchmark")
    print("  Junnior Marcellino Polla — 11231034")
    print("="*55)

    ok = check_connectivity()
    if not ok:
        print("\n[ERROR] Beberapa node tidak bisa diakses!")
        print("Jalankan dulu: docker-compose -f docker/docker-compose.yml up -d")
        sys.exit(1)

    print("\n[INFO] Semua node OK. Memulai benchmark...\n")

    benchmark_queue(200)
    benchmark_cache(200)
    benchmark_lock(50)

    print(f"\n{'='*55}")
    print("  Benchmark selesai!")
    print(f"{'='*55}\n")