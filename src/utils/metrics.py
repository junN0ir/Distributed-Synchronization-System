import time
import asyncio
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self):
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = defaultdict(float)
        self._histograms: dict[str, list[float]] = defaultdict(list)
        self._start_time = time.time()

    def increment(self, name: str, value: int = 1):
        self._counters[name] += value

    def set_gauge(self, name: str, value: float):
        self._gauges[name] = value

    def record(self, name: str, value: float):
        self._histograms[name].append(value)
        if len(self._histograms[name]) > 10000:
            self._histograms[name] = self._histograms[name][-10000:]

    def get_stats(self) -> dict:
        stats = {
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {},
        }
        for name, values in self._histograms.items():
            if values:
                sorted_v = sorted(values)
                n = len(sorted_v)
                stats["histograms"][name] = {
                    "count": n,
                    "min": round(sorted_v[0], 4),
                    "max": round(sorted_v[-1], 4),
                    "avg": round(sum(sorted_v) / n, 4),
                    "p50": round(sorted_v[int(n * 0.50)], 4),
                    "p95": round(sorted_v[int(n * 0.95)], 4),
                    "p99": round(sorted_v[int(n * 0.99)], 4),
                }
        return stats

    def time_it(self, name: str):
        return _Timer(self, name)


class _Timer:
    def __init__(self, metrics: MetricsCollector, name: str):
        self.metrics = metrics
        self.name = name
        self.start = None

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        elapsed = time.perf_counter() - self.start
        self.metrics.record(self.name, elapsed)


metrics = MetricsCollector()