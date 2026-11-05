"""Synthetic full-index CPU/allocation benchmark; no AWS or retained-data reads."""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import platform
import random
import statistics
import time
import tracemalloc
from pathlib import Path

from run_tests import BEFORE, SOURCE, World, module, pages_for, row, timestamp


class BenchWorld(World):
    def __init__(self, pages):
        self.calls = []
        self.writes = []
        self.ddb_writes = []
        self.case = {}
        self.scan_n = 0
        self.pages = pages

    def record(self, name, kwargs):
        pass

    def scan(self, **kwargs):
        result = self.pages[self.scan_n]
        self.scan_n += 1
        return result


def prepare(raw, pages):
    world = BenchWorld(pages)
    mod = module(raw, world, {})
    return mod, world


def benchmark(repetitions=9):
    raw = {"predecessor": BEFORE.read_bytes(), "heap": SOURCE.read_bytes()}
    results = []
    for n, feeds in ((50, 1), (500, 1), (5000, 1), (50000, 1), (200000, 1), (45000, 45)):
        for order in ("ascending", "descending", "shuffle", "ties"):
            items = [row(f"feed#data/{i % feeds}.json", timestamp(i if order != "ties" else i % 11), str(i))
                     for i in range(n)]
            if order == "descending": items.reverse()
            if order == "shuffle": random.Random(42).shuffle(items)
            pages = pages_for(items, max(50, (n + 99) // 100))
            timings = {k: [] for k in raw}
            peaks = {k: [] for k in raw}
            bodies = {}
            for iteration in range(repetitions + 1):
                keys = list(raw) if iteration % 2 == 0 else list(reversed(raw))
                for key in keys:
                    mod, world = prepare(raw[key], pages)
                    with contextlib.redirect_stdout(io.StringIO()):
                        start = time.process_time_ns()
                        mod._build_history_index()
                        elapsed = time.process_time_ns() - start
                    if iteration: timings[key].append(elapsed / 1e6)
                    bodies[key] = world.writes[-1]["Body"]
            assert bodies["predecessor"] == bodies["heap"]
            for _ in range(3):
                for key in raw:
                    mod, world = prepare(raw[key], pages)
                    with contextlib.redirect_stdout(io.StringIO()):
                        tracemalloc.start()
                        mod._build_history_index()
                        peaks[key].append(tracemalloc.get_traced_memory()[1])
                        tracemalloc.stop()
            results.append({"snapshots": n, "feeds": feeds, "order": order,
                "pages": len(pages), "cpu_ms_samples": timings,
                "cpu_ms_median": {k: statistics.median(v) for k, v in timings.items()},
                "traced_peak_bytes_samples": peaks,
                "traced_peak_bytes_median": {k: statistics.median(v) for k, v in peaks.items()}})
    return {"python": platform.python_version(), "platform": platform.platform(),
        "samples": repetitions,
        "scope": "Complete _build_history_index including scan loop, aggregation, finalization and JSON serialization; fake zero-latency scan/S3, fixed wall clock. Compilation, fixture inputs and recording overhead excluded. Timestamp strings remain owned by prebuilt inputs; allocation peaks conservatively measure retained pointers/sort workspace, not freed string bodies, RSS or Lambda memory. Scales/orders are synthetic; actual counts/order and AWS duration/billing unmeasured.",
        "results": results}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=Path, required=True)
    args = parser.parse_args()
    report = benchmark()
    args.json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for r in report["results"]:
        a, b = r["cpu_ms_median"].values()
        old, new = r["traced_peak_bytes_median"].values()
        print(f"N={r['snapshots']:6} feeds={r['feeds']:2} {r['order']:10} CPU {a:.3f} -> {b:.3f}ms peak {old} -> {new} bytes")
