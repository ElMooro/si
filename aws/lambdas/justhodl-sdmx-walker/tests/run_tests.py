"""Offline exact-predecessor differential and handler tests; no AWS access.

The fixture is the full unmodified source at ab0a502c047c1399815c9c081fe633d9dba3d608.
Only clocks, SDK/network boundaries and executor completion are substituted.
Run with --benchmark --json PATH to record local CPU and allocation evidence.
"""
from __future__ import annotations

import argparse
import ast
import copy
import gzip
import hashlib
import io
import itertools
import json
import platform
import random
import statistics
import sys
import time
import tracemalloc
import types
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "source/lambda_function.py"
PREDECESSOR = HERE / "fixtures/predecessor.py.txt"
PREDECESSOR_SHA256 = "b248726becaa8e11a3a225e9b1416d2d05474984b1a031c51bd6e10d2b503c5e"
STAMP = 1790899200
AGENCIES = ("bis", "eurostat", "ecb", "oecd", "statcan")


def load(path):
    module = types.ModuleType("sdmx_test_" + path.stem)
    sdk = types.ModuleType("boto3")
    sdk.client = lambda *a, **k: None
    with patch.dict(sys.modules, {"boto3": sdk}):
        exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    return module


def outcome(fn, ids, prefixes):
    try:
        return ("ok", fn(ids, prefixes))
    except Exception as error:
        return ("error", type(error).__name__, str(error))


def helper_tests(old, new):
    rng = random.Random(6401)
    count = 0
    pools = [[], ["x"], ["CLI", "tail", "cli", "CLI", "tail"],
             [None, False, True, 0, 1, 2.5, "FIN/a", "É"],
             [[], "CLI"], [{"id": "CLI"}], ["CLI", {}],
             [b"CLI", "", "ei_X", "EXR"], [["priority"]]]
    pools += [[rng.choice(["CLI", "cli", "tail", "nama_A", 0, None])
               for _ in range(rng.randrange(35))] for _ in range(160)]
    prefixes = [(), ("CLI",), ("CLI", "nama_"), ("",), ("[", "{"),
                (None,), (1,), None, "CLI"]
    for values, pref, shape, prefix_shape in itertools.product(
            pools, prefixes, (list, tuple, iter), (lambda x: x, iter)):
        if pref is None and prefix_shape is iter:
            continue
        a = outcome(old._order, shape(copy.deepcopy(values)), prefix_shape(pref))
        b = outcome(new._order, shape(copy.deepcopy(values)), prefix_shape(pref))
        assert a == b, (values, pref, shape, prefix_shape, a, b)
        count += 1
    # Inputs without a second pass must not hash a priority ID at all.
    assert new._order(iter([["priority"]]), ("[",)) == [["priority"]]
    assert new._order(iter([{ "priority": True }]), ("{",)) == [{"priority": True}]
    assert new._order([], None) == []
    for ids in (None, 3, True):
        assert outcome(old._order, ids, ()) == outcome(new._order, ids, ())
        count += 1
    # Strings and dicts are reusable iterables too; preserve their existing behavior.
    for ids in ("CLIxyz", {"CLI": 1, "tail": 2}, range(10), set(("CLI", "tail"))):
        assert outcome(old._order, ids, ("CLI",)) == outcome(new._order, ids, ("CLI",))
        count += 1
    # Verify actual construction counts, including no-priority and iterator cases.
    for values, pref, shape, expected in [(["CLI", "x", "CLI"], ("CLI",), list, 1),
                                         (["x", "y"], (), list, 1),
                                         ([], (), list, 0),
                                         (["CLI"], ("CLI",), iter, 0)]:
        calls = []
        def counted_set(items):
            calls.append(tuple(items))
            return set(items)
        with patch.dict(new.__dict__, {"set": counted_set}):
            new._order(shape(values), pref)
        assert len(calls) == expected, calls
    return count


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls.fromtimestamp(STAMP, tz)


class Clock:
    def __init__(self):
        self.value = STAMP
    def time(self):
        return self.value


class Storage:
    def __init__(self, objects, trace, fail_put=None):
        self.objects = copy.deepcopy(objects)
        self.trace = trace
        self.fail_put = fail_put
    def get_object(self, **kwargs):
        self.trace.append(("get", kwargs))
        if kwargs["Key"] not in self.objects:
            raise KeyError(kwargs["Key"])
        return {"Body": io.BytesIO(self.objects[kwargs["Key"]])}
    def put_object(self, **kwargs):
        self.trace.append(("put", kwargs))
        if self.fail_put and self.fail_put in kwargs["Key"]:
            raise RuntimeError("fixture storage failure")
        self.objects[kwargs["Key"]] = kwargs["Body"]


def catalogs():
    values = {"bis": ["Z", "A", "Z"],
              "eurostat": ["tail", "nama_A", "ei_X", "tail", "nama_A"],
              "ecb": ["tail", "BSI", "ECB:EXR", "ICP", "BSI"],
              "oecd": ["tail", "CLI", "MEI", "tail", "CLI"],
              "statcan": ["100", "200", "100"]}
    result = {}
    for agency, ids in values.items():
        key = f"data/warm/{agency}/catalog.json.gz"
        value = {"dataflows": [{"id": fid} for fid in ids]}
        if agency == "statcan":
            key = "data/warm/statcan/cube-catalog.json.gz"
            value = {"payload": [{"productId": fid} for fid in ids]}
        result[key] = gzip.compress(json.dumps(value).encode(), mtime=STAMP)
    result["data/warm/oecd/flow-triplets.json.gz"] = gzip.compress(
        json.dumps({"map": {"CLI": "OECD,CLI,1.0"}}).encode(), mtime=STAMP)
    return result


def run_handler(path, event, mode="normal", states=None, catalog_mode=None):
    module = load(path)
    trace = []
    objects = catalogs()
    if catalog_mode == "empty":
        for key in list(objects):
            if "catalog" in key:
                value = [] if "cube-" in key else {"dataflows": []}
                objects[key] = gzip.compress(json.dumps(value).encode(), mtime=STAMP)
    elif catalog_mode == "malformed":
        for agency in ("eurostat", "ecb", "oecd"):
            objects[f"data/warm/{agency}/catalog.json.gz"] = gzip.compress(
                json.dumps({"dataflows": [{"id": {}}, {"id": "CLI"}]}).encode(), mtime=STAMP)
    elif catalog_mode == "missing":
        objects = {}
    elif catalog_mode == "invalid_json":
        objects["data/warm/eurostat/catalog.json.gz"] = gzip.compress(b"{", mtime=STAMP)
    if mode in ("triplet_fallback", "triplet_failure"):
        objects.pop("data/warm/oecd/flow-triplets.json.gz", None)
    for agency, state in (states or {}).items():
        objects[f"data/_state/sdmx-walk-{agency}.json"] = json.dumps(state).encode()
    storage = Storage(objects, trace, "sdmx-walk-" if mode == "storage_failure" else None)
    clock = Clock()
    module.s3 = storage
    module.datetime = FrozenDateTime
    module.time = clock
    module.gzip = types.SimpleNamespace(
        decompress=gzip.decompress, compress=lambda b: gzip.compress(b, mtime=STAMP))
    def fetch(url, headers=None, timeout=240, cap=None):
        trace.append(("fetch", url, headers, timeout, cap))
        if "getFullTableDownloadCSV" in url:
            pid = url.split("/")[-2]
            return json.dumps({"object": f"https://fixture/{pid}.csv"}).encode(), False
        if "/dataflow/" in url:
            if mode == "triplet_failure":
                raise RuntimeError("fixture triplet resolution failure")
            return b'<Dataflow id="CLI" agencyID="OECD" version="1.0"/>', False
        if mode == "http_failure":
            raise urllib.error.HTTPError(url, 503, "fixture unavailable", {}, None)
        if mode == "negotiate" and headers == {"Accept": "text/csv"}:
            raise urllib.error.HTTPError(url, 406, "fixture negotiation", {}, None)
        if mode == "tiny":
            return b"tiny", False
        if "ec.europa.eu/eurostat" in url:
            return ("freq,unit,geo\\TIME_PERIOD\t2024\t2025\n" +
                    "A,I15,DE\t100\t101\n" * 20).encode(), mode == "truncated"
        return (url + "\n" + "0,fixture\n" * 40).encode(), mode == "truncated"
    module._fetch_capped = fetch
    original_walk = module._walk_generic
    def walk(agency, ids, url_fn, out_prefix, summary, **kwargs):
        trace.append(("walk", agency, copy.deepcopy(ids), out_prefix,
                      {k: v for k, v in kwargs.items() if k != "alt_attempts"},
                      [(f(fid), h) for f, h in kwargs.get("alt_attempts") or [] for fid in ids]))
        return original_walk(agency, ids, url_fn, out_prefix, summary, **kwargs)
    module._walk_generic = walk
    class Future:
        def __init__(self, fn, fid):
            try:
                self.value, self.error = fn(fid), None
            except Exception as error:
                self.value, self.error = None, error
        def result(self):
            if self.error:
                raise self.error
            return self.value
    class Pool:
        def __init__(self, **kwargs):
            trace.append(("pool", kwargs))
        def submit(self, fn, fid):
            trace.append(("submit", fid))
            return Future(fn, fid)
        def shutdown(self, **kwargs):
            trace.append(("shutdown", kwargs))
    def wait(pending, **kwargs):
        trace.append(("wait", len(pending), kwargs))
        if mode == "budget_cut":
            clock.value += 1000
            return set(), pending
        return pending, set()
    sdk = types.ModuleType("boto3")
    sdk.client = lambda *a, **k: types.SimpleNamespace(
        invoke=lambda **kw: trace.append(("invoke", kw)))
    stdout = io.StringIO()
    with patch.dict(sys.modules, {"boto3": sdk}), \
         patch("concurrent.futures.ThreadPoolExecutor", Pool), \
         patch("concurrent.futures.wait", wait), patch("sys.stdout", stdout):
        try:
            result = ("ok", module.lambda_handler(copy.deepcopy(event),
                      types.SimpleNamespace(function_name="justhodl-sdmx-walker")))
        except Exception as error:
            result = ("error", type(error).__name__, str(error))
    return result, trace, storage.objects, stdout.getvalue()


def handler_tests():
    scenarios = [("fanout", None, "normal", {}, None)]
    for agency in AGENCIES:
        event = {"agency": agency}
        scenarios += [(agency + ":" + mode, event, mode, {}, None)
                      for mode in ("normal", "tiny", "http_failure", "truncated", "budget_cut", "storage_failure")]
        scenarios += [(agency + ":catalog:" + mode, event, "normal", {}, mode)
                      for mode in ("empty", "malformed", "missing", "invalid_json")]
        state = {"done": ["CLI", "BSI", "100", "ei_X"],
                 "failures": {"CLI": "HTTPError: fixture", "tail": "ValueError: tiny"},
                 "truncated": ["CLI", "CLI", "tail"], "n_total": 20}
        for flag in ("retry_failures", "retry_truncated", "reset_done"):
            scenarios.append((agency + ":" + flag, {**event, flag: True, "per": 2,
                              "workers": 2, "cap_mb": 80, "budget": 10},
                              "normal", {agency: state}, None))
        scenarios.append((agency + ":lease", event, "normal",
                          {agency: {**state, "lease_until": STAMP + 2000}}, None))
        scenarios.append((agency + ":done", event, "normal", {agency: state}, None))
    for mode in ("normal", "tiny", "http_failure", "truncated", "negotiate"):
        for flags in ({"reset_done": True, "retry_truncated": True},
                      {"retry_failures": True, "retry_truncated": True, "reset_done": True}):
            scenarios.append(("ecb:combined:" + mode + str(flags), {"agency": "ecb", **flags},
                              mode, {"ecb": {"done": ["tail", "BSI"], "truncated": ["BSI", "BSI", "tail"],
                              "failures": {"tail": "HTTPError: fixture"}, "n_total": 9}}, None))
    for mode in ("triplet_fallback", "triplet_failure"):
        # Cache miss, including failure of the provider resolver, remains visible.
        catalog_mode = None
        scenarios.append(("oecd:" + mode, {"agency": "oecd", "retry_failures": True}, mode,
                          {"oecd": {"done": ["CLI"], "failures": {"CLI": "HTTPError: fixture"}}}, catalog_mode))
    scenarios.append(("ecb:negotiate", {"agency": "ecb"}, "negotiate", {}, None))
    scenarios.append(("unknown-agency", {"agency": "unknown"}, "normal", {}, None))
    fingerprints = {}
    for label, event, mode, states, catalog_mode in scenarios:
        a = run_handler(PREDECESSOR, event, mode, states, catalog_mode)
        b = run_handler(SOURCE, event, mode, states, catalog_mode)
        assert a == b, label
        # Hash the full observation, including every output byte and write option.
        fingerprints[label] = hashlib.sha256(repr(b).encode()).hexdigest()
    assert any(t[0] == "submit" for t in run_handler(SOURCE, {"agency": "eurostat"})[1])
    return fingerprints


def consumer_tests():
    root = HERE.parents[3]
    def functions(path, names, scope):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tree.body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        assert len(tree.body) == len(names)
        exec(compile(tree, str(path), "exec"), scope)
        return scope
    extractor = functions(root / 'aws/lambdas/justhodl-series-extractor/source/lambda_function.py',
                          ('_flow_id_from_key', 'extract_eurostat'), {'gzip': gzip, 'io': io})
    sentinel = functions(root / 'aws/lambdas/justhodl-import-sentinel/source/lambda_function.py',
                        ('age_min', 'classify_sdmx'), {'datetime': FrozenDateTime, 'timezone': timezone})
    observations = []
    for path in (PREDECESSOR, SOURCE):
        _, trace, _, _ = run_handler(path, {'agency': 'eurostat'})
        writes = [row[1] for row in trace if row[0] == 'put']
        rows = []
        states = []
        for item in writes:
            if item['Key'].endswith('.dat.gz'):
                fid = extractor['_flow_id_from_key'](item['Key'])
                rows.extend(extractor['extract_eurostat'](item['Body'], fid, {}))
            elif 'data/_state/' in item['Key']:
                states.append(sentinel['classify_sdmx']('eurostat', json.loads(item['Body'])))
        assert rows and all(row['last_value'] == 101 and row['geo'] == 'DE' for row in rows)
        assert states[-1][0] == 'COMPLETE'
        observations.append((rows, states))
    assert observations[0] == observations[1]
    return {'series_extractor_rows': len(observations[0][0]),
            'import_sentinel_states': len(observations[0][1])}


def benchmarks(old, new):
    rows = []
    for size, fraction in itertools.product((0, 1, 10, 100, 1000, 14000), (0, 0.1, 0.5, 1)):
        priority = int(size * fraction)
        ids = [f"CLI_{i}" if i < priority else f"tail_{i}" for i in range(size)]
        rng = random.Random(23)
        rng.shuffle(ids)
        assert old._order(ids, ("CLI",)) == new._order(ids, ("CLI",))
        measurements, peaks = {}, {}
        for label, module in (("before", old), ("after", new)):
            t0 = time.process_time_ns()
            module._order(ids, ("CLI",))
            estimate = max(1, time.process_time_ns() - t0)
            repeats = max(1, min(10000, int(80_000_000 / estimate)))
            samples = []
            for _ in range(5):
                t0 = time.process_time_ns()
                for _ in range(repeats):
                    module._order(ids, ("CLI",))
                samples.append((time.process_time_ns() - t0) / repeats)
            measurements[label] = statistics.median(samples)
            tracemalloc.start()
            module._order(ids, ("CLI",))
            peaks[label] = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
        rows.append({"ids": size, "priority_ids": priority,
                     "cpu_ns": measurements, "traced_peak_bytes": peaks,
                     "set_constructions": {"before": size, "after": int(size > 0)},
                     "speedup": round(measurements["before"] / measurements["after"], 3)})
    return {"python": platform.python_version(), "platform": platform.platform(),
            "clock": "process_time_ns", "samples": 5, "rows": rows,
            "limits": "Local synthetic CPU timings and traced peak allocation only; no AWS duration or bills measured. Peak allocation is not cumulative allocation."}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    assert hashlib.sha256(PREDECESSOR.read_bytes()).hexdigest() == PREDECESSOR_SHA256
    old, new = load(PREDECESSOR), load(SOURCE)
    # Keep the production scope mechanically limited to this one helper.
    def outside_order(path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tree.body = [node for node in tree.body if not isinstance(node, ast.FunctionDef) or node.name != "_order"]
        return ast.dump(tree)
    assert outside_order(PREDECESSOR) == outside_order(SOURCE)
    helper_count = helper_tests(old, new)
    handlers = handler_tests()
    consumers = consumer_tests()
    result = {"predecessor_commit": "ab0a502c047c1399815c9c081fe633d9dba3d608",
              "predecessor_sha256": PREDECESSOR_SHA256,
              "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
              "helper_comparisons": helper_count, "handler_scenarios": len(handlers),
              "handler_fingerprints": handlers, "consumer_checks": consumers, "passed": True}
    if args.benchmark:
        result["benchmark"] = benchmarks(old, new)
    if args.json:
        args.json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"SDMX predecessor differential passed: {helper_count} helper comparisons, {len(handlers)} full-handler scenarios")
    if args.benchmark:
        for row in result["benchmark"]["rows"]:
            print(f"n={row['ids']} priority={row['priority_ids']}: {row['speedup']}x CPU; peak={row['traced_peak_bytes']}")


if __name__ == "__main__":
    main()
