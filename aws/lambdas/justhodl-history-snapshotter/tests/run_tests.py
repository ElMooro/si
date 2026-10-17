"""Offline differential checks against the byte-qualified complete predecessor."""
from __future__ import annotations

import ast
import contextlib
import copy
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import types
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "source/lambda_function.py"
BEFORE = HERE / "fixtures/predecessor.py.txt"
STAMP = datetime(2026, 10, 2, 3, 2, tzinfo=timezone.utc)


class FakeClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class World:
    """Every AWS call, response-body read and output is retained locally."""
    def __init__(self, case):
        self.case = copy.deepcopy(case)
        self.calls = []
        self.writes = []
        self.ddb_writes = []
        self.scan_n = 0

    def record(self, name, kwargs):
        self.calls.append((name, copy.deepcopy(kwargs)))

    def client(self, service, **kwargs):
        return self

    resource = client

    def Table(self, name):
        self.record("Table", {"name": name})
        return self

    def load(self):
        self.record("load", {})
        if self.case.get("table_error"):
            raise FakeClientError(self.case["table_error"])

    def create_table(self, **kwargs):
        self.record("create_table", kwargs)

    def get_waiter(self, name):
        self.record("get_waiter", {"name": name})
        return self

    def wait(self, **kwargs):
        self.record("wait", kwargs)

    def update_time_to_live(self, **kwargs):
        self.record("update_time_to_live", kwargs)
        if self.case.get("ttl_error"):
            raise RuntimeError("fixture TTL failure")

    def get_object(self, **kwargs):
        self.record("get_object", kwargs)
        key = kwargs["Key"]
        assert not key.startswith("history/archive/"), "no archive reads"
        data = self.case.get("objects", {}).get(key, FakeClientError("NoSuchKey"))
        if isinstance(data, Exception):
            raise data
        world = self

        class Body:
            def read(self):
                world.record("body.read", {"key": key})
                if world.case.get("body_error") == key:
                    raise RuntimeError("fixture body failure")
                return data

        return {"Body": Body(), "ETag": '"fixture-etag"'}

    def query(self, **kwargs):
        self.record("query", kwargs)
        if self.case.get("query_error"):
            raise RuntimeError("fixture query failure")
        pk = kwargs["ExpressionAttributeValues"][":pk"]["S"]
        h = self.case.get("hashes", {}).get(pk)
        return {"Items": [] if h is None else [{"content_hash": {"S": h}}]}

    def put_item(self, **kwargs):
        self.record("put_item", kwargs)
        if self.case.get("put_error"):
            raise RuntimeError("fixture put failure")
        self.ddb_writes.append(copy.deepcopy(kwargs))

    def put_object(self, **kwargs):
        self.record("put_object", kwargs)
        if kwargs["Key"] in self.case.get("s3_write_errors", []):
            raise RuntimeError("fixture S3 write failure")
        self.writes.append(copy.deepcopy(kwargs))

    def scan(self, **kwargs):
        self.record("scan", kwargs)
        self.scan_n += 1
        if self.scan_n == self.case.get("scan_error"):
            raise RuntimeError("fixture scan failure")
        pages = self.case.get("pages")
        if pages is None:
            pages = [{"Items": self.case.get("items", []) +
                      [x["Item"] for x in self.ddb_writes]}]
        if self.scan_n > len(pages):
            raise RuntimeError("fixture pagination overrun")
        return copy.deepcopy(pages[self.scan_n - 1])


def module(raw, world, case):
    boto = types.ModuleType("boto3")
    boto.client, boto.resource = world.client, world.resource
    core = types.ModuleType("botocore")
    exceptions = types.ModuleType("botocore.exceptions")
    exceptions.ClientError = FakeClientError
    mod = types.ModuleType("snapshotter_fixture")
    with patch.dict(sys.modules, {"boto3": boto, "botocore": core,
                                  "botocore.exceptions": exceptions}), patch.dict(
            os.environ, {"S3_BUCKET": "fixture-bucket", "DDB_TABLE": "fixture-history", "TTL_DAYS": "365"}):
        exec(compile(raw, "snapshotter_fixture.py", "exec"), mod.__dict__)

    class FixedDate:
        calls = 0

        @classmethod
        def now(cls, tz=None):
            clocks = case.get("clocks", [STAMP])
            value = clocks[min(cls.calls, len(clocks) - 1)]
            cls.calls += 1
            return value

    # Fixed clocks make duration, hourly gating, snapshot SK/TTL and gzip
    # header bytes comparable. No live network or AWS client exists here.
    mod.datetime = FixedDate
    mod.time = types.SimpleNamespace(time=lambda: 1000.0)
    mod.gzip = types.SimpleNamespace(compress=lambda data, compresslevel=6:
                                    gzip.compress(data, compresslevel=compresslevel, mtime=0))
    mod.FEEDS_TO_SNAPSHOT = case.get("feeds", [])
    return mod


def run(raw, case, handler=False):
    world = World(case)
    output = io.StringIO()
    failure = None
    value = None
    with contextlib.redirect_stdout(output):
        try:
            mod = module(raw, world, case)
            value = mod.lambda_handler() if handler else mod._build_history_index()
        except Exception as exc:
            failure = (type(exc).__name__, str(exc))
    return {"value": value, "failure": failure, "stdout": output.getvalue(),
            "calls": world.calls, "writes": world.writes,
            "ddb_writes": world.ddb_writes}


def row(pk, sk, h="fixture-hash"):
    return {"pk": {"S": pk}, "sk": {"S": sk}, "content_hash": {"S": h}}


def timestamp(i):
    # Lexical strings, including ISO-looking values outside calendar validity,
    # remain unparsed just as in the predecessor.
    return f"2026-10-02T{i // 3600:02}:{i // 60 % 60:02}:{i % 60:02}Z"


def pages_for(items, size=37):
    chunks = [items[i:i + size] for i in range(0, len(items), size)] or [[]]
    return [{"Items": chunk, **({"LastEvaluatedKey": {"pk": {"S": f"cursor-{i}"}}}
             if i + 1 < len(chunks) else {})} for i, chunk in enumerate(chunks)]


def assert_equivalent(case, before, after, handler=False):
    a, b = run(before, case, handler), run(after, case, handler)
    assert a == b, f"differential mismatch: {case.get('label')} handler={handler}"
    return a


def validate():
    before, after = BEFORE.read_bytes(), SOURCE.read_bytes()
    metadata = json.loads((HERE / "fixtures/predecessor.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(before).hexdigest() == metadata["sha256"]
    old, new = ast.parse(before), ast.parse(after)
    def outside(tree):
        return [ast.dump(n, include_attributes=False) for n in tree.body
                if not (isinstance(n, ast.FunctionDef) and n.name == "_build_history_index")
                and not (isinstance(n, ast.Import) and [x.name for x in n.names] == ["heapq"])]
    assert outside(old) == outside(new), "unrelated production source changed"
    cases = []
    # Exact boundary sizes, scan order, duplicate/tied strings, Unicode and
    # insertion order of equally tracked feeds; reproducible seeded cases.
    for n in (0, 1, 2, 49, 50, 51, 100, 101, 500, 1000, 10000):
        for order in ("ascending", "descending", "shuffle", "ties"):
            items = [row("feed#data/a.json", timestamp(i if order != "ties" else i % 9), str(i))
                     for i in range(n)]
            if order == "descending": items.reverse()
            if order == "shuffle": random.Random(41 + n).shuffle(items)
            cases.append({"label": f"{n}-{order}", "pages": pages_for(items, max(37, (n + 99) // 100))})
    for seed in range(150):
        rng = random.Random(seed)
        items = [row(rng.choice(["feed#data/a.json", "plain", "feed#data/empty.json"]),
                     rng.choice([timestamp(rng.randrange(80)), "é", "☃", "bad-date", "0"]),
                     str(i)) for i in range(rng.randrange(250))]
        cases.append({"label": f"random-{seed}", "pages": pages_for(items, rng.randrange(1, 20))})
    malformed = [None, "", 0, False, True, 2, -1, 2.5, float("nan"), float("inf"), [], [1], {}, {"x": 1}]
    for value in malformed:
        for n in (1, 51, 101):
            cases.append({"label": f"homogeneous-malformed-{value!r}-{n}",
                          "items": [row("feed#x", copy.deepcopy(value)) for _ in range(n)]})
        for prefix in ([], [row("feed#x", "a")], [row("feed#x", "a")] * 70):
            cases.append({"label": f"mixed-malformed-{value!r}-{len(prefix)}",
                          "items": prefix + [row("feed#x", value)]})
        cases.append({"label": "malformed-pk", "items": [row(value, "a")]})
    cases.extend([
        {"label": "missing-fields", "items": [{}, {"pk": {}}, {"sk": {}},
            {"pk": {"N": "1"}, "sk": {"S": "a"}},
            {"pk": {"S": "feed#x"}, "sk": {"N": "1"}},
            {"pk": {"S": "feed#x"}, "sk": {"S": "a"}}, row("feed#x", "a", "tie")]},
        {"label": "malformed-field-containers", "items": [{"pk": None}]},
        {"label": "malformed-sk-container", "items": [{"pk": {"S": "feed#x"}, "sk": None}]},
        {"label": "malformed-hash-container", "items": [row("feed#x", "a") | {"content_hash": None}]},
        {"label": "missing-items", "pages": [{}]},
        {"label": "null-items", "pages": [{"Items": None}]},
        {"label": "empty-page-pagination", "pages": [
            {"Items": [], "LastEvaluatedKey": {"pk": {"S": "cursor"}}},
            {"Items": [row("feed#x", "b")]}]},
        {"label": "page-cutoff", "pages": [
            {"Items": [row("feed#x", timestamp(i))],
             "LastEvaluatedKey": {"pk": {"S": str(i)}}} for i in range(103)]},
        {"label": "scan-failure-first", "scan_error": 1},
        {"label": "scan-failure-later", "pages": pages_for([row("feed#x", "a")] * 90), "scan_error": 2},
        {"label": "index-write-failure", "s3_write_errors": ["data/history-index.json"]},
    ])
    for case in cases:
        assert_equivalent(case, before, after)
    cutoff = run(after, next(x for x in cases if x["label"] == "page-cutoff"))
    assert len([c for c in cutoff["calls"] if c[0] == "scan"]) == 101
    tie = run(after, {"items": [row("feed#x", "z", "first"), row("feed#x", "z", "second")]})
    feed = json.loads(tie["writes"][0]["Body"])["feeds"][0]
    assert feed["latest_hash"] == "first" and feed["recent_timestamps"] == ["z", "z"]
    bound_world = World({"items": [row("feed#x", timestamp(i)) for i in range(10000)]})
    bound_mod = module(after, bound_world, {})
    heap_sizes = []
    import heapq
    def measure(function):
        def wrapped(heap, value):
            result = function(heap, value)
            heap_sizes.append(len(heap))
            return result
        return wrapped
    bound_mod.heapq = types.SimpleNamespace(heappush=measure(heapq.heappush),
                                            heapreplace=measure(heapq.heapreplace))
    with contextlib.redirect_stdout(io.StringIO()):
        bound_mod._build_history_index()
    assert max(heap_sizes) == 50
    # Complete handlers exercise every source operation, output body/options,
    # heartbeat, index and exception path. Large archive writes are fake only.
    body = b'{"generated_at":"fixture", "value":3}'
    feeds = ["data/a.json", "data/b.json", "data/missing.json"]
    handler_cases = []
    for case in cases:
        handler_cases.append(case | {"feeds": feeds, "objects": {feeds[0]: body, feeds[1]: body},
                                     "hashes": {"feed#" + feeds[1]: hashlib.sha256(body).hexdigest()}})
    for flags in (
        {"query_error": True}, {"put_error": True}, {"table_error": "AccessDenied"},
        {"table_error": "ResourceNotFoundException"},
        {"table_error": "ResourceNotFoundException", "ttl_error": True},
        {"body_error": feeds[0]}, {"s3_write_errors": ["data/history-snapshotter-status.json"]},
        {"objects": {feeds[0]: FakeClientError("AccessDenied"), feeds[1]: body}},
        {"objects": {feeds[0]: b"\xff\xfe"}},
        {"objects": {feeds[0]: b"a" * 110000}},
        {"objects": {feeds[0]: random.Random(13).randbytes(450000)}},
        {"objects": {feeds[0]: random.Random(13).randbytes(450000)},
         "s3_write_errors": ["history/archive/feed/data/a.json/2026-10-02T03:02:00Z.gz"]},
        {"clocks": [STAMP.replace(minute=5)]},
        {"clocks": [STAMP.replace(minute=4), STAMP.replace(minute=4),
                    STAMP.replace(minute=4), STAMP.replace(minute=5)]},
        {"feeds": [], "objects": {}},
    ):
        handler_cases.append({"label": "handler-path", "feeds": feeds,
                              "objects": {feeds[0]: body, feeds[1]: body}} | flags)
    for case in handler_cases:
        assert_equivalent(case, before, after, handler=True)
    root = HERE.parents[3]
    sys.path.insert(0, str(root / "aws/shared"))
    api_source = (root / "aws/lambdas/justhodl-history-api/source/lambda_function.py").read_bytes()
    consumer_cases = []
    for case in (cases[0], cases[39], cases[43], cases[60]):
        a, b = run(before, case), run(after, case)
        bodies = [x["writes"][0]["Body"] for x in (a, b)]
        responses = []
        for body in bodies:
            api_case = {"objects": {"data/history-index.json": body}}
            api_world = World(api_case)
            api = module(api_source, api_world, api_case)
            responses.append((api.handle_index(), api_world.calls))
        assert responses[0] == responses[1]
        assert responses[0][0]["statusCode"] == 200
        assert json.loads(responses[0][0]["body"]) == json.loads(bodies[0])
        consumer_cases.append([json.loads(body) for body in bodies])
    subprocess.run(["node", str(HERE / "consumer.cjs")],
                   input=json.dumps(consumer_cases), text=True, check=True)
    result = {"predecessor_commit": metadata["commit"],
              "predecessor_sha256": metadata["sha256"],
              "candidate_sha256": hashlib.sha256(after).hexdigest(),
              "index_cases": len(cases), "handler_cases": len(handler_cases),
              "actual_api_and_audit_consumer_cases": len(consumer_cases),
              "exact_comparisons": "return, exception type/message, stdout, ordered AWS calls and complete write bytes/options",
              "network_calls": 0, "aws_calls": 0}
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    validate()
