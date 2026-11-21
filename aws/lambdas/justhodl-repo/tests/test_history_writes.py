"""Write-only-on-change of the repo history files (2026-11-21).  Network-free: a stub bucket that counts every call."""
import hashlib
import json
import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))
import history_writes as hw  # noqa: E402


class Bucket:
    def __init__(self, objects=None, page=1000, etag=None, fail_list=False):
        self.objects = dict(objects or {})
        self.page, self.etag, self.fail_list = page, etag or (lambda b: '"%s"' % hashlib.md5(b).hexdigest()), fail_list
        self.puts, self.lists = [], 0

    def list_objects_v2(self, **kw):
        self.lists += 1
        if self.fail_list:
            raise RuntimeError("AccessDenied")
        keys = sorted(k for k in self.objects if k.startswith(kw["Prefix"]))
        start = int(kw.get("ContinuationToken") or 0)
        chunk = keys[start:start + min(self.page, kw.get("MaxKeys", 1000))]
        out = {"Contents": [{"Key": k, "ETag": self.etag(self.objects[k])} for k in chunk]}
        if start + len(chunk) < len(keys):
            out["NextContinuationToken"] = str(start + len(chunk))
        return out

    def put_object(self, **kw):
        assert kw["ContentType"] == "application/json" and kw["CacheControl"] == "public, max-age=1800" and set(kw) == {"Bucket", "Key", "Body", "ContentType", "CacheControl"}
        self.puts.append(kw["Key"])
        self.objects[kw["Key"]] = kw["Body"]


def body(sid, n):
    return json.dumps({"id": sid, "label": sid, "dates": ["2026-11-%02d" % (d + 1) for d in range(n)], "values": [float(d) for d in range(n)]}, separators=(",", ":")).encode()


def run_once(bucket, series, log=None):
    etags = hw.history_etags(bucket, "b", log=log or (lambda *_: None))
    stats = {"written": 0, "unchanged": 0}
    for sid, n in series.items():
        hw.put_history(bucket, "b", "data/repo-history/%s.json" % sid, body(sid, n), etags, stats)
    return stats


def test_second_run_writes_nothing_and_a_new_observation_writes_one_file():
    b = Bucket(page=7)
    series = {"S%03d" % i: 10 + i % 5 for i in range(23)}
    assert run_once(b, series) == {"written": 23, "unchanged": 0} and len(b.puts) == 23
    before = dict(b.objects)
    assert run_once(b, series) == {"written": 0, "unchanged": 23} and len(b.puts) == 23 and b.objects == before        # identical bytes: not one write
    assert b.lists == 1 + 4                                                                                              # one listing a run (4 pages of 7), never a read per file
    series["S007"] += 1                                                                                                  # one series gains an observation
    assert run_once(b, series) == {"written": 1, "unchanged": 22} and b.puts[-1] == "data/repo-history/S007.json"
    assert b.objects["data/repo-history/S007.json"] == body("S007", series["S007"])
    series["NEW"] = 5
    assert run_once(b, series) == {"written": 1, "unchanged": 23} and b.puts[-1] == "data/repo-history/NEW.json"


def test_same_length_different_values_is_a_change():
    b = Bucket()
    etags, stats = hw.history_etags(b, "b"), {}
    one = json.dumps({"id": "X", "dates": ["2026-11-01"], "values": [1.0]}, separators=(",", ":")).encode()
    two = json.dumps({"id": "X", "dates": ["2026-11-01"], "values": [2.0]}, separators=(",", ":")).encode()
    assert len(one) == len(two)
    assert hw.put_history(b, "b", "data/repo-history/X.json", one, etags, stats) is True
    assert hw.put_history(b, "b", "data/repo-history/X.json", one, etags, stats) is False                               # twice in one run: the second is not written either
    assert hw.put_history(b, "b", "data/repo-history/X.json", two, etags, stats) is True and b.objects["data/repo-history/X.json"] == two
    assert stats == {"written": 2, "unchanged": 1}


def test_a_failed_listing_or_a_non_md5_etag_costs_the_saving_never_a_write():
    series = {"A": 3, "B": 4}
    seed = {"data/repo-history/%s.json" % s: body(s, n) for s, n in series.items()}
    said = []
    b = Bucket(seed, fail_list=True)
    assert run_once(b, series, log=said.append) == {"written": 2, "unchanged": 0} and len(said) == 1 and "every history file is written" in said[0]
    b = Bucket(seed, etag=lambda _b: '"0123456789abcdef0123456789abcdef-2"')                                             # multipart-style ETag
    assert run_once(b, series) == {"written": 2, "unchanged": 0}
    b = Bucket(seed, etag=lambda _b: None)
    assert run_once(b, series) == {"written": 2, "unchanged": 0}
    other = Bucket({"data/repo.json": b"{}", "data/repo-history-old/A.json": body("A", 3)})                             # only the prefix is listed
    assert hw.history_etags(other, "b") == {}


def test_the_engine_routes_both_history_writes_through_the_helper():
    src = (SRC / "lambda_function.py").read_text(encoding="utf-8")
    compile(src, "lambda_function.py", "exec")
    assert src.count("from history_writes import history_etags, put_history") == 1
    assert src.count("hist_etags = history_etags(s3, BUCKET)") == 1 and src.count("put_history(") == 2
    for m in re.finditer(r"put_object\(", src):
        call = src[m.start():m.start() + 400]
        assert "repo-history" not in call.split("ContentType")[0], "a history file is still written unconditionally"
    assert src.index("hist_etags = history_etags(s3, BUCKET)") < src.index("put_history(")
    assert '"history_written": hist_stats["written"]' in src and '"history_unchanged": hist_stats["unchanged"]' in src
    assert src.count('Key="data/repo.json"') == 1                                                                        # the hot index is still written every run (it carries as_of)


def main():
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            n += 1
    print("history writes: %d tests passed" % n)


if __name__ == "__main__":
    main()
