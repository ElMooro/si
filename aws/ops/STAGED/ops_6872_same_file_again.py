"""ops 6872 - READ-ONLY: of the files each engine writes in a day, how many are the same file again.

Khalid 2026-11-20 22:20 CT: "Yes do 1 then 2 then 3 ...".  Step 2: give the other heavy writers a write-only-on-change
check.  Before touching any engine this measures where such a check pays.  Every write to the live bucket is billed
twice (the write and its copy into the archive) and leaves an old version behind, whether or not the file changed.

  A. Yesterday's access logs (sampled, scaled to the day): writes per engine, and a random sample of the keys each wrote.
  B. For each sampled key, the newest two versions the bucket holds (it is versioned): are they
       the same bytes                      - same ETag: a plain put-if-changed would have skipped the write
       the same content, other bytes       - equal once un-gzipped (the gzip header carries the time of day)
       the same data, other stamps         - JSON equal except in fields that look like run stamps (named in the report)
       different                           - the data changed; the write was needed
       the only version                    - a key written once (a new key per run or per day: no check can save it)
     and the hours between the two writes.
  C. Per engine: writes a day, the share in each class, what that share costs a month.

Nothing is written but this report.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote
import concurrent.futures as cf
import gzip
import importlib.util
import json
import random
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6703", str(Path(__file__).resolve().parent / "ops_6703_banks_steady_cost.py"))
B = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(B)

LOGS_READ = 9000                     # access-log objects sampled for the day
KEYS_PER_WRITER = 70                 # keys compared per engine
MIN_WRITES = 600                     # engines below this many writes a day are summed, not examined
MAX_COMPARE_BYTES = 12 * 1024 * 1024
WRITE_PRICE = 0.005 / 1000           # one write; the archive copy costs the same again
STAMP = re.compile(r"^(generated|updated|created|fetched|pulled|built|computed|refreshed|written|saved|checked|run|last_run|last_updated|last_update|last_refresh|"
                   r"as_of|asof|timestamp|ts|time|now|elapsed|duration|took|latency|age|uptime|request_id|run_id|build_id|execution_id|invocation|cold_start|"
                   r"runtime|wall|generated_utc|server_time)(_[a-z0-9_]*)?$|.*(_at|_ts|_time|_utc|_iso|_ms|_seconds|_secs|_sec|_age|_elapsed|_duration|_took|_stamp|_epoch)$", re.I)


def leaf_diff(a, b, path, out, budget):
    """Count the leaf paths where two JSON values differ (list positions folded to []); budget[0] bounds the walk."""
    if budget[0] <= 0:
        out["(not fully compared)"] += 1
        return
    budget[0] -= 1
    if isinstance(a, dict) and isinstance(b, dict):
        for k in set(a) | set(b):
            if k not in a or k not in b:
                out[path + "." + str(k) if path else str(k)] += 1
            else:
                leaf_diff(a[k], b[k], path + "." + str(k) if path else str(k), out, budget)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out[path + "[length]"] += 1
            return
        for x, y in zip(a, b):
            leaf_diff(x, y, path + "[]", out, budget)
    elif a != b:
        out[path] += 1


def is_stamp(path):
    last = path.replace("[]", "").split(".")[-1]
    return bool(STAMP.match(last)) and not path.endswith("[length]")


def classify(raw_new, raw_old):
    """(class, differing fields) for two versions' bytes."""
    if raw_new == raw_old:
        return "same bytes", []

    def body(raw):
        if raw[:2] == b"\x1f\x8b":
            try:
                return gzip.decompress(raw)
            except Exception:  # noqa: BLE001
                return raw
        return raw
    a, b = body(raw_new), body(raw_old)
    if a == b:
        return "same content, other bytes", []
    try:
        ja, jb = json.loads(a), json.loads(b)
    except Exception:  # noqa: BLE001
        return "different", ["(not JSON)"]
    out, budget = Counter(), [400000]
    leaf_diff(ja, jb, "", out, budget)
    if not out:
        return "same content, other bytes", []          # key order or whitespace only
    fields = [k for k, _ in out.most_common(6)]
    if all(is_stamp(k) for k in out):
        return "same data, other stamps", fields
    return "different", [k for k in fields if not is_stamp(k)][:6] or fields


def selftest():
    assert classify(b"abc", b"abc") == ("same bytes", [])
    j = json.dumps({"generated_at": "t1", "rows": [{"a": 1, "updated_at": "x"}], "n": 1}).encode()
    j2 = json.dumps({"generated_at": "t2", "rows": [{"a": 1, "updated_at": "y"}], "n": 1}).encode()
    j3 = json.dumps({"generated_at": "t2", "rows": [{"a": 2, "updated_at": "y"}], "n": 1}).encode()
    assert classify(gzip.compress(j, mtime=1), gzip.compress(j, mtime=2))[0] == "same content, other bytes"
    c, f = classify(j2, j)
    assert c == "same data, other stamps" and set(f) == {"generated_at", "rows[].updated_at"}, (c, f)
    c, f = classify(gzip.compress(j3), gzip.compress(j))
    assert c == "different" and f == ["rows[].a"], (c, f)
    assert classify(json.dumps({"rows": [1, 2]}).encode(), json.dumps({"rows": [1]}).encode()) == ("different", ["rows[length]"])
    assert classify(b"a,b\n1,2\n", b"a,b\n1,3\n") == ("different", ["(not JSON)"])
    assert classify(b'{"a": 1,  "b": 2}', b'{"b":2,"a":1}')[0] == "same content, other bytes"
    assert is_stamp("meta.generated_at") and is_stamp("as_of") and is_stamp("rows[].fetched_at") and not is_stamp("price") and not is_stamp("rows[].date") and not is_stamp("data.latest")
    assert not is_stamp("rating") and not is_stamp("update") and is_stamp("elapsed_ms") and is_stamp("ts")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=64)
    s3 = boto3.client("s3", region_name=B.REGION, config=cfg)
    day = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    rng = random.Random(6872)
    with report("ops_6872_same_file_again") as r:
        r.heading("ops 6872 - the same file again: what each engine rewrites without a change (read-only)")

        r.section("A. Who wrote what on %s (access logs, sampled)" % day)
        lg = s3.get_bucket_logging(Bucket=B.LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        base = "%s%s/%s/%s/%s/" % (tp, B.ACCOUNT, B.REGION, B.LIVE, day.replace("-", "/"))
        keys, tok = [], None
        while True:
            kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            res = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in res.get("Contents", [])]
            if not res.get("IsTruncated") or len(keys) >= 1500000:
                break
            tok = res.get("NextContinuationToken")
        if not keys:
            r.fail("no access logs under %s" % base)
            sys.exit(0)
        sample = rng.sample(keys, min(LOGS_READ, len(keys)))
        scale = len(keys) / float(len(sample))

        def read(k):
            try:
                return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
            except Exception:  # noqa: BLE001
                return ""
        who = Counter()
        fam = defaultdict(Counter)
        wrote = defaultdict(set)
        with cf.ThreadPoolExecutor(max_workers=48) as ex:
            for text in ex.map(read, sample):
                for line in text.splitlines():
                    m = B.LOG_RE.match(line)
                    if m and m.group(7) in B.WRITE_OPS and m.group(10).startswith("2"):
                        w, key = B.writer(m.group(5)), unquote(m.group(8))
                        who[w] += 1
                        fam[w][B.family(key)] += 1
                        wrote[w].add(key)
        total = sum(who.values()) * scale
        r.log("%d log objects, read %d (x%.1f): about %d writes in the day" % (len(keys), len(sample), scale, total))
        examined = [w for w, n in who.most_common() if n * scale >= MIN_WRITES]
        rest = sum(n for w, n in who.items() if w not in examined) * scale
        r.log("%d engines wrote at least %d files; %d others wrote %d between them" % (len(examined), MIN_WRITES, len(who) - len(examined), rest))

        r.section("B. The newest two versions of the keys each engine wrote")

        def versions(key):
            try:
                res = s3.list_object_versions(Bucket=B.LIVE, Prefix=key, MaxKeys=30)
            except Exception as e:  # noqa: BLE001
                return key, None, str(e)[:120]
            vs = [v for v in res.get("Versions", []) if v["Key"] == key]
            vs.sort(key=lambda v: v["LastModified"], reverse=True)
            return key, vs[:2], None

        def compare(item):
            key, vs, err = item
            if err or vs is None:
                return key, "could not list", [], None, 0
            if not vs:
                return key, "gone", [], None, 0
            if len(vs) == 1:
                return key, "the only version", [], None, vs[0]["Size"]
            gap = (vs[0]["LastModified"] - vs[1]["LastModified"]).total_seconds() / 3600.0
            if vs[0]["ETag"] == vs[1]["ETag"] and vs[0]["Size"] == vs[1]["Size"]:
                return key, "same bytes", [], gap, vs[0]["Size"]
            if max(vs[0]["Size"], vs[1]["Size"]) > MAX_COMPARE_BYTES:
                return key, "different", ["(too large to read back: %.0f MB)" % (vs[0]["Size"] / 1e6)], gap, vs[0]["Size"]
            try:
                a = s3.get_object(Bucket=B.LIVE, Key=key, VersionId=vs[0]["VersionId"])["Body"].read()
                b = s3.get_object(Bucket=B.LIVE, Key=key, VersionId=vs[1]["VersionId"])["Body"].read()
            except Exception as e:  # noqa: BLE001
                return key, "could not read", [str(e)[:80]], gap, vs[0]["Size"]
            c, f = classify(a, b)
            return key, c, f, gap, vs[0]["Size"]
        CLASSES = ["same bytes", "same content, other bytes", "same data, other stamps", "different", "the only version"]
        rows = []
        for w in examined:
            ks = sorted(wrote[w])
            picked = rng.sample(ks, min(KEYS_PER_WRITER, len(ks)))
            with cf.ThreadPoolExecutor(max_workers=24) as ex:
                got = list(ex.map(lambda k: compare(versions(k)), picked))
            cls = Counter(c for _, c, _, _, _ in got)
            n = sum(cls[c] for c in CLASSES) or 1
            fields = Counter()
            for _, c, f, _, _ in got:
                if c in ("same data, other stamps", "different"):
                    fields.update("%s%s" % ("" if c == "different" else "~", x) for x in f[:4])
            gaps = sorted(g for _, c, _, g, _ in got if g is not None)
            med_gap = gaps[len(gaps) // 2] if gaps else None
            per_day = who[w] * scale
            idle = (cls["same bytes"] + cls["same content, other bytes"] + cls["same data, other stamps"]) / float(n)
            month = per_day * idle * 30 * WRITE_PRICE * 2
            size = sum(s for *_x, s in got) / float(len(got) or 1)
            rows.append((w, per_day, n, cls, idle, month, med_gap, fields, size))
            r.log("%-44s %8d writes | compared %3d: same bytes %3d, same content %3d, stamps only %3d, different %3d, only version %3d%s | between writes %s h | avg %.0f KB" % (
                w[:44], per_day, n, cls["same bytes"], cls["same content, other bytes"], cls["same data, other stamps"], cls["different"], cls["the only version"],
                (" (+%d unread)" % (cls["could not list"] + cls["could not read"] + cls["gone"])) if (cls["could not list"] + cls["could not read"] + cls["gone"]) else "",
                ("%.1f" % med_gap) if med_gap is not None else "-", size / 1024.0))
            r.log("      folders: %s" % ", ".join("%s %d" % (f, c * scale) for f, c in fam[w].most_common(4)))
            if fields:
                r.log("      fields that differ (~ = stamp-like): %s" % ", ".join("%s x%d" % (k, c) for k, c in fields.most_common(8)))
            ex_same = [k for k, c, _, _, _ in got if c in ("same bytes", "same content, other bytes", "same data, other stamps")][:2]
            ex_only = [k for k, c, _, _, _ in got if c == "the only version"][:2]
            if ex_same:
                r.log("      rewritten unchanged, e.g. %s" % ", ".join(ex_same))
            if ex_only:
                r.log("      written once, e.g. %s" % ", ".join(ex_only))

        r.section("C. Where a write-only-on-change check pays")
        rows.sort(key=lambda t: -t[5])
        saved = 0.0
        for w, per_day, n, cls, idle, month, med_gap, fields, size in rows:
            if month < 0.05:
                continue
            saved += month
            r.log("%-44s %8d writes a day, %3.0f%% unchanged -> about $%.2f a month (%s)" % (
                w[:44], per_day, idle * 100, month, "same bytes" if cls["same bytes"] >= max(cls["same content, other bytes"], cls["same data, other stamps"]) else
                "other bytes, same content" if cls["same content, other bytes"] >= cls["same data, other stamps"] else "only stamps differ"))
        once = sum(per_day * cls["the only version"] / float(n) for w, per_day, n, cls, *_ in rows)
        needed = sum(per_day * cls["different"] / float(n) for w, per_day, n, cls, *_ in rows)
        idle_all = sum(per_day * idle for w, per_day, n, cls, idle, *_ in rows)
        r.log("of about %d writes that day: %d rewrote a file without changing its data, %d changed it, %d wrote a key for the first time; %d came from engines too small to examine" % (
            total, idle_all, needed, once, rest))
        r.log("unchanged rewrites cost about $%.2f a month at this rate (both fees); a check in the engines above removes them" % (idle_all * 30 * WRITE_PRICE * 2))
        r.kv(day=day, writes=round(total), engines_examined=len(examined), unchanged_rewrites=round(idle_all), needed_writes=round(needed), first_writes=round(once),
             unchanged_cost_month=round(idle_all * 30 * WRITE_PRICE * 2, 2), small_engines_writes=round(rest))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
