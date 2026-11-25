"""ops 6948 - READ-ONLY: which prefixes of the live bucket were written on Nov 21-24 (the +80-100k object days).

The storage fuse (ops 6947, daily health run 2026-11-25) read a THIRD day near double the usual object growth on the
live bucket: Nov 21 +99,943, Nov 22 +81,712, Nov 23 +89,738 against +42-49k/day on Nov 17-20, with the GB growth
unchanged (+5-7.5 GB/day). Nov 21 and 22 had the Repo desk and the Sovereign desk builds behind them; Nov 23 had only
page work (ops 6928-6933), so the growth is a WRITER running daily, not a build. Yesterday's issue (#163) said: a third
day above +80k with no build session behind it means a per-object writer to find. This is that listing.

How: list every current object of the live bucket (one ListObjectsV2 page = 1,000 keys, ~10.4 M keys = ~10.4 k
requests, about $0.05), in parallel by first- and second-level prefix, and count keys by their LastModified day
(Nov 17-25 UTC) under their first three path segments. A prefix that wrote >= 1,000 keys on Nov 23 is a daily writer
worth naming; a prefix whose Nov 21-23 counts dwarf its Nov 17-20 counts is the growth. Only current objects are
listed (noncurrent versions are the lifecycle rule's business and capped at 10 per key), so a key rewritten daily
counts once, on its last write - the per-day counts are therefore a LOWER bound on writes and a fair census of what
was touched. A 40-minute wall clock publishes a partial census rather than losing the run.

Read-only: ListBucket only. Nothing is written, held, released or invoked. Exit 1 only if the bucket cannot be listed.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
DAYS = ["2026-11-%02d" % d for d in range(17, 26)]
FOCUS = "2026-11-23"
WALL_S = 40 * 60
DEPTH = 3
WORKERS = 32


def depth_prefix(key, depth=DEPTH):
    parts = key.split("/")
    if len(parts) <= depth:
        return "/".join(parts[:-1]) + "/" if len(parts) > 1 else "(root)"
    return "/".join(parts[:depth]) + "/"


def selftest():
    assert depth_prefix("data/warm/ofr/x.json.gz") == "data/warm/ofr/"
    assert depth_prefix("data/x.json") == "data/"
    assert depth_prefix("index.html") == "(root)"
    assert depth_prefix("a/b/c/d/e.json") == "a/b/c/"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=WORKERS + 8)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    t0 = time.time()
    now = datetime.now(timezone.utc)

    def split(prefix):
        """second-level split of a prefix with Delimiter, so the fan-out has enough shards."""
        out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "Delimiter": "/"}
        while True:
            page = s3.list_objects_v2(**kw)
            out.extend(cp["Prefix"] for cp in page.get("CommonPrefixes") or [])
            if not page.get("IsTruncated"):
                return out, bool(page.get("Contents"))
            kw["ContinuationToken"] = page["NextContinuationToken"]

    def census(prefix, leaf_only=False):
        """count keys by (depth prefix, LastModified day); leaf_only lists the prefix's direct files only."""
        by_day, total, requests, cut = defaultdict(Counter), 0, 0, False
        kw = {"Bucket": LIVE, "Prefix": prefix}
        if leaf_only:
            kw["Delimiter"] = "/"
        while True:
            if time.time() - t0 > WALL_S:
                cut = True
                break
            page = s3.list_objects_v2(**kw)
            requests += 1
            for o in page.get("Contents") or []:
                total += 1
                day = o["LastModified"].strftime("%Y-%m-%d")
                if day in DAYS:
                    by_day[depth_prefix(o["Key"])][day] += 1
            if not page.get("IsTruncated"):
                break
            kw["ContinuationToken"] = page["NextContinuationToken"]
        return prefix, by_day, total, requests, cut

    with report("ops_6948_object_growth_writers") as r:
        r.heading("ops 6948 - which prefixes of %s were written on Nov 17-25 (the +80-100k object days); read-only census at %s" % (LIVE, now.isoformat()))
        r.section("A. shards")
        top, _ = split("")
        shards, leaves = [], [""]
        for p in top:
            subs, has_files = split(p)
            if len(subs) >= 4:
                shards.extend(subs)
                leaves.append(p)
            else:
                shards.append(p)
        # data/warm is the deep one (banks of millions of files): split it up to two more levels
        for level in (2, 3):
            extra = []
            for p in list(shards):
                if p.count("/") == level and p.startswith("data/warm/"):
                    subs, _ = split(p)
                    if len(subs) >= 8:
                        extra.extend(subs)
                        leaves.append(p)
                        shards.remove(p)
            shards.extend(extra)
        r.log("top-level prefixes %d; shards %d (deep prefixes split one or two levels); leaf-only listings %d" % (len(top), len(shards), len(leaves)))

        r.section("B. census")
        merged, total, requests, cuts, done = defaultdict(Counter), 0, 0, 0, 0
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(census, p) for p in shards] + [ex.submit(census, p, True) for p in leaves]
            for f in cf.as_completed(futs):
                p, by_day, n, q, cut = f.result()
                done += 1
                total += n
                requests += q
                cuts += int(cut)
                for pre, c in by_day.items():
                    merged[pre].update(c)
        elapsed = time.time() - t0
        r.log("listed %d current objects in %d requests, %.0f s, %d of %d listings%s" % (total, requests, elapsed, done, len(shards) + len(leaves),
                                                                                       (" - %d CUT by the wall clock (PARTIAL census)" % cuts) if cuts else ""))
        if cuts:
            r.warn("partial census: %d listing(s) hit the %d s wall clock; counts below are lower bounds" % (cuts, WALL_S))
        per_day = Counter()
        for pre, c in merged.items():
            per_day.update(c)
        r.log("current objects last written per day: %s" % ", ".join("%s %d" % (d, per_day.get(d, 0)) for d in DAYS))
        r.log("(the fuse's deltas: Nov 17-20 +42-49k, Nov 21 +99,943, Nov 22 +81,712, Nov 23 +89,738; a day's count here is keys whose LAST write was that day)")

        r.section("C. writers on %s (prefixes with >= 500 keys last written that day), with their Nov 17-25 shape" % FOCUS)
        rows = sorted(merged.items(), key=lambda kv: -kv[1].get(FOCUS, 0))
        named = 0
        for pre, c in rows:
            if c.get(FOCUS, 0) < 500:
                break
            named += 1
            base = [c.get(d, 0) for d in DAYS[:4]]
            r.log("  %-48s %s: %6d | Nov17-20 %s | Nov21 %d Nov22 %d Nov24 %d Nov25 %d" % (
                pre[:48], FOCUS[5:], c.get(FOCUS, 0), "/".join(str(x) for x in base), c.get("2026-11-21", 0), c.get("2026-11-22", 0), c.get("2026-11-24", 0), c.get("2026-11-25", 0)))
        r.log("%d prefix(es) wrote >= 500 keys on %s" % (named, FOCUS))

        r.section("D. the growth: prefixes whose Nov 21-23 keys exceed 3x their Nov 18-20 keys (and >= 2,000 keys over Nov 21-23)")
        grown = []
        for pre, c in merged.items():
            new = sum(c.get(d, 0) for d in ("2026-11-21", "2026-11-22", "2026-11-23"))
            old = sum(c.get(d, 0) for d in ("2026-11-18", "2026-11-19", "2026-11-20"))
            if new >= 2000 and new > 3 * max(old, 1):
                grown.append((new, old, pre))
        for new, old, pre in sorted(grown, reverse=True)[:40]:
            r.log("  %-48s Nov21-23 %6d vs Nov18-20 %6d  (%s)" % (pre[:48], new, old, ", ".join("%s %d" % (d[5:], merged[pre].get(d, 0)) for d in DAYS)))
        if grown:
            r.warn("%d prefix(es) grew: the top rows are the per-object writers behind the +80-100k days - engine owners are the next read (engine-manifest.json keys/prefixes)" % len(grown))
        else:
            r.log("no prefix grew 3x: the extra objects are spread thin, or they are noncurrent versions (not listed here) - then the daily rewrite count rose, not new keys")

        r.section("E. the whole-day picture: top 25 prefixes by keys last written Nov 21-25")
        for pre, c in sorted(merged.items(), key=lambda kv: -sum(kv[1].get(d, 0) for d in DAYS[4:]))[:25]:
            r.log("  %-48s %s" % (pre[:48], " ".join("%s:%d" % (d[8:], c.get(d, 0)) for d in DAYS)))

        r.kv(objects_listed=str(total), requests=str(requests), seconds=str(int(elapsed)), partial=str(bool(cuts)), writers_on_focus_day=str(named),
             grown_prefixes=str(len(grown)), top_grown=("; ".join("%s %d" % (p, n) for n, _o, p in sorted(grown, reverse=True)[:5]) or "none"),
             per_day=", ".join("%s %d" % (d[5:], per_day.get(d, 0)) for d in DAYS))
        r.ok("read-only census; nothing written, held, released or invoked")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
