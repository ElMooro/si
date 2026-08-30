"""ops_5054 -- walk the browser's path for real, then stand down.

Everything has been verified in pieces: the index reconciles to the
manifest at delta 0, the blocks are the bytes the map promises, the
proxy returns 206. What has never been done is the ONE thing a user
does -- open provider.html, type a query, click a dataset -- executed
end to end against the live domain. Verifying components and never the
composition is how a system passes every test and fails every user.

So this op is provider.html's own code path, replayed over HTTP exactly
as the browser would run it:

    GET /data/index/{provider}/flows.json.gz      (the Tier-0 document)
    search 8,354 flows client-side
    -> a LARGE flow:  GET {FLOW}.blocks.json, binary-search on `k`,
                      Range-read one block, parse the rows
    -> a SMALL flow:  GET pages lo..lo+9, filter rows by `flow`
Both branches must return real series with usable fields, because
provider.html renders name / geo / last_value / last_obs and a branch
that returns rows with none of those is a blank table, not a feature.

  P0 finish Tier 1
  P1 fetch provider.html from the live site and confirm the deployed
     HTML is the fixed one -- no all-pages loop
  P2 the LARGE-flow branch, over HTTP
  P3 the SMALL-flow branch, over HTTP
  P4 restore hourly cadence and report the final state
"""
import bisect
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
SITE = "https://justhodl.ai"
FN = "justhodl-series-extractor"
RULE = "justhodl-series-extractor-5min"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)


def jget(k):
    import gzip
    b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
    if k.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def http(url, rng=None, cap=4000000):
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 justhodl-ops-5054"})
    if rng:
        req.add_header("Range", rng)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, dict(r.headers), r.read(cap)
    except Exception as e:
        return getattr(e, "code", -1), {}, str(e)[:150].encode()


with report("ops_5054_browser_path") as R:
    fails = []
    out = {"op": "ops_5054"}

    R.section("P0 finish Tier 1")
    left = None
    for i in range(9):
        t = jget("data/_state/t1-eurostat.json")
        left = t.get("candidates_left")
        R.log("  t+%2dmin eurostat t1 flows=%d left=%s entries=%s "
              "%.2f GB" % (i * 4, len(t.get("flows_done") or []), left,
                           f"{t.get('entries') or 0:,}",
                           (t.get("bytes") or 0) / 1e9))
        if left == 0:
            R.log("  TIER 1 COMPLETE")
            break
        time.sleep(240)
    out["t1_left"] = left

    R.section("P1 is the deployed provider.html the fixed one")
    st, hd, body = http("%s/provider.html" % SITE)
    html = body.decode("utf-8", "replace")
    R.log("  GET /provider.html -> %s, %s bytes" % (st, f"{len(html):,}"))
    checks = [("all-pages loop removed",
               "for(var i=0;i<m.n_pages;i++)" not in html),
              ("_ensureSeriesCache gone", "_ensureSeriesCache" not in html),
              ("index search present", "_getIdx" in html),
              ("flow opener present", "_showFlow" in html),
              ("range read present", "status!==206" in html
               or "status !== 206" in html)]
    for label, good in checks:
        R.log("  %-26s %s" % (label, "OK" if good else "*** NO ***"))
        if not good:
            fails.append("P1:%s" % label.split()[0])

    R.section("P2 LARGE-flow branch (Tier 1) over HTTP")
    prov = "eurostat"
    idx = jget("data/index/%s/flows.json.gz" % prov)
    t1s = jget("data/_state/t1-%s.json" % prov)
    built = set(t1s.get("flows_done") or [])
    big = sorted(((idx["flows"][f].get("series") or 0), f)
                 for f in built if f in idx["flows"])[-1:]
    if not big:
        R.log("  no built flow to test")
        fails.append("P2:none")
    for cnt, f in big:
        base = "%s/data/index/%s/t1/%s" % (SITE, prov, f)
        st, _, b = http(base + ".blocks.json")
        bm = json.loads(b)
        blocks = bm["blocks"]
        R.log("  %s: %s series, %d blocks (map %s bytes)" % (
            f, f"{cnt:,}", len(blocks), f"{len(b):,}"))
        keys = [x["k"] for x in blocks]
        probe = blocks[len(blocks) // 3]["k"]
        j = bisect.bisect_right(keys, probe) - 1
        blk = blocks[j]
        t0 = time.time()
        st, hd, seg = http(base + ".jsonl",
                           rng="bytes=%d-%d" % (blk["o"],
                                                blk["o"] + blk["c"] - 1))
        ms = (time.time() - t0) * 1000
        rows = [json.loads(l) for l in seg.splitlines() if l.strip()]
        R.log("  binary search -> block %d/%d, HTTP %s, %s bytes in "
              "%.0f ms, %d rows" % (j, len(blocks), st, f"{len(seg):,}",
                                    ms, len(rows)))
        r0 = rows[0] if rows else {}
        R.log("  first row: id=%s f=%s l=%s n=%s" % (
            str(r0.get("id"))[:56], r0.get("f"), r0.get("l"),
            r0.get("n")))
        okrow = bool(r0.get("id")) and r0.get("l") is not None
        R.log("  renderable (id + last_obs present): %s" % okrow)
        if st != 206 or not rows or not okrow:
            fails.append("P2:%s" % f)
        out["large"] = {"flow": f, "series": cnt, "blocks": len(blocks),
                        "read_bytes": len(seg), "ms": round(ms)}

    R.section("P3 SMALL-flow branch (page range) over HTTP")
    small = sorted(((idx["flows"][f]["hi"] - idx["flows"][f]["lo"] + 1), f)
                   for f in idx["flows"] if f not in built)
    small = [x for x in small if x[0] <= 10][:1] or small[:1]
    for span, f in small:
        rec = idx["flows"][f]
        lo, hi = rec["lo"], min(rec["hi"], rec["lo"] + 9)
        got, fetched = [], 0
        t0 = time.time()
        for p in range(lo, hi + 1):
            st, _, b = http("%s/data/providers/%s/series/page-%04d.json"
                            % (SITE, prov, p))
            if st == 200:
                fetched += len(b)
                for r in (json.loads(b).get("rows") or []):
                    if r.get("flow") == f:
                        got.append(r)
        ms = (time.time() - t0) * 1000
        R.log("  %s: span %d pages, fetched %d pages / %.1f MB in "
              "%.0f ms -> %d rows for this flow" % (
                  f, span, hi - lo + 1, fetched / 1e6, ms, len(got)))
        r0 = got[0] if got else {}
        R.log("  first row: id=%s name=%s geo=%s last_obs=%s "
              "last_value=%s" % (str(r0.get("id"))[:42],
                                 str(r0.get("name"))[:26], r0.get("geo"),
                                 r0.get("last_obs"), r0.get("last_value")))
        if not got:
            R.log("  *** page-range branch returned nothing -- the flow "
                  "filter or the range is wrong ***")
            fails.append("P3:%s" % f)
        out["small"] = {"flow": f, "span": span, "rows": len(got),
                        "mb": round(fetched / 1e6, 2)}

    R.section("P4 stand down")
    try:
        if left == 0:
            ev.put_rule(Name=RULE, ScheduleExpression="rate(1 hour)",
                        State="ENABLED")
            R.log("  cadence -> rate(1 hour)")
        else:
            R.log("  Tier 1 still building (%s left) -- leaving "
                  "rate(2 minutes)" % left)
        d = ev.describe_rule(Name=RULE)
        tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  rule=%s targets=%d concurrency=%s" % (
            d.get("ScheduleExpression"), len(tg),
            rc.get("ReservedConcurrentExecutions")))
        for p in ("eurostat", "ecb"):
            st_ = jget("data/_state/series-extract-%s.json" % p)
            t_ = jget("data/_state/t1-%s.json" % p)
            R.log("  %-9s series %s in %s pages · t1 %d flows / %s "
                  "entries" % (p, f"{st_.get('series_count') or 0:,}",
                               f"{st_.get('n_pages') or 0:,}",
                               len(t_.get("flows_done") or []),
                               f"{t_.get('entries') or 0:,}"))
    except Exception as e:
        R.log("  standdown err %s" % str(e)[:130])
        fails.append("P4")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-serving.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5054 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(t1_left=out.get("t1_left"),
         large_read_kb=round((out.get("large", {}).get("read_bytes") or 0)
                             / 1024),
         small_rows=out.get("small", {}).get("rows"))
    R.log("ops 5054 GREEN -- the browser's path works end to end")
