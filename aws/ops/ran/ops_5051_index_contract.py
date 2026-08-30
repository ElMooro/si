"""ops_5051 -- finish Tier 1, publish the index contract, stand down.

ops 5050 proved the serving shape against real objects:
    ecb/IVF   273,854 entries · 67 blocks · map 6KB · data 24.1MB
      -> binary search -> ONE 354KB Range read -> found. 66x less read.
ECB Tier 1 finished inside a single invocation (17 flows, 2,619,231
entries). Eurostat was at 445 flows / 26.7M entries and climbing ~38
flows/min.

Three things close this out.

 1. STAND DOWN. The rule was raised to rate(2 minutes) for the build
    window. Once nothing is left to build it goes back to hourly, or
    four targets fire every two minutes forever. The t1 targets STAY --
    at hourly they cost nothing and any flow added later gets indexed
    without anyone remembering to do it.
 2. THE STRESS CASE. IVF is 274k series. The real test is the largest
    built flow, where the block map itself gets big. If a client has to
    download a 5MB map to save one 350KB read, Tier 1 has failed at
    exactly the size it exists for.
 3. THE CONTRACT. Both tiers are correct and neither is reachable: no
    front end knows they exist. An index that does not publish how to
    read it is a private artifact. index/manifest.json is the entry
    point -- one small document naming every part, its schema, and the
    lookup protocol, so wiring the UI is reading a contract rather than
    reverse-engineering a layout.

  P0 progress on both lanes
  P1 stress the largest built flow with a real Range read
  P2 publish index/manifest.json
  P3 restore hourly cadence when the build is done
"""
import bisect
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-series-extractor"
RULE = "justhodl-series-extractor-5min"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)


def jget(k):
    try:
        import gzip
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return {}


with report("ops_5051_index_contract") as R:
    fails = []
    out = {"op": "ops_5051"}

    R.section("P0 Tier-1 progress")
    prog = {}
    for i in range(10):
        for p in ("ecb", "eurostat"):
            t = jget("data/_state/t1-%s.json" % p)
            prog[p] = t
            R.log("  t+%2dmin %-9s flows=%d left=%s/%s entries=%s "
                  "blocks=%s %.2f GB" % (
                      i * 3, p, len(t.get("flows_done") or []),
                      t.get("candidates_left"), t.get("candidates_total"),
                      f"{t.get('entries') or 0:,}",
                      f"{t.get('blocks') or 0:,}",
                      (t.get("bytes") or 0) / 1e9))
        left = prog.get("eurostat", {}).get("candidates_left")
        if left == 0:
            R.log("  eurostat Tier 1 COMPLETE")
            break
        time.sleep(180)
    for p in ("ecb", "eurostat"):
        t = prog.get(p) or {}
        out[p] = {"flows": len(t.get("flows_done") or []),
                  "left": t.get("candidates_left"),
                  "entries": t.get("entries"), "blocks": t.get("blocks"),
                  "bytes": t.get("bytes")}
        st = jget("data/_state/series-extract-%s.json" % p)
        R.log("  %-9s series lane still intact: flows=%d n_pages=%s" % (
            p, len(st.get("flows_done") or []), st.get("n_pages")))

    R.section("P1 stress the LARGEST built flow")
    for provider in ("eurostat", "ecb"):
        biggest, kw = None, {"Bucket": LIVE,
                             "Prefix": "index/%s/t1/" % provider,
                             "MaxKeys": 1000}
        try:
            while True:
                r = s3.list_objects_v2(**kw)
                for o in r.get("Contents", []):
                    if o["Key"].endswith(".jsonl") and (
                            not biggest or o["Size"] > biggest[1]):
                        biggest = (o["Key"], o["Size"])
                if not r.get("IsTruncated"):
                    break
                kw["ContinuationToken"] = r.get("NextContinuationToken")
        except Exception as e:
            R.log("  list err %s" % str(e)[:110])
        if not biggest:
            R.log("  %s: nothing built yet" % provider)
            continue
        dkey, dsz = biggest
        bkey = dkey[:-6] + ".blocks.json"
        try:
            bm = jget(bkey)
            blocks = bm.get("blocks") or []
            hb = s3.head_object(Bucket=LIVE, Key=bkey)
            R.log("  %s largest: %s" % (provider,
                                        dkey.rsplit("/", 1)[-1]))
            R.log("    %s entries · %d blocks · data %.1f MB · map %.0f "
                  "KB (%.3f%% of the data)" % (
                      f"{bm.get('n') or 0:,}", len(blocks), dsz / 1e6,
                      hb["ContentLength"] / 1024,
                      100.0 * hb["ContentLength"] / max(1, dsz)))
            keys = [b["k"] for b in blocks]
            target = blocks[len(blocks) * 3 // 4]["k"]
            j = bisect.bisect_right(keys, target) - 1
            b = blocks[j]
            t0 = time.time()
            seg = s3.get_object(Bucket=LIVE, Key=dkey,
                                Range="bytes=%d-%d"
                                % (b["o"], b["o"] + b["c"] - 1)
                                )["Body"].read()
            ms = (time.time() - t0) * 1000
            hit = any(json.loads(ln).get("id") == target
                      for ln in seg.splitlines() if ln.strip())
            total = hb["ContentLength"] + len(seg)
            R.log("    lookup: %d-block binary search -> block %d -> "
                  "Range %.0f KB in %.0f ms -> found=%s" % (
                      len(blocks), j, len(seg) / 1024, ms, hit))
            R.log("    total bytes to answer one query: %.0f KB vs %.1f "
                  "MB whole-flow (%.0fx less)" % (
                      total / 1024, dsz / 1e6, dsz / max(1, total)))
            if not hit:
                fails.append("P1:%s" % provider)
            out.setdefault("stress", {})[provider] = {
                "flow": dkey.rsplit("/", 1)[-1], "entries": bm.get("n"),
                "blocks": len(blocks), "data_mb": round(dsz / 1e6, 1),
                "map_kb": round(hb["ContentLength"] / 1024),
                "query_kb": round(total / 1024)}
        except Exception as e:
            R.log("  stress err %s" % str(e)[:150])
            fails.append("P1:%s-err" % provider)

    R.section("P2 publish the index contract")
    man = {"schema": 1,
           "generated_at": datetime.now(timezone.utc).isoformat(
               timespec="seconds"),
           "providers": {},
           "protocol": {
               "tier0": ("GET index/{provider}/flows.json.gz -- one "
                         "gzipped document, every flow with lo/hi page "
                         "range and an exact series count. Search it "
                         "client-side; it is ~100KB."),
               "pages": ("data/providers/{provider}/series/"
                         "page-{n:04d}.json holds 500 series rows. A "
                         "flow's rows live in pages lo..hi INCLUSIVE, "
                         "which is a superset -- pages straddle flows, "
                         "so filter rows by the `flow` field."),
               "tier1": ("For flows with a t1 entry: GET "
                         "index/{provider}/t1/{FLOW}.blocks.json, "
                         "binary-search `blocks` on `k` (first id in "
                         "each block), then issue an HTTP Range request "
                         "for bytes o..o+c-1 of "
                         "index/{provider}/t1/{FLOW}.jsonl. One block, "
                         "one read."),
               "threshold": ("Tier 1 exists only for flows spanning "
                             "more than 100 pages; smaller flows are "
                             "served by fetching their page range.")}}
    for p in ("eurostat", "ecb"):
        t0d = jget("index/%s/flows.json.gz" % p)
        t1s = jget("data/_state/t1-%s.json" % p)
        man["providers"][p] = {
            "tier0": "index/%s/flows.json.gz" % p,
            "tier0_schema": t0d.get("schema"),
            "flows": t0d.get("flows_total"),
            "series": t0d.get("series_total"),
            "counts_exact": t0d.get("counts_exact"),
            "pages": t0d.get("pages_total"),
            "page_size": 500,
            "tier1_prefix": "index/%s/t1/" % p,
            "tier1_flows": len(t1s.get("flows_done") or []),
            "tier1_entries": t1s.get("entries"),
            "tier1_block_size": 4096,
            "tier1_complete": t1s.get("candidates_left") == 0}
        R.log("  %-9s flows=%s series=%s exact=%s tier1_flows=%d "
              "complete=%s" % (
                  p, man["providers"][p]["flows"],
                  f"{man['providers'][p]['series'] or 0:,}",
                  man["providers"][p]["counts_exact"],
                  man["providers"][p]["tier1_flows"],
                  man["providers"][p]["tier1_complete"]))
    try:
        s3.put_object(Bucket=LIVE, Key="index/manifest.json",
                      Body=json.dumps(man, indent=1).encode(),
                      ContentType="application/json",
                      CacheControl="public, max-age=300")
        R.log("  -> index/manifest.json (the entry point a front end "
              "fetches first)")
    except Exception as e:
        R.log("  write err %s" % str(e)[:120])
        fails.append("P2")

    R.section("P3 stand down")
    done = (prog.get("eurostat", {}).get("candidates_left") == 0)
    try:
        if done:
            ev.put_rule(Name=RULE, ScheduleExpression="rate(1 hour)",
                        State="ENABLED")
            R.log("  cadence -> rate(1 hour); t1 targets KEPT so flows "
                  "added later get indexed unattended")
        else:
            R.log("  build still running -- leaving rate(2 minutes) so "
                  "it finishes; restore hourly next session")
        d = ev.describe_rule(Name=RULE)
        tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        R.log("  rule %s · targets=%d %s" % (
            d.get("ScheduleExpression"), len(tg),
            [t.get("Id") for t in tg]))
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  reserved concurrency=%s" % rc.get(
            "ReservedConcurrentExecutions"))
    except Exception as e:
        R.log("  rule err %s" % str(e)[:130])
        fails.append("P3")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-tier1.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/index-tier1.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5051 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(eurostat_t1=out.get("eurostat", {}).get("flows"),
         left=out.get("eurostat", {}).get("left"),
         ecb_t1=out.get("ecb", {}).get("flows"))
    R.log("ops 5051 GREEN -- index contract published")
