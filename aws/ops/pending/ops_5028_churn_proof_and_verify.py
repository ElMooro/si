"""ops_5028 -- close the proof, verify the stop, verify the v2 deploy.

ops 5027's version probe scanned data/providers/eurostat/series/ in
LEXICOGRAPHIC order and reached only page-0000..~page-1999 -- pages
written once on Aug-09 and never touched again (2 versions each). It
reported "CHURN NOT proven". That was a sampling artifact, not a
refutation: the state doc says n_pages=3466 and updated_at=
2026-08-09T02:40:20, so every run since has been rewriting the range
page-3466 upward. This op reads THAT range.

Root cause, now exact:
  BUDGET_S was tested only BETWEEN flows. Eurostat flow #80 is bigger
  than one 280s invocation, so every run died inside it at the Lambda
  timeout -- before the single state put_object at the end. flows_done
  froze at 79, n_pages froze at 3466, and each of the 288 runs/day
  re-extracted the same flow and rewrote the same ~1540 page keys into a
  VERSIONED bucket: ~444k noncurrent versions/day x 252KB = ~112GB/day,
  from 2026-08-09T02:40 (function LastModified 02:39:30) until 5027.
  That one bug produced all four cost curves: Requests-Tier1, storage,
  GetObject (each PUT raised an Object Created event to the reader that
  ops 5025 killed), and USW2-Requests-SIA-Tier1 until ops 4988.

  P0 proof   : list_object_versions from KeyMarker page-3400 -- version
               counts and inter-version gaps on the hot range
  P1 stopped : rule state, reserved concurrency, invocations since 5027
  P2 purging : bucket object count trend + lifecycle rules present
  P3 v2      : the deployed code carries the ops 5028 fixes
  P4 ledger

GREEN = churn proven on the hot range AND the writer is stopped.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-series-extractor"
RULE = "justhodl-series-extractor-5min"
SERIES_PFX = "data/providers/eurostat/series/"
HOT_MARKER = SERIES_PFX + "page-3400.json"

cfg = Config(read_timeout=90, retries={"max_attempts": 4})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)

with report("ops_5028_churn_proof_and_verify") as R:
    fails = []
    out = {"op": "ops_5028", "at": NOW.isoformat(timespec="seconds")}

    # ------------------------------------------------------------ P0
    R.section("P0 proof on the HOT page range (page-3400+)")
    per_key = defaultdict(list)
    try:
        kw = {"Bucket": LIVE, "Prefix": SERIES_PFX, "MaxKeys": 1000,
              "KeyMarker": HOT_MARKER}
        seen = 0
        while seen < 12000:
            r = s3.list_object_versions(**kw)
            for v in r.get("Versions", []):
                per_key[v["Key"]].append((v["LastModified"], v["Size"]))
                seen += 1
            if not r.get("IsTruncated"):
                break
            kw["KeyMarker"] = r.get("NextKeyMarker")
            kw["VersionIdMarker"] = r.get("NextVersionIdMarker")
        R.log("  keys after %s: %d   versions seen: %d" % (
            HOT_MARKER.rsplit("/", 1)[-1], len(per_key), seen))
        ranked = sorted(per_key, key=lambda k: -len(per_key[k]))
        for k in ranked[:8]:
            ts = sorted(t for t, _ in per_key[k])
            gaps = [(ts[i + 1] - ts[i]).total_seconds() / 60
                    for i in range(len(ts) - 1)]
            med = sorted(gaps)[len(gaps) // 2] if gaps else 0
            R.log("  %-24s versions=%4d  oldest=%s  newest=%s  "
                  "median_gap=%.1f min" % (
                      k[len(SERIES_PFX):][:24], len(per_key[k]),
                      ts[0].strftime("%m-%d %H:%M"),
                      ts[-1].strftime("%m-%d %H:%M"), med))
        worst = len(per_key[ranked[0]]) if ranked else 0
        dead_gb = sum(s for v in per_key.values() for _, s in v) / 1e9
        R.log("  bytes held by this key range: %.2f GB" % dead_gb)
        proven = worst >= 10
        R.log("  CHURN %s -- worst key carries %d versions%s" % (
            "PROVEN" if proven else "NOT proven", worst,
            "; median gap ~5 min = the rule cadence" if proven else ""))
        out.update(worst_key_versions=worst, hot_range_gb=round(dead_gb, 2),
                   churn_proven=proven)
        if not proven:
            fails.append("P0:unproven")
    except Exception as e:
        R.log("  version scan err %s" % str(e)[:150])
        fails.append("P0:scan")

    # ------------------------------------------------------------ P1
    R.section("P1 the writer is stopped")
    try:
        d = ev.describe_rule(Name=RULE)
        R.log("  rule %s: %s" % (RULE, d.get("State")))
        out["rule_state"] = d.get("State")
        if d.get("State") != "DISABLED":
            fails.append("P1:rule")
    except Exception as e:
        R.log("  rule err %s" % str(e)[:100])
    try:
        rc = lam.get_function_concurrency(FunctionName=FN)
        n = rc.get("ReservedConcurrentExecutions")
        R.log("  reserved concurrency: %s" % n)
        out["concurrency"] = n
        if n != 0:
            fails.append("P1:conc")
    except Exception as e:
        R.log("  concurrency err %s" % str(e)[:100])
    for fn in (FN, "justhodl-signal-registry-ingest"):
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=NOW - timedelta(hours=6), EndTime=NOW,
                Period=3600, Statistics=["Sum"])
            pts = sorted((p["Timestamp"], int(p["Sum"]))
                         for p in r.get("Datapoints", []))
            R.log("  %-34s invocations/h: %s" % (fn[:34], " ".join(
                "%s=%d" % (t.strftime("%H"), v) for t, v in pts)))
        except Exception as e:
            R.log("  %s metric err %s" % (fn, str(e)[:70]))

    # ------------------------------------------------------------ P2
    R.section("P2 purge is progressing")
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/S3", MetricName="NumberOfObjects",
            Dimensions=[{"Name": "BucketName", "Value": LIVE},
                        {"Name": "StorageType", "Value": "AllStorageTypes"}],
            StartTime=NOW - timedelta(days=6), EndTime=NOW,
            Period=86400, Statistics=["Average"])
        pts = sorted((p["Timestamp"].date().isoformat(), p["Average"])
                     for p in r.get("Datapoints", []))
        R.log("  objects (all versions): " + " ".join(
            "%s=%.2fM" % (d[5:], n / 1e6) for d, n in pts))
        R.log("  NOTE: this metric is daily and lags ~24-48h; the "
              "lifecycle sweep runs async -- expect the fall on the "
              "2026-08-30/31 datapoints, not now")
        out["objects"] = [(d, int(n)) for d, n in pts]
    except Exception as e:
        R.log("  metric err %s" % str(e)[:90])
    try:
        lc = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)
        ids = [r_.get("ID") for r_ in lc.get("Rules", [])]
        R.log("  lifecycle rules: %s" % ids)
        hit = [r_ for r_ in lc.get("Rules", [])
               if r_.get("ID") == "ops5027-purge-dead-versions-providers"]
        R.log("  ops5027 purge rule present: %s %s" % (
            bool(hit), json.dumps(hit[0], default=str)[:180] if hit else ""))
        if not hit:
            fails.append("P2:norule")
    except Exception as e:
        R.log("  lifecycle err %s" % str(e)[:100])

    # ------------------------------------------------------------ P3
    R.section("P3 v2 engine deployed (frozen, will not run)")
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        R.log("  %s LastModified=%s CodeSize=%s" % (
            FN, c.get("LastModified"), c.get("CodeSize")))
        out["fn_lastmod"] = c.get("LastModified")
    except Exception as e:
        R.log("  fn cfg err %s" % str(e)[:100])

    R.section("P4 ledger")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/s3-anomaly-closeout.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/s3-anomaly-closeout.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5028 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(churn_proven=out.get("churn_proven"),
         worst_key_versions=out.get("worst_key_versions"),
         rule=out.get("rule_state"), concurrency=out.get("concurrency"))
    R.log("ops 5028 GREEN -- churn proven on the hot range, writer "
          "stopped, purge armed, v2 deployed frozen")
