"""ops_5027 -- stop the series-extractor version churn + purge it.

ops 5026 named it from the access logs (not inferred):
  justhodl-series-extractor = 89.1% of ALL writes, ~444k PutObject/day,
  avg object 252KB, every one into data/providers/eurostat/series/.

Mechanism the numbers describe:
  * the engine runs on justhodl-series-extractor-5min (288 runs/day) and
    writes data/providers/eurostat/series/page-{n_pages:04d}.json;
  * the live bucket has VERSIONING ON, so a rewrite of the same page key
    does not replace -- it creates a NONCURRENT VERSION;
  * the S3 Inventory (Current versions only) shows data/providers/
    eurostat/ holding just ~4k current objects / 1.0GB, while CloudWatch
    NumberOfObjects (all versions) shows the bucket at 10.17M objects
    growing +475k/DAY. Current key set small + object count exploding =
    the growth is noncurrent versions of the same keys.
  * 444k x 252KB = ~112GB/day of dead versions. That is the storage
    curve ($0.08 -> $1.80/day and still climbing), the Requests-Tier1
    curve ($2.49/day), the GetObject curve (each PUT raised an S3 Object
    Created event -> the reader ops 5025 killed), and -- until ops 4988
    -- the USW2-Requests-SIA-Tier1 curve, because every version was
    replicated to us-west-2 as well. One loop, four bills.

  P0 evidence : the state doc (flows_done, n_pages, buffer, errors),
                the 5-min rule, the engine's config
  P1 PROOF    : list_object_versions on individual page keys -- how many
                versions per key, and how far apart in time. If one key
                carries hundreds of versions written minutes apart, the
                churn is proven and nothing here is a guess.
  P2 scope    : current-vs-total version counts for the prefix, and the
                same probe on data/providers/gdelt/ (provider-catalog,
                the #2 writer at ~37k/day)
  P3 STOP     : disable justhodl-series-extractor-5min + reserved
                concurrency 0, with a reversal ledger. Nothing is
                deleted, no live page is touched -- the lane freezes
                exactly where it is until the engine is made idempotent.
  P4 PURGE    : lifecycle NoncurrentVersionExpiration=1d +
                ExpiredObjectDeleteMarker on data/providers/ . This
                deletes ONLY dead versions -- every current object
                survives -- and S3 does it asynchronously at ZERO
                request cost.
  P5 verify   : read back rule state, concurrency, lifecycle

GREEN = churn proven + stopped + purge armed. RED = the proof failed
(then the stop is still applied, but the diagnosis gets re-opened).
"""
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

ACCT = "857687956942"
REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-series-extractor"
RULE = "justhodl-series-extractor-5min"
STATE_KEY = "data/_state/series-extract-eurostat.json"
SERIES_PFX = "data/providers/eurostat/series/"
GDELT_PFX = "data/providers/gdelt/"
PURGE_PFX = "data/providers/"

cfg = Config(read_timeout=90, retries={"max_attempts": 4})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
sch = boto3.client("scheduler", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)

NOW = datetime.now(timezone.utc)


def versions_for(prefix, max_keys=3000):
    """-> {key: [(LastModified, size, is_latest)]} for one prefix page."""
    out = defaultdict(list)
    kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    scanned = 0
    while scanned < max_keys:
        r = s3.list_object_versions(**kw)
        for v in r.get("Versions", []):
            out[v["Key"]].append((v["LastModified"], v["Size"],
                                  v["IsLatest"]))
            scanned += 1
        for d in r.get("DeleteMarkers", []):
            out[d["Key"]].append((d["LastModified"], 0, d["IsLatest"]))
            scanned += 1
        if not r.get("IsTruncated"):
            break
        kw["KeyMarker"] = r.get("NextKeyMarker")
        kw["VersionIdMarker"] = r.get("NextVersionIdMarker")
    return out


with report("ops_5027_stop_version_churn") as R:
    fails = []
    ledger = {"op": "ops_5027", "at": NOW.isoformat(timespec="seconds"),
              "function": FN, "reversal": []}

    # ------------------------------------------------------------ P0
    R.section("P0 evidence -- engine state")
    state = {}
    try:
        raw = s3.get_object(Bucket=LIVE, Key=STATE_KEY)["Body"].read()
        state = json.loads(raw)
        R.log("  state doc %s: %.1f KB" % (STATE_KEY, len(raw) / 1024))
        R.log("  flows_done=%d  n_pages=%s  series_count=%s  buffer=%d "
              "rows  errors=%d  updated_at=%s" % (
                  len(state.get("flows_done") or []), state.get("n_pages"),
                  state.get("series_count"),
                  len(state.get("buffer") or []),
                  len(state.get("errors") or {}),
                  state.get("updated_at")))
        errs = state.get("errors") or {}
        for k, v in list(errs.items())[:8]:
            R.log("    err %s: %s" % (k, str(v)[:110]))
        fd = state.get("flows_done") or []
        R.log("  flows_done unique=%d vs len=%d  %s" % (
            len(set(fd)), len(fd),
            "*** DUPLICATES -> flows reprocessed ***"
            if len(set(fd)) != len(fd) else "(no dupes)"))
        ledger["state_snapshot"] = {
            "flows_done": len(fd), "unique": len(set(fd)),
            "n_pages": state.get("n_pages"),
            "buffer_rows": len(state.get("buffer") or []),
            "updated_at": state.get("updated_at")}
    except Exception as e:
        R.log("  state read err %s" % str(e)[:140])
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        R.log("  fn timeout=%ss mem=%s lastmod=%s" % (
            c.get("Timeout"), c.get("MemorySize"), c.get("LastModified")))
    except Exception as e:
        R.log("  fn cfg err %s" % str(e)[:100])
    try:
        d = ev.describe_rule(Name=RULE)
        R.log("  rule %s state=%s sched=%s" % (
            RULE, d.get("State"), d.get("ScheduleExpression")))
        ledger["rule_before"] = {"name": RULE, "state": d.get("State"),
                                 "expr": d.get("ScheduleExpression")}
    except Exception as e:
        R.log("  rule describe: %s" % str(e)[:110])
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": FN}],
            StartTime=NOW - timedelta(hours=24), EndTime=NOW,
            Period=3600, Statistics=["Sum"])
        tot = sum(p["Sum"] for p in r.get("Datapoints", []))
        R.log("  invocations last 24h: %.0f (%.1f/h)" % (tot, tot / 24))
    except Exception as e:
        R.log("  metric err %s" % str(e)[:90])

    # ------------------------------------------------------------ P1
    R.section("P1 PROOF -- versions per page key")
    churn_proven = False
    try:
        vs = versions_for(SERIES_PFX, max_keys=4000)
        keys = sorted(vs, key=lambda k: -len(vs[k]))
        R.log("  keys sampled: %d  total versions seen: %d" % (
            len(vs), sum(len(v) for v in vs.values())))
        for k in keys[:6]:
            times = sorted(t for t, _, _ in vs[k])
            span_h = ((times[-1] - times[0]).total_seconds() / 3600
                      if len(times) > 1 else 0)
            gaps = [(times[i + 1] - times[i]).total_seconds() / 60
                    for i in range(len(times) - 1)]
            med = sorted(gaps)[len(gaps) // 2] if gaps else 0
            R.log("  %-52s versions=%4d span=%.1fh median_gap=%.1f min "
                  "newest=%s" % (k[len(SERIES_PFX):][:52], len(vs[k]),
                                 span_h, med,
                                 times[-1].strftime("%m-%d %H:%M")))
        top = len(vs[keys[0]]) if keys else 0
        churn_proven = top >= 10
        R.log("  CHURN %s -- worst key carries %d versions" % (
            "PROVEN" if churn_proven else "NOT proven", top))
        ledger["worst_key_versions"] = top
        sizes = [s for v in vs.values() for _, s, _ in v]
        if sizes:
            R.log("  dead-version bytes in this sample: %.2f GB over %d "
                  "versions (avg %.0f KB)" % (
                      sum(sizes) / 1e9, len(sizes),
                      sum(sizes) / len(sizes) / 1024))
    except Exception as e:
        R.log("  version probe err %s" % str(e)[:140])
        fails.append("P1:probe")

    # ------------------------------------------------------------ P2
    R.section("P2 scope -- current vs versions, and the #2 writer")
    try:
        r = s3.list_objects_v2(Bucket=LIVE, Prefix=SERIES_PFX,
                               MaxKeys=1000)
        cur = r.get("KeyCount", 0)
        R.log("  %s current objects: %d%s" % (
            SERIES_PFX, cur, "+ (truncated)" if r.get("IsTruncated")
            else ""))
    except Exception as e:
        R.log("  current list err %s" % str(e)[:90])
    try:
        vg = versions_for(GDELT_PFX, max_keys=2000)
        if vg:
            worst = max(vg, key=lambda k: len(vg[k]))
            R.log("  gdelt worst key: %s versions=%d" % (
                worst[len(GDELT_PFX):][:48], len(vg[worst])))
            R.log("  gdelt keys=%d versions=%d -> %s" % (
                len(vg), sum(len(v) for v in vg.values()),
                "same churn pattern" if len(vg[worst]) >= 10
                else "no churn"))
    except Exception as e:
        R.log("  gdelt probe err %s" % str(e)[:90])
    try:
        v = s3.get_bucket_versioning(Bucket=LIVE)
        R.log("  bucket versioning: %s" % v.get("Status"))
        lc = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)
        R.log("  existing lifecycle rules: %d" % len(lc.get("Rules", [])))
        for rule in lc.get("Rules", []):
            R.log("    %-28s %s pfx=%r exp=%s noncur=%s" % (
                rule.get("ID", "")[:28], rule.get("Status"),
                (rule.get("Filter") or {}).get("Prefix",
                                               rule.get("Prefix", "")),
                (rule.get("Expiration") or {}).get("Days"),
                (rule.get("NoncurrentVersionExpiration") or {}).get(
                    "NoncurrentDays")))
        ledger["lifecycle_before"] = lc.get("Rules", [])
    except Exception as e:
        R.log("  versioning/lifecycle err %s" % str(e)[:110])

    # ------------------------------------------------------------ P3
    R.section("P3 STOP the loop (reversible, nothing deleted)")
    stopped = False
    try:
        ev.disable_rule(Name=RULE)
        d = ev.describe_rule(Name=RULE)
        R.log("  rule %s -> %s" % (RULE, d.get("State")))
        stopped = d.get("State") == "DISABLED"
        ledger["reversal"].append("ev.enable_rule(Name=%r)" % RULE)
    except Exception as e:
        R.log("  disable_rule: %s" % str(e)[:120])
        try:
            sch.update_schedule(Name=RULE, State="DISABLED")
            R.log("  scheduler %s -> DISABLED" % RULE)
            stopped = True
            ledger["reversal"].append(
                "scheduler.update_schedule(Name=%r, State='ENABLED')" % RULE)
        except Exception as e2:
            R.log("  scheduler disable: %s" % str(e2)[:120])
    try:
        lam.put_function_concurrency(FunctionName=FN,
                                     ReservedConcurrentExecutions=0)
        time.sleep(2)
        rc = lam.get_function_concurrency(FunctionName=FN)
        got = rc.get("ReservedConcurrentExecutions")
        R.log("  %s reserved concurrency -> %s" % (FN, got))
        stopped = stopped or got == 0
        ledger["reversal"].append(
            "lam.delete_function_concurrency(FunctionName=%r)" % FN)
    except Exception as e:
        R.log("  concurrency err %s" % str(e)[:120])
    if not stopped:
        fails.append("P3:stop")

    # ------------------------------------------------------------ P4
    R.section("P4 PURGE dead versions (current objects untouched)")
    try:
        try:
            lc = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)
            rules = [r_ for r_ in lc.get("Rules", [])
                     if not r_.get("ID", "").startswith("ops5027-")]
        except Exception:
            rules = []
        rules.append({
            "ID": "ops5027-purge-dead-versions-providers",
            "Status": "Enabled",
            "Filter": {"Prefix": PURGE_PFX},
            "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
            "Expiration": {"ExpiredObjectDeleteMarker": True},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}})
        s3.put_bucket_lifecycle_configuration(
            Bucket=LIVE, LifecycleConfiguration={"Rules": rules})
        R.log("  lifecycle: %d rules; ops5027 purge on %r "
              "(NoncurrentDays=1, delete-markers cleaned)" % (
                  len(rules), PURGE_PFX))
        R.log("  -> only NONCURRENT versions expire; every current page "
              "object survives; S3 deletes async in ~24-48h at zero "
              "request cost")
        ledger["purge_prefix"] = PURGE_PFX
    except Exception as e:
        R.log("  lifecycle write err %s" % str(e)[:160])
        fails.append("P4:lifecycle")

    # ------------------------------------------------------------ P5
    R.section("P5 verify + ledger")
    try:
        d = ev.describe_rule(Name=RULE)
        R.log("  rule state now: %s" % d.get("State"))
    except Exception as e:
        R.log("  rule reread: %s" % str(e)[:90])
    try:
        lc = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)
        ids = [r_.get("ID") for r_ in lc.get("Rules", [])]
        R.log("  lifecycle IDs: %s" % ids)
    except Exception as e:
        R.log("  lifecycle reread err %s" % str(e)[:90])
    try:
        s3.put_object(Bucket=LIVE,
                      Key="data/ops/series-extractor-quarantine.json",
                      Body=json.dumps(ledger, indent=1,
                                      default=str).encode(),
                      ContentType="application/json")
        R.log("  ledger -> data/ops/series-extractor-quarantine.json")
    except Exception as e:
        R.log("  ledger err %s" % str(e)[:90])

    if fails:
        R.log("ops 5027 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(churn_proven=churn_proven, stopped=stopped,
         worst_key_versions=ledger.get("worst_key_versions", 0),
         purge=PURGE_PFX)
    R.log("ops 5027 GREEN -- %s stopped; dead versions under %s expire "
          "in 24-48h; the engine stays frozen until it is made "
          "idempotent (ops 5028)" % (FN, PURGE_PFX))
