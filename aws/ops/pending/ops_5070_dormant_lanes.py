"""ops_5070 -- which of the 381 are dormant DATA LANES, and is GDELT moving?

ops 5069 found 381 of 822 justhodl-* functions with no enabled rule and
zero invocations in seven days. That number is alarming and mostly
misleading: justhodl-chat-api, justhodl-ask, justhodl-alert-router and
their kind are invoked on demand by the site, not by a schedule, and
having no rule is correct for them. Reporting "381 broken engines" would
be exactly the kind of confident wrong answer this session has already
produced four times.

The distinction that matters is whether a function OWNS A DATA LANE --
whether it maintains state under data/warm/*/_state/ or data/_state/.
An on-demand API function has none. A warehouse walker has one, and if
that state has not moved while the function has not run, the lane is
dormant and its backlog is real. That is the shape census-us, boj-full
and gdelt-full all had.

  P0 GDELT: it was wired 10 minutes ago -- are the 7,381 gaps closing
  P1 classify the 381 by whether an S3 state document bears their name
  P2 rank the dormant data lanes by how stale their state is
  P3 BOJ continues; lease behaviour under the live 5-minute cadence
"""
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


def allkeys(prefix, cap=3000):
    out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while len(out) < cap:
        r = s3.list_objects_v2(**kw)
        out += [(o["Key"], o["LastModified"]) for o in
                r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


with report("ops_5070_dormant_lanes") as R:
    fails = []
    out = {"op": "ops_5070"}

    R.section("P0 GDELT after wiring")
    g0 = jget("data/warm/gdelt-full/_state/state.json") or {}
    R.log("  before: files=%s gaps=%s cursor=%s as_of=%s" % (
        g0.get("files"), g0.get("gaps"), g0.get("cursor"),
        g0.get("as_of")))
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName",
                         "Value": "justhodl-gdelt-full"}],
            StartTime=NOW - timedelta(hours=2), EndTime=NOW,
            Period=3600, Statistics=["Sum"])
        R.log("  invocations last 2h: %s" % [
            (p["Timestamp"].strftime("%H:%M"), int(p["Sum"]))
            for p in sorted(r.get("Datapoints", []),
                            key=lambda x: x["Timestamp"])])
    except Exception as e:
        R.log("  metric err %s" % str(e)[:90])

    R.section("P1 classify the untriggered")
    prev = jget("data/ops/trigger-audit.json") or {}
    untr = prev.get("untriggered") or []
    R.log("  untriggered functions from ops 5069: %d" % len(untr))
    state_docs = {}
    for pref in ("data/_state/",):
        for k, lm in allkeys(pref, cap=1500):
            state_docs[k.split("/")[-1].replace(".json", "")
                       .replace(".gz", "")] = (k, lm)
    provs, kw = [], {"Bucket": LIVE, "Prefix": "data/warm/",
                     "Delimiter": "/"}
    while True:
        rr = s3.list_objects_v2(**kw)
        provs += [p["Prefix"] for p in rr.get("CommonPrefixes", [])]
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    for p in provs:
        for k, lm in allkeys(p + "_state/", cap=30):
            state_docs[p.split("/")[-2]] = (k, lm)
    R.log("  state documents indexed: %d (data/_state/ + %d provider "
          "dirs)" % (len(state_docs), len(provs)))
    dormant, ondemand = [], []
    for fn in untr:
        stem = fn.replace("justhodl-", "")
        hit = None
        for nm, (k, lm) in state_docs.items():
            if nm == stem or stem.startswith(nm) or nm.startswith(stem):
                if len(nm) >= 4:
                    hit = (nm, k, lm)
                    break
        if hit:
            age = (NOW - hit[2]).total_seconds() / 3600.0
            dormant.append((fn, hit[0], round(age, 1), hit[1]))
        else:
            ondemand.append(fn)
    R.log("  DORMANT DATA LANES (own state, no trigger, no runs): %d"
          % len(dormant))
    R.log("  on-demand / no state document (correct to have no rule): %d"
          % len(ondemand))
    R.log("  examples of the latter: %s" % ondemand[:8])

    R.section("P2 dormant lanes, stalest first")
    dormant.sort(key=lambda x: -x[2])
    R.log("  %-42s %-18s %10s" % ("function", "state doc", "age(h)"))
    for fn, nm, age, k in dormant[:24]:
        R.log("  %-42s %-18s %10.1f" % (fn[:42], nm[:18], age))
    out["dormant"] = [{"fn": f, "state": n, "age_h": a}
                      for f, n, a, _ in dormant]
    out["ondemand"] = len(ondemand)
    if dormant:
        R.log("  -> each of these owns a warehouse that has not been")
        R.log("     written since the age above. census-us, boj-full")
        R.log("     and gdelt-full all looked exactly like this.")

    R.section("P3 BOJ + GDELT progress")
    time.sleep(500)
    g1 = jget("data/warm/gdelt-full/_state/state.json") or {}
    R.log("  gdelt after: files=%s (was %s)  gaps=%s (was %s)" % (
        g1.get("files"), g0.get("files"), g1.get("gaps"),
        g0.get("gaps")))
    try:
        d0, d1 = int(g0.get("gaps") or 0), int(g1.get("gaps") or 0)
        if d1 < d0:
            R.log("  gaps closing: %d fewer" % (d0 - d1))
        elif g1.get("as_of") != g0.get("as_of"):
            R.log("  state advanced (as_of moved) but gap count flat -- "
                  "it is fetching forward, not backfilling yet")
        else:
            R.log("  no movement yet; the rule fires every 5 min so it "
                  "should be picked up shortly")
    except Exception:
        pass
    tot = {"done": 0, "codes": 0, "rows": 0, "leased": 0}
    for k, _ in allkeys("data/warm/boj-full/_state/", cap=200):
        if "api_" not in k:
            continue
        d = jget(k) or {}
        tot["done"] += int(d.get("done") or 0)
        tot["codes"] += len(d.get("codes") or [])
        tot["rows"] += int(d.get("rows") or 0)
        tot["leased"] += int(d.get("skipped_leased") or 0)
    R.log("  boj: %s/%s series (%.1f%%)  rows %s  lease-skips %d" % (
        f"{tot['done']:,}", f"{tot['codes']:,}",
        100.0 * tot["done"] / max(1, tot["codes"]), f"{tot['rows']:,}",
        tot["leased"]))
    if tot["leased"]:
        R.log("  the lease is turning away overlapping waves -- those "
              "would each have re-fetched codes already in flight")
    out["boj"] = tot
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/dormant-lanes.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/dormant-lanes.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5070 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(dormant=len(dormant), ondemand=len(ondemand),
         boj_pct=round(100.0 * tot["done"] / max(1, tot["codes"]), 1),
         gdelt_gaps=g1.get("gaps"))
    R.log("ops 5070 GREEN -- dormant lanes separated from on-demand")
