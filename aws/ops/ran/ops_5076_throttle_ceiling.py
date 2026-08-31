"""ops_5076 -- we are invoke-throttled; stop adding parallelism blind.

ops 5075 produced two corrections, one of them mine.

 1. census-us is NOT broken. The invoke returned
        TooManyRequestsException ... Rate Exceeded
    and I logged "the walker is broken, not merely untriggered". The
    invoke never reached the function. A throttled call and a failing
    function look identical if you only read your own conclusion.
    This is the same account throttling that made three of six shards
    write nothing in ops 5062 and that I noted then and did not chase.

 2. The sentinel deploy DID land -- LastModified 02:02:25 against a
    02:01:51 push, 34 seconds later. So the dead-lanes check is in the
    deployed package and still produces no chip, which is a code-path
    problem, not a deploy one. Verified here by downloading the actual
    deployed zip and grepping it, rather than inferring from a
    timestamp again.

The throttling matters more than either. We now run census-econ on 12
shards, gdelt on 12, boj fanning out to 22 dbs, several on 2-5 minute
rules. Adding more workers into an account that is already refusing
invokes does not import data faster -- it converts work into
TooManyRequests and silent no-ops, which is exactly the waste Khalid
asked me to avoid. Measure the ceiling first.

  P0 account concurrency limits and FLEET-WIDE throttle counts
  P1 prove the sentinel's deployed code contains the check
  P2 census-us with backoff -- does the walker actually work
  P3 what the ceiling implies for the running lanes
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CST = "data/warm/census-us/_state/state.json"

cfg = Config(read_timeout=600, retries={"max_attempts": 6,
                                        "mode": "adaptive"})
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


with report("ops_5076_throttle_ceiling") as R:
    fails = []
    out = {"op": "ops_5076"}

    R.section("P0 the ceiling")
    try:
        a = lam.get_account_settings()
        lim = a.get("AccountLimit") or {}
        usg = a.get("AccountUsage") or {}
        R.log("  ConcurrentExecutions limit : %s" % lim.get(
            "ConcurrentExecutions"))
        R.log("  UnreservedConcurrentExecutions: %s" % lim.get(
            "UnreservedConcurrentExecutions"))
        R.log("  functions=%s  code storage %.1f GB of %.1f GB" % (
            usg.get("FunctionCount"),
            (usg.get("TotalCodeSize") or 0) / 1e9,
            (lim.get("TotalCodeSize") or 0) / 1e9))
        out["limit"] = lim.get("ConcurrentExecutions")
    except Exception as e:
        R.log("  account settings err %s" % str(e)[:110])
    for m in ("Throttles", "Invocations", "ConcurrentExecutions"):
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=m,
                StartTime=NOW - timedelta(hours=12), EndTime=NOW,
                Period=3600,
                Statistics=["Sum"] if m != "ConcurrentExecutions"
                else ["Maximum"])
            pts = sorted((p["Timestamp"], p.get("Sum", p.get("Maximum")))
                         for p in r.get("Datapoints", []))
            R.log("  %-22s %s" % (m, " ".join(
                "%s=%.0f" % (t.strftime("%H"), v) for t, v in pts[-12:])))
            if m == "Throttles":
                out["throttles_12h"] = int(sum(v for _, v in pts))
        except Exception as e:
            R.log("  %s err %s" % (m, str(e)[:80]))
    R.log("  fleet-wide throttles in 12h: %s" % out.get("throttles_12h"))
    if (out.get("throttles_12h") or 0) > 0:
        R.log("  -> invokes ARE being refused. More shards would be")
        R.log("     converted into TooManyRequests, not into data.")

    R.section("P1 is the check really in the deployed package")
    try:
        g = lam.get_function(FunctionName="justhodl-import-sentinel")
        url = (g.get("Code") or {}).get("Location")
        R.log("  deployed LastModified=%s" % (
            g.get("Configuration") or {}).get("LastModified"))
        raw = urllib.request.urlopen(url, timeout=120).read()
        z = zipfile.ZipFile(io.BytesIO(raw))
        src = z.read("lambda_function.py").decode("utf-8", "replace")
        R.log("  package %s bytes, lambda_function.py %s bytes" % (
            f"{len(raw):,}", f"{len(src):,}"))
        for tok in ("DEAD_LANE_H", "dead-lanes", "_state/"):
            R.log("  contains %-14s : %s" % (tok, tok in src))
        if "DEAD_LANE_H" not in src:
            R.log("  the deployed code does NOT contain the check -- the")
            R.log("  02:02 deploy predates or skipped that commit")
            fails.append("P1:missing")
        else:
            i = src.find("dead-lanes")
            R.log("  context: ...%s..." % src[max(0, i - 90):i + 60]
                  .replace("\\n", " ")[:150])
    except Exception as e:
        R.log("  package read err %s" % str(e)[:140])

    R.section("P2 census-us with backoff")
    c0 = jget(CST) or {}
    R.log("  before updated_at=%s phase=%s" % (c0.get("updated_at"),
                                               c0.get("phase")))
    ok = False
    for attempt in range(6):
        try:
            r = lam.invoke(FunctionName="justhodl-census-us",
                           InvocationType="Event", Payload=b"{}")
            R.log("  Event invoke accepted (attempt %d) status=%s"
                  % (attempt + 1, r.get("StatusCode")))
            ok = True
            break
        except Exception as e:
            R.log("  attempt %d refused: %s" % (attempt + 1,
                                                str(e)[:90]))
            time.sleep(20 * (attempt + 1))
    if not ok:
        R.log("  every attempt refused -- the account, not the lane")
        fails.append("P2:throttled")
    moved = False
    for i in range(12):
        time.sleep(45)
        c1 = jget(CST) or {}
        if c1.get("updated_at") != c0.get("updated_at"):
            moved = True
            R.log("  state MOVED %s -> %s : the walker works, it was "
                  "only ever untriggered" % (c0.get("updated_at"),
                                             c1.get("updated_at")))
            break
    if not moved:
        R.log("  state still unmoved after 9 min")
    out["census_moved"] = moved

    R.section("P3 what the ceiling means for the lanes")
    tot = {"done": 0, "codes": 0, "rows": 0}
    kw = {"Bucket": LIVE, "Prefix": "data/warm/boj-full/_state/",
          "MaxKeys": 1000}
    while True:
        rr = s3.list_objects_v2(**kw)
        for o in rr.get("Contents", []):
            if "api_" not in o["Key"]:
                continue
            d = jget(o["Key"]) or {}
            tot["done"] += int(d.get("done") or 0)
            tot["codes"] += len(d.get("codes") or [])
            tot["rows"] += int(d.get("rows") or 0)
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    ce = sum(len((jget("data/_state/census-econ-s%d.json" % k) or {})
                 .get("done") or []) for k in range(12))
    R.log("  BOJ %s/%s (%.1f%%) rows %s" % (
        f"{tot['done']:,}", f"{tot['codes']:,}",
        100.0 * tot["done"] / max(1, tot["codes"]), f"{tot['rows']:,}"))
    R.log("  census-econ %s/1,226" % f"{ce:,}")
    R.log("  Both are sharded already. With throttles present the next")
    R.log("  lever is NOT more shards -- it is a concurrency limit")
    R.log("  increase from AWS Support, or staggering the fan-outs so")
    R.log("  they do not all fire on the same minute boundary.")
    out.update(boj=tot["done"], census_econ=ce)
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/throttle-ceiling.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5076 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(limit=out.get("limit"), throttles_12h=out.get("throttles_12h"),
         census_moved=out.get("census_moved"), boj=out.get("boj"),
         census_econ=out.get("census_econ"))
    R.log("ops 5076 GREEN -- ceiling measured before more parallelism")
