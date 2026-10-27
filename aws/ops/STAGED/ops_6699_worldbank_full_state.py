"""ops 6699 - READ-ONLY: justhodl-worldbank-full, the writer behind the +82.5 GB/day alarm (ops 6698: 70.8 GB on Oct 26,
75.6 GB in the first 19 h of Oct 27, ~1,000+ versions of one ~2 MB key).  The code writes its whole state document
(_state/state.json, indented, the full `have` map) twice per indicator.  Before the fix: the state's size, phase and
progress, the versions on the state key, the schedules and parked chain tickets that drive it, its invocations in the
last 48 h, the sealed archive bucket's daily growth (every version is replicated there and kept), and the largest
indicator zips (to size the write-if-changed buffer).  Nothing is written.  No key involved.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"
ARCHIVE = "justhodl-warehouse-archive-857687956942"
PFX = "data/warm/worldbank-full/"
FN = "justhodl-worldbank-full"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=120)
GB = 1024 ** 3


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    logs = boto3.client("logs", region_name="us-east-1", config=CFG)
    cw = boto3.client("cloudwatch", region_name="us-east-1", config=CFG)
    sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
    ev = boto3.client("events", region_name="us-east-1", config=CFG)
    with report("ops_6699_worldbank_full_state") as r:
        r.heading("ops 6699 - justhodl-worldbank-full: state, drivers, archive impact (read-only)")
        obj = s3.get_object(Bucket=LIVE, Key=PFX + "_state/state.json")
        raw = obj["Body"].read()
        st = json.loads(raw)
        have = st.get("have") or {}
        r.log("state.json %d bytes (LastModified %s): phase %s, n_indicators %s, have %d (fresh %d, no_data %d), queue %d, failures %d, as_of %s, lease_until %s" % (
            len(raw), obj["LastModified"].isoformat(), st.get("phase"), st.get("n_indicators"), len(have),
            sum(1 for v in have.values() if v.get("status") == "fresh"), sum(1 for v in have.values() if v.get("status") == "no_data"),
            len(st.get("queue") or []), len(st.get("failures") or {}), st.get("as_of"), st.get("lease_until")))
        r.log("bytes banked (state): %.2f GB; queue head %s" % ((st.get("bytes_total") or 0) / 1e9, (st.get("queue") or [])[:3]))
        sizes = sorted((v.get("bytes") or 0 for v in have.values()), reverse=True)
        if sizes:
            r.log("zip sizes: max %.1f MB, p99 %.2f MB, median %.3f MB, sum %.2f GB" % (
                sizes[0] / 1e6, sizes[len(sizes) // 100] / 1e6, sizes[len(sizes) // 2] / 1e6, sum(sizes) / 1e9))
        n, nbytes, newest, oldest, tok = 0, 0, None, None, {}
        while True:
            resp = s3.list_object_versions(Bucket=LIVE, Prefix=PFX + "_state/state.json", MaxKeys=1000, **tok)
            for v in resp.get("Versions", []):
                n += 1
                nbytes += v["Size"]
                newest = max(newest or v["LastModified"], v["LastModified"])
                oldest = min(oldest or v["LastModified"], v["LastModified"])
            if not resp.get("IsTruncated") or n >= 200000:
                break
            tok = {"KeyMarker": resp["NextKeyMarker"], "VersionIdMarker": resp["NextVersionIdMarker"]}
        r.log("state.json versions in the live bucket: %d, %.1f GB, %s .. %s" % (n, nbytes / GB, oldest, newest))
        r.section("drivers")
        cfg = lam.get_function_configuration(FunctionName=FN)
        try:
            rc = lam.get_function_concurrency(FunctionName=FN).get("ReservedConcurrentExecutions")
        except Exception as e:  # noqa: BLE001
            rc = type(e).__name__
        r.log("function: %s MB / %s s, LastModified %s, reserved concurrency %s" % (cfg["MemorySize"], cfg["Timeout"], cfg["LastModified"], rc))
        for page in sch.get_paginator("list_schedules").paginate():
            for s in page.get("Schedules", []):
                if "worldbank" in s["Name"]:
                    d = sch.get_schedule(Name=s["Name"], GroupName=s.get("GroupName", "default"))
                    r.log("schedule %s [%s] %s target %s input %s" % (s["Name"], d.get("State"), d.get("ScheduleExpression"),
                                                                    d["Target"]["Arn"].split(":")[-1], (d["Target"].get("Input") or "")[:120]))
        for page in ev.get_paginator("list_rule_names_by_target").paginate(TargetArn=cfg["FunctionArn"]):
            for name in page.get("RuleNames", []):
                rule = ev.describe_rule(Name=name)
                r.log("rule %s [%s] %s" % (name, rule.get("State"), rule.get("ScheduleExpression") or rule.get("EventPattern")))
        parked = s3.list_objects_v2(Bucket=LIVE, Prefix="data/_state/chain-parked/" + FN + "/").get("Contents", [])
        r.log("parked chain tickets: %d %s" % (len(parked), [p["LastModified"].isoformat()[:19] for p in parked[:5]]))
        since = int((datetime.now(timezone.utc) - timedelta(hours=48)).timestamp() * 1000)
        evs = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=since, filterPattern='"REPORT RequestId"', limit=10000).get("events", [])
        dur = [float(e["message"].split("Duration: ")[1].split(" ms")[0]) for e in evs if "Duration: " in e["message"]]
        r.log("invocations (REPORT) in 48 h: %d, total duration %.1f h, first %s last %s" % (
            len(evs), sum(dur) / 3.6e6, datetime.fromtimestamp(evs[0]["timestamp"] / 1000, timezone.utc).isoformat()[:16] if evs else None,
            datetime.fromtimestamp(evs[-1]["timestamp"] / 1000, timezone.utc).isoformat()[:16] if evs else None))
        r.section("the sealed archive bucket (every version replicated, kept)")
        end = datetime.now(timezone.utc)
        q = [{"Id": "b%d" % i, "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "BucketSizeBytes", "Dimensions": [
            {"Name": "BucketName", "Value": ARCHIVE}, {"Name": "StorageType", "Value": c}]}, "Period": 86400, "Stat": "Average"}}
             for i, c in enumerate(["StandardStorage", "StandardIAStorage", "IntelligentTieringFAStorage", "IntelligentTieringIAStorage"])]
        res = cw.get_metric_data(MetricDataQueries=q, StartTime=end - timedelta(days=9), EndTime=end)["MetricDataResults"]
        days = {}
        for m in res:
            for t, v in zip(m["Timestamps"], m["Values"]):
                days[t.date().isoformat()] = days.get(t.date().isoformat(), 0) + v
        prev = None
        for d in sorted(days):
            r.log("archive %s: %.1f GB%s" % (d, days[d] / GB, "" if prev is None else " (%+.1f GB)" % ((days[d] - prev) / GB)))
            prev = days[d]
        try:
            vs = s3.list_object_versions(Bucket=ARCHIVE, Prefix=PFX + "_state/state.json", MaxKeys=1000).get("Versions", [])
            r.log("archive copies of state.json (first page): %d, %.2f GB" % (len(vs), sum(v["Size"] for v in vs) / GB))
        except Exception as e:  # noqa: BLE001
            r.log("archive listing: %s" % type(e).__name__)
        r.ok("ops 6699 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
