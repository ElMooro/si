"""ops 6624 - READ-ONLY: the detail behind the two biggest movable lines of the steady-state bill (Khalid 2026-10-23).

ops 6623 found billed S3 egress at 16.6 GB on Oct 22 and a 43 GB/day pace on Oct 23, more than half of it direct
(not through the Worker) from one Safari-on-Mac session on Apple's 17.166.0.0/16 network, plus two CloudWatch
Metrics Insights alarms behind CW:MetricInsightAlarmUsage ($0.62/day). This op, read-only:

  A. splits the billed GETs of Oct 22 and Oct 23 (so far) by requester class x key with request counts and HTTP
     status (200 full body vs 304 revalidated vs 206 range), and the direct (anonymous-other) reads by hour, so the
     pages behind them can be fixed one by one;
  B. reads the governor's per-function meter correctly (day -> {function: [invocations, usd]}) for the last 3 days;
  C. prints both Metrics Insights alarms in full (threshold, actions) and checks whether account-level metrics
     exist that a standard alarm could watch instead (AWS/Logs IncomingBytes and AWS/Lambda
     RecursiveInvocationsDropped with no dimensions), plus how many per-resource metrics each query analyses;
  D. the two EC2 instances that own the in-use EBS volumes (state and when they stopped).
Nothing is written to AWS. Cost: ~10 k small GETs of log objects + listings (< $0.05).
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import concurrent.futures as cf
import json
import re
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
PRIVATE = "justhodl-ops-private-857687956942"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
CF_NETS = ("162.158.", "162.159.", "172.64.", "172.65.", "172.66.", "172.67.", "172.68.", "172.69.", "172.70.", "172.71.",
           "104.16.", "104.17.", "104.18.", "104.19.", "104.20.", "104.21.", "104.22.", "104.23.", "104.24.",
           "104.25.", "104.26.", "104.27.", "108.162.", "141.101.", "190.93.", "188.114.", "197.234.", "198.41.",
           "173.245.", "103.21.", "103.22.", "103.31.", "131.0.72.")
s3 = boto3.client("s3", region_name=REGION, config=CFG)


def billed_class(req, ip):
    if "assumed-role" in req:
        return None                       # in-region Lambda / replication reads: no transfer charge
    if ":user/" in req:
        return "runner:" + req.split("/")[-1]
    if req == "-":
        return "cloudflare" if ip.startswith(CF_NETS) else "direct"
    return None


def sample_day(r, tb, base, day, n_sample):
    keys, token = [], None
    while True:
        kw = {"Bucket": tb, "Prefix": base + day + "/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [(o["Key"], o["LastModified"]) for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated") or len(keys) >= 900000:
            break
        token = resp.get("NextContinuationToken")
    if not keys:
        return None
    newest, oldest = max(lm for _, lm in keys), min(lm for _, lm in keys)
    step_n = max(1, len(keys) // n_sample)
    sample = [k for k, _ in keys[::step_n]][:n_sample]
    scale = len(keys) / float(len(sample) or 1)
    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    span_h = max(1.0, (newest - oldest).total_seconds() / 3600.0)
    f = scale * (24.0 / span_h if day == today else 1.0)

    def read(k):
        try:
            return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    by_ck = defaultdict(lambda: [0, 0, Counter()])   # (class, key) -> [bytes, requests, statuses]
    direct_hour, direct_ip, direct_ua, cls_status = Counter(), Counter(), Counter(), Counter()
    with cf.ThreadPoolExecutor(max_workers=48) as ex:
        for text in ex.map(read, sample):
            for line in text.splitlines():
                m = LOG_RE.match(line)
                if not m or m.group(7) not in ("REST.GET.OBJECT", "REST.HEAD.OBJECT"):
                    continue
                cls = billed_class(m.group(5), m.group(4))
                if not cls:
                    continue
                b = int(m.group(12)) if m.group(12).isdigit() else 0
                key = m.group(8)
                st = m.group(10)
                row = by_ck[(cls, key)]
                row[0] += b
                row[1] += 1
                row[2][st] += 1
                cls_status["%s %s" % (cls, st)] += 1
                if cls == "direct":
                    direct_hour[m.group(3)[12:14]] += b
                    direct_ip[".".join(m.group(4).split(".")[:3]) + ".*"] += b
                    direct_ua[(m.group(17) or "-")[:90]] += b
    cls_tot = Counter()
    for (cls, _), (b, _, _) in by_ck.items():
        cls_tot[cls] += b
    out = {"day": day, "scale_per_day": round(f, 2), "log_objects": len(keys), "read": len(sample),
           "gb_day_by_class": {c: round(v * f / 1e9, 3) for c, v in cls_tot.items()},
           "requests_day_by_class_status": {k: round(v * f) for k, v in cls_status.most_common(20)},
           "top": {}, "direct_by_hour_gb": {h: round(v * f / 1e9, 3) for h, v in sorted(direct_hour.items())},
           "direct_by_ip24_gb": [(k, round(v * f / 1e9, 3)) for k, v in direct_ip.most_common(8)],
           "direct_by_ua_gb": [(k, round(v * f / 1e9, 3)) for k, v in direct_ua.most_common(5)]}
    for cls in sorted(cls_tot):
        rows = sorted(((k, v) for (c, k), v in by_ck.items() if c == cls), key=lambda x: -x[1][0])[:25]
        out["top"][cls] = [{"key": k, "gb_day": round(v[0] * f / 1e9, 3), "req_day": round(v[1] * f),
                            "mb_per_req": round(v[0] / max(1, v[1]) / 1e6, 2), "status": dict(v[2])} for k, v in rows]
    r.log("%s (x%.1f/day): billed GB/day by class %s" % (day, f, out["gb_day_by_class"]))
    r.log("   requests/day by class+status: %s" % out["requests_day_by_class_status"])
    for cls, rows in out["top"].items():
        r.log("   %s top keys: %s" % (cls, "; ".join("%s %.2f GB (%d req, %.1f MB/req, %s)" % (
            x["key"], x["gb_day"], x["req_day"], x["mb_per_req"], x["status"]) for x in rows[:10])))
    r.log("   direct by hour (GB): %s" % out["direct_by_hour_gb"])
    r.log("   direct by /24: %s | by UA: %s" % (out["direct_by_ip24_gb"][:4], out["direct_by_ua_gb"][:3]))
    return out


def meter_days(state):
    """Find {YYYY-MM-DD: {function: [invocations, usd]}} anywhere in the governor state."""
    hits = []

    def walk(o, path, depth):
        if depth > 6 or not isinstance(o, dict):
            return
        days = [k for k in o if isinstance(k, str) and re.match(r"\d{4}-\d{2}-\d{2}$", k)]
        if days and all(isinstance(o[k], dict) for k in days):
            sample = next(iter(o[days[-1]].values()), None) if o[days[-1]] else None
            if isinstance(sample, list) and len(sample) >= 2:
                hits.append((path, o))
                return
        for k, v in o.items():
            walk(v, path + "/" + str(k), depth + 1)
    walk(state, "", 0)
    return hits


def main():
    now = datetime.now(timezone.utc)
    data = {"generated_at": now.isoformat()}
    with report("ops_6624_egress_and_brakes_detail") as r:
        r.heading("ops 6624 - egress by page key, Lambda by engine, the two Metrics Insights alarms (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))
        failures = 0
        r.section("A. billed S3 reads by class x key")
        try:
            lg = s3.get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
            tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
            base = "%s%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET)
            data["egress"] = [x for x in (sample_day(r, tb, base, d, 6000) for d in ("2026/10/22", now.strftime("%Y/%m/%d"))) if x]
        except Exception as ex:  # noqa: BLE001
            failures += 1
            r.warn("egress section failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.section("B. governor meter, Lambda $ by engine")
        try:
            state = json.loads(s3.get_object(Bucket=PRIVATE, Key="spend-governor/state.json")["Body"].read())
            hits = meter_days(state)
            data["meter"] = {}
            for path, days in hits[:2]:
                for day in sorted(days)[-3:]:
                    rows = sorted(((fn, v[0], v[1]) for fn, v in days[day].items() if isinstance(v, list) and len(v) >= 2), key=lambda x: -x[2])
                    tot = sum(x[2] for x in rows)
                    data["meter"]["%s %s" % (path, day)] = {"usd": round(tot, 4), "n": len(rows), "top": [(a, int(b), round(c, 4)) for a, b, c in rows[:30]]}
                    r.log("%s %s: $%.2f over %d engines; top: %s" % (path, day, tot, len(rows), "; ".join("%s $%.3f (%d)" % (a, c, b) for a, b, c in rows[:15])))
            if not hits:
                r.warn("no per-function day map found; top-level keys: %s" % list(state)[:30])
        except Exception as ex:  # noqa: BLE001
            failures += 1
            r.warn("meter read failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.section("C. the two Metrics Insights alarms and their account-level alternatives")
        try:
            cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
            res = cw.describe_alarms(AlarmNames=["justhodl-guard-lambda-recursion-dropped", "justhodl-logs-ingestion-2gb-hour",
                                                 "justhodl-guard-lambda-recursion-dropped-account"])
            data["mi_alarms"] = []
            for a in res.get("MetricAlarms", []):
                slim = {k: a.get(k) for k in ("AlarmName", "StateValue", "Threshold", "ComparisonOperator", "EvaluationPeriods",
                                               "DatapointsToAlarm", "TreatMissingData", "AlarmActions", "OKActions", "Namespace",
                                               "MetricName", "Period", "Statistic", "Dimensions", "AlarmDescription")}
                slim["Metrics"] = a.get("Metrics")
                data["mi_alarms"].append(slim)
                r.log("alarm %s" % json.dumps(slim, default=str)[:900])
            checks = {}
            for ns, metric, dim in (("AWS/Logs", "IncomingBytes", "LogGroupName"), ("AWS/Lambda", "RecursiveInvocationsDropped", "FunctionName")):
                n_dim, n_none = 0, 0
                for page in cw.get_paginator("list_metrics").paginate(Namespace=ns, MetricName=metric, RecentlyActive="PT3H"):
                    for mtr in page.get("Metrics", []):
                        if not mtr.get("Dimensions"):
                            n_none += 1
                        elif any(d["Name"] == dim for d in mtr["Dimensions"]):
                            n_dim += 1
                checks["%s/%s" % (ns, metric)] = {"per_resource_metrics_active_3h": n_dim, "account_level_metric": n_none}
            data["metric_checks"] = checks
            r.log("metrics the two queries analyse (active in the last 3 h) and account-level twins: %s" % json.dumps(checks))
        except Exception as ex:  # noqa: BLE001
            failures += 1
            r.warn("alarm detail failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.section("D. the EC2 instances behind the two in-use EBS volumes")
        try:
            ec2 = boto3.client("ec2", region_name=REGION, config=CFG)
            res = ec2.describe_instances(InstanceIds=["i-0236308c6be77cee8", "i-0a38d50ce030fed84"])
            data["instances"] = []
            for rs in res.get("Reservations", []):
                for i in rs.get("Instances", []):
                    row = {"id": i["InstanceId"], "type": i.get("InstanceType"), "state": i["State"]["Name"],
                           "launched": str(i.get("LaunchTime"))[:10], "why": i.get("StateTransitionReason"),
                           "name": next((t["Value"] for t in i.get("Tags", []) if t["Key"] == "Name"), None)}
                    data["instances"].append(row)
                    r.log("instance %s" % json.dumps(row))
        except Exception as ex:  # noqa: BLE001
            r.warn("instances read failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        latest = ROOT / "aws" / "ops" / "reports" / "latest"
        latest.mkdir(parents=True, exist_ok=True)
        (latest / "ops_6624_egress_and_brakes_detail_data.md").write_text(
            "# ops 6624 raw data (JSON)\n\n```json\n" + json.dumps(data, default=str, separators=(",", ":")) + "\n```\n", encoding="utf-8")
        r.kv(sections_failed=failures, egress_days=len(data.get("egress", [])))
        if failures >= 3:
            r.fail("%d sections failed" % failures)
            sys.exit(1)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    main()
