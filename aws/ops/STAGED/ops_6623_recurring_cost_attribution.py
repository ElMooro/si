"""ops 6623 - READ-ONLY: who makes the ~$7.47/day steady-state bill (Khalid 2026-10-23: "supposed to be $200").

ops 6622 proved the Oct 22 $32.55 was $25.08 of one-time archive backfill + ~$7.47 of normal running cost. A normal
day is ~$225/month, above the $200 plan. This op names the writers and readers behind each recurring line, read-only:

  A. S3 server access logs, Oct 22 and Oct 23 (so far), sampled evenly across each day's log objects and scaled:
     - PUT/multipart writes by writer (Lambda session = function name) and by key prefix -> what the archive's
       live replication copies every day (~84 k Standard-IA PUTs/day = $0.84/day),
     - GET bytes by requester class (in-region Lambda reads are free; Cloudflare, runner and direct readers are the
       billed egress) and by key / user agent / network,
     - LIST requests and bytes by requester (a LIST answered to the runner is billed egress too);
  B. the governor's own per-function Lambda meter for Oct 21-23 (spend-governor/state.json, read only);
  C. every CloudWatch alarm, separating standard alarms from Metrics Insights query alarms (CW:MetricInsightAlarmUsage
     $0.64/day on Oct 22);
  D. CloudTrail management events GetMetricData for the last 24 h by caller (CW:GMD-Metrics $0.39/day);
  E. EBS volumes, Secrets Manager secrets (names and dates only, never values), ECR repository sizes, the largest
     CloudWatch log groups.
Nothing is written to AWS. Cost: ~10 k small GETs of log objects + listings (< $0.05), no Cost Explorer calls.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
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
CF_NETS = ("162.158.", "172.64.", "172.65.", "172.66.", "172.67.", "172.68.", "172.69.", "172.70.", "172.71.",
           "104.16.", "104.17.", "104.18.", "104.19.", "104.20.", "104.21.", "104.22.", "104.23.", "104.24.",
           "104.25.", "104.26.", "104.27.", "108.162.", "141.101.", "190.93.", "188.114.", "197.234.", "198.41.",
           "173.245.", "103.21.", "103.22.", "103.31.", "131.0.72.")
WRITE_OPS = ("REST.PUT.OBJECT", "REST.COPY.OBJECT", "REST.POST.UPLOAD", "REST.PUT.PART", "REST.POST.UPLOADS",
             "REST.COPY.PART")
LIST_OPS = ("REST.GET.BUCKET", "REST.GET.BUCKETVERSIONS")
s3 = boto3.client("s3", region_name=REGION, config=CFG)


def requester(req, ip):
    """(class, writer) - class decides whether a GET is billed egress; writer is the Lambda function or principal."""
    if "assumed-role" in req:
        parts = req.split("/")
        role = parts[-2] if len(parts) >= 3 else "role"
        session = parts[-1] if len(parts) >= 3 else "-"
        if "replication" in role.lower() or "batch" in role.lower():
            return "aws-replication:" + role, role
        return "lambda/role:" + role, session
    if ":user/" in req:
        return "iam-user:" + req.split("/")[-1], req.split("/")[-1]
    if req == "-":
        return ("anonymous-cloudflare" if ip.startswith(CF_NETS) else "anonymous-other"), "anonymous"
    if req.endswith(".amazonaws.com") or "AWSServiceRole" in req or req.startswith("svc:"):
        return "aws-service:" + req[:40], req[:40]
    return "other:" + req[-40:], req[-40:]


def prefix2(key):
    k = key.split("?")[0]
    parts = k.split("/")
    if len(parts) <= 2:
        return k if len(parts) == 1 else parts[0] + "/" + (parts[1] if "." not in parts[1] else "*.file")
    return parts[0] + "/" + parts[1] + "/"


def sample_day(r, tb, base, day, n_sample):
    keys, token = [], None
    t0 = time.time()
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
        r.warn("%s: no log objects" % day)
        return None
    newest = max(lm for _, lm in keys)
    oldest = min(lm for _, lm in keys)
    step_n = max(1, len(keys) // n_sample)
    sample = [k for k, _ in keys[::step_n]][:n_sample]
    scale = len(keys) / float(len(sample) or 1)

    def read(k):
        try:
            return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    get_cls, get_key, get_ua, get_net = Counter(), Counter(), Counter(), Counter()
    put_writer, put_prefix, put_op, put_writer_prefix = Counter(), Counter(), Counter(), Counter()
    list_cls_n, list_cls_b, list_who = Counter(), Counter(), Counter()
    ops_all = Counter()
    lines = 0
    with cf.ThreadPoolExecutor(max_workers=48) as ex:
        for text in ex.map(read, sample):
            for line in text.splitlines():
                m = LOG_RE.match(line)
                if not m:
                    continue
                lines += 1
                op, ip, req, key = m.group(7), m.group(4), m.group(5), m.group(8)
                b = int(m.group(12)) if m.group(12).isdigit() else 0
                cls, who = requester(req, ip)
                ops_all[op] += 1
                if op == "REST.GET.OBJECT":
                    get_cls[cls] += b
                    if not cls.startswith(("lambda/", "aws-")):
                        get_key[key] += b
                        get_ua[(m.group(17) or "-")[:70]] += b
                        get_net[".".join(ip.split(".")[:2]) + ".*"] += b
                elif op in WRITE_OPS and m.group(10).startswith("2"):
                    put_op[op] += 1
                    put_writer[who] += 1
                    put_prefix[prefix2(key)] += 1
                    put_writer_prefix["%s -> %s" % (who, prefix2(key))] += 1
                elif op in LIST_OPS:
                    list_cls_n[cls] += 1
                    list_cls_b[cls] += b
                    if not cls.startswith(("lambda/", "aws-")):
                        list_who["%s %s" % (cls, (m.group(17) or "-")[:40])] += b
    elapsed_h = max(1.0, (newest - oldest).total_seconds() / 3600.0)
    per_day = 24.0 / elapsed_h if day.endswith(datetime.now(timezone.utc).strftime("%d")) else 1.0
    f = scale * per_day

    def top(c, n, unit=1.0, nd=2):
        return [(k, round(v * f / unit, nd)) for k, v in c.most_common(n)]

    billed_get = sum(v for k, v in get_cls.items() if not k.startswith(("lambda/", "aws-")))
    billed_list = sum(v for k, v in list_cls_b.items() if not k.startswith(("lambda/", "aws-")))
    out = {
        "day": day, "log_objects": len(keys), "read": len(sample), "scale": round(scale, 2), "lines_read": lines,
        "span_hours": round(elapsed_h, 2), "per_day_factor": round(per_day, 3),
        "billed_get_gb_day": round(billed_get * f / 1e9, 2), "billed_list_gb_day": round(billed_list * f / 1e9, 3),
        "writes_per_day": round(sum(put_op.values()) * f),
        "get_gb_by_class": top(get_cls, 12, 1e9), "get_gb_top_keys": top(get_key, 25, 1e9, 3),
        "get_gb_by_ua": top(get_ua, 8, 1e9), "get_gb_by_net": top(get_net, 8, 1e9),
        "writes_by_op": top(put_op, 8, 1, 0), "writes_by_writer": top(put_writer, 40, 1, 0),
        "writes_by_prefix": top(put_prefix, 40, 1, 0), "writes_by_writer_prefix": top(put_writer_prefix, 40, 1, 0),
        "list_requests_by_class": top(list_cls_n, 10, 1, 0), "list_gb_by_class": top(list_cls_b, 10, 1e9, 3),
        "list_gb_billed_by_who": top(list_who, 10, 1e9, 3), "ops_mix": top(ops_all, 15, 1, 0),
    }
    r.log("%s: %d log objects listed in %.0fs (read %d, x%.1f, span %.1f h, per-day x%.2f); billed GET %.2f GB/day, "
          "billed LIST %.3f GB/day, writes %s/day" % (day, len(keys), time.time() - t0, len(sample), scale, elapsed_h,
                                                       per_day, out["billed_get_gb_day"], out["billed_list_gb_day"],
                                                       "{:,}".format(out["writes_per_day"])))
    r.log("   GET GB/day by requester: %s" % out["get_gb_by_class"])
    r.log("   billed GET GB/day top keys: %s" % out["get_gb_top_keys"][:15])
    r.log("   billed GET GB/day by user agent: %s" % out["get_gb_by_ua"][:6])
    r.log("   writes/day by op: %s" % out["writes_by_op"])
    r.log("   writes/day by writer: %s" % out["writes_by_writer"][:20])
    r.log("   writes/day by prefix: %s" % out["writes_by_prefix"][:20])
    r.log("   LIST requests/day by requester: %s; billed LIST GB/day: %s" % (out["list_requests_by_class"][:6], out["list_gb_billed_by_who"][:5]))
    return out


def find_daily(obj, depth=0):
    """Locate the governor meter's {day: {fn: [invocations, usd]}} block without assuming its exact path."""
    if depth > 5 or not isinstance(obj, dict):
        return None
    d = obj.get("daily")
    if isinstance(d, dict) and d and all(re.match(r"\d{4}-\d{2}-\d{2}$", k) for k in d):
        return d
    for v in obj.values():
        hit = find_daily(v, depth + 1)
        if hit:
            return hit
    return None


def main():
    now = datetime.now(timezone.utc)
    data = {"generated_at": now.isoformat()}
    with report("ops_6623_recurring_cost_attribution") as r:
        r.heading("ops 6623 - who makes the ~$7.47/day steady-state bill (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))
        failures = 0

        r.section("A. S3 access logs: writers, billed readers, listings")
        try:
            lg = s3.get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
            tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
            if not tb:
                r.warn("server access logging is off on the live bucket")
            else:
                base = "%s%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET)
                data["access_logs"] = []
                for day in ("2026/10/22", now.strftime("%Y/%m/%d")):
                    try:
                        res = sample_day(r, tb, base, day, 5000)
                        if res:
                            data["access_logs"].append(res)
                    except Exception as ex:  # noqa: BLE001
                        failures += 1
                        r.warn("%s sample failed: %s %s" % (day, type(ex).__name__, str(ex)[:200]))
        except Exception as ex:  # noqa: BLE001
            failures += 1
            r.warn("access-log section failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.section("B. the governor's per-function Lambda meter (read only)")
        try:
            state = json.loads(s3.get_object(Bucket=PRIVATE, Key="spend-governor/state.json")["Body"].read())
            daily = find_daily(state) or {}
            data["meter_daily"] = {}
            for day in sorted(daily)[-4:]:
                fns = daily[day]
                rows = sorted(((fn, v[0], v[1]) for fn, v in fns.items() if isinstance(v, list) and len(v) >= 2),
                              key=lambda x: -x[2])
                tot = sum(x[2] for x in rows)
                data["meter_daily"][day] = {"usd": round(tot, 4), "functions": len(rows),
                                            "top": [(fn, int(n), round(u, 4)) for fn, n, u in rows[:30]]}
                r.log("%s: meter $%.2f over %d functions; top: %s" % (day, tot, len(rows), "; ".join(
                    "%s $%.3f (%d inv)" % (fn, u, n) for fn, n, u in rows[:12])))
        except Exception as ex:  # noqa: BLE001
            failures += 1
            r.warn("meter read failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.section("C. CloudWatch alarms: standard vs Metrics Insights")
        try:
            cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
            alarms = []
            for page in cw.get_paginator("describe_alarms").paginate(AlarmTypes=["MetricAlarm", "CompositeAlarm"]):
                alarms += [("metric", a) for a in page.get("MetricAlarms", [])]
                alarms += [("composite", a) for a in page.get("CompositeAlarms", [])]
            mi, std, comp = [], [], []
            for kind, a in alarms:
                if kind == "composite":
                    comp.append(a["AlarmName"])
                    continue
                exprs = [m.get("Expression", "") for m in (a.get("Metrics") or []) if m.get("Expression")]
                if any("SELECT" in e.upper() for e in exprs):
                    mi.append({"name": a["AlarmName"], "state": a.get("StateValue"), "period": [m.get("Period") for m in a.get("Metrics") or [] if m.get("Period")],
                               "eval": a.get("EvaluationPeriods"), "expr": [e[:220] for e in exprs]})
                else:
                    std.append({"name": a["AlarmName"], "state": a.get("StateValue"), "period": a.get("Period"),
                                "metric": a.get("MetricName") or ("math" if a.get("Metrics") else None),
                                "ns": a.get("Namespace"), "hi_res": (a.get("Period") or 60) < 60})
            data["alarms"] = {"metrics_insights": mi, "standard": std, "composite": comp}
            r.log("alarms: %d standard, %d Metrics Insights, %d composite" % (len(std), len(mi), len(comp)))
            for a in mi:
                r.log("  MI %s [%s] period %s eval %s: %s" % (a["name"], a["state"], a["period"], a["eval"], a["expr"]))
            r.log("  standard (first 80): %s" % ", ".join("%s%s" % (a["name"], "*hi-res" if a["hi_res"] else "") for a in std[:80]))
        except Exception as ex:  # noqa: BLE001
            failures += 1
            r.warn("alarm listing failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.section("D. CloudTrail: GetMetricData callers, last 24 h")
        try:
            ct = boto3.client("cloudtrail", region_name=REGION, config=CFG)
            callers, n, token, pages = Counter(), 0, None, 0
            while pages < 120:
                kw = dict(LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": "GetMetricData"}],
                          StartTime=now - timedelta(hours=24), EndTime=now, MaxResults=50)
                if token:
                    kw["NextToken"] = token
                res = ct.lookup_events(**kw)
                pages += 1
                for e in res.get("Events", []):
                    n += 1
                    who = e.get("Username") or "-"
                    try:
                        ui = json.loads(e.get("CloudTrailEvent") or "{}").get("userIdentity", {})
                        arn = ui.get("arn") or ""
                        who = arn.split("/")[-1] if "assumed-role" in arn else (arn.split(":")[-1] or who)
                    except Exception:  # noqa: BLE001
                        pass
                    callers[who] += 1
                token = res.get("NextToken")
                if not token:
                    break
                time.sleep(0.55)
            data["gmd_callers"] = {"events": n, "pages": pages, "truncated": bool(token), "by_caller": callers.most_common(25)}
            r.log("GetMetricData calls in 24 h: %d (pages %d%s); by caller: %s" % (n, pages, ", truncated" if token else "", callers.most_common(15)))
        except Exception as ex:  # noqa: BLE001
            failures += 1
            r.warn("CloudTrail lookup failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        r.section("E. EBS volumes, secrets (names only), ECR, largest log groups")
        try:
            ec2 = boto3.client("ec2", region_name=REGION, config=CFG)
            vols = []
            for page in ec2.get_paginator("describe_volumes").paginate():
                for v in page.get("Volumes", []):
                    vols.append({"id": v["VolumeId"], "gb": v["Size"], "type": v.get("VolumeType"), "state": v["State"],
                                 "created": v["CreateTime"].isoformat()[:10],
                                 "attached": [a.get("InstanceId") for a in v.get("Attachments", [])],
                                 "tags": {t["Key"]: t["Value"][:60] for t in v.get("Tags", [])}})
            data["ebs"] = vols
            r.log("EBS volumes: %s" % json.dumps(vols)[:1200])
        except Exception as ex:  # noqa: BLE001
            r.warn("EBS read failed: %s %s" % (type(ex).__name__, str(ex)[:160]))
        try:
            sm = boto3.client("secretsmanager", region_name=REGION, config=CFG)
            secs = []
            for page in sm.get_paginator("list_secrets").paginate():
                for s in page.get("SecretList", []):
                    secs.append({"name": s["Name"], "created": str(s.get("CreatedDate"))[:10],
                                 "last_accessed": str(s.get("LastAccessedDate"))[:10],
                                 "last_changed": str(s.get("LastChangedDate"))[:10]})
            data["secrets"] = secs
            r.log("Secrets Manager (%d, names and dates only): %s" % (len(secs), json.dumps(secs)[:1500]))
        except Exception as ex:  # noqa: BLE001
            r.warn("secrets list failed: %s %s" % (type(ex).__name__, str(ex)[:160]))
        try:
            ecr = boto3.client("ecr", region_name=REGION, config=CFG)
            repos = []
            for page in ecr.get_paginator("describe_repositories").paginate():
                for rp in page.get("repositories", []):
                    size, newest, count = 0, None, 0
                    for ip in ecr.get_paginator("describe_images").paginate(repositoryName=rp["repositoryName"]):
                        for im in ip.get("imageDetails", []):
                            count += 1
                            size += im.get("imageSizeInBytes", 0)
                            pushed = im.get("imagePushedAt")
                            newest = max(newest, pushed) if (newest and pushed) else (pushed or newest)
                    repos.append({"repo": rp["repositoryName"], "images": count, "gb": round(size / 1e9, 2),
                                  "newest": str(newest)[:10]})
            data["ecr"] = repos
            r.log("ECR: %s" % json.dumps(repos))
        except Exception as ex:  # noqa: BLE001
            r.warn("ECR read failed: %s %s" % (type(ex).__name__, str(ex)[:160]))
        try:
            logs = boto3.client("logs", region_name=REGION, config=CFG)
            groups = []
            for page in logs.get_paginator("describe_log_groups").paginate():
                for g in page.get("logGroups", []):
                    groups.append((g.get("storedBytes", 0), g["logGroupName"], g.get("retentionInDays")))
            groups.sort(reverse=True)
            data["log_groups"] = {"count": len(groups), "stored_gb": round(sum(g[0] for g in groups) / 1e9, 2),
                                  "no_retention": sum(1 for g in groups if not g[2]),
                                  "top": [(n, round(b / 1e9, 3), ret) for b, n, ret in groups[:12]]}
            r.log("log groups: %d, stored %.2f GB, %d without retention; top: %s" % (
                len(groups), data["log_groups"]["stored_gb"], data["log_groups"]["no_retention"], data["log_groups"]["top"][:8]))
        except Exception as ex:  # noqa: BLE001
            r.warn("log group read failed: %s %s" % (type(ex).__name__, str(ex)[:160]))

        latest = ROOT / "aws" / "ops" / "reports" / "latest"
        latest.mkdir(parents=True, exist_ok=True)
        (latest / "ops_6623_recurring_cost_attribution_data.md").write_text(
            "# ops 6623 raw data (JSON)\n\n```json\n" + json.dumps(data, default=str, separators=(",", ":")) + "\n```\n",
            encoding="utf-8")
        r.kv(sections_failed=failures, access_log_days=len(data.get("access_logs", [])),
             mi_alarms=len((data.get("alarms") or {}).get("metrics_insights", [])),
             std_alarms=len((data.get("alarms") or {}).get("standard", [])))
        if failures >= 3:
            r.fail("%d sections failed" % failures)
            sys.exit(1)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    main()
