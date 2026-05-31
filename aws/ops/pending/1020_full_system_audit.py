#!/usr/bin/env python3
"""Step 1020 — JustHodl.AI Full System Audit.

Runs in GH Actions runner with full IAM creds. Gathers a complete picture
of the live system across:

  - Every Lambda (440 today): config, last modified, code size
  - Every EventBridge rule: schedule, state, Lambda targets
  - CloudWatch metrics: invocations + errors + throttles last 7 days for
    every SCHEDULED Lambda (we skip on-demand ones to keep this efficient)
  - S3 data/ namespace: every key, size, age
  - DynamoDB tables: rough size + item count
  - SSM /justhodl/* parameters

Produces:
  aws/ops/reports/1020_audit.json   — full structured data
  aws/ops/audit/1020_audit.md       — human-readable report with top issues

This is the FOUNDATION for the coordination improvements that follow.
Build on this audit, not on assumptions about what's running.
"""
import io, json, os, time, pathlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import boto3

REPORT_JSON = "aws/ops/reports/1020_full_audit.json"
REPORT_MD   = "aws/ops/audit/1020_full_audit.md"

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ACCOUNT_ID = "857687956942"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def list_all_lambdas():
    """All Lambdas in the account — name, runtime, mem, timeout, last_modified, code_size."""
    items = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            items.append({
                "name": fn["FunctionName"],
                "runtime": fn.get("Runtime"),
                "memory_mb": fn.get("MemorySize"),
                "timeout_s": fn.get("Timeout"),
                "last_modified": fn.get("LastModified"),
                "code_size_kb": round(fn.get("CodeSize", 0) / 1024, 1),
                "description": (fn.get("Description") or "")[:140],
                "state": fn.get("State"),
                "last_update_status": fn.get("LastUpdateStatus"),
            })
    return items


def list_schedules_for_lambdas():
    """Map function_name → {rule, schedule, state}."""
    out = {}
    paginator = events.get_paginator("list_rules")
    for page in paginator.paginate():
        for rule in page.get("Rules", []):
            sched = rule.get("ScheduleExpression")
            if not sched:
                continue
            rule_name = rule["Name"]
            try:
                tgts = events.list_targets_by_rule(Rule=rule_name).get("Targets", [])
            except Exception:
                continue
            for t in tgts:
                arn = t.get("Arn", "")
                if ":function:" in arn:
                    fn_name = arn.split(":function:")[-1].split(":")[0]
                    out[fn_name] = {
                        "rule": rule_name,
                        "schedule": sched,
                        "state": rule.get("State"),
                        "description": (rule.get("Description") or "")[:80],
                    }
    return out


def get_invoke_metrics(fn_name: str, days: int = 7) -> dict:
    """Last N days of invocations + errors + throttles for one Lambda."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    out = {"days": days, "invocations": 0, "errors": 0, "throttles": 0,
           "duration_avg_ms": 0, "last_invoked": None}
    
    def _sum(metric):
        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                StartTime=start, EndTime=end, Period=86400,
                Statistics=["Sum"],
            )
            return int(sum(p["Sum"] for p in resp.get("Datapoints", [])))
        except Exception:
            return 0
    
    def _avg(metric):
        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                StartTime=start, EndTime=end, Period=86400,
                Statistics=["Average"],
            )
            datapoints = resp.get("Datapoints", [])
            if not datapoints:
                return 0
            return round(sum(p["Average"] for p in datapoints) / len(datapoints), 1)
        except Exception:
            return 0
    
    out["invocations"] = _sum("Invocations")
    out["errors"] = _sum("Errors")
    out["throttles"] = _sum("Throttles")
    out["duration_avg_ms"] = _avg("Duration")
    out["error_rate_pct"] = round(out["errors"] / max(1, out["invocations"]) * 100, 2)
    return out


def get_last_log_event(fn_name: str) -> str:
    """When did this Lambda last write a log line?"""
    try:
        lg_name = f"/aws/lambda/{fn_name}"
        resp = logs.describe_log_streams(
            logGroupName=lg_name, orderBy="LastEventTime",
            descending=True, limit=1,
        )
        streams = resp.get("logStreams", [])
        if not streams:
            return None
        return datetime.fromtimestamp(streams[0].get("lastEventTimestamp", 0) / 1000,
                                       tz=timezone.utc).isoformat()
    except Exception:
        return None


def list_s3_data_keys(prefix: str = "data/", limit: int = 5000) -> list:
    """List S3 keys under data/ with size + last modified."""
    items = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents") or []:
            items.append({
                "key":      obj["Key"],
                "size_kb":  round(obj["Size"] / 1024, 1),
                "modified": obj["LastModified"].isoformat(),
            })
            if len(items) > limit:
                return items
    return items


def list_ddb_tables() -> list:
    """All DynamoDB tables + rough size."""
    out = []
    paginator = ddb.get_paginator("list_tables")
    for page in paginator.paginate():
        for t in page.get("TableNames", []):
            try:
                desc = ddb.describe_table(TableName=t)["Table"]
                out.append({
                    "name":     t,
                    "items":    desc.get("ItemCount"),
                    "size_mb":  round(desc.get("TableSizeBytes", 0) / 1024 / 1024, 2),
                    "billing_mode": desc.get("BillingModeSummary", {}).get("BillingMode"),
                    "created":  str(desc.get("CreationDateTime")),
                })
            except Exception as e:
                out.append({"name": t, "err": str(e)[:100]})
    return out


def list_ssm_params(path_prefix: str = "/justhodl/") -> list:
    """All SSM parameters under /justhodl/ — gives a picture of inter-engine config."""
    out = []
    paginator = ssm.get_paginator("describe_parameters")
    try:
        for page in paginator.paginate(
            ParameterFilters=[{"Key": "Name", "Option": "BeginsWith",
                                "Values": [path_prefix]}]):
            for p in page.get("Parameters", []):
                out.append({
                    "name":         p["Name"],
                    "type":         p.get("Type"),
                    "last_modified": str(p.get("LastModifiedDate", "")),
                    "tier":         p.get("Tier"),
                })
    except Exception as e:
        return [{"err": str(e)[:200]}]
    return out


def categorize_issues(lambdas, schedules, metrics, s3_keys):
    """Detect actual problems based on aggregate data."""
    now = datetime.now(timezone.utc)
    issues = {
        "dead_unscheduled": [],   # not scheduled, no recent invokes
        "broken_scheduled": [],   # scheduled but failing >50% error rate
        "stale_outputs":    [],   # S3 keys older than expected for the engine
        "low_use_scheduled": [],  # scheduled but rarely fires (cron too sparse?)
        "high_error_rate":  [],   # error rate >10% across active Lambdas
        "expensive_invokes": [],  # avg duration > 60s
        "disabled_rules":   [],   # EventBridge rule exists but disabled
        "no_recent_logs":   [],   # not invoked OR not writing logs in last 14d
    }
    
    metrics_by_name = {m["name"]: m for m in metrics if m.get("metrics")}
    
    for L in lambdas:
        name = L["name"]
        sched = schedules.get(name)
        m = metrics_by_name.get(name, {}).get("metrics", {})
        last_log = metrics_by_name.get(name, {}).get("last_log")
        invocations = m.get("invocations", 0)
        errors = m.get("errors", 0)
        error_rate = m.get("error_rate_pct", 0)
        duration = m.get("duration_avg_ms", 0)
        
        if sched and sched.get("state") == "DISABLED":
            issues["disabled_rules"].append({"name": name, "rule": sched["rule"]})
        
        if not sched and invocations == 0:
            issues["dead_unscheduled"].append({
                "name": name,
                "last_modified": L["last_modified"],
                "code_size_kb": L["code_size_kb"],
            })
        
        if sched and invocations > 0 and error_rate > 50:
            issues["broken_scheduled"].append({
                "name": name, "errors_7d": errors, "invocations_7d": invocations,
                "error_rate_pct": error_rate, "schedule": sched["schedule"],
            })
        
        if invocations > 10 and error_rate > 10:
            issues["high_error_rate"].append({
                "name": name, "error_rate_pct": error_rate,
                "errors_7d": errors, "invocations_7d": invocations,
            })
        
        if duration > 60_000:
            issues["expensive_invokes"].append({
                "name": name, "duration_avg_ms": duration,
                "timeout_s": L["timeout_s"], "memory_mb": L["memory_mb"],
            })
        
        if last_log:
            try:
                last_log_dt = datetime.fromisoformat(last_log.replace("Z", "+00:00"))
                age_days = (now - last_log_dt).total_seconds() / 86400
                if age_days > 14 and (sched or invocations > 0):
                    issues["no_recent_logs"].append({
                        "name": name, "last_log_age_days": round(age_days, 1),
                        "scheduled": bool(sched),
                    })
            except Exception:
                pass
    
    # S3 output staleness — relative to engine's expected refresh
    # We can't auto-determine each engine's freshness expectation, but we can
    # flag anything older than 7 days under the active data/ keys.
    seven_days_ago = now - timedelta(days=7)
    for key_obj in s3_keys:
        try:
            modified = datetime.fromisoformat(key_obj["modified"].replace("Z", "+00:00"))
            # Skip archives + audit + history dirs (those are intentionally old)
            k = key_obj["key"]
            if any(p in k for p in ("/archive/", "/audit/", "/history/", "/snapshots/",
                                       "/track-record/", "/misses/", "/errors/")):
                continue
            if modified < seven_days_ago:
                issues["stale_outputs"].append({
                    "key": k,
                    "modified": key_obj["modified"],
                    "age_days": round((now - modified).total_seconds() / 86400, 1),
                    "size_kb": key_obj["size_kb"],
                })
        except Exception:
            continue
    
    # Sort issues by severity / count for visibility
    issues["stale_outputs"].sort(key=lambda r: -r.get("age_days", 0))
    issues["broken_scheduled"].sort(key=lambda r: -r.get("error_rate_pct", 0))
    issues["high_error_rate"].sort(key=lambda r: -r.get("error_rate_pct", 0))
    issues["expensive_invokes"].sort(key=lambda r: -r.get("duration_avg_ms", 0))
    issues["no_recent_logs"].sort(key=lambda r: -r.get("last_log_age_days", 0))
    
    return issues


def write_markdown(audit, issues, out_path):
    pathlib.Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    
    md = []
    md.append("# JustHodl.AI — Full System Audit")
    md.append(f"\nGenerated: {audit['generated_at']}")
    md.append(f"Account: {ACCOUNT_ID} · Region: {REGION}\n")
    
    md.append("## System Scale")
    md.append("")
    md.append(f"- **Lambdas**:           {audit['n_lambdas']}")
    md.append(f"- **Scheduled Lambdas**: {audit['n_scheduled']}")
    md.append(f"- **S3 data keys**:       {audit['n_s3_keys']}")
    md.append(f"- **DDB tables**:         {audit['n_ddb_tables']}")
    md.append(f"- **SSM parameters**:     {audit['n_ssm_params']}")
    md.append("")
    
    md.append("## Issues Summary")
    md.append("")
    counts = {k: len(v) for k, v in issues.items()}
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        md.append(f"- **{k}**: {v}")
    md.append("")
    
    md.append("## Top Issues (Detail)")
    
    for category, items in issues.items():
        if not items:
            continue
        md.append(f"\n### {category} ({len(items)})")
        for item in items[:15]:
            md.append(f"- {json.dumps(item, default=str)}")
        if len(items) > 15:
            md.append(f"- … and {len(items)-15} more (see JSON report)")
    
    pathlib.Path(out_path).write_text("\n".join(md))


def main():
    started = datetime.now(timezone.utc)
    audit = {"started": started.isoformat()}
    
    print("[audit] phase 1: Lambda inventory…")
    lambdas = list_all_lambdas()
    audit["n_lambdas"] = len(lambdas)
    print(f"[audit]   {len(lambdas)} Lambdas")
    
    print("[audit] phase 2: schedules…")
    schedules = list_schedules_for_lambdas()
    audit["n_scheduled"] = len(schedules)
    print(f"[audit]   {len(schedules)} scheduled Lambdas")
    
    print("[audit] phase 3: CloudWatch metrics for scheduled Lambdas + key engines…")
    # To keep CW API calls bounded: metrics ONLY for scheduled Lambdas + a
    # curated 'key engines' list (these are the must-be-healthy ones).
    KEY_ENGINES = {
        "justhodl-conviction-engine", "justhodl-signal-board",
        "justhodl-signal-logger", "justhodl-outcome-checker",
        "justhodl-calibrator", "justhodl-alpha-calibrator",
        "justhodl-opportunity-calibrator", "justhodl-gsi-calibrator",
        "justhodl-miss-calibrator", "justhodl-miss-detector",
        "justhodl-near-miss-monitor", "justhodl-engine-signal-map",
        "justhodl-magnitude-distributions", "justhodl-alpha-compass",
        "justhodl-signal-scorecard", "justhodl-master-ranker",
        "justhodl-universe-builder", "justhodl-pnl-attribution",
        "justhodl-position-sizer-v2", "justhodl-regime-conditional-router",
        "justhodl-ai-chat", "justhodl-telegram-bot",
        "justhodl-crisis-composite", "justhodl-eurodollar-stress",
    }
    targets_for_metrics = set(schedules.keys()) | KEY_ENGINES
    targets_for_metrics &= {L["name"] for L in lambdas}   # only existing
    
    metrics = []
    for i, fn_name in enumerate(sorted(targets_for_metrics)):
        m = get_invoke_metrics(fn_name, days=7)
        last_log = get_last_log_event(fn_name)
        metrics.append({"name": fn_name, "metrics": m, "last_log": last_log})
        if (i + 1) % 25 == 0:
            print(f"[audit]   metrics: {i+1}/{len(targets_for_metrics)}")
    print(f"[audit]   collected metrics for {len(metrics)} targets")
    
    print("[audit] phase 4: S3 data/ inventory…")
    s3_keys = list_s3_data_keys()
    audit["n_s3_keys"] = len(s3_keys)
    print(f"[audit]   {len(s3_keys)} S3 data keys")
    
    print("[audit] phase 5: DynamoDB tables…")
    ddb_tables = list_ddb_tables()
    audit["n_ddb_tables"] = len(ddb_tables)
    
    print("[audit] phase 6: SSM /justhodl/ parameters…")
    ssm_params = list_ssm_params()
    audit["n_ssm_params"] = len(ssm_params)
    
    print("[audit] phase 7: issue detection…")
    issues = categorize_issues(lambdas, schedules, metrics, s3_keys)
    issue_counts = {k: len(v) for k, v in issues.items()}
    print(f"[audit]   issues by category: {issue_counts}")
    
    audit["generated_at"] = started.isoformat()
    audit["finished_at"] = datetime.now(timezone.utc).isoformat()
    audit["elapsed_s"] = round((datetime.now(timezone.utc) - started).total_seconds(), 1)
    audit["lambdas_sample"] = lambdas[:5]
    audit["schedules_sample"] = dict(list(schedules.items())[:5])
    audit["metrics_sample"] = metrics[:5]
    audit["issue_counts"] = issue_counts
    audit["issues"] = issues
    audit["ddb_tables"] = ddb_tables
    audit["ssm_params"] = ssm_params
    # Don't dump 440 lambdas + 5000 s3 keys into JSON — keep sample
    audit["full_lambda_list"] = [L["name"] for L in lambdas]
    audit["full_schedule_map"] = schedules
    audit["full_metrics"] = metrics
    audit["s3_keys_sample"] = s3_keys[:50]
    audit["n_s3_keys_total"] = len(s3_keys)
    
    pathlib.Path(REPORT_JSON).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT_JSON).write_text(
        json.dumps(audit, indent=2, default=str)
    )
    write_markdown(audit, issues, REPORT_MD)
    
    print(f"[audit] DONE. wrote {REPORT_JSON} + {REPORT_MD}")
    print(f"[audit] elapsed {audit['elapsed_s']}s")


if __name__ == "__main__":
    main()
