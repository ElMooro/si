#!/usr/bin/env python3
"""565 — Audit 13F Smart Money: Lambda config, schedule, sidecar schema,
freshness, fund coverage, change distribution. Before any backfill, know what
exists."""
import io, json, os, time as _time
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/565_13f_audit.json"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
logs_cli = boto3.client("logs", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Find 13F Lambda(s)
    try:
        paginator = lam.get_paginator("list_functions")
        candidates = []
        for page in paginator.paginate():
            for f in page.get("Functions", []):
                n = f["FunctionName"]
                if any(tok in n.lower() for tok in ["13f", "smart-money", "smartmoney", "fund-positions"]):
                    candidates.append({
                        "name": n,
                        "memory": f.get("MemorySize"),
                        "timeout": f.get("Timeout"),
                        "last_modified": f.get("LastModified"),
                        "state": f.get("State"),
                    })
        out["lambda_candidates"] = candidates
    except Exception as e:
        out["lambda_inv_err"] = str(e)[:200]

    # 2. EventBridge rules
    try:
        rules_found = []
        for prefix in ["justhodl-13f", "justhodl-smart"]:
            resp = events.list_rules(NamePrefix=prefix)
            for r in resp.get("Rules", []):
                targets = events.list_targets_by_rule(Rule=r["Name"])
                rules_found.append({
                    "name": r["Name"],
                    "schedule": r.get("ScheduleExpression"),
                    "state": r.get("State"),
                    "targets": [t.get("Arn") for t in targets.get("Targets", [])],
                })
        out["eventbridge_rules"] = rules_found
    except Exception as e:
        out["events_err"] = str(e)[:200]

    # 3. Sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/13f-positions.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "top_level_keys": list(p.keys())[:25],
        }
        # Fund inventory
        by_fund = p.get("by_fund") or p.get("funds") or {}
        if isinstance(by_fund, dict):
            funds_summary = {}
            for fund_name, fund_data in list(by_fund.items())[:20]:
                if isinstance(fund_data, dict):
                    cs = fund_data.get("changes_summary") or {}
                    funds_summary[fund_name] = {
                        "n_new": cs.get("n_new"),
                        "n_added": cs.get("n_added"),
                        "n_trimmed": cs.get("n_trimmed"),
                        "n_exits": len(cs.get("exits", []) if isinstance(cs.get("exits"), list) else []),
                        "n_holdings": len(fund_data.get("holdings", []) if isinstance(fund_data.get("holdings"), list) else []),
                        "report_date": fund_data.get("report_date") or fund_data.get("filing_date") or fund_data.get("as_of"),
                        "filed_date": fund_data.get("filed_date"),
                        "manager": fund_data.get("manager") or fund_data.get("cik"),
                    }
            out["sidecar"]["funds_count"] = len(by_fund)
            out["sidecar"]["funds_summary"] = funds_summary
        # Composite-level aggregates
        for k in ["most_bought", "most_sold", "consensus_buys", "consensus_sells",
                   "rotation_in", "rotation_out", "summary", "generated_at",
                   "n_funds", "n_holdings_total", "schema_version", "version"]:
            if k in p:
                v = p[k]
                if isinstance(v, list):
                    out["sidecar"][f"{k}_len"] = len(v)
                    out["sidecar"][f"{k}_first_3"] = v[:3]
                elif isinstance(v, dict):
                    out["sidecar"][f"{k}_keys"] = list(v.keys())[:10]
                else:
                    out["sidecar"][k] = v
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    # 4. Recent invocation history (CloudWatch metrics)
    try:
        candidates = out.get("lambda_candidates") or []
        if candidates:
            primary = candidates[0]["name"]
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=2)
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": primary}],
                StartTime=start, EndTime=end, Period=3600,
                Statistics=["Sum"],
            )
            points = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
            out["invocations_48h"] = [
                {"ts": p["Timestamp"].isoformat()[:19], "n": int(p.get("Sum", 0))}
                for p in points
            ]
            # Errors
            err_resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Errors",
                Dimensions=[{"Name": "FunctionName", "Value": primary}],
                StartTime=start, EndTime=end, Period=3600,
                Statistics=["Sum"],
            )
            err_points = sorted(err_resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
            out["errors_48h"] = [
                {"ts": p["Timestamp"].isoformat()[:19], "n": int(p.get("Sum", 0))}
                for p in err_points if p.get("Sum", 0) > 0
            ]
            # Duration
            dur_resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Duration",
                Dimensions=[{"Name": "FunctionName", "Value": primary}],
                StartTime=start, EndTime=end, Period=3600,
                Statistics=["Average", "Maximum"],
            )
            dur_points = sorted(dur_resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
            out["duration_48h"] = [
                {"ts": p["Timestamp"].isoformat()[:19],
                  "avg_ms": int(p.get("Average", 0)), "max_ms": int(p.get("Maximum", 0))}
                for p in dur_points
            ]
    except Exception as e:
        out["cw_err"] = str(e)[:200]

    # 5. Recent CloudWatch logs (any errors?)
    try:
        candidates = out.get("lambda_candidates") or []
        if candidates:
            primary = candidates[0]["name"]
            log_group = f"/aws/lambda/{primary}"
            start_ms = int((datetime.now(timezone.utc) - timedelta(hours=12)).timestamp() * 1000)
            events_resp = logs_cli.filter_log_events(
                logGroupName=log_group, startTime=start_ms, limit=200,
                filterPattern="?ERROR ?Error ?error ?failed ?exception ?Traceback ?429",
            )
            err_lines = []
            for e in events_resp.get("events", []):
                msg = e.get("message", "").rstrip()
                if msg:
                    err_lines.append({"ts": datetime.fromtimestamp(e["timestamp"]/1000, timezone.utc).isoformat()[:19],
                                       "msg": msg[:280]})
            out["recent_error_log_lines"] = err_lines[-30:]
    except Exception as e:
        out["logs_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
