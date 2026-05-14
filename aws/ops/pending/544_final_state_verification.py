#!/usr/bin/env python3
"""544 — Add daily schedule to justhodl-insider-cluster-scanner +
final state verification of all 14 shipped Bloomberg-Gap modules."""
import json, os, time as _time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/544_final_state_verification.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ─── Add daily 14:30 UTC schedule to insider-cluster-scanner ───
    NAME = "justhodl-insider-cluster-scanner"
    RULE = f"{NAME}-daily"
    SCHEDULE = "cron(30 14 ? * MON-FRI *)"
    try:
        eb.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED",
                     Description="Daily SEC Form 4 insider cluster scan at 14:30 UTC")
        arn = lam.get_function(FunctionName=NAME)["Configuration"]["FunctionArn"]
        eb.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=NAME, StatementId=f"{NAME}-eb-perm",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=eb.describe_rule(Name=RULE)["Arn"])
        except lam.exceptions.ResourceConflictException: pass
        out["insider_schedule"] = {"rule": RULE, "schedule": SCHEDULE, "state": "ENABLED"}
    except Exception as e:
        out["insider_schedule_err"] = str(e)[:200]

    # ─── Final state of all 14 modules ───
    modules = [
        ("BUILD 1+13", "justhodl-dealer-gex", "data/dealer-gex.json"),
        ("BUILD 2", "justhodl-finra-short", "data/finra-short.json"),
        ("BUILD 3", "justhodl-13f-positions", "data/13f-positions.json"),
        ("BUILD 4", "justhodl-dix", "data/dix-history.json"),
        ("BUILD 5", "justhodl-vix-curve", "data/vix-curve.json"),
        ("BUILD 6", "justhodl-crypto-funding", "data/crypto-funding.json"),
        ("BUILD 7", "justhodl-earnings-nlp", "data/earnings-nlp.json"),
        ("BUILD 8", "justhodl-credit-stress", "data/credit-stress.json"),
        ("BUILD 9", "justhodl-retail-sentiment", "data/retail-sentiment.json"),
        ("BUILD 10", "justhodl-news-velocity", "data/news-velocity.json"),
        ("BUILD 11", "justhodl-cb-stance", "data/cb-stance.json"),
        ("BUILD 14", "justhodl-global-markets", "data/global-markets.json"),
        ("BUILD 15", "justhodl-commodity-curves", "data/commodity-curves.json"),
        ("BONUS A", "justhodl-insider-cluster-scanner", "data/insider-clusters.json"),
        ("BONUS B", "justhodl-options-flow-scanner", "data/options-flow.json"),
    ]

    final = []
    now = datetime.now(timezone.utc)
    for label, fn, sidecar in modules:
        m = {"label": label, "lambda": fn, "sidecar": sidecar}
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            m["lambda_exists"] = True
            m["lambda_mem"] = cfg.get("MemorySize")
            m["lambda_timeout"] = cfg.get("Timeout")
            # Get associated rules
            rules = []
            for r in eb.list_rules()["Rules"]:
                try:
                    ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                    if any(fn in t.get("Arn", "") for t in ts):
                        rules.append(f"{r.get('ScheduleExpression')} ({r.get('State')})")
                except: pass
            m["rules"] = rules
        except lam.exceptions.ResourceNotFoundException:
            m["lambda_exists"] = False
        except Exception as e:
            m["lambda_err"] = str(e)[:120]

        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key=sidecar)
            m["sidecar_size_kb"] = round(head["ContentLength"]/1024, 1)
            m["sidecar_age_min"] = round((now - head["LastModified"]).total_seconds() / 60, 1)
            m["sidecar_fresh"] = m["sidecar_age_min"] < (48 * 60)  # 48h tolerance
            # Pull regime
            full = s3.get_object(Bucket="justhodl-dashboard-live", Key=sidecar)
            p = json.loads(full["Body"].read())
            m["regime"] = (p.get("composite_regime")
                            or (p.get("fed") or {}).get("regime")
                            or p.get("market_regime")
                            or p.get("regime")
                            or "—")
        except s3.exceptions.NoSuchKey:
            m["sidecar_exists"] = False
        except Exception as e:
            m["sidecar_err"] = str(e)[:120]

        final.append(m)
    out["modules"] = final

    # Summary stats
    out["summary"] = {
        "n_lambdas_live": sum(1 for m in final if m.get("lambda_exists")),
        "n_sidecars_fresh": sum(1 for m in final if m.get("sidecar_fresh")),
        "n_with_schedules": sum(1 for m in final if m.get("rules")),
        "total_modules": len(final),
    }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
