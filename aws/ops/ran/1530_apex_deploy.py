# ops 1530 — deploy global-tide + apex-fusion + smart-wake; schedule cascade-validator; IAM events grant
import json, time, sys
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, "aws/ops")
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda, ensure_eb_rule

iam = boto3.client("iam")
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"
res = {"ops": 1530}

with report("1530-apex-deploy") as r:
    r.heading("Ops 1530 — exponential layer deploy")

    # 0) IAM: smart-wake needs events Enable/Disable/List on lambda-execution-role
    r.section("0. IAM events grant")
    try:
        iam.put_role_policy(
            RoleName="lambda-execution-role", PolicyName="smartwake-events",
            PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow",
                "Action": ["events:ListRules", "events:EnableRule", "events:DisableRule"],
                "Resource": "*"}]}))
        res["iam_events"] = "attached"
        r.ok("smartwake-events policy attached")
    except Exception as e:
        res["iam_events"] = str(e)[:120]
        r.log(f"IAM attach failed (smart-wake will report per-rule errors): {str(e)[:100]}")

    # 1) global-tide — daily 11:30 UTC
    r.section("A. global-tide")
    deploy_lambda(report=r, function_name="justhodl-global-tide",
                  source_dir=Path("aws/lambdas/justhodl-global-tide/source"),
                  env_vars={"S3_BUCKET": B},
                  eb_rule_name="justhodl-global-tide-daily", eb_schedule="cron(30 11 * * ? *)",
                  timeout=180, memory=512, description="G4 liquidity + global risk composite (Global Tide)",
                  reserved_concurrency=1, create_function_url=False, smoke=True)

    # 2) apex-fusion — every 3h
    r.section("B. apex-fusion")
    deploy_lambda(report=r, function_name="justhodl-apex-fusion",
                  source_dir=Path("aws/lambdas/justhodl-apex-fusion/source"),
                  env_vars={"S3_BUCKET": B, "SIGNALS_TABLE": "justhodl-signals"},
                  eb_rule_name="justhodl-apex-fusion-3h", eb_schedule="rate(3 hours)",
                  timeout=120, memory=512, description="Learned cross-engine pump conviction fusion",
                  reserved_concurrency=1, create_function_url=False, smoke=True)

    # 3) smart-wake — hourly
    r.section("C. smart-wake")
    deploy_lambda(report=r, function_name="justhodl-smart-wake",
                  source_dir=Path("aws/lambdas/justhodl-smart-wake/source"),
                  env_vars={"S3_BUCKET": B},
                  eb_rule_name="justhodl-smart-wake-hourly", eb_schedule="rate(1 hour)",
                  timeout=90, memory=256, description="Volatility-gated scheduling: wake hibernated alpha feeds on stress",
                  reserved_concurrency=1, create_function_url=False, smoke=True)

    # 4) cascade-validator: give it the schedule it never had
    r.section("D. cascade-validator daily rule")
    try:
        ensure_eb_rule(report=r, rule_name="justhodl-cascade-validator-daily",
                       schedule="cron(30 22 * * ? *)", function_name="justhodl-cascade-validator")
        res["validator_rule"] = "cron(30 22 * * ? *)"
    except Exception as e:
        res["validator_rule"] = str(e)[:120]
        r.log(f"rule err: {str(e)[:100]}")

    # 5) verification reads
    r.section("E. verify briefs")
    time.sleep(4)
    def rd(k):
        try:
            return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
        except Exception as e:
            return {"_err": str(e)[:60]}
    gt = rd("data/global-tide.json")
    res["global_tide"] = {"headline": gt.get("headline"), "gli": gt.get("gli"),
                          "risk": gt.get("risk"), "fed": gt.get("fed"),
                          "boj": gt.get("boj"), "china": gt.get("china"),
                          "n_flashing": gt.get("n_flashing"), "indicators": list((gt.get("indicators") or {}).keys()),
                          "err_any": gt.get("_err")}
    ax = rd("data/apex-fusion.json")
    res["apex"] = {"n_universe": ax.get("n_universe"), "by_tier": ax.get("by_tier"),
                   "weights": ax.get("weights_used"), "weight_sources": ax.get("weight_sources"),
                   "tier_inversion": ax.get("tier_inversion"),
                   "n_logged": ax.get("n_logged_to_ddb"),
                   "top6": [{k: t.get(k) for k in ("ticker", "apex_score", "tier", "n_sources", "sources")}
                            for t in (ax.get("top") or [])[:6]],
                   "err_any": ax.get("_err")}
    sw = rd("data/smart-wake.json")
    res["smart_wake"] = {"mode": sw.get("mode"), "stress": sw.get("stress"),
                         "errors": sw.get("errors"), "read": sw.get("read"), "err_any": sw.get("_err")}
    r.log(json.dumps(res["apex"]["top6"], default=str))

open("aws/ops/reports/1530_deploy.json", "w").write(json.dumps(res, indent=2, default=str))
print(json.dumps({"gt": res["global_tide"]["headline"], "apex_tiers": res["apex"]["by_tier"],
                  "sw": res["smart_wake"]["mode"], "iam": res["iam_events"]}, default=str))
