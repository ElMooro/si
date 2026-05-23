"""ops 1093 — clean idempotent fix-up after deploy-lambdas.yml created
   the tax-plan Lambda but run-ops scripts failed.

State per ops 1092:
  ✅ Lambda exists (sha Gjj4gIAjiUKO, Active)
  ✅ Function URL: https://a2orhvvfva3kh5r6jx5soijcbm0uecuw.lambda-url.us-east-1.on.aws/
  ❌ tax-plan-daily schedule missing
  ❌ wealth-plan-daily-warmup schedule missing
  ❌ tax-plan in manifest
  ❌ tax-plan.html has REPLACE_WITH_FUNCTION_URL placeholder
  ❌ tax-plan-snapshot.json never created (never invoked)

Each step wrapped independently so partial-success is recoverable.
"""
import io, json, os, time, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
BUCKET = "justhodl-dashboard-live"
MANIFEST_KEY = "data/_freshness-manifest.json"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def safe_step(name, fn):
    try:
        result = fn()
        return {"step": name, "status": "OK", **result}
    except Exception as e:
        return {"step": name, "status": "ERR", "error": str(e)[:300]}


def main():
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "steps": []}

    # ── 1. Get the tax-plan Function URL ───────────────────────────────
    def get_tax_url():
        u = lam.get_function_url_config(FunctionName="justhodl-tax-plan")
        return {"url": u["FunctionUrl"]}
    s1 = safe_step("get_tax_plan_url", get_tax_url)
    report["steps"].append(s1)
    tax_url = s1.get("url")

    # ── 2. Tax-plan daily schedule ─────────────────────────────────────
    def setup_tax_schedule():
        rule_name = "tax-plan-daily"
        cron = "cron(45 11 ? * * *)"
        fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:justhodl-tax-plan"
        events.put_rule(Name=rule_name, ScheduleExpression=cron, State="ENABLED",
                        Description="Daily 11:45 UTC tax-plan default snapshot")
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName="justhodl-tax-plan",
                StatementId=f"AllowEB-{rule_name}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
            )
            return {"perm": "ADDED", "cron": cron}
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                return {"perm": "EXISTS", "cron": cron}
            raise
    report["steps"].append(safe_step("tax_plan_schedule", setup_tax_schedule))

    # ── 3. Wealth-plan warmup schedule ─────────────────────────────────
    def setup_wealth_schedule():
        rule_name = "wealth-plan-daily-warmup"
        cron = "cron(30 11 ? * * *)"
        fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:justhodl-wealth-plan"
        events.put_rule(Name=rule_name, ScheduleExpression=cron, State="ENABLED",
                        Description="Daily 11:30 UTC wealth-plan warmup")
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName="justhodl-wealth-plan",
                StatementId=f"AllowEB-{rule_name}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
            )
            return {"perm": "ADDED", "cron": cron}
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                return {"perm": "EXISTS", "cron": cron}
            raise
    report["steps"].append(safe_step("wealth_plan_schedule", setup_wealth_schedule))

    # ── 4. Freshness manifest: add tax-plan + wealth-plan ──────────────
    def update_manifest():
        m = json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
        m.setdefault("key_overrides", {})
        m["key_overrides"]["data/tax-plan-snapshot.json"] = {
            "max_age_h": 30,
            "description": "Tax-Aware Portfolio default snapshot — daily 11:45 UTC.",
        }
        # wealth-plan might already be there
        if "data/wealth-plan-snapshot.json" not in m["key_overrides"]:
            m["key_overrides"]["data/wealth-plan-snapshot.json"] = {
                "max_age_h": 30,
                "description": "Wealth Plan default snapshot — daily 11:30 UTC warmup.",
            }
        m["_last_updated"] = datetime.now(timezone.utc).isoformat()
        m["_last_updater"] = "ops/1093"
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                      Body=json.dumps(m, indent=2).encode(),
                      ContentType="application/json")
        return {"total_overrides": len(m["key_overrides"])}
    report["steps"].append(safe_step("freshness_manifest", update_manifest))

    # ── 5. Patch tax-plan.html with actual Function URL ────────────────
    def patch_html():
        if not tax_url:
            return {"skipped": "no_url"}
        html_file = os.path.join(REPO_ROOT, "tax-plan.html")
        if not os.path.isfile(html_file):
            return {"skipped": "no_file"}
        with open(html_file) as f:
            html = f.read()
        placeholder = "REPLACE_WITH_FUNCTION_URL"
        if placeholder in html:
            new_html = html.replace(placeholder, tax_url.rstrip("/"))
            with open(html_file, "w") as f:
                f.write(new_html)
            return {"patched": True, "url": tax_url}
        else:
            return {"already_patched": True}
    report["steps"].append(safe_step("html_patch", patch_html))

    # ── 6. Test invoke tax-plan ────────────────────────────────────────
    def invoke_tax_plan():
        inv = lam.invoke(FunctionName="justhodl-tax-plan",
                         InvocationType="RequestResponse", LogType="Tail")
        log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
        # Read S3 output
        time.sleep(3)
        try:
            o = s3.get_object(Bucket=BUCKET, Key="data/tax-plan-snapshot.json")
            data = json.loads(o["Body"].read())
            return {
                "invoke_status": inv["StatusCode"],
                "fn_err": inv.get("FunctionError"),
                "snapshot_size_kb": round(o["ContentLength"] / 1024, 1),
                "n_positions": len(data.get("portfolio_tax_view", {}).get("positions", [])),
                "n_after_tax_assets": len(data.get("after_tax_forward_returns", [])),
                "n_tlh_candidates": len(data.get("tax_loss_harvest_candidates", [])),
                "n_actions": len(data.get("verdict", {}).get("action_items", [])),
                "summary": data.get("verdict", {}).get("summary_line"),
                "elapsed": data.get("elapsed_seconds"),
                "log_tail": log[-600:],
            }
        except Exception as e:
            return {
                "invoke_status": inv["StatusCode"],
                "fn_err": inv.get("FunctionError"),
                "log_tail": log[-1200:],
                "s3_err": str(e)[:200],
            }
    report["steps"].append(safe_step("invoke_tax_plan", invoke_tax_plan))

    # ── 7. Kick freshness monitor ──────────────────────────────────────
    def kick_monitor():
        lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="Event")
        return {"kicked": True}
    report["steps"].append(safe_step("kick_monitor", kick_monitor))

    # Verdict
    errs = [s for s in report["steps"] if s["status"] == "ERR"]
    report["overall"] = "OK" if not errs else f"{len(errs)} errors"
    report["finished_at"] = datetime.now(timezone.utc).isoformat()

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1093.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
