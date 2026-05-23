"""ops 1091 — deploy justhodl-tax-plan: Lambda + Function URL + schedule
   + freshness manifest + patch HTML endpoint placeholder + invoke verify.

Pattern: same as ops 1085/1086 (wealth-plan deployment).
"""
import io, json, os, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-tax-plan"
BUCKET = "justhodl-dashboard-live"
SNAPSHOT_KEY = "data/tax-plan-snapshot.json"
MANIFEST_KEY = "data/_freshness-manifest.json"
HTML_FILE = os.path.join(REPO_ROOT, "tax-plan.html")
HTML_PLACEHOLDER = "REPLACE_WITH_FUNCTION_URL"


def zip_source(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for f in files:
                full = os.path.join(root, f)
                z.write(full, os.path.relpath(full, src_dir))
    return buf.getvalue()


def wait_idle(lam, fn, max_wait=180):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return c
            if c.get("LastUpdateStatus") == "Failed":
                return None
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                pass
            else:
                raise
        time.sleep(3)
    return None


def main():
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "fn": FN}

    cfg_path = os.path.join(REPO_ROOT, "aws", "lambdas", FN, "config.json")
    src_dir = os.path.join(REPO_ROOT, "aws", "lambdas", FN, "source")
    with open(cfg_path) as f:
        cfg = json.load(f)

    # 1. Create or update Lambda
    print("1) Lambda create/update...")
    existing = wait_idle(lam, FN, 60)
    if existing:
        report["initial_state"] = "EXISTS"
        report["initial_sha"] = existing.get("CodeSha256", "")[:12]
    else:
        try:
            zb = zip_source(src_dir)
            r = lam.create_function(
                FunctionName=FN,
                Runtime=cfg["runtime"],
                Role=cfg["role"],
                Handler=cfg["handler"],
                Code={"ZipFile": zb},
                Description=cfg["description"][:255],
                Timeout=int(cfg["timeout"]),
                MemorySize=int(cfg["memory"]),
                Architectures=cfg["architectures"],
                Environment={"Variables": cfg["env"]},
            )
            report["create"] = "CREATED"
            report["arn"] = r["FunctionArn"]
            wait_idle(lam, FN, 90)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                wait_idle(lam, FN, 90)
                report["create"] = "RACED_EXISTS"
            else:
                report["create_err"] = str(e)[:300]
                return _save(report)

    # 2. Sync code (in case existed from deploy-lambdas.yml race)
    print("2) Direct update code...")
    zb = zip_source(src_dir)
    lam.update_function_code(FunctionName=FN, ZipFile=zb, Publish=False)
    wait_idle(lam, FN, 90)

    # 3. Sync config
    cur = lam.get_function_configuration(FunctionName=FN)
    needs = (cur.get("MemorySize") != cfg["memory"] or cur.get("Timeout") != cfg["timeout"])
    if needs:
        lam.update_function_configuration(
            FunctionName=FN,
            Timeout=int(cfg["timeout"]),
            MemorySize=int(cfg["memory"]),
            Environment={"Variables": cfg["env"]},
            Description=cfg["description"][:255],
        )
        wait_idle(lam, FN, 60)
        report["config_sync"] = "UPDATED"
    else:
        report["config_sync"] = "ALREADY_MATCHES"

    # 4. Function URL
    print("4) Function URL...")
    fu_cfg = cfg.get("function_url", {})
    try:
        u = lam.create_function_url_config(
            FunctionName=FN,
            AuthType=fu_cfg.get("auth_type", "NONE"),
            Cors={
                "AllowOrigins": fu_cfg.get("cors", {}).get("AllowOrigins", ["*"]),
                "AllowMethods": fu_cfg.get("cors", {}).get("AllowMethods", ["GET", "POST"]),
                "AllowHeaders": fu_cfg.get("cors", {}).get("AllowHeaders", ["content-type"]),
                "MaxAge": fu_cfg.get("cors", {}).get("MaxAge", 300),
            },
        )
        report["function_url"] = u["FunctionUrl"]
        report["url_state"] = "CREATED"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            u = lam.get_function_url_config(FunctionName=FN)
            report["function_url"] = u["FunctionUrl"]
            report["url_state"] = "EXISTS"
        else:
            report["url_err"] = str(e)[:300]

    # Public invoke permission
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId="AllowPublicFunctionUrl",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
        report["public_permission"] = "ADDED"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            report["public_permission"] = "EXISTS"
        else:
            report["perm_err"] = str(e)[:200]

    # 5. EventBridge daily schedule
    print("5) Schedule...")
    sched = cfg.get("schedule") or {}
    rule_name = sched.get("rule_name")
    cron_expr = sched.get("cron")
    if rule_name and cron_expr:
        fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{FN}"
        events.put_rule(Name=rule_name, ScheduleExpression=cron_expr, State="ENABLED",
                        Description=sched.get("description", "")[:512])
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=FN,
                StatementId=f"AllowEB-{rule_name}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
            )
            report["schedule"] = {"rule": rule_name, "cron": cron_expr, "perm": "ADDED"}
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                report["schedule"] = {"rule": rule_name, "cron": cron_expr, "perm": "EXISTS"}
            else:
                raise

    # 6. Patch HTML endpoint placeholder
    print("6) Patch HTML endpoint...")
    if os.path.isfile(HTML_FILE) and report.get("function_url"):
        with open(HTML_FILE) as f:
            html = f.read()
        if HTML_PLACEHOLDER in html:
            new_html = html.replace(HTML_PLACEHOLDER, report["function_url"].rstrip("/"))
            with open(HTML_FILE, "w") as f:
                f.write(new_html)
            report["html_patched"] = True
        else:
            report["html_patched"] = "ALREADY_PATCHED_OR_PLACEHOLDER_MISSING"

    # 7. Add to freshness manifest
    print("7) Freshness manifest...")
    try:
        m = json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
        m.setdefault("key_overrides", {})
        m["key_overrides"][SNAPSHOT_KEY] = {
            "max_age_h": 30,
            "description": "Tax-Aware Portfolio default snapshot — daily 11:45 UTC. Alert if >30h stale.",
        }
        m["_last_updated"] = datetime.now(timezone.utc).isoformat()
        m["_last_updater"] = "ops/1091"
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                      Body=json.dumps(m, indent=2).encode(), ContentType="application/json")
        report["freshness"] = "ADDED"
    except Exception as e:
        report["freshness_err"] = str(e)[:120]

    # 8. Invoke + verify
    print("8) Test invoke...")
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail")
    report["invoke_status"] = inv["StatusCode"]
    report["fn_err"] = inv.get("FunctionError")
    log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
    report["log_tail"] = log[-1200:]

    time.sleep(3)
    try:
        o = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)
        data = json.loads(o["Body"].read())
        report["verify"] = {
            "size_kb": round(o["ContentLength"] / 1024, 1),
            "n_positions": len(data.get("portfolio_tax_view", {}).get("positions", [])),
            "n_after_tax_assets": len(data.get("after_tax_forward_returns", [])),
            "n_tlh_candidates": len(data.get("tax_loss_harvest_candidates", [])),
            "n_actions": len(data.get("verdict", {}).get("action_items", [])),
            "summary": data.get("verdict", {}).get("summary_line"),
            "effective_rates": data.get("effective_rates"),
            "annual_tax": data.get("annual_tax_bill_estimate", {}).get("total_federal_tax_usd"),
            "elapsed": data.get("elapsed_seconds"),
        }
    except Exception as e:
        report["verify_err"] = str(e)[:200]

    return _save(report)


def _save(report):
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1091.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()

# trigger-retry after billing update
