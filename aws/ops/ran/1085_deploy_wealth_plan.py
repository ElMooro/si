"""
ops 1085 — deploy justhodl-wealth-plan:
  1. Create Lambda function (or update if deploy-lambdas.yml beat us to it)
  2. Create Function URL with public CORS
  3. Invoke with default params to verify Monte Carlo runs
  4. Patch wealth-plan.html with the actual Function URL (replace placeholder)
  5. Commit the patched HTML back via this ops run-ops auto-commit path
"""
import io, json, os, time, zipfile, base64
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
FN = "justhodl-wealth-plan"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
SRC_DIR = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
CFG_PATH = os.path.join(REPO_ROOT, "aws/lambdas", FN, "config.json")
HTML_PATH = os.path.join(REPO_ROOT, "wealth-plan.html")


def zip_dir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                full = os.path.join(root, f)
                z.write(full, os.path.relpath(full, d))
    return buf.getvalue()


def wait_idle(lam, max_wait=180):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") in ("Successful", None):
                return cfg
            if cfg.get("LastUpdateStatus") == "Failed":
                return None
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
        time.sleep(3)
    return None


def main():
    lam = boto3.client("lambda", region_name=REGION)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "fn": FN}

    with open(CFG_PATH) as f:
        cfg = json.load(f)

    # Phase 1 — create or update
    existing = wait_idle(lam, max_wait=60)
    if existing:
        report["initial_state"] = "EXISTS"
        report["initial_sha"] = existing.get("CodeSha256", "")[:12]
        # Update code from latest source
        zip_bytes = zip_dir(SRC_DIR)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        wait_idle(lam, 90)
        # Sync config
        lam.update_function_configuration(
            FunctionName=FN,
            Environment={"Variables": cfg.get("env", {})},
            Timeout=int(cfg.get("timeout", 60)),
            MemorySize=int(cfg.get("memory", 1024)),
            Description=cfg.get("description", "")[:255],
        )
        wait_idle(lam, 60)
        report["create"] = "UPDATED_EXISTING"
    else:
        zip_bytes = zip_dir(SRC_DIR)
        try:
            r = lam.create_function(
                FunctionName=FN,
                Runtime=cfg.get("runtime", "python3.12"),
                Role=cfg["role"],
                Handler=cfg.get("handler", "lambda_function.lambda_handler"),
                Description=cfg.get("description", "")[:255],
                Timeout=int(cfg.get("timeout", 60)),
                MemorySize=int(cfg.get("memory", 1024)),
                Environment={"Variables": cfg.get("env", {})},
                Architectures=cfg.get("architectures", ["x86_64"]),
                Code={"ZipFile": zip_bytes},
            )
            report["create"] = "CREATED"
            report["arn"] = r["FunctionArn"]
            wait_idle(lam, 90)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                wait_idle(lam, 90)
                report["create"] = "RACED_EXISTS"
            else:
                report["create_err"] = str(e)[:300]
                return _save(report)

    # Phase 2 — create or update Function URL
    fn_url_cfg = cfg.get("function_url", {})
    try:
        r = lam.create_function_url_config(
            FunctionName=FN,
            AuthType=fn_url_cfg.get("auth_type", "NONE"),
            Cors=fn_url_cfg.get("cors", {}),
        )
        report["function_url"] = r["FunctionUrl"]
        report["url_state"] = "CREATED"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            # Already has one
            existing_url = lam.get_function_url_config(FunctionName=FN)
            report["function_url"] = existing_url["FunctionUrl"]
            report["url_state"] = "EXISTS"
            # Update CORS
            lam.update_function_url_config(
                FunctionName=FN,
                AuthType=fn_url_cfg.get("auth_type", "NONE"),
                Cors=fn_url_cfg.get("cors", {}),
            )
            report["url_cors_updated"] = True
        else:
            report["url_err"] = str(e)[:300]

    # Phase 3 — public invoke permission (required for NONE auth URL)
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId="FunctionUrlAllowPublic",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
        report["public_permission"] = "ADDED"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            report["public_permission"] = "EXISTS"
        else:
            report["public_permission_err"] = str(e)[:200]

    # Phase 4 — invoke with default params
    print("Invoking with defaults …")
    try:
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail",
                         Payload=json.dumps({"queryStringParameters": {}}).encode())
        report["invoke_status"] = inv["StatusCode"]
        report["fn_err"] = inv.get("FunctionError")
        log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
        report["log_tail"] = log[-1500:]
        body = json.loads(inv["Payload"].read())
        if "body" in body:
            payload = json.loads(body["body"])
            report["verify"] = {
                "version": payload.get("version"),
                "prob_success": payload.get("monte_carlo", {}).get("prob_success"),
                "terminal_p50": payload.get("monte_carlo", {}).get("terminal_nav_p50"),
                "verdict_status": payload.get("verdict", {}).get("status"),
                "verdict_msg": payload.get("verdict", {}).get("message", "")[:200],
                "allocation_E_r": payload.get("allocation", {}).get("expected_return_pct"),
                "allocation_vol": payload.get("allocation", {}).get("volatility_pct"),
                "n_assets_in_book": len(payload.get("allocation", {}).get("weights", {})),
                "compass_age_hours": (
                    (datetime.now(timezone.utc) - datetime.fromisoformat(
                        payload["compass_generated_at"].replace("Z", "+00:00")
                    )).total_seconds() / 3600
                    if payload.get("compass_generated_at") else None
                ),
                "elapsed": payload.get("elapsed_seconds"),
            }
    except Exception as e:
        report["invoke_err"] = str(e)[:300]

    # Phase 5 — patch wealth-plan.html with real URL
    url = report.get("function_url")
    if url and os.path.exists(HTML_PATH):
        with open(HTML_PATH) as f:
            html = f.read()
        if "__LAMBDA_URL__" in html:
            html = html.replace("__LAMBDA_URL__", url.rstrip("/"))
            with open(HTML_PATH, "w") as f:
                f.write(html)
            report["html_patched"] = True
        else:
            # Already patched, replace existing URL line
            import re
            html2 = re.sub(
                r'const LAMBDA_URL = "[^"]*";',
                f'const LAMBDA_URL = "{url.rstrip("/")}";',
                html,
            )
            if html2 != html:
                with open(HTML_PATH, "w") as f:
                    f.write(html2)
                report["html_patched"] = "URL_UPDATED"
            else:
                report["html_patched"] = "NO_CHANGE_NEEDED"

    return _save(report)


def _save(report):
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1085.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
