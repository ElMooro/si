"""ops 1086 — finalize wealth-plan after fixes.
   1. update_function_code from latest source
   2. create_function_url_config with corrected CORS (AllowHeaders=['content-type'])
   3. add public invoke permission
   4. invoke + verify positive terminal NAVs
   5. patch wealth-plan.html with real URL
"""
import io, json, os, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
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
        except Exception:
            pass
        time.sleep(3)
    return None


def main():
    lam = boto3.client("lambda", region_name=REGION)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "fn": FN}

    with open(CFG_PATH) as f:
        cfg = json.load(f)

    # 1. Update code
    cfg0 = wait_idle(lam)
    if not cfg0:
        report["err"] = "Function not found / not idle"
        return _save(report)
    report["before_sha"] = cfg0.get("CodeSha256", "")[:12]

    zip_bytes = zip_dir(SRC_DIR)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
    cfg1 = wait_idle(lam, 90)
    report["after_sha"] = cfg1.get("CodeSha256", "")[:12] if cfg1 else None
    report["sha_changed"] = report["before_sha"] != report["after_sha"]

    # 2. Create Function URL with fixed CORS
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
            ex = lam.get_function_url_config(FunctionName=FN)
            report["function_url"] = ex["FunctionUrl"]
            report["url_state"] = "EXISTS"
            lam.update_function_url_config(
                FunctionName=FN,
                AuthType=fn_url_cfg.get("auth_type", "NONE"),
                Cors=fn_url_cfg.get("cors", {}),
            )
            report["url_cors_updated"] = True
        else:
            report["url_err"] = str(e)[:400]
            return _save(report)

    # 3. Permission
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

    # 4. Invoke with defaults
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail",
                     Payload=json.dumps({"queryStringParameters": {}}).encode())
    report["invoke_status"] = inv["StatusCode"]
    report["fn_err"] = inv.get("FunctionError")
    log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
    report["log_tail"] = log[-1000:]
    body = json.loads(inv["Payload"].read())
    if "body" in body:
        payload = json.loads(body["body"])
        mc = payload.get("monte_carlo", {})
        v = payload.get("verdict", {})
        a = payload.get("allocation", {})
        report["verify"] = {
            "prob_success": mc.get("prob_success"),
            "terminal_nav_p10": mc.get("terminal_nav_p10"),
            "terminal_nav_p50": mc.get("terminal_nav_p50"),
            "terminal_nav_p90": mc.get("terminal_nav_p90"),
            "terminal_p50_positive": mc.get("terminal_nav_p50", 0) >= 0,
            "n_bankrupt": mc.get("n_bankrupt"),
            "n_sims": mc.get("n_sims"),
            "verdict_status": v.get("status"),
            "verdict_msg": v.get("message", "")[:300],
            "expected_return_pct": a.get("expected_return_pct"),
            "volatility_pct": a.get("volatility_pct"),
            "elapsed": payload.get("elapsed_seconds"),
            "today_dollars_p50": payload.get("in_todays_dollars", {}).get("p50_today_dollars"),
            "sensitivities_keys": list(payload.get("sensitivities", {}).keys()),
        }

    # 5. Patch HTML with the real URL
    url = report.get("function_url", "").rstrip("/")
    if url and os.path.exists(HTML_PATH):
        with open(HTML_PATH) as f:
            html = f.read()
        new_html = html
        if "__LAMBDA_URL__" in html:
            new_html = html.replace("__LAMBDA_URL__", url)
        else:
            import re
            new_html = re.sub(
                r'const LAMBDA_URL = "[^"]*";',
                f'const LAMBDA_URL = "{url}";',
                html,
            )
        if new_html != html:
            with open(HTML_PATH, "w") as f:
                f.write(new_html)
            report["html_patched"] = True
        else:
            report["html_patched"] = "NO_CHANGE"

    return _save(report)


def _save(report):
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1086.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
