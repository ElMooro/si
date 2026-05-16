"""ops/725 — create the portfolio-admin Function URL (POST-capable).

deploy-lambdas.yml only provisions GET/OPTIONS Function URLs. This endpoint
is a POST API, so the URL is created directly via boto3 with the correct
CORS (POST + OPTIONS, x-justhodl-token header, justhodl.ai origins) and a
public-invoke resource permission. Idempotent — updates if it already exists.
"""
import json, os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
FN = "justhodl-portfolio-admin"
CORS = {
    "AllowCredentials": False,
    "AllowHeaders": ["content-type", "x-justhodl-token"],
    "AllowMethods": ["POST", "OPTIONS"],
    "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai"],
    "MaxAge": 300,
}
lam = boto3.client("lambda", region_name=REGION)
report = {"ops": 725, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "portfolio-admin POST Function URL creation"}

# ── 1. create or update the Function URL config ──
url = None
try:
    r = lam.create_function_url_config(FunctionName=FN, AuthType="NONE",
                                       Cors=CORS)
    url = r["FunctionUrl"]
    report["url_action"] = "created"
except lam.exceptions.ResourceConflictException:
    r = lam.update_function_url_config(FunctionName=FN, AuthType="NONE",
                                       Cors=CORS)
    url = r["FunctionUrl"]
    report["url_action"] = "updated"
except Exception as e:
    report["url_action"] = "error"
    report["url_error"] = str(e)[:240]

report["function_url"] = url

# ── 2. public invoke permission for the URL ──
try:
    lam.add_permission(FunctionName=FN, StatementId="FunctionURLPublicInvoke",
                       Action="lambda:InvokeFunctionUrl", Principal="*",
                       FunctionUrlAuthType="NONE")
    report["permission"] = "added"
except lam.exceptions.ResourceConflictException:
    report["permission"] = "already_present"
except Exception as e:
    report["permission"] = "error"
    report["permission_error"] = str(e)[:240]

# ── 3. confirm via get ──
try:
    cfg = lam.get_function_url_config(FunctionName=FN)
    report["confirmed_url"] = cfg.get("FunctionUrl")
    report["confirmed_cors"] = cfg.get("Cors")
except Exception as e:
    report["confirm_error"] = str(e)[:200]

report["all_pass"] = bool(url) and report.get("permission") in (
    "added", "already_present")
report["verdict"] = ("URL LIVE — portfolio-admin reachable for POST"
                     if report["all_pass"] else "REVIEW — see fields")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/725_portfolio_admin_url.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/725_portfolio_admin_url.json")
