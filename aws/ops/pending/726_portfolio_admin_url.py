"""ops/726 — create the portfolio-admin Function URL (corrected CORS).

ops/725 failed: 'OPTIONS' is not a valid cors.allowMethods value — the
Function URL service handles the preflight implicitly. Methods must be
real HTTP verbs. Uses AllowMethods=["POST"]; idempotent.
"""
import json, os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
FN = "justhodl-portfolio-admin"
CORS = {
    "AllowCredentials": False,
    "AllowHeaders": ["content-type", "x-justhodl-token"],
    "AllowMethods": ["POST"],
    "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai"],
    "MaxAge": 300,
}
lam = boto3.client("lambda", region_name=REGION)
report = {"ops": 726, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "portfolio-admin Function URL — corrected CORS"}

url = None
try:
    r = lam.create_function_url_config(FunctionName=FN, AuthType="NONE",
                                       Cors=CORS)
    url, report["url_action"] = r["FunctionUrl"], "created"
except lam.exceptions.ResourceConflictException:
    r = lam.update_function_url_config(FunctionName=FN, AuthType="NONE",
                                       Cors=CORS)
    url, report["url_action"] = r["FunctionUrl"], "updated"
except Exception as e:
    report["url_action"] = "error"
    report["url_error"] = str(e)[:300]
report["function_url"] = url

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
with open("aws/ops/reports/726_portfolio_admin_url.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/726_portfolio_admin_url.json")
