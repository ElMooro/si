"""ops/728 — provision the Portfolio Manager access PIN.

Routing portfolio-admin writes through the Cloudflare Worker means the
powerful Lambda infra token never touches the browser. But a write
endpoint to a personal book must not be world-editable, so the Worker
also checks a lightweight manager PIN (an x-mgr-pass header).

This creates that PIN in SSM SecureString /justhodl/portfolio-admin/
manager-pass and delivers it to Khalid over Telegram. The Worker secret
PORTFOLIO_MGR_PASS is sourced from this param by deploy-workers.yml.
"""
import json, os, hashlib, secrets, urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
SSM_NAME = "/justhodl/portfolio-admin/manager-pass"
TG_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT = "8678089260"

ssm = boto3.client("ssm", region_name=REGION)
report = {"ops": 728, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Portfolio Manager access PIN provisioning"}

pin = secrets.token_urlsafe(15)
report["pin_fingerprint"] = hashlib.sha256(pin.encode()).hexdigest()[:12]

try:
    ssm.put_parameter(Name=SSM_NAME, Value=pin, Type="SecureString",
                      Overwrite=True,
                      Description="Manager PIN checked by the Worker "
                                  "portfolio-admin route (x-mgr-pass header)")
    report["ssm_set"] = True
except Exception as e:
    report["ssm_set"] = False
    report["ssm_error"] = str(e)[:240]

if report.get("ssm_set"):
    msg = ("🔑 <b>Portfolio Manager PIN</b>\n\n"
           "This replaces the old admin token on the Portfolio Manager page "
           "(justhodl.ai/portfolio-manager.html). Writes now route through "
           "the Cloudflare Worker — the AWS infra token no longer touches "
           "your browser at all.\n\n"
           f"<code>{pin}</code>\n\n"
           f"Stored in SSM {SSM_NAME}.")
    try:
        data = json.dumps({"chat_id": TG_CHAT, "text": msg,
                           "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            report["telegram_sent"] = (r.status == 200)
    except Exception as e:
        report["telegram_sent"] = False
        report["telegram_error"] = str(e)[:200]

report["all_pass"] = bool(report.get("ssm_set") and report.get("telegram_sent"))
report["verdict"] = ("PROVISIONED — manager PIN in SSM + delivered via Telegram"
                     if report["all_pass"] else "PARTIAL — see fields")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/728_manager_pin_provision.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/728_manager_pin_provision.json")
