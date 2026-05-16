"""ops/724 — provision the portfolio-admin secured Function URL.

Part B of wiring a holdings feed into the PM cockpit.

Does:
  1. Generates a strong admin token and stores it in SSM SecureString
     /justhodl/portfolio-admin/token (Overwrite=True — idempotent).
  2. Discovers the Function URL created for justhodl-portfolio-admin by the
     deploy-lambdas workflow (retries — the two workflows run in parallel).
  3. Delivers the token to Khalid over Telegram (private channel) so it
     never touches git, the committed report, or a public log.

The committed report contains only a non-secret fingerprint of the token.
"""
import json, os, time, hashlib, secrets, urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
SSM_NAME = "/justhodl/portfolio-admin/token"
FN = "justhodl-portfolio-admin"
TG_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT = "8678089260"

ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

report = {"ops": 724, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "portfolio-admin secured Function URL provisioning"}

# ── 1. token → SSM SecureString ──
token = secrets.token_urlsafe(32)
fp = hashlib.sha256(token.encode()).hexdigest()[:12]
report["token_fingerprint"] = fp
try:
    ssm.put_parameter(Name=SSM_NAME, Value=token, Type="SecureString",
                      Overwrite=True,
                      Description="Auth token for portfolio-admin Function URL")
    report["ssm_set"] = True
except Exception as e:
    report["ssm_set"] = False
    report["ssm_error"] = str(e)[:240]

# ── 2. discover Function URL (deploy-lambdas runs in parallel — retry) ──
url = None
for attempt in range(9):
    try:
        cfg = lam.get_function_url_config(FunctionName=FN)
        url = cfg.get("FunctionUrl")
        if url:
            break
    except Exception as e:
        report["url_lookup_last_err"] = str(e)[:160]
    time.sleep(20)
report["function_url"] = url
report["function_url_found"] = bool(url)

# ── 3. deliver token to Khalid via Telegram ──
if report.get("ssm_set"):
    msg = ("🔐 <b>Portfolio Admin token</b>\n\n"
           "Paste this into the Portfolio Manager page when prompted "
           "(justhodl.ai/portfolio-manager.html). It authorises adding / "
           "editing positions in your book.\n\n"
           f"<code>{token}</code>\n\n"
           f"Fingerprint: {fp} · stored in SSM {SSM_NAME}")
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

report["all_pass"] = bool(report.get("ssm_set")
                          and report.get("function_url_found")
                          and report.get("telegram_sent"))
report["verdict"] = (
    "PROVISIONED — token in SSM + delivered via Telegram, Function URL live"
    if report["all_pass"]
    else "PARTIAL — see fields; Function URL may still be deploying")

# never print the raw token to the committed report
safe = {k: v for k, v in report.items()}
print(json.dumps(safe, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/724_portfolio_admin_provision.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/724_portfolio_admin_provision.json")
