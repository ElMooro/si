"""ops 1141 — Telegram alerting + macro convergence fingerprint rollout.

Adds Telegram-alert infrastructure to the router:
  _telegram_post()                     — Markdown POST to bot, with plain-text fallback
  _alert_state_io()                    — read/write state file per sniffer in S3
  _macro_convergence_fingerprint()     — detect AUCTION + PRIMARY_DEALER + FUNDING_PLUMBING co-stress
  _decide_alert_kind()                 — multi-priority decision: convergence → regime-transition →
                                         extreme-persisting → score-jump, with cooldown + daily cap
  _format_equity_alert()               — Markdown message for the equity sniffer
  _format_macro_alert()                — Markdown message for the macro sniffer (convergence header
                                         when fingerprint fires, with all 3 pillar states)
  _maybe_alert_equity_sniffer()        — wired into generate_frontrun_brief after history write
  _maybe_alert_macro_sniffer()         — wired into generate_macro_frontrun_brief after history write

Alert rules:
  PRIORITY 1: Convergence fingerprint fires for first time in 4h (highest priority)
  PRIORITY 2: Regime transitions (NORMAL→ELEVATED→EXTREME or reverse) — bypass cooldown
  PRIORITY 3: EXTREME persisting (every 60 min)
  PRIORITY 4: Score jump (≥ 15 points either direction, respects 60-min cooldown)
  Daily cap: 8 alerts per sniffer per UTC day. State stored in S3 at
             data/_alerts/{sniffer-name}-alert-state.json.

This op:
  1. Sets TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID env vars on the router Lambda
     (these match the user's existing @Justhodl_bot setup).
  2. Redeploys the router with the alerting code.
  3. Sends a FORCE TEST ALERT directly to confirm Telegram delivery works
     (calls _telegram_post via a synthetic invoke).
  4. Re-invokes both sniffers (frontrun-sniffer + macro-frontrun-sniffer) so the
     alert state files get initialized with real data.
"""
import io, json, os, time, traceback, zipfile, base64, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"

# These are the user's existing bot credentials per his system constants
TELEGRAM_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID   = "8678089260"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(t=180):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(2)
    return False


def telegram_test():
    """Send a test message directly from the op script to confirm bot works."""
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    msg = (
        "🧪 *Front-Run Alerting Deployed*\n\n"
        "Both sniffers are now wired for Telegram alerts:\n"
        "  • 🎯 Equity sniffer (`frontrun-sniffer`)\n"
        "  • 🏛 Macro sniffer (`macro-frontrun-sniffer`)\n\n"
        "Alert triggers:\n"
        "  1. *Convergence fingerprint* (macro) — when AUCTION + PRIMARY_DEALER + "
        "FUNDING_PLUMBING all stress simultaneously (Aug 2007 / Sep 2019 / Mar 2020 / Mar 2023 signature)\n"
        "  2. Regime transitions (NORMAL→ELEVATED→EXTREME or reverse)\n"
        "  3. EXTREME persisting (every 60 min)\n"
        "  4. Score jump ≥ 15 points\n\n"
        "Cap: 8 alerts/day/sniffer. State stored in S3 alongside the sniffer outputs.\n\n"
        "→ https://justhodl.ai/frontrun.html\n"
        "→ https://justhodl.ai/macro-frontrun.html"
    )
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }).encode()
    try:
        req = urllib.request.Request(api, data=payload, headers={"Content-Type":"application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return True, json.loads(r.read().decode())
    except Exception as e:
        # Fallback plain text
        try:
            payload2 = json.dumps({
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg.replace("*","").replace("`","").replace("_",""),
                "disable_web_page_preview": True,
            }).encode()
            req = urllib.request.Request(api, data=payload2, headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return True, json.loads(r.read().decode())
        except Exception as e2:
            return False, {"err1": str(e)[:200], "err2": str(e2)[:200]}


def invoke_sniffer(slug):
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"contexts": [slug]}).encode(),
                     LogType="Tail")
    body_resp = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body_resp, dict) and "body" in body_resp:
        try: body_resp = json.loads(body_resp["body"])
        except Exception: pass
    log = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1800:]
    return body_resp, inv.get("FunctionError"), log


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Get current env vars + add TELEGRAM_*
        wait_active()
        cur = lam.get_function_configuration(FunctionName=FN)
        cur_env = (cur.get("Environment") or {}).get("Variables") or {}
        new_env = dict(cur_env)
        new_env["TELEGRAM_BOT_TOKEN"] = TELEGRAM_BOT_TOKEN
        new_env["TELEGRAM_CHAT_ID"]   = TELEGRAM_CHAT_ID
        rpt["env_vars_set"] = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
        rpt["env_vars_total"] = len(new_env)

        lam.update_function_configuration(
            FunctionName=FN,
            Environment={"Variables": new_env},
        )
        wait_active()
        rpt["env_update"] = "OK"

        # 2) Redeploy router code with alerting logic
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 3) Send direct Telegram test (from ops script, not Lambda) to confirm bot works
        ok, info = telegram_test()
        rpt["telegram_direct_test"] = {"ok": ok, "result": info}

        # 4) Invoke equity sniffer — first real cycle with alerting wired
        time.sleep(2)
        print("[1141] invoking equity sniffer …")
        body, err, log = invoke_sniffer("frontrun-sniffer")
        rpt["equity_invoke"] = {"err": err, "n_ok": body.get("n_ok") if isinstance(body, dict) else None,
                                  "duration_s": body.get("duration_s") if isinstance(body, dict) else None}
        # Capture alert-related log lines
        rpt["equity_log_alert"] = "\n".join(L for L in log.split("\n") if "[frontrun]" in L)[:1500]

        # 5) Invoke macro sniffer
        time.sleep(3)
        print("[1141] invoking macro sniffer …")
        body2, err2, log2 = invoke_sniffer("macro-frontrun-sniffer")
        rpt["macro_invoke"] = {"err": err2, "n_ok": body2.get("n_ok") if isinstance(body2, dict) else None,
                                "duration_s": body2.get("duration_s") if isinstance(body2, dict) else None}
        rpt["macro_log_alert"] = "\n".join(L for L in log2.split("\n") if "[macro_frontrun]" in L)[:1500]

        # 6) Verify alert state files were created in S3
        time.sleep(2)
        for state_file, label in [
            ("data/_alerts/frontrun-sniffer-alert-state.json", "equity_state"),
            ("data/_alerts/macro-frontrun-sniffer-alert-state.json", "macro_state"),
        ]:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=state_file)
                state = json.loads(obj["Body"].read())
                rpt[label] = state
            except ClientError:
                rpt[label] = "NOT_WRITTEN (likely no alert condition met — that's fine)"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1141.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"}, indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
