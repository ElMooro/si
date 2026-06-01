"""1097 — verify Wave D Telegram alert layer.

Steps:
1. Confirm AI Lambda has the new code (line count or function presence in zip)
2. Delete any existing alert-state file so we get a guaranteed SYSTEM_INITIALIZED
   alert on this invocation (clean test signal)
3. Invoke the Lambda
4. Check that:
   - alerts_sent in response includes SYSTEM_INITIALIZED
   - data/auction-crisis-alert-state.json now exists in S3
   - Telegram message was sent (the response telegram_sent flag)
5. Then invoke a SECOND time and confirm no alerts (state matches → no transitions)
6. Save the full Telegram message that was sent for confirmation
"""
import io, json, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1097_alert_verify.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Confirm new code is deployed
    print("[1097] phase 1: verify new code in Lambda zip…")
    info = lam.get_function(FunctionName="justhodl-auction-crisis-ai")
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zb = r.read()
    with zipfile.ZipFile(io.BytesIO(zb)) as zf:
        src = zf.read("lambda_function.py").decode("utf-8")
    out["lambda_meta"] = {
        "last_modified": info["Configuration"]["LastModified"],
        "lines":         len(src.split("\n")),
        "has_send_telegram":    "def send_telegram" in src,
        "has_compute_alert":    "def compute_alert_state" in src,
        "has_detect_trans":     "def detect_transitions" in src,
        "has_maybe_send":       "def maybe_send_alerts" in src,
        "has_format_msg":       "def format_telegram_message" in src,
    }
    
    # 2. Delete prior alert state to guarantee SYSTEM_INITIALIZED on this run
    print("[1097] phase 2: clear prior alert state…")
    try:
        s3.delete_object(Bucket="justhodl-dashboard-live",
                          Key="data/auction-crisis-alert-state.json")
        out["alert_state_cleared"] = True
    except Exception as e:
        out["alert_state_clear_err"] = str(e)[:120]
    
    # 3. First invoke — should trigger SYSTEM_INITIALIZED + Telegram
    print("[1097] phase 3: first invoke (expect SYSTEM_INITIALIZED + Telegram)…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-auction-crisis-ai",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["invoke1_elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            inner = json.loads(p["body"])
            out["invoke1_summary"] = inner
    except Exception:
        out["invoke1_raw"] = body[:300]
    
    # 4. Check that state file was written + read full AI output
    print("[1097] phase 4: verify state + AI output…")
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/auction-crisis-alert-state.json")
        state = json.loads(obj["Body"].read())
        out["alert_state"] = state
        out["alert_state_size_bytes"] = obj["ContentLength"]
        out["alert_state_last_modified"] = obj["LastModified"].isoformat()
    except Exception as e:
        out["alert_state_err"] = str(e)[:120]
    
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/auction-crisis-ai.json")
        d = json.loads(obj["Body"].read())
        out["ai_output_alerts_sent"] = d.get("alerts_sent", [])
        out["ai_output_alerts_error"] = d.get("alerts_error")
        # Echo decisive call for context
        out["decisive_call"] = (d.get("ai_commentary") or {}).get("decisive_call", "")[:300]
    except Exception as e:
        out["ai_output_err"] = str(e)[:120]
    
    # 5. Reconstruct the Telegram message that was sent
    if out.get("alert_state") and out.get("ai_output_alerts_sent"):
        try:
            # Pull the AI commentary for message formatting
            ai_data = json.loads(s3.get_object(
                Bucket="justhodl-dashboard-live", Key="data/auction-crisis-ai.json"
            )["Body"].read())
            data_data = json.loads(s3.get_object(
                Bucket="justhodl-dashboard-live", Key="data/auction-crisis.json"
            )["Body"].read())
            alerts = out["ai_output_alerts_sent"]
            # Use the Lambda's actual formatter via subprocess — but simpler to just reconstruct
            msg_lines = ["🚨 *Treasury Auction Crisis Alert* 🚨" if any(
                a.get("type") in ("REGIME_ESCALATION", "INDICATOR_FIRED", "TAIL_RISK_CROSSED")
                for a in alerts
            ) else "📊 *Treasury Auction System Notice*", ""]
            for a in alerts:
                t = a.get("type")
                if t == "SYSTEM_INITIALIZED":
                    msg_lines.append(f"🆕 System ONLINE — alert layer initialized.")
                    msg_lines.append(f"   Regime: *{a.get('current_regime')}* · composite {a.get('composite'):.1f}/100")
            out["reconstructed_telegram"] = "\n".join(msg_lines)
        except Exception as e:
            out["telegram_reconstruct_err"] = str(e)[:120]
    
    # 6. Second invoke — should produce NO alerts (state matches)
    print("[1097] phase 5: second invoke (expect 0 alerts — state matches)…")
    time.sleep(3)
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-auction-crisis-ai",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["invoke2_elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            inner = json.loads(p["body"])
            out["invoke2_summary"] = inner
    except Exception:
        out["invoke2_raw"] = body[:300]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1097] DONE")


if __name__ == "__main__":
    main()
