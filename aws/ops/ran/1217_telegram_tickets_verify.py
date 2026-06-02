"""1217 — Embed trade tickets in Telegram. Reset state for cascade_laggard +
velocity + convergence + early_movers + options_flow + re-invoke router."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1217_telegram_tickets_verify.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# Wait for deploy
time.sleep(75)

# Step 1: Reset router state for ticker-bearing signal types ONLY
# (so user gets a fresh Telegram message with embedded trade tickets)
print("[1217] 1. Reset router state for buy-candidate signal types")
try:
    state_obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/prepump-router-state.json")
    state = json.loads(state_obj["Body"].read())
    pre = {k: len(v) for k, v in (state.get("alerted_by_signal") or {}).items()}

    # Clear only buy-candidate signals so they re-fire WITH trade tickets
    for sig_type in ["cascade_laggard", "velocity", "convergence_ultra_new",
                      "early_mover_alert", "options_flow"]:
        if sig_type in state.get("alerted_by_signal", {}):
            del state["alerted_by_signal"][sig_type]

    post = {k: len(v) for k, v in (state.get("alerted_by_signal") or {}).items()}
    s3.put_object(
        Bucket=BUCKET, Key="data/_alerts/prepump-router-state.json",
        Body=json.dumps(state, default=str).encode(),
        ContentType="application/json",
    )
    out["state_reset"] = {"before": pre, "after": post}
    print(f"  ✓ cleared 5 buy-candidate signal types")
except Exception as e:
    out["state_reset_err"] = str(e)[:200]

# Step 2: Trigger trade-tickets to make sure data is fresh
print(f"\n[1217] 2. Refresh trade-tickets (latest prices)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-trade-tickets",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["tickets_refresh"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                                "function_error": resp.get("FunctionError"),
                                "body": payload[:800]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
except Exception as e:
    out["tickets_refresh"] = {"error": str(e)[:300]}

# Step 3: Invoke router — should send Telegram with embedded tickets
print(f"\n[1217] 3. Invoke prepump-alerts-router (will send Telegram WITH tickets)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-prepump-alerts-router",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["router_invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                              "function_error": resp.get("FunctionError"),
                              "body": payload[:2000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
    else:
        try:
            outer = json.loads(payload)
            inner = json.loads(outer.get("body", "{}"))
            print(f"  msgs_sent={inner.get('n_messages_sent')}")
            print(f"  counts: {inner.get('counts')}")
        except Exception:
            pass
except Exception as e:
    out["router_invoke"] = {"error": str(e)[:300]}

# Step 4: Read updated state to see what just got alerted
print(f"\n[1217] 4. State after invoke")
try:
    state = json.loads(s3.get_object(Bucket=BUCKET, Key="data/_alerts/prepump-router-state.json")["Body"].read())
    alerted = state.get("alerted_by_signal") or {}
    out["new_state"] = {k: len(v) for k, v in alerted.items()}
    out["new_alerts_detail"] = {k: v[:6] for k, v in alerted.items() 
                                  if k in ["cascade_laggard", "velocity",
                                            "convergence_ultra_new", "early_mover_alert",
                                            "options_flow"]}
    print(f"  Re-fired signals (with embedded tickets):")
    for k in ["cascade_laggard", "velocity", "convergence_ultra_new",
              "early_mover_alert", "options_flow"]:
        if k in alerted:
            print(f"    {k:25s}: {len(alerted[k])} alerts → {alerted[k][:5]}")
except Exception as e:
    out["new_state"] = {"error": str(e)[:200]}

# Step 5: Verify the trade-tickets file is being read correctly
print(f"\n[1217] 5. Verify trade-tickets file accessible to router")
try:
    tickets_doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-tickets.json")["Body"].read())
    tickets = tickets_doc.get("tickets") or []
    out["tickets_summary"] = {
        "n_tickets": len(tickets),
        "generated_at": tickets_doc.get("generated_at"),
        "sample": [{"ticker": t["ticker"], "entry": t["entry"], "stop": t["stop_loss"],
                    "tp3": t["tp3"], "rr": t["rr_tp3"]}
                    for t in tickets[:5]],
    }
    print(f"  ✓ {len(tickets)} tickets ready for embedding")
except Exception as e:
    out["tickets_summary"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1217] DONE")
