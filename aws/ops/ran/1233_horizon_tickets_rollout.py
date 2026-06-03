"""1233 — Deploy horizon-aware trade-tickets + invoke + verify ticket schema."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1233_horizon_tickets_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-trade-tickets"
SOURCE_DIR = "aws/lambdas/justhodl-trade-tickets/source"
REGION = "us-east-1"

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# Deploy
print(f"[1233] 1. Update {LAMBDA} with horizon-aware sizing")
try:
    zip_bytes = build_zip()
    lam.update_function_code(FunctionName=LAMBDA, ZipFile=zip_bytes)
    for _ in range(15):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    out["update"] = {"state": c.get("State"), "code_sha": c.get("CodeSha256")[:16]}
    print(f"  ✓ updated · sha={c.get('CodeSha256')[:16]}")
except Exception as e:
    out["update_err"] = str(e)[:300]

# Invoke
print(f"\n[1233] 2. Invoke (test horizon-aware ticket generation)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                      "function_error": resp.get("FunctionError"), "body": payload[:1200]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  n_tickets: {inner.get('n_tickets')}")
        except: pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Verify new ticket schema fields
print(f"\n[1233] 3. Verify ticket schema has horizon-aware fields")
try:
    tickets_doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-tickets.json")["Body"].read())
    tickets = (tickets_doc.get("tickets") or [])
    horizon_tickets = [t for t in tickets if t.get("expected_horizon_days") is not None]
    out["schema_check"] = {
        "n_tickets_total": len(tickets),
        "n_with_horizon": len(horizon_tickets),
        "regimes_seen": list(set(t.get("horizon_regime") for t in horizon_tickets if t.get("horizon_regime"))),
        "horizon_sources_seen": list(set(t.get("horizon_source") for t in horizon_tickets if t.get("horizon_source"))),
        "sample": [
            {
                "ticker": t.get("ticker"),
                "setup_type": t.get("setup_type"),
                "expected_horizon_days": t.get("expected_horizon_days"),
                "horizon_regime": t.get("horizon_regime"),
                "horizon_source": t.get("horizon_source"),
                "atr_mult_horizon_adj": t.get("atr_mult_horizon_adj"),
                "entry": t.get("entry"),
                "stop_loss": t.get("stop_loss"),
                "tp3": t.get("tp3"),
                "rr_tp3": t.get("rr_tp3"),
                "risk_pct": t.get("risk_pct"),
            }
            for t in horizon_tickets[:8]
        ],
    }
    print(f"  ✓ {len(horizon_tickets)}/{len(tickets)} tickets have horizon info")
    print(f"  ✓ Regimes seen: {out['schema_check']['regimes_seen']}")
    print(f"  ✓ Sources: {out['schema_check']['horizon_sources_seen']}")
except Exception as e:
    out["schema_check"] = {"error": str(e)[:200]}

# Show the distribution of horizons across tickets
print(f"\n[1233] 4. Horizon distribution")
if "schema_check" in out and "sample" in out["schema_check"]:
    by_regime = {}
    by_setup = {}
    for t in out["schema_check"]["sample"]:
        r = t.get("horizon_regime")
        s = t.get("setup_type")
        if r: by_regime[r] = by_regime.get(r, 0) + 1
        if s: by_setup[s] = by_setup.get(s, 0) + 1
    out["distribution"] = {"by_regime": by_regime, "by_setup": by_setup}
    print(f"  By regime: {by_regime}")
    print(f"  By setup type: {by_setup}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1233] DONE")
