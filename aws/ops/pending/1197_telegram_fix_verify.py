"""1197 — Verify Telegram digest now sends with HTML parse mode."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1197_telegram_fix_verify.json"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

print("[1197] Sync invoke justhodl-flows-ai-analysis (full AI run + Telegram)")
try:
    t0 = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-flows-ai-analysis",
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    try:
        parsed = json.loads(payload)
        body = json.loads(parsed.get("body", "{}"))
    except Exception:
        body = {"raw": payload[:500]}
    out["invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": body,
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
    else:
        tg = body.get("telegram") or {}
        print(f"  regime={body.get('regime')}  calls={body.get('n_calls')}")
        print(f"  Telegram: sent={tg.get('sent')} chars={tg.get('chars')} http={tg.get('http_status')}")
        print(f"  Telegram response summary: {tg.get('telegram_response_summary')}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"[1197] DONE")
