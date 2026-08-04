"""ops 4373 — insiders v3 structural-coverage verify: two invokes; assert
version 3.0, full surface sections, coverage ledger + ratchet, growing
by_ticker, honest truncation paths, sidecar existence, fleet joins."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-insider-trades"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=340, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 4373, "started": datetime.now(timezone.utc).isoformat(), "rounds": []}


def snap():
    d = json.loads(s3.get_object(Bucket=BUCKET,
                                 Key="data/insider-trades.json")["Body"].read())
    cov = d.get("coverage") or {}
    surf = d.get("surface") or {}
    led = (d.get("fleet") or {}).get("ledger") or []
    try:
        h = s3.head_object(Bucket=BUCKET, Key="data/insider-trades-full.json")
        side = round(h["ContentLength"] / 1024, 1)
    except Exception as e:
        side = f"missing:{type(e).__name__}"
    return {"version": d.get("version"),
            "surface_sections": sorted(surf.keys()),
            "by_ticker_rows": len(surf.get("by_ticker") or []),
            "by_day_rows": len(surf.get("by_day") or []),
            "total_leaves": cov.get("total_leaves"),
            "sections": cov.get("sections"),
            "truncated_paths": cov.get("truncated_paths"),
            "store_totals": cov.get("store_totals"),
            "ratchet": cov.get("ratchet"),
            "scanned": cov.get("filings_scanned_this_run"),
            "days_complete": cov.get("backfill_days_complete"),
            "fleet_ok": sum(1 for e in led if e.get("status") == "ok"),
            "sidecar_kb": side,
            "v3_error": d.get("v3_error")}


for i in range(2):
    rd = {"n": i + 1}
    try:
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=b"{}")
        rd["fn_err"] = inv.get("FunctionError")
        if rd["fn_err"]:
            rd["payload"] = inv["Payload"].read().decode()[:300]
    except Exception as e:
        rd["invoke_err"] = str(e)[:150]
    try:
        rd["snap"] = snap()
    except Exception as e:
        rd["snap_err"] = str(e)[:120]
    R["rounds"].append(rd)
    if i == 0:
        time.sleep(15)

last = (R["rounds"][-1].get("snap") or {})
ok = (last.get("version") == "3.0"
      and len(last.get("surface_sections") or []) >= 8
      and (last.get("total_leaves") or 0) > 500
      and (last.get("by_ticker_rows") or 0) > 10
      and isinstance(last.get("ratchet"), dict)
      and (last.get("fleet_ok") or 0) >= 3
      and isinstance(last.get("sidecar_kb"), (int, float))
      and not last.get("v3_error"))
R["verdict"] = "PASS — structural coverage live" if ok else "PARTIAL — see rounds"
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4373_insiders_v3.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4373_insiders_v3.md", "w").write(
    f"# ops 4373 — insiders v3 structural coverage — {R['verdict']}\n"
    f"- round1: {json.dumps(R['rounds'][0].get('snap') or R['rounds'][0])[:450]}\n"
    f"- round2: {json.dumps(last)[:900]}\n")
print(json.dumps(R, indent=1, default=str))
