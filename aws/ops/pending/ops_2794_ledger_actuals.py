"""ops 2794 — report ACTUAL measured LLM spend/cache from the ledger (no estimates)."""
import os, json, time
from datetime import datetime, timezone, timedelta
import boto3
ddb = boto3.client("dynamodb", region_name="us-east-1")
R = {"ops": 2794, "ts": datetime.now(timezone.utc).isoformat()}
def _n(i, k):
    try: return float(i.get(k, {}).get("N", "0"))
    except Exception: return 0.0
now = datetime.now(timezone.utc)
tot = {"cost": 0.0, "calls": 0, "real_calls": 0, "cache_hits": 0, "in_tok": 0, "out_tok": 0}
days_with_data = []
for i in range(30):
    d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
    try:
        r = ddb.query(TableName="justhodl-llm-cost", KeyConditionExpression="#d = :d",
                      ExpressionAttributeNames={"#d": "date"}, ExpressionAttributeValues={":d": {"S": d}})
    except Exception as e:
        R["err"] = str(e)[:120]; break
    items = [it for it in r.get("Items", []) if not it["engine_model"]["S"].startswith(("local|", "ops-"))]
    if items:
        dc = sum(_n(it, "cost_usd") for it in items)
        days_with_data.append({"date": d, "cost": round(dc, 4),
                               "real_calls": int(sum(_n(it, "real_calls") for it in items)),
                               "cache_hits": int(sum(_n(it, "cache_hits") for it in items))})
    for it in items:
        for k in tot: tot[k] += _n(it, "cost_usd" if k == "cost" else k)
served = tot["real_calls"] + tot["cache_hits"]
R["measured"] = {
    "real_spend_usd": round(tot["cost"], 4),
    "real_calls": int(tot["real_calls"]),
    "cache_hits": int(tot["cache_hits"]),
    "total_requests_served": int(served),
    "cache_hit_rate_pct": round(100 * tot["cache_hits"] / served, 1) if served else 0.0,
    "days_with_real_data": len(days_with_data),
    "recent_days": days_with_data[:7],
}
R["note"] = "excludes local|/ops- self-test rows"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2794_ledger_actuals.json", "w"), indent=1, default=str)
print(json.dumps(R["measured"], indent=1))
print("OPS 2794 COMPLETE")
