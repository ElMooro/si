"""
justhodl-llm-cost-dashboard — aggregates the justhodl-llm-cost DDB ledger into
data/llm-cost.json for the LLM Cost Desk page. Runs hourly + on demand.

Surfaces, per day (14d) and per engine/model (today): calls, real calls, cache
hits, cache-hit-rate, tokens, USD; today's spend vs the SSM daily budget; the
governance mode (normal/economy/off); and the estimated $ saved by the content
cache (cached calls priced at the trailing average cost of a real call).
"""
import json
import boto3
from datetime import datetime, timezone, timedelta

REGION = "us-east-1"
TABLE = "justhodl-llm-cost"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/llm-cost.json"

ddb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def _n(item, k):
    try:
        return float(item.get(k, {}).get("N", "0"))
    except Exception:
        return 0.0


def _query_day(date):
    items, kw = [], dict(
        TableName=TABLE, KeyConditionExpression="#d = :d",
        ExpressionAttributeNames={"#d": "date"},
        ExpressionAttributeValues={":d": {"S": date}})
    while True:
        r = ddb.query(**kw)
        items += r.get("Items", [])
        if "LastEvaluatedKey" in r:
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        else:
            break
    return items


def _ssm(name, default):
    try:
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except Exception:
        return default


def handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    days = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(14)]
    per_day, per_engine, per_model = [], {}, {}

    for idx, d in enumerate(days):
        items = _query_day(d)
        dc = dict(date=d, cost=0.0, calls=0, real_calls=0, cache_hits=0, in_tok=0, out_tok=0)
        for it in items:
            em = it["engine_model"]["S"]
            eng, mod = (em.split("|", 1) + ["?"])[:2] if "|" in em else (em, "?")
            cost, calls = _n(it, "cost_usd"), _n(it, "calls")
            rc, ch = _n(it, "real_calls"), _n(it, "cache_hits")
            itok, otok = _n(it, "in_tok"), _n(it, "out_tok")
            dc["cost"] += cost; dc["calls"] += calls; dc["real_calls"] += rc
            dc["cache_hits"] += ch; dc["in_tok"] += itok; dc["out_tok"] += otok
            if idx == 0:  # today -> per-engine + per-model breakdowns
                e = per_engine.setdefault(eng, dict(engine=eng, cost=0.0, calls=0, real_calls=0, cache_hits=0))
                e["cost"] += cost; e["calls"] += calls; e["real_calls"] += rc; e["cache_hits"] += ch
                m = per_model.setdefault(mod, dict(model=mod, cost=0.0, calls=0, in_tok=0, out_tok=0))
                m["cost"] += cost; m["calls"] += calls; m["in_tok"] += itok; m["out_tok"] += otok
        dc["cache_hit_rate"] = round(100 * dc["cache_hits"] / dc["calls"], 1) if dc["calls"] else 0.0
        dc["cost"] = round(dc["cost"], 4)
        per_day.append(dc)

    tot_real = sum(x["real_calls"] for x in per_day)
    tot_cost = sum(x["cost"] for x in per_day)
    tot_hits = sum(x["cache_hits"] for x in per_day)
    avg_real = (tot_cost / tot_real) if tot_real else 0.0
    est_savings = avg_real * tot_hits

    budget = float(_ssm("/justhodl/llm/daily-budget-usd", "50") or 50)
    mode = _ssm("/justhodl/llm/mode", "normal")
    today_c = per_day[0]

    for e in per_engine.values():
        e["cache_hit_rate"] = round(100 * e["cache_hits"] / e["calls"], 1) if e["calls"] else 0.0
        e["cost"] = round(e["cost"], 4)
    for m in per_model.values():
        m["cost"] = round(m["cost"], 4)

    out = dict(
        generated_at=now.isoformat(),
        mode=mode,
        daily_budget_usd=budget,
        today=today_c,
        budget_used_pct=round(100 * today_c["cost"] / budget, 1) if budget else 0.0,
        per_day=list(reversed(per_day)),
        per_engine=sorted(per_engine.values(), key=lambda x: -x["cost"]),
        per_model=sorted(per_model.values(), key=lambda x: -x["cost"]),
        trailing_14d=dict(
            cost=round(tot_cost, 4), real_calls=int(tot_real), cache_hits=int(tot_hits),
            cache_hit_rate=round(100 * tot_hits / (tot_real + tot_hits), 1) if (tot_real + tot_hits) else 0.0,
            est_cache_savings_usd=round(est_savings, 4)),
    )
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=60")
    return {"ok": True, "today_cost": today_c["cost"], "engines": len(out["per_engine"]),
            "cache_hit_rate_14d": out["trailing_14d"]["cache_hit_rate"]}


def lambda_handler(event=None, context=None):
    return handler(event, context)
