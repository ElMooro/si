"""
justhodl-money-flow-state — whole-market DOLLAR money-flow by stock, industry, and sector.

One Polygon grouped-daily call returns every US ticker's price+volume for a day. Pull a handful
of trading days, keep the names in universe.json (each tagged sector+industry), and compute a
transparent dollar money-flow per stock:

    flow_usd = (N-day return) x (avg daily dollar-volume)     # signed dollar pressure
    up_dvol / down_dvol = dollar volume on up days vs down days

Roll those up to 144 industries and 11 sectors. Emits a single feed:
    s3://justhodl-dashboard-live/data/money-flow-state.json
with stocks_in/out, industries_in/out, and sector net flow — the stock+industry layer the
sector-flow page and downstream engines were missing.
"""
import json
import boto3
import urllib.request
from datetime import datetime, timezone, date, timedelta

S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
N_DAYS = 6


def grouped(d):
    url = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{d}"
           f"?adjusted=true&apiKey={POLY}")
    try:
        j = json.loads(urllib.request.urlopen(url, timeout=45).read())
        return j.get("results") or []
    except Exception as e:
        print(f"[grouped] {d}: {str(e)[:50]}")
        return []


def lambda_handler(event, context):
    uni = json.loads(S3.get_object(Bucket=BUCKET, Key="data/universe.json")["Body"].read())
    smap = {s["symbol"]: {"sector": s.get("sector"), "industry": s.get("industry"), "name": s.get("name")}
            for s in uni.get("stocks", []) if s.get("symbol")}

    days, d, tries = [], date.today(), 0
    while len(days) < N_DAYS and tries < 14:
        d = d - timedelta(days=1)
        tries += 1
        if d.weekday() >= 5:
            continue
        res = grouped(d.isoformat())
        if res:
            days.append((d.isoformat(), res))
    days.reverse()
    if len(days) < 3:
        return {"ok": False, "reason": "insufficient grouped data", "got": len(days)}

    series = {}
    for dt, res in days:
        for r in res:
            t = r.get("T")
            if t in smap and r.get("c"):
                dvol = (r.get("vw") or r.get("c")) * (r.get("v") or 0)
                series.setdefault(t, []).append((r["c"], dvol))

    rows = []
    for sym, ser in series.items():
        if len(ser) < 3:
            continue
        c_now, c_old = ser[-1][0], ser[0][0]
        if not c_old:
            continue
        ret = c_now / c_old - 1
        dvols = [x[1] for x in ser]
        avg_dvol = sum(dvols) / len(dvols)
        up = down = 0.0
        for i in range(1, len(ser)):
            if ser[i][0] >= ser[i - 1][0]:
                up += ser[i][1]
            else:
                down += ser[i][1]
        m = smap[sym]
        rows.append({"ticker": sym, "name": m["name"], "sector": m["sector"], "industry": m["industry"],
                     "ret_pct": round(ret * 100, 2), "avg_dvol_usd": round(avg_dvol),
                     "flow_usd": round(ret * avg_dvol), "up_dvol": round(up), "down_dvol": round(down)})
    rows.sort(key=lambda x: -x["flow_usd"])

    def rollup(key):
        agg = {}
        for r in rows:
            k = r.get(key)
            if not k:
                continue
            a = agg.setdefault(k, {"net_flow_usd": 0.0, "up_dvol": 0.0, "down_dvol": 0.0, "n": 0, "sector": r.get("sector")})
            a["net_flow_usd"] += r["flow_usd"]
            a["up_dvol"] += r["up_dvol"]
            a["down_dvol"] += r["down_dvol"]
            a["n"] += 1
        out = [{key: k, "net_flow_usd": round(v["net_flow_usd"]), "up_dvol": round(v["up_dvol"]),
                "down_dvol": round(v["down_dvol"]), "n": v["n"], "sector": v["sector"]} for k, v in agg.items()]
        out.sort(key=lambda x: -x["net_flow_usd"])
        return out

    industries, sectors = rollup("industry"), rollup("sector")

    inst_sector = {}
    try:
        inst = json.loads(S3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")["Body"].read())
    except Exception:
        inst = {}
    for bucket in ("most_bought", "most_sold"):
        for x in (inst.get(bucket) or []):
            tk = x.get("ticker")
            if not tk or tk not in smap:
                continue
            sc = smap[tk].get("sector")
            if not sc:
                continue
            a = inst_sector.setdefault(sc, {"sector": sc, "net_fund_actions": 0, "adding": 0, "trimming": 0})
            a["adding"] += x.get("n_funds_adding") or 0
            a["trimming"] += x.get("n_funds_trimming") or 0
            a["net_fund_actions"] += (x.get("n_funds_adding") or 0) - (x.get("n_funds_trimming") or 0)
    inst_tilt = sorted(inst_sector.values(), key=lambda x: -x["net_fund_actions"])
    doc = {
        "engine": "justhodl-money-flow-state", "version": "1.1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of": days[-1][0], "n_days": len(days), "n_stocks": len(rows),
        "methodology": ("Per-stock dollar money-flow = N-day return x avg daily dollar-volume (signed); "
                        "up/down dvol = dollar volume on up vs down days. Polygon grouped-daily x universe "
                        "sector/industry map. Rolled up to industry and sector. Institutional sector tilt = "
                        "13F funds adding minus trimming, rolled to sector."),
        "stocks_in": rows[:25], "stocks_out": rows[-25:][::-1],
        "industries_in": industries[:15], "industries_out": industries[-15:][::-1],
        "sectors": sectors, "institutional_sector_tilt": inst_tilt,
    }
    S3.put_object(Bucket=BUCKET, Key="data/money-flow-state.json",
                  Body=json.dumps(doc).encode(), ContentType="application/json")
    print(f"money-flow: {len(rows)} stocks, {len(industries)} industries, {len(sectors)} sectors, as_of {days[-1][0]}")
    return {"ok": True, "n_stocks": len(rows), "n_industries": len(industries),
            "top_in": rows[0]["ticker"] if rows else None}
