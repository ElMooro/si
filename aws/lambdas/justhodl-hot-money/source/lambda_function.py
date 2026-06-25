"""
justhodl-hot-money  ·  v1.0  —  CROSS-BORDER HOT-MONEY TRACKER
================================================================================
"Hot money" = short-term speculative capital that moves across borders chasing
returns. This engine answers three questions, in order:

  1. WHICH COUNTRY is hot money flowing into (worldwide)?
  2. WHICH SECTORS in that country is it going into?
  3. WHICH STOCKS in those sectors?

Layer 1 — country leaderboard. ~40 single-country / regional ETFs → countries.
Per country, a composite hot_money_score from FOUR corroborating signals (each
z-scored across the universe):
    • real ETF flow   — creation/redemption Δshares×price (the genuine tell), 0.40
    • relative momentum — country ETF 20d return minus the world (ACWX),       0.30
    • volume surge     — 5d vs 60d average volume,                              0.15
    • currency strength — the local FX vs USD (inflows bid up the currency),    0.15
Weights renormalize when a signal is missing. Countries with multiple ETFs
(China = FXI/MCHI/KWEB/ASHR) aggregate to one country score.

Layer 2/3 — for the top inflow countries, FMP ETF sector-weightings give the
sector map and ETF holdings give the names, ranked by weight + recent momentum
(where US-priced). Top inflow-country ETFs log to the scorecard, graded on
forward excess-vs-ACWX — measure-before-trust, same as everything else.
"""
import os, json, time, math, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/hot-money.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
VERSION = "1.0.0"
WORLD = "ACWX"          # world ex-US benchmark for relative momentum
N_DRILL = 6             # how many top inflow countries to drill into sectors/stocks

# ETF -> (country, region). One country can have several ETFs.
ETF_COUNTRY = {
    "EWZ": ("Brazil", "LatAm"), "EWW": ("Mexico", "LatAm"), "ARGT": ("Argentina", "LatAm"),
    "ECH": ("Chile", "LatAm"), "GXG": ("Colombia", "LatAm"), "ILF": ("LatAm", "LatAm"),
    "INDA": ("India", "Asia"), "INDY": ("India", "Asia"), "FXI": ("China", "Asia"),
    "MCHI": ("China", "Asia"), "KWEB": ("China", "Asia"), "ASHR": ("China", "Asia"),
    "EWH": ("Hong Kong", "Asia"), "EWT": ("Taiwan", "Asia"), "EWY": ("South Korea", "Asia"),
    "EWJ": ("Japan", "Asia"), "EWS": ("Singapore", "Asia"), "THD": ("Thailand", "Asia"),
    "EIDO": ("Indonesia", "Asia"), "EPHE": ("Philippines", "Asia"), "VNM": ("Vietnam", "Asia"),
    "EWM": ("Malaysia", "Asia"), "AAXJ": ("Asia ex-Japan", "Asia"),
    "EWG": ("Germany", "Europe"), "EWU": ("UK", "Europe"), "EWQ": ("France", "Europe"),
    "EWI": ("Italy", "Europe"), "EWP": ("Spain", "Europe"), "EWL": ("Switzerland", "Europe"),
    "EWN": ("Netherlands", "Europe"), "EWD": ("Sweden", "Europe"), "EPOL": ("Poland", "Europe"),
    "GREK": ("Greece", "Europe"), "TUR": ("Turkey", "Europe"),
    "EZA": ("South Africa", "MEA"), "EGPT": ("Egypt", "MEA"), "KSA": ("Saudi Arabia", "MEA"),
    "EIS": ("Israel", "MEA"),
    "EWA": ("Australia", "Oceania"), "EWC": ("Canada", "N.America"),
    "EEM": ("EM broad", "Global"), "VWO": ("EM broad", "Global"), "EFA": ("DM ex-US", "Global"),
}
# country -> (fx-regime pair, sign)  sign=+1 if a positive pair-return = local FX strengthening
COUNTRY_FX = {
    "Brazil": ("USD_BRL", -1), "Mexico": ("USD_MXN", -1), "India": ("USD_INR", -1),
    "China": ("USD_CNH", -1), "South Korea": ("USD_KRW", -1), "Japan": ("USD_JPY", -1),
    "South Africa": ("USD_ZAR", -1), "Turkey": ("USD_TRY", -1), "Canada": ("USD_CAD", -1),
    "Australia": ("AUD_USD", 1), "UK": ("GBP_USD", 1), "Switzerland": ("USD_CHF", -1),
    "Germany": ("EUR_USD", 1), "France": ("EUR_USD", 1), "Italy": ("EUR_USD", 1),
    "Spain": ("EUR_USD", 1), "Netherlands": ("EUR_USD", 1),
}


def _get(url, timeout=30):
    try:
        return json.loads(urllib.request.urlopen(url, timeout=timeout).read())
    except Exception as e:
        return {"_err": str(e)[:60]}


def aggs(tk, days=80):
    frm = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
    to = datetime.now(timezone.utc).date().isoformat()
    j = _get(f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/{frm}/{to}"
             f"?adjusted=true&sort=asc&limit=200&apiKey={POLY}")
    return j.get("results") or []


def fund_flow(tk, days=30):
    """Real creation/redemption flow from Polygon's ETF Global feed (the $99/mo edge)."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days + 10)
    j = _get(f"https://api.polygon.io/etf-global/v1/fund-flows?composite_ticker={tk}"
             f"&processed_date.gte={start.isoformat()}&processed_date.lte={end.isoformat()}"
             f"&order=desc&sort=processed_date&limit=40&apiKey={POLY}")
    res = j.get("results") or []
    if not res:
        return {}
    flows = [r.get("fund_flow") for r in res if r.get("fund_flow") is not None]
    latest = res[0]
    nav, sh = latest.get("nav"), latest.get("shares_outstanding")
    aum = (nav * sh) if (nav and sh) else None
    return {"flow_5d_usd": (sum(flows[:5]) if flows else None),
            "flow_21d_usd": (sum(flows[:21]) if flows else None), "aum_usd": aum}


def _ret(closes, n):
    if len(closes) <= n:
        return None
    a, b = closes[-1 - n], closes[-1]
    return (b / a - 1) * 100 if a else None


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _z(vals):
    xs = [v for v in vals if v is not None]
    if len(xs) < 3:
        return {i: 0.0 for i in range(len(vals))}
    m = sum(xs) / len(xs)
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) or 1.0
    return {i: ((v - m) / sd if v is not None else 0.0) for i, v in enumerate(vals)}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    flows = (_read("data/etf-true-flows.json").get("by_etf")) or {}
    fx = (_read("data/polygon-fx-regime.json").get("pair_data")) or {}

    # world benchmark momentum
    wclose = [r["c"] for r in aggs(WORLD)]
    world_20 = _ret(wclose, 20) or 0.0

    # per-ETF metrics
    rows = []
    for tk, (country, region) in ETF_COUNTRY.items():
        if time.time() - t0 > 480:
            break
        res = aggs(tk)
        if len(res) < 25:
            continue
        closes = [r["c"] for r in res]
        vols = [r.get("v") or 0 for r in res]
        r20 = _ret(closes, 20)
        vol5 = sum(vols[-5:]) / 5 if len(vols) >= 5 else 0
        vol60 = sum(vols[-60:]) / min(60, len(vols)) if vols else 0
        vsurge = (vol5 / vol60) if vol60 else None
        ff = fund_flow(tk)                              # real creation/redemption flow
        nf5 = ff.get("flow_5d_usd")
        aum = ff.get("aum_usd")
        flow_norm = (nf5 / aum * 100) if (nf5 is not None and aum) else None
        if flow_norm is None:                           # fallback: FMP Δshares×price
            fl = flows.get(tk) or {}
            nf5 = fl.get("net_flow_5d_usd")
            a2 = (fl.get("aum_est_b") or 0) * 1e9
            flow_norm = (nf5 / a2 * 100) if (nf5 is not None and a2) else fl.get("shares_chg_5d_pct")
        rows.append({"etf": tk, "country": country, "region": region,
                     "price": round(closes[-1], 2), "ret_5d": _ret(closes, 5), "ret_20d": r20,
                     "rel_mom": (r20 - world_20) if r20 is not None else None,
                     "vol_surge": round(vsurge, 2) if vsurge else None,
                     "flow_5d_usd": nf5, "flow_norm_pct": flow_norm})

    # z-score components across ETFs
    zflow = _z([r["flow_norm_pct"] for r in rows])
    zmom = _z([r["rel_mom"] for r in rows])
    zvol = _z([r["vol_surge"] for r in rows])
    for i, r in enumerate(rows):
        # fx strength for the country
        fxz = None
        cf = COUNTRY_FX.get(r["country"])
        if cf and cf[0] in fx:
            ret20 = fx[cf[0]].get("return_20d_pct")
            if ret20 is not None:
                fxz = ret20 * cf[1] / 3.0     # scale ~ vol; sign-adjusted
        have_flow = r["flow_norm_pct"] is not None
        w_flow, w_mom, w_vol, w_fx = 0.40, 0.30, 0.15, 0.15
        if not have_flow:
            w_mom += w_flow * 0.6; w_vol += w_flow * 0.4; w_flow = 0.0
        if fxz is None:
            w_mom += w_fx * 0.7; w_vol += w_fx * 0.3; w_fx = 0.0
        r["score"] = round(w_flow * zflow[i] + w_mom * zmom[i] + w_vol * zvol[i]
                           + w_fx * (fxz or 0.0), 3)
        r["fx_strength"] = round(fxz, 2) if fxz is not None else None

    # aggregate to country
    countries = {}
    for r in rows:
        c = countries.setdefault(r["country"], {"country": r["country"], "region": r["region"],
                                                 "etfs": [], "scores": [], "rel_moms": [], "flows": []})
        c["etfs"].append(r["etf"]); c["scores"].append(r["score"])
        if r["rel_mom"] is not None:
            c["rel_moms"].append(r["rel_mom"])
        if r["flow_5d_usd"] is not None:
            c["flows"].append(r["flow_5d_usd"])
    clist = []
    for c in countries.values():
        c["hot_money_score"] = round(sum(c["scores"]) / len(c["scores"]), 3)
        c["rel_mom_20d"] = round(sum(c["rel_moms"]) / len(c["rel_moms"]), 2) if c["rel_moms"] else None
        c["net_flow_5d_usd"] = round(sum(c["flows"])) if c["flows"] else None
        c.pop("scores"); c.pop("rel_moms"); c.pop("flows")
        clist.append(c)
    clist.sort(key=lambda x: x["hot_money_score"], reverse=True)
    for rank, c in enumerate(clist, 1):
        c["rank"] = rank

    inflow = [c for c in clist if c["hot_money_score"] > 0 and c["country"] not in ("EM broad", "DM ex-US")]
    outflow = [c for c in clist if c["hot_money_score"] < 0 and c["country"] not in ("EM broad", "DM ex-US")]

    # ── Layer 2/3: drill the top inflow countries into sectors + stocks ──
    primary_etf = {}
    for tk, (country, _r) in ETF_COUNTRY.items():
        primary_etf.setdefault(country, tk)        # first listed = primary
    drill = {}
    for c in inflow[:N_DRILL]:
        if time.time() - t0 > 700:
            break
        etf = primary_etf.get(c["country"])
        if not etf:
            continue
        secs = _get(f"https://financialmodelingprep.com/stable/etf/sector-weightings?symbol={etf}&apikey={FMP}")
        holds = _get(f"https://financialmodelingprep.com/stable/etf/holdings?symbol={etf}&apikey={FMP}")
        sectors = sorted([{"sector": s.get("sector"), "weight_pct": round(float(s.get("weightPercentage") or 0), 1)}
                          for s in secs if isinstance(s, dict)],
                         key=lambda x: -x["weight_pct"])[:6] if isinstance(secs, list) else []
        names = []
        if isinstance(holds, list):
            for h in sorted(holds, key=lambda x: -float(x.get("weightPercentage") or 0))[:15]:
                names.append({"ticker": h.get("asset"), "name": (h.get("name") or "")[:30],
                              "weight_pct": round(float(h.get("weightPercentage") or 0), 2)})
        # momentum for US-priced holdings via one batched snapshot
        us = [n["ticker"] for n in names if n["ticker"] and n["ticker"].isalpha() and len(n["ticker"]) <= 5]
        if us:
            snap = _get("https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
                        f"?tickers={','.join(us)}&apiKey={POLY}")
            chg = {t["ticker"]: t.get("todaysChangePerc") for t in (snap.get("tickers") or [])} if isinstance(snap, dict) else {}
            for n in names:
                if n["ticker"] in chg:
                    n["day_chg_pct"] = round(chg[n["ticker"]], 2)
        drill[c["country"]] = {"etf": etf, "hot_money_score": c["hot_money_score"],
                               "rel_mom_20d": c["rel_mom_20d"], "net_flow_5d_usd": c["net_flow_5d_usd"],
                               "top_sectors": sectors, "top_holdings": names}

    out = {"engine": "hot-money", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1), "world_benchmark": WORLD,
           "world_ret_20d_pct": round(world_20, 2), "n_countries": len(clist),
           "thesis": ("Cross-border hot money funnel: real ETF creation/redemption flow + relative momentum vs "
                      "the world + volume surge + currency strength rank which countries capital is rotating "
                      "INTO, then ETF holdings drill into the sectors and stocks inside the hot countries."),
           "method": "composite z-score (flow 0.40 / rel-mom 0.30 / vol 0.15 / FX 0.15), weights renormalize when a signal is missing",
           "inflow_leaders": inflow[:15], "outflow_leaders": list(reversed(outflow))[:15],
           "all_countries": clist, "drilldowns": drill}

    # closed loop — log top inflow-country ETFs, graded on forward excess-vs-ACWX
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        for c in inflow[:6]:
            etf = primary_etf.get(c["country"])
            pr = next((r["price"] for r in rows if r["etf"] == etf), None)
            if not pr:
                continue
            tbl.put_item(Item={
                "signal_id": f"hotmoney-UP#{etf}#{nowt.date().isoformat()}",
                "signal_type": "hot_money_inflow", "predicted_direction": "UP",
                "signal_value": str(c["hot_money_score"]), "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_acwx", "baseline_price": str(pr), "benchmark": WORLD,
                "check_windows": ["day_5", "day_21", "day_63"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 21, "schema_version": "2",
                "ttl": int(nowt.timestamp()) + 120 * 86400,
                "metadata": {"engine": "hot-money", "v": VERSION, "country": c["country"],
                             "score": str(c["hot_money_score"])},
                "rationale": f"Hot money inflow #{c['rank']} {c['country']} ({etf}) score {c['hot_money_score']}"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:70]}")

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    top = ", ".join(f"{c['country']}({c['hot_money_score']})" for c in inflow[:5])
    print(f"[hot-money] countries={len(clist)} drill={len(drill)} top-inflow: {top} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"n_countries": len(clist), "drill": len(drill)})}
