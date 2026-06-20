"""
justhodl-dark-pool — PER-NAME DARK-POOL / OFF-EXCHANGE ACCUMULATION (FINRA ATS transparency)
═══════════════════════════════════════════════════════════════════════════════════════════
Institutions execute large blocks OFF the lit tape — in ATS dark pools and via
wholesaler/internalizer (non-ATS OTC) flow — to avoid moving price. A rising
off-exchange share of a name's volume, especially while price stays flat, is
quiet accumulation that often PRECEDES the move. FINRA publishes this free,
weekly, by security (~2-3wk lag). This is distinct from justhodl-dix (market-
level SqueezeMetrics DIX) — it is per-name.

DATA (FINRA OTC Transparency, free, no auth)
  api.finra.org/data/group/otcMarket/name/weeklySummary
    summaryTypeCode ATS_W_SMBL  → ATS (true dark-pool) weekly shares per symbol
    summaryTypeCode OTC_W_SMBL  → non-ATS off-exchange (wholesaler/internalizer) per symbol
  Polygon grouped daily aggs → total consolidated weekly volume (for the %).

PER NAME (latest reported week):
  • ats_shares, offex_shares (ATS+OTC), dark_pool_pct = ATS/total, offex_pct = offex/total
  • dark_accel = latest ATS vs trailing-4wk average (rising = building)
  • price action over the week → ACCUMULATION (dark rising + price flat/up) vs
    DISTRIBUTION (dark rising + price down) vs NEUTRAL
  • dark_accumulation_score 0-100

top_picks = ACCUMULATION names (quiet build, price not yet moved) → signal-harvester
(eng:dark-pool) MEASURE-BEFORE-TRUST forward-excess-vs-SPY grading. Also feeds
justhodl-ignition's P4 dark-share dimension (previously dead — FINRA filter bug).
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/dark-pool.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", region_name=REGION)
FINRA = "https://api.finra.org/data/group/otcMarket/name/weeklySummary"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
MIN_WEEKLY_VOL = 2_000_000   # liquidity gate (shares/week)


def finra_post(body):
    try:
        req = urllib.request.Request(FINRA, data=json.dumps(body).encode(),
                                     headers={**UA, "Content-Type": "application/json",
                                              "Accept": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=40) as r:
            j = json.loads(r.read())
            return j if isinstance(j, list) else (j.get("data") or j.get("results") or [])
    except Exception as e:
        print(f"[dark-pool] FINRA fail: {str(e)[:120]}")
        return []


def poly_get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=30) as r:
            return json.loads(r.read())
    except Exception:
        return None


def fetch_equity_set():
    """Polygon reference → set of real common stocks + ADRs (CS, ADRC). Excludes ETFs/ETNs/
    funds/warrants/units, which structurally have huge off-exchange % (not alpha-relevant)."""
    eq = set()
    for typ in ("CS", "ADRC"):
        url = (f"https://api.polygon.io/v3/reference/tickers?type={typ}&market=stocks"
               f"&active=true&limit=1000&apiKey={POLY}")
        for _ in range(8):
            j = poly_get(url)
            for r in (j or {}).get("results") or []:
                t = r.get("ticker")
                if t:
                    eq.add(t.upper())
            nxt = (j or {}).get("next_url")
            if not nxt:
                break
            url = nxt + f"&apiKey={POLY}"
    return eq


def fetch_offexchange(weeks_back=45):
    """Pull ATS_W_SMBL + OTC_W_SMBL for the last ~6 weeks; index {sym: {week: {ats, otc}}}."""
    end = date.today()
    start = end - timedelta(days=weeks_back)
    out = {}
    for code, key in (("ATS_W_SMBL", "ats"), ("OTC_W_SMBL", "otc")):
        offset = 0
        for _ in range(8):
            rows = finra_post({
                "limit": 5000, "offset": offset,
                "compareFilters": [{"fieldName": "summaryTypeCode", "compareType": "EQUAL", "fieldValue": code}],
                "dateRangeFilters": [{"fieldName": "weekStartDate", "startDate": start.isoformat(), "endDate": end.isoformat()}],
            })
            if not rows:
                break
            for r in rows:
                sym = (r.get("issueSymbolIdentifier") or "").upper().strip()
                wk = r.get("weekStartDate")
                qty = r.get("totalWeeklyShareQuantity")
                if not sym or not wk or qty is None:
                    continue
                d = out.setdefault(sym, {}).setdefault(wk, {"ats": 0.0, "otc": 0.0})
                d[key] += float(qty)
            if len(rows) < 5000:
                break
            offset += 5000
    return out


def week_trading_days(week_start):
    d0 = date.fromisoformat(week_start)
    return [(d0 + timedelta(days=i)).isoformat() for i in range(5)]  # Mon-Fri


def fetch_total_volume(days):
    """Polygon grouped daily → {sym: total volume} summed across the week's trading days."""
    vol = {}
    closes = {}
    for ds in days:
        j = poly_get(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{ds}?adjusted=true&apiKey={POLY}")
        for r in (j or {}).get("results") or []:
            t = r.get("T")
            if not t:
                continue
            vol[t] = vol.get(t, 0.0) + (r.get("v") or 0.0)
            closes.setdefault(t, []).append((ds, r.get("c")))
    return vol, closes


def lambda_handler(event=None, context=None):
    t0 = time.time()
    offex = fetch_offexchange()
    if not offex:
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps({"engine": "justhodl-dark-pool", "ok": False,
                                       "error": "FINRA weeklySummary empty",
                                       "generated_at": datetime.now(timezone.utc).isoformat()}).encode(),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False})}

    all_weeks = sorted({w for sym in offex for w in offex[sym]})
    latest_week = all_weeks[-1]
    prior_weeks = all_weeks[-5:-1]   # up to 4 prior weeks
    vol, closes = fetch_total_volume(week_trading_days(latest_week))
    equity_set = fetch_equity_set()   # real common stocks + ADRs only (drop ETFs/funds)
    print(f"[dark-pool] equity_set={len(equity_set)} symbols")

    rows = []
    for sym, wk in offex.items():
        if equity_set and sym not in equity_set:
            continue   # exclude ETFs/ETNs/funds — structural off-exchange %, not alpha
        lw = wk.get(latest_week)
        if not lw:
            continue
        ats = lw["ats"]; otc = lw["otc"]; off = ats + otc
        tvol = vol.get(sym)
        if not tvol or tvol < MIN_WEEKLY_VOL:
            continue
        # trailing ATS average (prior weeks present)
        prior_ats = [wk[w]["ats"] for w in prior_weeks if w in wk]
        avg_prior = sum(prior_ats) / len(prior_ats) if prior_ats else None
        dark_accel = round(ats / avg_prior - 1, 3) if (avg_prior and avg_prior > 0) else None
        dark_pct = round(ats / tvol * 100, 2)
        offex_pct = round(off / tvol * 100, 2)
        # price action over the week
        cl = sorted(closes.get(sym, []))
        wk_ret = None
        if len(cl) >= 2 and cl[0][1]:
            wk_ret = round((cl[-1][1] / cl[0][1] - 1) * 100, 2)
        # classify
        rising = dark_accel is not None and dark_accel > 0.15
        if rising and wk_ret is not None and wk_ret >= -1.0:
            state = "ACCUMULATION"
        elif rising and wk_ret is not None and wk_ret < -1.0:
            state = "DISTRIBUTION"
        else:
            state = "NEUTRAL"
        # score 0-100
        score = 0.0
        score += 30 * min(1.0, dark_pct / 40)              # dark-pool share of volume
        score += 25 * min(1.0, offex_pct / 65)             # total off-exchange share
        if dark_accel is not None:
            score += 30 * min(1.0, max(0.0, dark_accel / 0.6))   # acceleration
        if wk_ret is not None:
            score += 15 * (1.0 if -1.0 <= wk_ret <= 6.0 else 0.3)  # quiet (not yet moved / mild up)
        score = round(score, 1)
        rows.append({"ticker": sym, "state": state, "score": score,
                     "dark_pool_pct": dark_pct, "offex_pct": offex_pct,
                     "dark_accel": dark_accel, "week_return_pct": wk_ret,
                     "ats_shares_wk": int(ats), "offex_shares_wk": int(off),
                     "total_vol_wk": int(tvol)})
    rows.sort(key=lambda r: r["score"], reverse=True)

    accumulation = [r for r in rows if r["state"] == "ACCUMULATION"]
    distribution = [r for r in rows if r["state"] == "DISTRIBUTION"]
    top_picks = [{"ticker": r["ticker"], "score": r["score"], "direction": "long",
                  "state": r["state"], "dark_pool_pct": r["dark_pool_pct"],
                  "dark_accel": r["dark_accel"], "offex_pct": r["offex_pct"],
                  "week_return_pct": r["week_return_pct"]}
                 for r in accumulation
                 if r["score"] >= 45
                 and r["week_return_pct"] is not None and -2.0 <= r["week_return_pct"] <= 8.0][:20]

    # ── dark map for justhodl-ignition (fixes its dead P4 dark-share dimension) ──
    # {ticker: latest-week ATS shares} so ignition can compute dark_to_adv.
    dark_map = {r["ticker"]: r["ats_shares_wk"] for r in rows}

    payload = {
        "engine": "justhodl-dark-pool", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Per-name off-exchange accumulation from FINRA ATS transparency. Rising "
                   "dark-pool share of volume while price stays flat = quiet institutional "
                   "build that often precedes the move. Distinct from market-level DIX."),
        "latest_week": latest_week, "n_scored": len(rows),
        "distribution": {"accumulation": len(accumulation), "distribution": len(distribution)},
        "board": rows[:60],
        "top_picks": top_picks,
        "top_accumulation": accumulation[:20],
        "top_distribution": distribution[:12],
        "dark_map": dark_map,
        "data_source": "FINRA OTC Transparency weeklySummary (ATS+OTC) + Polygon grouped daily volume",
        "caveats": [
            "FINRA ATS/OTC transparency lags ~2-3 weeks (Tier 1 NMS weekly); this is a "
            "positioning/accumulation read, not intraday.",
            "Off-exchange % rising + price flat = accumulation inference; pairing with price "
            "disambiguates accumulation from off-exchange distribution.",
            "MEASURE-BEFORE-TRUST: top_picks → signal-harvester (eng:dark-pool) graded forward "
            "vs SPY; NOT in decision engines until alpha-proven. Also feeds ignition P4.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[dark-pool] week={latest_week} scored={len(rows)} accum={len(accumulation)} "
          f"picks={len(top_picks)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "latest_week": latest_week, "n_scored": len(rows),
        "n_accumulation": len(accumulation), "n_picks": len(top_picks),
        "top": [(r["ticker"], r["score"], r["state"], r["dark_pool_pct"], r["dark_accel"]) for r in rows[:8]]})}
