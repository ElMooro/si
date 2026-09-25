"""
justhodl-squeeze-fuel — PER-NAME SHORT-SQUEEZE FUEL GAUGE (100% free data)
═════════════════════════════════════════════════════════════════════════════
A boom needs an IGNITION (earnings beat, guidance raise, analyst re-rating —
covered by other engines) AND FUEL (trapped short capital that MUST buy back if
the stock moves). This engine measures the fuel, per name, from authoritative
FREE sources — no Ortex/S3-Partners subscription required.

DATA SOURCES (all free, real, auto-updating)
────────────────────────────────────────────
  1. FINRA Consolidated Short Interest  (OFFICIAL, bi-monthly settlement)
       api.finra.org/data/group/otcMarket/name/consolidatedShortInterest
       → currentShortPositionQuantity, previousShortPositionQuantity,
         daysToCoverQuantity, averageDailyVolumeQuantity, changePercent
  2. SEC Fails-to-Deliver  (CNS, semi-monthly, ~T+15)
       sec.gov/files/data/fails-deliver-data/cnsfails{YYYYMM}{a|b}.zip
       → per-symbol fail-to-deliver share balance (settlement pressure)
  3. justhodl-finra-short.json  (this platform, daily T+1 short VOLUME)
       → daily short-volume ratio, z-score, squeeze setup score (live "fuse")
  4. FMP /stable/shares-float  → floatShares (for true short % of float)
  5. FMP /stable/quote         → price vs 50d MA / 52w-high (squeeze confirm)

SQUEEZE-FUEL SCORE (0-100, transparent additive)
─────────────────────────────────────────────────
  crowding         (max 35)  short % of float (or days-to-cover if float n/a)
  exit_difficulty  (max 25)  days-to-cover
  building         (max 15)  SI change vs prior settlement (shorts piling in)
  ftd_pressure     (max 10)  fails-to-deliver vs float (mechanical buy-ins)
  ignition         (max 15)  daily short-volume z + price strength (live fuse)

  ≥70 LOADED · 55-69 BUILDING · 35-54 ELEVATED · <35 LOW
  top_picks = score ≥ 55 AND price confirmation (px > 50d MA OR ≥85% of 52w-high)
              → logged to signal-harvester (eng:squeeze-fuel) for forward
                excess-vs-SPY grading. MEASURE-BEFORE-TRUST: NOT wired into any
                decision engine until the scorecard proves net-of-cost alpha.
"""
import json
import os
import time
import io
import zipfile
import re
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
from donor_contract import inspect_donor, numeric, parse_timestamp
from equity_donor_inputs import safe_evidence, load_inputs, short_context

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/squeeze-fuel.json"
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
S3 = boto3.client("s3", region_name=REGION)

UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
FINRA_URL = "https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
SEC_INDEX = "https://www.sec.gov/data/foiadocsfailsdatahtm"
HTTP_TIMEOUT = 45

# Core watchlist always scored (mega/large-cap + perennial squeeze names)
CORE_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMD", "MU", "AMZN", "META", "GOOGL", "NFLX",
    "GME", "AMC", "CVNA", "UPST", "AFRM", "PLTR", "SOFI", "RIVN", "LCID", "HOOD",
    "COIN", "MARA", "RIOT", "CLSK", "BYND", "CHWY", "DKNG", "RBLX", "SNAP", "PINS",
    "W", "ETSY", "FUBO", "SMCI", "ARM", "DELL", "INTC", "ON", "WOLF", "ENPH",
    "FSLR", "RUN", "PLUG", "BE", "CHPT", "QS", "ASTS", "RKLB", "IONQ", "RGTI",
]


def _http(url, data=None, headers=None, timeout=HTTP_TIMEOUT, raw=False):
    h = dict(UA)
    if headers:
        h.update(headers)
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=h,
                                 method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
        return b if raw else b.decode("utf-8", "replace")


def _safe(fn, label):
    try:
        return fn()
    except Exception as e:
        print(f"[squeeze-fuel] {label} failed: {str(e)[:140]}")
        return None


# ── 1. FINRA consolidated short interest (full latest-settlement snapshot) ───
def fetch_finra_si():
    """Paginate the latest bi-monthly settlement; index by symbol."""
    end = date.today()
    start = end - timedelta(days=30)
    out = {}
    latest_settlement = None
    offset = 0
    for _ in range(6):  # up to 30k rows
        body = {
            "limit": 5000, "offset": offset,
            "dateRangeFilters": [{"fieldName": "settlementDate",
                                  "startDate": start.isoformat(),
                                  "endDate": end.isoformat()}],
        }
        resp = _http(FINRA_URL, data=body,
                     headers={"Content-Type": "application/json",
                              "Accept": "application/json"})
        rows = json.loads(resp)
        if isinstance(rows, dict):
            rows = rows.get("data") or rows.get("results") or []
        if not rows:
            break
        for r in rows:
            sym = (r.get("symbolCode") or "").upper().strip()
            sd = r.get("settlementDate")
            if not sym:
                continue
            if latest_settlement is None or (sd and sd > latest_settlement):
                latest_settlement = sd
            cur = r.get("currentShortPositionQuantity")
            adv = r.get("averageDailyVolumeQuantity")
            dtc = r.get("daysToCoverQuantity")
            # ── LIQUIDITY / QUALITY GATE ──
            # Raw FINRA SI spans ~22k names, most illiquid OTC/foreign where DTC
            # and %-change are near-zero-volume artifacts (DTC sentinel 999.99,
            # huge % on tiny base). Real squeeze candidates are liquid + materially
            # shorted. Gate: ADV ≥ 500k shares, SI ≥ 1M shares, sane DTC ≤ 40.
            try:
                if not (adv and float(adv) >= 500_000):
                    continue
                if not (cur and float(cur) >= 1_000_000):
                    continue
                if dtc is None or not (0.3 <= float(dtc) <= 40):
                    continue
            except (TypeError, ValueError):
                continue
            prev = out.get(sym)
            if prev is None or (sd and sd >= prev.get("settlementDate", "")):
                out[sym] = {
                    "settlementDate": sd,
                    "short_interest": cur,
                    "prev_short_interest": r.get("previousShortPositionQuantity"),
                    "days_to_cover": dtc,
                    "avg_daily_vol": adv,
                    "si_change_pct": r.get("changePercent"),
                    "exchange": r.get("marketClassCode"),
                    "name": r.get("issueName"),
                }
        if len(rows) < 5000:
            break
        offset += 5000
    print(f"[squeeze-fuel] FINRA SI: {len(out)} symbols, settlement {latest_settlement}")
    return out, latest_settlement


# ── 2. SEC fails-to-deliver (latest semi-monthly file) ──────────────────────
def fetch_sec_ftd():
    """Latest CNS fails file → per-symbol max fail balance + last value."""
    html = _http(SEC_INDEX, timeout=40)
    links = re.findall(r'href=["\']([^"\']*cnsfails[^"\']*\.zip)["\']', html)
    if not links:
        print("[squeeze-fuel] SEC FTD: no links found")
        return {}, None
    url = links[0]
    if url.startswith("/"):
        url = "https://www.sec.gov" + url
    raw = _http(url, timeout=60, raw=True)
    zf = zipfile.ZipFile(io.BytesIO(raw))
    txt = zf.read(zf.namelist()[0]).decode("utf-8", "replace")
    fname = url.split("/")[-1]
    out = {}
    lines = txt.splitlines()
    for ln in lines[1:]:
        p = ln.split("|")
        if len(p) < 4:
            continue
        sym = (p[2] or "").upper().strip()
        try:
            qty = float(p[3])
            sd = p[0]
        except (ValueError, IndexError):
            continue
        if not sym:
            continue
        rec = out.get(sym)
        if rec is None:
            out[sym] = {"max_fails": qty, "last_fails": qty, "last_date": sd, "days_present": 1}
        else:
            rec["max_fails"] = max(rec["max_fails"], qty)
            if sd >= rec["last_date"]:
                rec["last_fails"] = qty
                rec["last_date"] = sd
            rec["days_present"] += 1
    print(f"[squeeze-fuel] SEC FTD: {len(out)} symbols from {fname}")
    return out, fname


# ── 3. existing daily short-volume engine output (the live fuse) ────────────
def fetch_daily_shortvol():
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/finra-short.json")["Body"].read())
    except Exception as e:
        print(f"[squeeze-fuel] finra-short.json unavailable: {e}")
        return {}
    tickers = d.get("tickers")
    idx = {}
    if isinstance(tickers, dict):
        items = tickers.items()
    elif isinstance(tickers, list):
        items = [((t.get("symbol") or t.get("ticker")), t) for t in tickers]
    else:
        items = []
    for sym, t in items:
        if not sym:
            continue
        idx[sym.upper()] = {
            "svr": t.get("svr") or t.get("short_volume_ratio") or t.get("svr_today"),
            "svr_z": t.get("svr_zscore") or t.get("zscore") or t.get("z"),
            "short_momentum": t.get("short_momentum") or t.get("momentum"),
            "daily_squeeze_score": t.get("squeeze_setup_score") or t.get("squeeze_score") or t.get("setup_score"),
        }
    print(f"[squeeze-fuel] daily short-vol: {len(idx)} symbols")
    return idx


# ── 4/5. FMP float + price (only for the scored candidate set) ──────────────
def fmp_float_and_price(sym):
    out = {"sym": sym}
    try:
        f = json.loads(_http(f"https://financialmodelingprep.com/stable/shares-float?symbol={sym}&apikey={FMP_KEY}", timeout=12))
        if isinstance(f, list) and f:
            out["float_shares"] = f[0].get("floatShares")
            out["float_as_of"] = f[0].get("date")
    except Exception:
        pass
    try:
        q = json.loads(_http(f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FMP_KEY}", timeout=12))
        if isinstance(q, list) and q:
            qq = q[0]
            out["price"] = qq.get("price")
            out["quote_timestamp"] = qq.get("timestamp")
            out["ma50"] = qq.get("priceAvg50")
            out["year_high"] = qq.get("yearHigh")
    except Exception:
        pass
    return out


def _clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def score_name(si, ftd, daily, enrich):
    """Transparent additive 0-100 squeeze-fuel score."""
    reasons = []
    cur_si = si.get("short_interest")
    dtc = si.get("days_to_cover")
    chg = si.get("si_change_pct")
    flt = numeric((enrich or {}).get("float_shares"))
    float_date = parse_timestamp((enrich or {}).get("float_as_of"))
    float_age = (datetime.now(timezone.utc) - float_date).total_seconds() / 86400 if float_date else None
    float_usable = flt is not None and flt > 0 and float_age is not None and 0 <= float_age <= 90
    if not float_usable: flt = None

    # crowding (max 35): short % of float, else days-to-cover proxy
    pct_float = None
    if cur_si and flt and flt > 0:
        pct_float = round(cur_si / flt * 100, 2)
        crowding = 35 * _clamp((pct_float - 2) / 18)  # 2%→0, 20%→35
        if pct_float >= 10:
            reasons.append(f"{pct_float}% of float short")
    elif dtc is not None:
        crowding = 35 * _clamp((dtc - 1) / 9) * 0.8   # discount: no true float
        if dtc >= 5:
            reasons.append(f"DTC {dtc}d (no float)")
    else:
        crowding = 0.0

    # exit difficulty (max 25): days to cover
    exit_diff = 25 * _clamp(((dtc or 0) - 1) / 9) if dtc is not None else 0.0
    if dtc and dtc >= 5:
        reasons.append(f"{dtc}d to cover")

    # building (max 15): shorts increasing vs prior settlement = loading the gun
    building = 0.0
    if isinstance(chg, (int, float)):
        if chg > 0:
            building = 15 * _clamp(chg / 25)  # +25% SI growth → full
            if chg >= 10:
                reasons.append(f"SI +{round(chg)}% vs prior")
        elif chg < -15:
            reasons.append(f"SI {round(chg)}% (covering)")

    # ftd pressure (max 10): fails vs float (mechanical forced buy-ins)
    ftd_p = 0.0
    if ftd:
        fails = ftd.get("max_fails") or 0
        if flt and flt > 0:
            ftd_ratio = fails / flt
            ftd_p = 10 * _clamp(ftd_ratio / 0.01)  # 1% of float in fails → full
        elif fails > 500_000:
            ftd_p = 10 * _clamp(fails / 5_000_000)
        if fails > 1_000_000:
            reasons.append(f"{round(fails/1e6,1)}M fails-to-deliver")

    # ignition (max 15): live daily short-volume z + price strength
    ignition = 0.0
    if daily:
        dz = daily.get("svr_z")
        dss = daily.get("daily_squeeze_score")
        if isinstance(dss, (int, float)):
            ignition = max(ignition, 15 * _clamp(dss / 100))
        if isinstance(dz, (int, float)) and dz >= 2:
            ignition = max(ignition, 15 * _clamp((dz - 1) / 3))
            reasons.append(f"daily short-vol z={round(dz,1)}")

    score = round(crowding + exit_diff + building + ftd_p + ignition, 1)

    # price confirmation (squeeze needs price moving up against shorts)
    px = (enrich or {}).get("price")
    ma50 = (enrich or {}).get("ma50")
    yh = (enrich or {}).get("year_high")
    price_confirm = False
    if px:
        if (ma50 and px > ma50) or (yh and yh > 0 and px >= 0.85 * yh):
            price_confirm = True

    if score >= 70:
        state = "LOADED"
    elif score >= 55:
        state = "BUILDING"
    elif score >= 35:
        state = "ELEVATED"
    else:
        state = "LOW"
    # covering overrides
    if isinstance(chg, (int, float)) and chg < -15 and not price_confirm:
        state = "COVERING"

    return {
        "score": score, "state": state, "price_confirm": price_confirm,
        "pct_of_float": pct_float, "days_to_cover": dtc,
        "short_interest": cur_si, "si_change_pct": chg,
        "float_evidence": {"source": "FMP shares-float", "as_of": (enrich or {}).get("float_as_of"),
                           "float_shares": numeric((enrich or {}).get("float_shares")), "unit": "shares",
                           "status": "DATED" if float_usable else "UNAVAILABLE_OR_STALE", "max_age_days": 90},
        "components": {"crowding": round(crowding, 1), "exit_difficulty": round(exit_diff, 1),
                       "building": round(building, 1), "ftd_pressure": round(ftd_p, 1),
                       "ignition": round(ignition, 1)},
        "reasons": reasons,
    }


def merge_short_interest(si_map, doc, now=None):
    """D27: newest dated position inventory replaces older direct FINRA evidence.

    latest_short_pct is deliberately excluded. The v2 measurement contract
    keeps daily flow, short float and dated FINRA inventory independently sourced.
    """
    now = now or datetime.now(timezone.utc)
    merged, evidence = dict(si_map), {}
    rows = doc.get("by_ticker", {}) if isinstance(doc, dict) else {}
    if not isinstance(rows, dict): return merged, evidence
    for ticker, source in rows.items():
        if not isinstance(source, dict): continue
        if ticker not in si_map and ticker not in CORE_WATCHLIST and (source.get("short_interest") is None or not source.get("settlement_date")): continue
        context = short_context(ticker, doc, now)
        receipt = inspect_donor({**source, "generated_at": doc.get("generated_at")},
            "data/short-interest.json", 72, observed_paths=("settlement_date",),
            required_paths=("short_interest", "days_to_cover", "short_interest_source", "short_interest_as_of", "days_to_cover_source", "days_to_cover_as_of"),
            units={"short_interest": "shares", "days_to_cover": "days", "si_change_pct": "percent"},
            max_observation_age_hours=35*24, now=now)
        shares, dtc, change = numeric(source.get("short_interest")), numeric(source.get("days_to_cover")), numeric(source.get("si_change_pct"))
        if (doc.get("version") != "1.1" or doc.get("measurement_contract") != "short-positioning.v2"
                or any((doc.get("measurement_units") if isinstance(doc.get("measurement_units"), dict) else {}).get(k) != unit for k, unit in
                       {"short_interest": "shares", "days_to_cover": "days", "si_change_pct": "percent"}.items())
                or source.get("short_interest_source") != "FINRA consolidated short interest"
                or source.get("days_to_cover_source") != "FINRA consolidated short interest"
                or parse_timestamp(source.get("short_interest_as_of")) != parse_timestamp(source.get("settlement_date"))
                or parse_timestamp(source.get("days_to_cover_as_of")) != parse_timestamp(source.get("settlement_date"))
                or source.get("ticker", ticker) != ticker
                or shares is None or shares < 0 or dtc is None or dtc < 0
                or (source.get("si_change_pct") is not None and change is None)):
            receipt.update(status="INVALID", usable=False); receipt["errors"].append("Invalid dated short-position fields")
        context.update(source_metadata=doc.get("data_sources"), health=receipt, positioning_usable=receipt["usable"], applied=False,
                       daily_short_volume_used=False, float_shares_used=False,
                       latest_short_pct_semantics="compatibility alias of daily short volume; excluded from inventory calculations")
        evidence[ticker] = context
        if not receipt["usable"] or shares < 1_000_000 or not 0.3 <= dtc <= 40: continue
        # DTC is SI divided by average volume; keep this inferred screening
        # quantity explicit. It is never treated as an executable liquidity quote.
        implied_adv = shares / dtc
        if implied_adv < 500_000: continue
        previous = merged.get(ticker) or {}
        if previous.get("settlementDate") and previous["settlementDate"] > source["settlement_date"]: continue
        merged[ticker] = {**previous, "settlementDate": source["settlement_date"],
                         "short_interest": shares, "days_to_cover": dtc, "si_change_pct": change,
                         "implied_avg_daily_vol": implied_adv, "source_artifact": "data/short-interest.json"}
        context["applied"] = True
    return merged, evidence


def lambda_handler(event, context):
    t0 = time.time()
    si_map, settlement = _safe(fetch_finra_si, "FINRA SI") or ({}, None)
    donor_docs, donor_receipts = load_inputs(S3, BUCKET, [("data/short-interest.json", 72, ("by_ticker",))])
    si_map, positioning = merge_short_interest(si_map, donor_docs.get("data/short-interest.json", {}))
    settlement = max((row.get("settlementDate") for row in si_map.values() if row.get("settlementDate")), default=settlement)
    ftd_map, ftd_file = _safe(fetch_sec_ftd, "SEC FTD") or ({}, None)
    daily_map = _safe(fetch_daily_shortvol, "daily short-vol") or {}

    if not si_map:
        payload = {"engine": "justhodl-squeeze-fuel", "ok": False,
                   "error": "No usable dated short interest", "generated_at": datetime.now(timezone.utc).isoformat(),
                   "donor_health": donor_receipts, "short_positioning": positioning}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(safe_evidence(payload), allow_nan=False).encode(),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False})}

    # candidate universe: watchlist ∪ top FINRA names by a pre-score (DTC + build + FTD)
    def pre(sym):
        s = si_map.get(sym, {})
        dtc = s.get("days_to_cover") or 0
        chg = s.get("si_change_pct") or 0
        f = (ftd_map.get(sym) or {}).get("max_fails") or 0
        return (dtc or 0) * 2 + max(0, chg) / 5 + (1 if f > 1_000_000 else 0) * 3

    finra_syms = sorted(si_map.keys(), key=pre, reverse=True)
    candidates = list(dict.fromkeys([s for s in CORE_WATCHLIST if s in si_map] + finra_syms[:280]))

    positioning = {symbol: positioning[symbol] for symbol in candidates if symbol in positioning}

    # enrich candidates with float + price (threaded)
    enrich = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(fmp_float_and_price, s): s for s in candidates}
        for fut in as_completed(futs):
            r = fut.result()
            enrich[r["sym"]] = r

    scored = []
    for sym in candidates:
        rec = score_name(si_map.get(sym, {}), ftd_map.get(sym), daily_map.get(sym), enrich.get(sym))
        if rec["score"] <= 0:
            continue
        rec.update({"ticker": sym, "name": (si_map.get(sym) or {}).get("name"),
                    "settlement_date": (si_map.get(sym) or {}).get("settlementDate"),
                    "short_positioning": positioning.get(sym),
                    "daily_short_volume": {"source_artifact": "data/finra-short.json", "record": daily_map.get(sym),
                                           "classification": "daily flow; not outstanding short interest"},
                    "borrow_availability": None, "locate_verified": False, "execution_eligible": False})
        scored.append(rec)
    scored.sort(key=lambda r: r["score"], reverse=True)

    board = scored[:60]
    # top_picks = high fuel + price confirmation (the lit fuse), bullish/long
    top_picks = [{"ticker": r["ticker"], "score": r["score"], "direction": "long",
                  "state": r["state"], "pct_of_float": r["pct_of_float"],
                  "days_to_cover": r["days_to_cover"], "reasons": r["reasons"]}
                 for r in scored if r["score"] >= 55 and r["price_confirm"]][:20]

    dist = {
        "n_loaded": sum(1 for r in scored if r["state"] == "LOADED"),
        "n_building": sum(1 for r in scored if r["state"] == "BUILDING"),
        "n_elevated": sum(1 for r in scored if r["state"] == "ELEVATED"),
    }

    payload = {
        "engine": "justhodl-squeeze-fuel", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Per-name short-squeeze FUEL gauge from 100% free authoritative data "
                   "(FINRA consolidated short interest + SEC fails-to-deliver + daily "
                   "short-volume + float). Fuel ≠ ignition: this scores trapped short "
                   "capital, not the catalyst. Pair with earnings/guidance/flow engines."),
        "si_settlement_date": settlement,
        "donor_health": donor_receipts,
        "short_positioning": positioning,
        "ftd_file": ftd_file,
        "n_finra_universe": len(si_map),
        "n_scored": len(scored),
        "distribution": dist,
        "board": board,
        "top_picks": top_picks,
        "data_sources": {
            "short_interest": "FINRA direct plus newer dated data/short-interest.json positions; daily short volume never substitutes for SI",
            "fails_to_deliver": "SEC CNS fails-to-deliver (semi-monthly)",
            "daily_short_volume": "justhodl-finra-short (FINRA Reg SHO daily, T+1)",
            "float_and_price": "FMP /stable/ shares-float + quote",
        },
        "caveats": [
            "Discovery engine — MEASURE-BEFORE-TRUST. top_picks logged to signal-harvester "
            "for forward excess-vs-SPY grading; NOT wired into best-setups/master-ranker until "
            "the scorecard proves net-of-cost alpha (~3-4 weeks).",
            "Official SI is bi-monthly with ~1wk lag; FTD is semi-monthly with ~T+15 lag. "
            "Daily short volume is a separate delayed flow, not outstanding short interest.",
            "Free data has no real-time borrow fee / utilization — the live squeeze fuse. "
            "If this engine proves alpha, a paid borrow feed (Ortex/S3) is the justified upgrade.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(safe_evidence(payload), default=str, allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[squeeze-fuel] DONE scored={len(scored)} loaded={dist['n_loaded']} "
          f"building={dist['n_building']} picks={len(top_picks)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_scored": len(scored), "n_picks": len(top_picks),
        "settlement": settlement, "distribution": dist,
        "top": [(r["ticker"], r["score"], r["state"]) for r in board[:8]]})}
