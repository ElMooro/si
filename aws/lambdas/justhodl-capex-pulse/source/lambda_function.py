"""justhodl-capex-pulse — REAL CapEx dollars across the investable universe.

Khalid's mandate: the global-flow-desk tracked where PORTFOLIO money goes;
this tracks where CORPORATE money goes — capital expenditure, the other half
of the flow picture. Structural-presignals only counts capex MENTIONS in
filings; this engine measures the dollars.

  UNIVERSE   top ~160 by market cap from stock-xray cards (+ guaranteed
             hyperscalers: AMZN MSFT GOOGL META AAPL NVDA ORCL AVGO).
  DATA       FMP /stable/cash-flow-statement (period=quarter, limit=8):
             capex_ttm = sum |capitalExpenditure| of last 4 quarters;
             yoy vs the prior 4. (/v3+/v4 are dead — /stable only.)
  OUTPUT     data/capex-pulse.json: per-name rows, SECTOR aggregates
             (ttm $B, yoy%%, capex/mcap intensity), HYPERSCALER tile
             (the AI-buildout spend pulse), boards top_accelerators /
             top_cutters, market totals + history for trend.
  CONSUMERS  global-flow-desk capex block, signal-board "CapEx Pulse",
             global-flow-desk.html tile.
"""
import json, time, urllib.request
from datetime import datetime, timezone
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/capex-pulse.json"
HIST = "data/history/capex-pulse.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
HYPERSCALERS = ["AMZN", "MSFT", "GOOGL", "META", "AAPL", "NVDA", "ORCL", "AVGO"]
N_TOP = 160
s3 = boto3.client("s3", region_name="us-east-1")

def _j(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

# ── FX: convert foreign reportedCurrency capex to USD ──
# Primary: fleet FRED cache (data/fred-cache.json, DEX* daily); fallback: FMP
# /stable/quote forex pairs. TTM and prior convert at the same spot, so yoy%%
# is identical to the local-currency truth — only levels are translated.
_FRED_DEX = {"JPY": ("DEXJPUS", "per_usd"), "CNY": ("DEXCHUS", "per_usd"),
             "TWD": ("DEXTAUS", "per_usd"), "DKK": ("DEXDNUS", "per_usd"),
             "KRW": ("DEXKOUS", "per_usd"), "INR": ("DEXINUS", "per_usd"),
             "BRL": ("DEXBZUS", "per_usd"), "CHF": ("DEXSZUS", "per_usd"),
             "SEK": ("DEXSDUS", "per_usd"), "HKD": ("DEXHKUS", "per_usd"),
             "SGD": ("DEXSIUS", "per_usd"), "CAD": ("DEXCAUS", "per_usd"),
             "MXN": ("DEXMXUS", "per_usd"), "ZAR": ("DEXSFUS", "per_usd"),
             "NOK": ("DEXNOUS", "per_usd"), "EUR": ("DEXUSEU", "usd_per"),
             "GBP": ("DEXUSUK", "usd_per"), "AUD": ("DEXUSAL", "usd_per"),
             "NZD": ("DEXUSNZ", "usd_per")}
_FX_CACHE = {}
_FRED_DOC = {}

def _usd_per(ccy):
    """USD per 1 unit of ccy, memoized; FRED cache primary, FMP quote fallback."""
    ccy = (ccy or "USD").upper()
    if ccy == "USD": return 1.0, "native"
    if ccy in _FX_CACHE: return _FX_CACHE[ccy]
    rate, src = None, None
    global _FRED_DOC
    if not _FRED_DOC:
        _FRED_DOC = _j("data/fred-cache.json", {}) or {}
    m = _FRED_DEX.get(ccy)
    if m:
        ser = _FRED_DOC.get(m[0]) or []
        for ob in ser[:10]:
            v = ob.get("value") if isinstance(ob, dict) else None
            if isinstance(v, (int, float)) and v > 0:
                rate = (1.0 / v) if m[1] == "per_usd" else v
                src = "FRED " + m[0]; break
    if rate is None:
        pair, inv = (ccy + "USD", False) if ccy in ("EUR", "GBP", "AUD", "NZD") else ("USD" + ccy, True)
        try:
            url = "https://financialmodelingprep.com/stable/quote?symbol=%s&apikey=%s" % (pair, FMP)
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=15) as r:
                q = json.loads(r.read())
            px = (q[0] if isinstance(q, list) and q else {}).get("price")
            if isinstance(px, (int, float)) and px > 0:
                rate = (1.0 / px) if inv else px
                src = "FMP " + pair
        except Exception:
            pass
    _FX_CACHE[ccy] = (rate, src)
    return rate, src


def _fmp_cf(sym):
    url = ("https://financialmodelingprep.com/stable/cash-flow-statement"
           "?symbol=%s&period=quarter&limit=8&apikey=%s" % (sym, FMP))
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=20) as r:
        return json.loads(r.read())

def _fred_intentions():
    """Macro capex-intentions layer -- regional-Fed survey expectations
    lead actual capex by roughly 6 months (capex-predictor deepening,
    leg 1). Philly Fed Future Capital Expenditures diffusion index via
    FRED. Real data only; emits None cleanly if key/series unavailable."""
    import os, urllib.request, urllib.parse, json as _j
    key = os.environ.get("FRED_KEY") or os.environ.get("FRED_API_KEY")
    if not key:
        return None
    out = {}
    for name, sid in (("philly_future_capex", "CEFDFSA066MSFRBPHI"),):
        try:
            q = urllib.parse.urlencode({
                "series_id": sid, "api_key": key, "file_type": "json",
                "sort_order": "desc", "limit": 13})
            obs = _j.loads(urllib.request.urlopen(
                "https://api.stlouisfed.org/fred/series/observations?"
                + q, timeout=20).read()).get("observations") or []
            vals = [(o["date"], float(o["value"])) for o in obs
                    if o.get("value") not in (None, ".", "")]
            if len(vals) < 4:
                continue
            latest_d, latest_v = vals[0]
            avg3 = round(sum(v for _, v in vals[:3]) / 3, 1)
            yr = next((v for d, v in vals if d <= latest_d[:4]
                       and abs((int(latest_d[:4]) - int(d[:4])) * 12
                               + int(latest_d[5:7]) - int(d[5:7])) >= 12),
                      None)
            out[name] = {"series": sid, "asof": latest_d,
                         "latest": round(latest_v, 1), "avg_3m": avg3,
                         "delta_12m": (round(latest_v - yr, 1)
                                       if yr is not None else None),
                         "read": ("EXPANSION" if avg3 > 10 else
                                  "CONTRACTION" if avg3 < 0 else
                                  "FLAT")}
        except Exception:
            continue
    return out or None


def lambda_handler(event=None, context=None):
    x = _j("data/stock-xray.json", {}) or {}
    cards = x.get("cards") or {}
    ranked = sorted(((t, c) for t, c in cards.items() if c.get("mc_b")),
                    key=lambda tc: tc[1]["mc_b"], reverse=True)
    uni = [t for t, _ in ranked[:N_TOP]]
    for h in HYPERSCALERS:
        if h not in uni: uni.append(h)
    # dedupe dual share classes (keep the primary already in list order)
    DUP = {"GOOG": "GOOGL", "BRK.B": "BRK.A", "BRK-B": "BRK-A", "FOX": "FOXA", "NWS": "NWSA"}
    uni = [t for t in uni if not (t in DUP and DUP[t] in uni)]
    rows = []; fails = 0; excluded = []
    for t in uni:
        try:
            qs = _fmp_cf(t)
            if not isinstance(qs, list) or len(qs) < 5:
                fails += 1; continue
            cx = [abs(q.get("capitalExpenditure") or 0) for q in qs[:8]]
            ttm, prior = sum(cx[:4]), sum(cx[4:8])
            if ttm <= 0: fails += 1; continue
            ccy = (qs[0].get("reportedCurrency") or "USD").upper()
            fx_meta = None
            if ccy != "USD":
                rate, fsrc = _usd_per(ccy)
                if rate is None:
                    excluded.append({"ticker": t, "capex_ttm_b": round(ttm / 1e9, 1),
                                     "why": "fx unavailable for %s" % ccy})
                    continue
                ttm *= rate; prior *= rate
                fx_meta = {"ccy": ccy, "usd_per_ccy": round(rate, 6), "src": fsrc}
            yoy = round(100 * (ttm / prior - 1), 1) if prior > 0 else None
            c = cards.get(t) or {}
            mcb = c.get("mc_b")
            # sanity gate: FMP mislabels investing-activity totals as capex for some
            # financials — no real firm spends >35%% of mcap/yr on capex (hyperscalers ~2-4%%)
            if mcb and ttm / 1e9 > 0.35 * mcb:
                excluded.append({"ticker": t, "capex_ttm_b": round(ttm / 1e9, 1), "mc_b": mcb,
                                 "why": "capex>35%% mcap — FMP field contamination (financials)"})
                continue
            row = {"ticker": t, "sector": c.get("sec") or "?",
                   "capex_ttm_b": round(ttm / 1e9, 2), "yoy_pct": yoy,
                   "mc_b": c.get("mc_b"),
                   "intensity_pct": round(100 * ttm / (c["mc_b"] * 1e9), 2) if c.get("mc_b") else None,
                   "asof": qs[0].get("date")}
            if fx_meta: row["fx"] = fx_meta
            rows.append(row)
        except Exception:
            fails += 1
        time.sleep(0.1)
    conv = [r for r in rows if r.get("fx")]
    print("[capex] rows=%d fails=%d converted=%d %s excluded=%d %s" % (
        len(rows), fails, len(conv), [(r["ticker"], r["fx"]["ccy"]) for r in conv][:8],
        len(excluded), [e["ticker"] for e in excluded][:6]))

    sectors = {}
    for r in rows:
        e = sectors.setdefault(r["sector"], {"capex_ttm_b": 0.0, "prior_proxy_b": 0.0, "n": 0, "names": []})
        e["capex_ttm_b"] += r["capex_ttm_b"]; e["n"] += 1
        if r["yoy_pct"] is not None:
            e["prior_proxy_b"] += r["capex_ttm_b"] / (1 + r["yoy_pct"] / 100)
        e["names"].append((r["ticker"], r["capex_ttm_b"]))
    for s_, e in sectors.items():
        e["yoy_pct"] = round(100 * (e["capex_ttm_b"] / e["prior_proxy_b"] - 1), 1) if e["prior_proxy_b"] > 0 else None
        e["capex_ttm_b"] = round(e["capex_ttm_b"], 1)
        e["top"] = [t for t, _ in sorted(e.pop("names"), key=lambda kv: kv[1], reverse=True)[:4]]
        e.pop("prior_proxy_b", None)

    hyp = [r for r in rows if r["ticker"] in HYPERSCALERS]
    hyp_ttm = round(sum(r["capex_ttm_b"] for r in hyp), 1)
    hyp_prior = sum(r["capex_ttm_b"] / (1 + r["yoy_pct"] / 100) for r in hyp if r["yoy_pct"] is not None)
    hyperscalers = {"total_ttm_b": hyp_ttm,
                    "yoy_pct": round(100 * (sum(r["capex_ttm_b"] for r in hyp if r["yoy_pct"] is not None) / hyp_prior - 1), 1) if hyp_prior else None,
                    "rows": sorted(hyp, key=lambda r: r["capex_ttm_b"], reverse=True),
                    "read": "the AI-buildout spend pulse"}

    big = [r for r in rows if r["capex_ttm_b"] >= 0.5 and r["yoy_pct"] is not None]
    boards = {"top_accelerators": sorted(big, key=lambda r: r["yoy_pct"], reverse=True)[:12],
              "top_cutters": sorted(big, key=lambda r: r["yoy_pct"])[:12]}
    mkt_ttm = round(sum(r["capex_ttm_b"] for r in rows), 1)
    prior_m = sum(r["capex_ttm_b"] / (1 + r["yoy_pct"] / 100) for r in rows if r["yoy_pct"] is not None)
    mkt_yoy = round(100 * (sum(r["capex_ttm_b"] for r in rows if r["yoy_pct"] is not None) / prior_m - 1), 1) if prior_m else None

    hist = _j(HIST, {}) or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today] = {"mkt_ttm_b": mkt_ttm, "mkt_yoy": mkt_yoy, "hyp_ttm_b": hyp_ttm, "hyp_yoy": hyperscalers["yoy_pct"]}
    hist = dict(sorted(hist.items())[-400:])
    s3.put_object(Bucket=BUCKET, Key=HIST, Body=json.dumps(hist).encode(), ContentType="application/json")

    doc = {"engine": "justhodl-capex-pulse", "version": "1.1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "n": len(rows), "fails": fails,
           "market": {"capex_ttm_b": mkt_ttm, "yoy_pct": mkt_yoy, "universe": "top-%d mcap (stock-xray) + hyperscalers" % N_TOP},
           "macro_intentions": _fred_intentions(), "capex_intentions_v": "1.0", "hyperscalers": hyperscalers, "sectors": sectors, "boards": boards, "rows": rows,
           "excluded_outliers": excluded,
           "fx_converted": [{"ticker": r["ticker"], **r["fx"], "capex_ttm_b": r["capex_ttm_b"]} for r in conv],
           "method": ("FMP /stable/cash-flow-statement quarterly x8 per name; TTM = last 4q "
                      "|capitalExpenditure|, yoy vs prior 4q; sector aggregates dollar-weighted; "
                      "intensity = capex/mcap. Foreign issuers (FMP reportedCurrency != USD) converted "
                      "to USD at spot (FRED DEX cache primary, FMP forex quote fallback); TTM and "
                      "prior share the spot so yoy%% equals the local-currency truth.")}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"ok": True, "n": len(rows), "fails": fails, "mkt_ttm_b": mkt_ttm, "mkt_yoy": mkt_yoy,
            "hyp": {"ttm_b": hyp_ttm, "yoy": hyperscalers["yoy_pct"]}}
