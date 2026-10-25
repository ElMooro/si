"""Active entrypoint: reported calendar/currency research without forecast authority.
The preceding methodology and native implementation below are retained for audit.

justhodl-capex-pulse — historical CapEx methodology.

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

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import json, time, urllib.request
from capex_measurements import CONTRACT, decode, number, dossier, aggregate, intentions
from datetime import datetime, timezone
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

BUCKET, OUT = "justhodl-dashboard-live", "data/capex-pulse.json"
HIST = "data/history/capex-pulse.json"
FMP = managed_secret(('FMP', 'FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
HYPERSCALERS = ["AMZN", "MSFT", "GOOGL", "META", "AAPL", "NVDA", "ORCL", "AVGO"]
N_TOP = 160
s3 = boto3.client("s3", region_name="us-east-1")

def _j(k, d=None):
    try: return decode(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey'):return d
        raise

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
        return decode(r.read())

def _legacy_fred_intentions():
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


def aggregate_capex(rows):
    """Like-for-like acquired-window totals; not proof of annual comparability.

    Use the unrounded current/prior amounts. A missing prior window cannot add
    to the comparison numerator, and rounding a -99.999% change to -100% cannot
    trigger a division by zero. Every received row remains in the native output.
    """
    import math
    total, matched_current, matched_prior = 0.0, 0.0, 0.0
    matched = 0
    for row in rows:
        current = row.get('current_window_usd')
        prior = row.get('prior_window_usd')
        if type(current) not in (int, float) or not math.isfinite(current) or current < 0:
            raise ValueError('Finite observed current window required')
        total += current
        if (type(row.get('current_window_rows')) is int and row['current_window_rows'] == 4
                and type(row.get('prior_window_rows')) is int and row['prior_window_rows'] == 4
                and type(prior) in (int, float) and math.isfinite(prior) and prior > 0):
            matched_current += current; matched_prior += prior; matched += 1
    if not all(math.isfinite(v) for v in (total, matched_current, matched_prior)):
        raise ValueError('Finite complete cohort totals required')
    growth = (100 * (matched_current / matched_prior - 1)) if matched_prior > 0 else None
    if growth is not None and not math.isfinite(growth): growth = None
    return {'capex_ttm_b': round(total / 1e9, 1),
            'yoy_pct': round(growth, 1) if growth is not None else None,
            'n': len(rows), 'comparison_n': matched, 'comparison_missing_n': len(rows) - matched,
            'comparison_current_usd': matched_current if matched else None,
            'comparison_prior_usd': matched_prior if matched else None,
            'comparison_scope': 'Same source rows with four returned observations in each window; issuer identity, annual dates, currency consistency and statement duration remain unverified.',
            'annual_comparability_verified': False}


def _legacy_lambda_handler(event=None, context=None):
    import math
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
            values = [q.get("capitalExpenditure") for q in qs[:8]]
            if any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
                raise ValueError('Measured cash-flow amounts required; missing is not zero')
            cx = [abs(v) for v in values]
            ttm, prior = sum(cx[:4]), sum(cx[4:8])
            if not math.isfinite(ttm) or not math.isfinite(prior):
                raise ValueError('Finite acquired-window totals required')
            ccy = (qs[0].get("reportedCurrency") or "USD").upper()
            fx_meta = None
            if ccy != "USD":
                rate, fsrc = _usd_per(ccy)
                if rate is None:
                    excluded.append({"ticker": t, "capex_ttm_b": round(ttm / 1e9, 1),
                                     "why": "fx unavailable for %s" % ccy})
                    continue
                if type(rate) not in (int, float) or not math.isfinite(rate) or rate <= 0:
                    raise ValueError('Finite positive currency translation required')
                ttm *= rate; prior *= rate
                if not math.isfinite(ttm) or not math.isfinite(prior):
                    raise ValueError('Finite translated windows required')
                fx_meta = {"ccy": ccy, "usd_per_ccy": round(rate, 6), "src": fsrc}
            yoy = round(100 * (ttm / prior - 1), 1) if len(cx) == 8 and prior > 0 else None
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
                   "asof": qs[0].get("date"),
                   "current_window_usd": ttm, "prior_window_usd": prior if len(cx) == 8 else None,
                   "current_window_rows": len(cx[:4]), "prior_window_rows": len(cx[4:8]),
                   "annual_comparability_verified": False}
            if fx_meta: row["fx"] = fx_meta
            rows.append(row)
        except Exception:
            fails += 1
        time.sleep(0.1)
    conv = [r for r in rows if r.get("fx")]
    print("[capex] rows=%d fails=%d converted=%d %s excluded=%d %s" % (
        len(rows), fails, len(conv), [(r["ticker"], r["fx"]["ccy"]) for r in conv][:8],
        len(excluded), [e["ticker"] for e in excluded][:6]))
    if not rows:
        return {"ok": False, "status": "no_usable_capex_rows", "kept_prior": True, "fails": fails}

    sectors = {}
    for sector in dict.fromkeys(r['sector'] for r in rows):
        cohort = [r for r in rows if r['sector'] == sector]
        sectors[sector] = aggregate_capex(cohort)
        sectors[sector]['top'] = [r['ticker'] for r in sorted(cohort, key=lambda r: r['current_window_usd'], reverse=True)[:4]]

    hyp = [r for r in rows if r["ticker"] in HYPERSCALERS]
    hyp_aggregate = aggregate_capex(hyp)
    hyp_ttm = hyp_aggregate['capex_ttm_b']
    hyperscalers = {**hyp_aggregate, "total_ttm_b": hyp_ttm,
                    "rows": sorted(hyp, key=lambda r: r["capex_ttm_b"], reverse=True),
                    "read": "Reported spending windows; annual comparability unverified"}

    big = [r for r in rows if r["capex_ttm_b"] >= 0.5 and r["yoy_pct"] is not None]
    boards = {"top_accelerators": sorted(big, key=lambda r: r["yoy_pct"], reverse=True)[:12],
              "top_cutters": sorted(big, key=lambda r: r["yoy_pct"])[:12]}
    market_aggregate = aggregate_capex(rows)
    mkt_ttm = market_aggregate['capex_ttm_b']; mkt_yoy = market_aggregate['yoy_pct']

    hist = _j(HIST, {}) or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today] = {"mkt_ttm_b": mkt_ttm, "mkt_yoy": mkt_yoy, "hyp_ttm_b": hyp_ttm, "hyp_yoy": hyperscalers["yoy_pct"]}
    hist = dict(sorted(hist.items()))  # Preserve every existing history entry.
    s3.put_object(Bucket=BUCKET, Key=HIST, Body=json.dumps(hist).encode(), ContentType="application/json")

    doc = {"engine": "justhodl-capex-pulse", "version": "1.1.1",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "call": None, "calls_eligible": False, "forecast_qualified": False, "sizing_eligible": False,
           "quality": {"status": "partial", "annual_comparability_verified": False,
                       "provider_originals_replayed": False,
                       "reason": "Paired acquired-window arithmetic corrected; period/currency validation and whole-source replay remain pending."},
           "n": len(rows), "fails": fails,
           "market": {**market_aggregate, "universe": "top-%d mcap (stock-xray) + hyperscalers" % N_TOP},
           "macro_intentions": _fred_intentions(), "capex_intentions_v": "1.0", "hyperscalers": hyperscalers, "sectors": sectors, "boards": boards, "rows": rows,
           "excluded_outliers": excluded,
           "fx_converted": [{"ticker": r["ticker"], **r["fx"], "capex_ttm_b": r["capex_ttm_b"]} for r in conv],
           "method": ("Existing TTM/YoY keys describe two windows of four returned cash-flow rows requested as quarterly. "
                      "Calendar alignment, discrete statement durations and cross-row currency consistency are not yet verified. "
                      "A comparison requires four rows in both windows. Sector, market and hyperscaler comparisons use the same "
                      "issuer cohort and unrounded amounts. Foreign levels use the existing spot translation, not historical FX. "
                      "Full provider-response replay and investment qualification remain pending.")}

    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"ok": True, "n": len(rows), "fails": fails, "mkt_ttm_b": mkt_ttm, "mkt_yoy": mkt_yoy,
            "hyp": {"ttm_b": hyp_ttm, "yoy": hyperscalers["yoy_pct"]}}


def _fred_intentions():
    import os, urllib.parse
    key=os.environ.get('FRED_KEY') or os.environ.get('FRED_API_KEY')
    if not key:return None
    query=urllib.parse.urlencode({'series_id':'CEFDFSA066MSFRBPHI','api_key':key,'file_type':'json','sort_order':'desc','limit':13})
    try:
        with urllib.request.urlopen('https://api.stlouisfed.org/fred/series/observations?'+query,timeout=20) as response:
            original=decode(response.read())
        return {'philly_future_capex':intentions(original,datetime.now(timezone.utc).date().isoformat())}
    except Exception:return None


def lambda_handler(event=None,context=None):
    x=_j('data/stock-xray.json',{}) or {}
    cards=x.get('cards')
    if not isinstance(cards,dict):return {'ok':False,'kept_prior':True,'reason':'source_universe_unavailable'}
    ranked=sorted(((t,c) for t,c in cards.items() if isinstance(c,dict) and number(c.get('mc_b')) is not None and c['mc_b']>0),
                  key=lambda tc:tc[1]['mc_b'],reverse=True)
    universe=[t for t,c in ranked[:N_TOP]]
    for symbol in HYPERSCALERS:
        if symbol not in universe:universe.append(symbol)
    # Preserve the preceding acquisition universe without interpreting this
    # request-plan alias list as verified economic issuer deduplication.
    duplicate_plan={'GOOG':'GOOGL','BRK.B':'BRK.A','BRK-B':'BRK-A','FOX':'FOXA','NWS':'NWSA'}
    omitted=[t for t in universe if t in duplicate_plan and duplicate_plan[t] in universe]
    universe=[t for t in universe if t not in omitted]
    today=datetime.now(timezone.utc).date().isoformat()
    rows=[];failures=[]
    for symbol in universe:
        try:
            received=_fmp_cf(symbol)
            if not isinstance(received,list) or not received:
                failures.append({'ticker':symbol,'reason':'no_received_statement_rows','received_response':received});continue
            raw_context=cards.get(symbol)
            row=dossier(symbol,received,raw_context if isinstance(raw_context,dict) else {},today)
            if raw_context is not None and not isinstance(raw_context,dict):row['malformed_source_context']=raw_context
            # Retain the preceding FX acquisition path as explicitly unverified
            # context, with no additional calls for short/invalid responses.
            old_rows=received[:8]
            if len(received)>=5 and all(isinstance(q,dict) and number(q.get('capitalExpenditure')) is not None for q in old_rows):
                currency=received[0].get('reportedCurrency')
                if isinstance(currency,str) and currency.upper()!='USD':
                    try:
                        rate,source=_usd_per(currency)
                        row['legacy_spot_fx_context']={'reported_currency':currency,'usd_per_currency':number(rate),
                            'source':source,'historical_flow_conversion_eligible':False,'observation_date_verified':False}
                    except Exception:
                        row['legacy_spot_fx_context']={'reported_currency':currency,'usd_per_currency':None,
                            'historical_flow_conversion_eligible':False,'observation_date_verified':False}
            rows.append(row)
        except Exception as exc:
            # Never publish request URLs, credentials or exception bodies.
            failures.append({'ticker':symbol,'reason':'acquisition_or_compilation_unavailable','error_type':type(exc).__name__})
        time.sleep(0.1)
    if not rows:return {'ok':False,'kept_prior':True,'reason':'no_received_statement_rows','fails':len(failures)}
    market=aggregate(rows)
    sectors={sector:aggregate([r for r in rows if r['sector']==sector]) for sector in dict.fromkeys(r['sector'] for r in rows)}
    for sector,group in sectors.items():group['top']=[]
    hypers=[r for r in rows if r['ticker'] in HYPERSCALERS]
    hyper=aggregate(hypers)
    hyper.update(total_ttm_b=hyper['capex_ttm_b'],rows=hypers,read='Reported calendar windows; original SEC and FX-flow comparability remain unverified')
    prior=_j(HIST,{})
    if prior is None:prior={}
    if not isinstance(prior,dict):raise ValueError('Complete prior history required')
    history=dict(prior)
    point={'mkt_ttm_b':market['capex_ttm_b'],'mkt_yoy':market['yoy_pct'],
           'hyp_ttm_b':hyper['total_ttm_b'],'hyp_yoy':hyper['yoy_pct'],'measurement_contract':CONTRACT}
    if today in history:
        previous=history[today]
        if isinstance(previous,dict) and {k:v for k,v in previous.items() if k!='previous_same_day_original'}==point:
            point=previous
        else:point['previous_same_day_original']=previous
    history[today]=point
    doc={'engine':'justhodl-capex-pulse','version':'1.2.0','measurement_contract':CONTRACT,
         'generated_at':datetime.now(timezone.utc).isoformat(timespec='seconds'),'call':None,
         'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,
         'quality':{'status':'partial','annual_comparability_verified':False,'provider_originals_replayed':False,
                    'point_in_time_availability_verified':False,'reason':'Explicit reported currency and calendar windows only; original SEC replay and forecast qualification remain open.'},
         'n':len(rows),'fails':len(failures),'failures':failures,
         'requested_universe':universe,'request_plan_omissions':omitted,'source_universe_complete':False,
         'market':dict(market,universe='Existing top-%d market-cap request plan plus hyperscalers'%N_TOP),
         'macro_intentions':_fred_intentions(),'capex_intentions_v':'2.0','hyperscalers':hyper,'sectors':sectors,
         'boards':{'top_accelerators':[],'top_cutters':[]},'rows':rows,'excluded_outliers':[], 'fx_converted':[],
         'method':'Whole received statement rows retained. Annual amounts require explicit contiguous quarter durations, one issuer and currency. Local-currency annual changes remain local. USD cohorts require identical windows and unambiguous CIKs; foreign spending is not translated with present-day FX. Market-cap intensity and forecast boards remain unavailable. Provider original HTTP/SEC and first-publication replay are unverified.'}
    # Serialize both complete candidates before any write; no default=str or NaN.
    history_bytes=json.dumps(history,allow_nan=False,separators=(',',':')).encode()
    output_bytes=json.dumps(doc,allow_nan=False,separators=(',',':')).encode()
    s3.put_object(Bucket=BUCKET,Key=HIST,Body=history_bytes,ContentType='application/json')
    s3.put_object(Bucket=BUCKET,Key=OUT,Body=output_bytes,ContentType='application/json',CacheControl='public, max-age=3600')
    return {'ok':True,'n':len(rows),'fails':len(failures),'mkt_ttm_b':market['capex_ttm_b'],'mkt_yoy':market['yoy_pct']}
