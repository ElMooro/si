"""
justhodl-global-macro — Per-country economic regime aggregator.

WHY THIS EXISTS
───────────────
Platform has divergence-v2 country data + manufacturing-global-agent country
PMIs, but no SINGLE per-country economic snapshot showing which economies are
hot/cold right now. No D3 choropleth or heatmap exists.

ALGORITHM
─────────
For 13 major countries (US, DE, UK, FR, IT, ES, JP, KR, CN-via-Korea-proxy,
CA, AU, BR, IN, MX, CH), aggregate 5 dimensions:

  1. UNEMPLOYMENT (lower = better)       FRED OECD harmonized series
  2. MANUFACTURING PMI (>50 = expansion) FRED country PMI series
  3. INDUSTRIAL PRODUCTION YoY (higher = better)  FRED country IP series
  4. EQUITY ETF 3M RETURN (higher = better)        FMP price history
  5. CURRENCY VS USD 3M (depends on context)       FRED FX series

Composite "Economic Health Score" 0-100:
  Each dimension normalized to 0-100, weighted equally (20% each).
  >70 = HOT (expansion), 40-70 = MIXED, <40 = COLD (contraction).

OUTPUT
──────
  s3://justhodl-dashboard-live/data/global-macro.json
  {
    as_of, n_countries,
    countries: [
      { code, name,
        unemployment: {value, delta_3m, score_0_100},
        pmi:          {value, score_0_100},
        ip_yoy:       {value, score_0_100},
        equity_3m:    {ticker, return_pct, score_0_100},
        currency_3m:  {pair, change_pct, score_0_100},
        composite_score: 0-100,
        regime: "HOT" | "MIXED" | "COLD"
      }
    ],
    rankings: { hottest: [..], coldest: [..] }
  }

SCHEDULE
────────
  rate(1 day) — daily refresh, low API budget
  ~13 countries × 3 FRED series + 13 ETFs = ~52 API calls/day

ZERO DETERIORATION
  ✓ No Lambda touched
  ✓ Reuses FRED + FMP keys from env
  ✓ Failure-safe: missing series → score component is None
"""
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/global-macro.json")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

S3 = boto3.client("s3", region_name=REGION)


# ─── Country specs ─────────────────────────────────────────────────────────
# (code, name, unemployment_fred_id, pmi_fred_id, ip_fred_id, equity_etf, fx_fred_id, fx_invert)
COUNTRIES = [
    ("US", "United States", "UNRATE",
     None,  # ISM rather than OECD PMI; use INDPRO as proxy
     "INDPRO", "SPY", None, False),
    ("DE", "Germany", "LRHUTTTTDEM156S",
     "DEUPRMNTO01IXOBM", "DEUPROINDMISMEI", "EWG", "DEXUSEU", True),
    ("GB", "United Kingdom", "LRHUTTTTGBM156S",
     "GBRPRMNTO01IXOBM", "GBRPROINDMISMEI", "EWU", "DEXUSUK", True),
    ("FR", "France", "LRHUTTTTFRM156S",
     "FRAPRMNTO01IXOBM", "FRAPROINDMISMEI", "EWQ", "DEXUSEU", True),
    ("IT", "Italy", "LRHUTTTTITM156S",
     None, "ITAPROINDMISMEI", "EWI", "DEXUSEU", True),
    ("ES", "Spain", "LRHUTTTTESM156S",
     None, "ESPPROINDMISMEI", "EWP", "DEXUSEU", True),
    ("JP", "Japan", "LRHUTTTTJPM156S",
     "JPNPRMNTO01IXOBM", "JPNPROINDMISMEI", "EWJ", "DEXJPUS", False),
    ("KR", "South Korea", "LRHUTTTTKRM156S",
     None, "KORPROINDMISMEI", "EWY", "DEXKOUS", False),
    ("CN", "China (proxy)", None,
     "CHEFMNM156N", None, "FXI", "DEXCHUS", False),
    ("CA", "Canada", "LRHUTTTTCAM156S",
     None, "CANPROINDMISMEI", "EWC", "DEXCAUS", False),
    ("AU", "Australia", "LRHUTTTTAUM156S",
     None, None, "EWA", "DEXUSAL", True),
    ("BR", "Brazil", "LRHUTTTTBRM156S",
     None, "BRAPROINDMISMEI", "EWZ", "DEXBZUS", False),
    ("IN", "India", None, None, None, "INDA", "DEXINUS", False),
    ("MX", "Mexico", "LRHUTTTTMXM156S",
     None, "MEXPROINDMISMEI", "EWW", "DEXMXUS", False),
    ("CH", "Switzerland", "LRHUTTTTCHQ156S",
     None, None, "EWL", "DEXSZUS", False),
]


def http_get_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "global-macro/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return None


def fetch_fred_series(series_id, n_obs=24):
    if not series_id:
        return None
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY,
        "file_type": "json", "limit": n_obs, "sort_order": "desc",
    })
    d = http_get_json(f"https://api.stlouisfed.org/fred/series/observations?{qs}", timeout=15)
    if not d:
        return None
    obs = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v and v != ".":
            try:
                obs.append({"date": o["date"], "value": float(v)})
            except ValueError:
                continue
    return obs[::-1]  # chronological


def fetch_fmp_history(ticker, n_days=120):
    """Fetch last n daily bars from FMP. Returns most-recent-first list.
    Uses /stable/historical-price-eod/light — current FMP endpoint after
    August 2025 deprecation of v3. Returns records with: symbol, date, price, volume.
    """
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
           f"?symbol={ticker}&apikey={FMP_KEY}")
    d = http_get_json(url, timeout=15)
    if not d:
        return None
    if isinstance(d, list):
        return d[:n_days]
    return []


def normalize_score(value, low, high, invert=False):
    """Map a value in [low, high] to score 0-100 (or 100-0 if invert)."""
    if value is None:
        return None
    pct = max(0, min(100, (value - low) / (high - low) * 100))
    return round(100 - pct if invert else pct, 1)


def assemble_country(spec):
    code, name, unemp_id, pmi_id, ip_id, etf, fx_id, fx_invert = spec
    out = {"code": code, "name": name}

    # Unemployment
    if unemp_id:
        obs = fetch_fred_series(unemp_id, n_obs=18)
        if obs and len(obs) >= 4:
            latest = obs[-1]["value"]
            prior = obs[-4]["value"] if len(obs) >= 4 else None
            delta_3m = latest - prior if prior else None
            # Lower unemployment = better score (invert range 1-12% → 100-0)
            score = normalize_score(latest, 1.0, 12.0, invert=True)
            out["unemployment"] = {
                "value": round(latest, 2),
                "delta_3m": round(delta_3m, 2) if delta_3m is not None else None,
                "score_0_100": score,
            }

    # PMI (>50 = expansion)
    if pmi_id:
        obs = fetch_fred_series(pmi_id, n_obs=6)
        if obs:
            latest = obs[-1]["value"]
            score = normalize_score(latest, 40, 60)
            out["pmi"] = {"value": round(latest, 1), "score_0_100": score}

    # Industrial Production YoY
    if ip_id:
        obs = fetch_fred_series(ip_id, n_obs=18)
        if obs and len(obs) >= 13:
            latest = obs[-1]["value"]
            year_ago = obs[-13]["value"]
            yoy_pct = (latest - year_ago) / year_ago * 100 if year_ago else 0
            score = normalize_score(yoy_pct, -10, 10)
            out["ip_yoy"] = {"value": round(yoy_pct, 2), "score_0_100": score}

    # Equity ETF 3M return
    if etf:
        h = fetch_fmp_history(etf, n_days=70)
        if h and len(h) >= 60:
            # /stable/historical-price-eod/light returns: {symbol, date, price, volume}
            # Keep fallback to 'close'/'adjClose' for older endpoint compatibility
            latest_close = h[0].get("price") or h[0].get("close") or h[0].get("adjClose")
            three_mo_close = h[59].get("price") or h[59].get("close") or h[59].get("adjClose")
            if latest_close and three_mo_close:
                ret_pct = (latest_close - three_mo_close) / three_mo_close * 100
                score = normalize_score(ret_pct, -15, 15)
                out["equity_3m"] = {
                    "ticker": etf,
                    "return_pct": round(ret_pct, 2),
                    "score_0_100": score,
                }

    # Currency vs USD 3M
    if fx_id:
        obs = fetch_fred_series(fx_id, n_obs=80)
        if obs and len(obs) >= 60:
            latest = obs[-1]["value"]
            three_mo = obs[-60]["value"]
            change_pct = (latest - three_mo) / three_mo * 100 if three_mo else 0
            # If pair is USD/foreign (e.g., DEXJPUS=YEN/USD), invert sign
            # We display as "currency stronger or weaker vs USD"
            stronger_pct = -change_pct if fx_invert else change_pct
            score = normalize_score(stronger_pct, -10, 10)
            out["currency_3m"] = {
                "pair": fx_id,
                "change_pct": round(stronger_pct, 2),
                "score_0_100": score,
            }

    # Composite score: average of available components
    components = ["unemployment", "pmi", "ip_yoy", "equity_3m", "currency_3m"]
    scores = []
    for c in components:
        if out.get(c) and out[c].get("score_0_100") is not None:
            scores.append(out[c]["score_0_100"])
    if scores:
        composite = round(statistics.mean(scores), 1)
        out["composite_score"] = composite
        out["n_components"] = len(scores)
        if composite >= 65:
            out["regime"] = "HOT"
        elif composite >= 45:
            out["regime"] = "MIXED"
        else:
            out["regime"] = "COLD"
    else:
        out["composite_score"] = None
        out["regime"] = "UNKNOWN"
        out["n_components"] = 0

    return out


def lambda_handler(event, context):
    started = time.time()
    print(f"[global-macro] Aggregating {len(COUNTRIES)} countries…")

    results = []
    with ThreadPoolExecutor(max_workers=4) as exe:
        futures = {exe.submit(assemble_country, spec): spec[0] for spec in COUNTRIES}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                code = futures[fut]
                print(f"[global-macro] {code} failed: {e}")
                results.append({"code": code, "err": str(e)[:200]})

    # Sort by composite score
    valid_results = [r for r in results if r.get("composite_score") is not None]
    valid_results.sort(key=lambda r: r["composite_score"], reverse=True)

    rankings = {
        "hottest": [{"code": r["code"], "name": r["name"], "score": r["composite_score"]}
                    for r in valid_results[:5]],
        "coldest": [{"code": r["code"], "name": r["name"], "score": r["composite_score"]}
                    for r in valid_results[-5:][::-1]],
    }

    # Aggregate stats
    n_hot = sum(1 for r in valid_results if r.get("regime") == "HOT")
    n_mixed = sum(1 for r in valid_results if r.get("regime") == "MIXED")
    n_cold = sum(1 for r in valid_results if r.get("regime") == "COLD")
    avg_composite = round(statistics.mean([r["composite_score"] for r in valid_results]), 1) if valid_results else 0

    payload = {
        "schema_version": "1.0",
        "method": "global_macro_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_countries": len(results),
        "n_with_data": len(valid_results),
        "regime_counts": {"hot": n_hot, "mixed": n_mixed, "cold": n_cold},
        "global_avg_composite": avg_composite,
        "global_regime":
            "GLOBAL EXPANSION" if avg_composite >= 60
            else ("GLOBAL CONTRACTION" if avg_composite <= 40
                  else "GLOBAL MIXED"),
        "countries": results,
        "rankings": rankings,
        "duration_s": round(time.time() - started, 1),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=600",
    )
    print(f"[global-macro] DONE in {payload['duration_s']}s · "
          f"{len(valid_results)}/{len(results)} countries · "
          f"avg={avg_composite} ({n_hot}H/{n_mixed}M/{n_cold}C)")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_with_data": len(valid_results),
            "global_regime": payload["global_regime"],
            "avg_composite": avg_composite,
            "duration_s": payload["duration_s"],
        }),
    }
