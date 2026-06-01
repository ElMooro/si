"""justhodl-vol-surface v3.2 — Bloomberg OPV-class cross-asset volatility engine.

Approach evolution:
  v1 — Polygon /v3/snapshot/options/ — plan lacks options entitlement (RETIRED)
  v2 — Yahoo Finance /v7/finance/options/ — 429-blocked from AWS IPs (RETIRED)
  v3 — FRED VIX-family, but used several discontinued series IDs (FAILED)
  v3.2 — Only FRED series confirmed alive on the Volatility Indexes category
         page + Bloomberg-style single-name equity VIX cross-section + best-effort
         Yahoo for VVIX/SKEW (which FRED no longer carries since their CBOE feed
         was reorganized).

Data sources:

CORE FRED VOL INDEXES (14 series, all currently updating daily):
  • Index-level — VIXCLS(SPX), VXVCLS(SPX 3-month), VXNCLS(NDX),
                  RVXCLS(RUT), VXDCLS(DJIA), VXEEMCLS(EEM), VXEWZCLS(EWZ Brazil)
  • Commodity   — OVXCLS(USO oil), GVZCLS(GLD gold)
  • Single-name — VXAPLCLS(AAPL), VXGOGCLS(GOOG), VXAZNCLS(AMZN),
                  VXGSCLS(GS), VXIBMCLS(IBM)
  This single-name cross-section is what makes the surface Bloomberg-class:
  retail platforms surface VIX; only institutional terminals routinely surface
  per-name vol-of-vol on the SPX heavy weights.

YAHOO BEST-EFFORT (graceful degradation if 429):
  • ^VVIX — CBOE vol-of-vol (FRED does not carry this any more)
  • ^SKEW — CBOE SKEW Index (FRED does not carry this any more)

Outputs:
  data/vol-surface.json           — current snapshot + regime + alerts
  data/vol-surface-history.json   — rolling 168 hourly snapshots

Regime classifier (composite_stress_score 0-100):
  STRESS_RISK_OFF (>=70) / DEFENSIVE_BID (>=50) / MILDLY_ELEVATED (>=35)
  / COMPLACENCY (<25 & steep contango) / NORMAL

Telegram alerts on: regime flip, term inversion, SKEW>=90th pctile, VVIX>=110,
                    small-cap stress, oil-dominant vol, equity-VIX dispersion blow-up
"""
import json, os, time, math
from datetime import datetime, timezone
from urllib import request, error
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/vol-surface.json"
S3_HISTORY_KEY = "data/vol-surface-history.json"
HISTORY_MAX = 168

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# All series confirmed alive on https://fred.stlouisfed.org/categories/32425
# as of 2026-05-16. DISCONTINUED: EVZCLS(2025-03), VXTYN(2020), VXSLVCLS/VXFXICLS
# /VXXLECLS/VXGDXCLS (2022), VXOCLS (2021). Removed from rotation.
VOL_INDICES = {
    "VIX_30D":   {"fred": "VIXCLS",     "for": "SPX",   "tenor_d": 30, "tier": "index"},
    "VIX_3M":    {"fred": "VXVCLS",     "for": "SPX",   "tenor_d": 90, "tier": "index"},
    "NDX_VOL":   {"fred": "VXNCLS",     "for": "NDX",   "tenor_d": 30, "tier": "index"},
    "RUT_VOL":   {"fred": "RVXCLS",     "for": "RUT",   "tenor_d": 30, "tier": "index"},
    "DJIA_VOL":  {"fred": "VXDCLS",     "for": "DJIA",  "tenor_d": 30, "tier": "index"},
    "EEM_VOL":   {"fred": "VXEEMCLS",   "for": "EEM",   "tenor_d": 30, "tier": "intl"},
    "EWZ_VOL":   {"fred": "VXEWZCLS",   "for": "EWZ",   "tenor_d": 30, "tier": "intl"},
    "OIL_VOL":   {"fred": "OVXCLS",     "for": "USO",   "tenor_d": 30, "tier": "commodity"},
    "GOLD_VOL":  {"fred": "GVZCLS",     "for": "GLD",   "tenor_d": 30, "tier": "commodity"},
    "AAPL_VOL":  {"fred": "VXAPLCLS",   "for": "AAPL",  "tenor_d": 30, "tier": "single_name"},
    "GOOG_VOL":  {"fred": "VXGOGCLS",   "for": "GOOG",  "tenor_d": 30, "tier": "single_name"},
    "AMZN_VOL":  {"fred": "VXAZNCLS",   "for": "AMZN",  "tenor_d": 30, "tier": "single_name"},
    "GS_VOL":    {"fred": "VXGSCLS",    "for": "GS",    "tenor_d": 30, "tier": "single_name"},
    "IBM_VOL":   {"fred": "VXIBMCLS",   "for": "IBM",   "tenor_d": 30, "tier": "single_name"},
}

s3 = boto3.client("s3", region_name="us-east-1")


def _get_json(url, timeout=15, retries=3, headers=None):
    last_err = None
    base_headers = {"User-Agent": "Mozilla/5.0 (compatible; JustHodlBot/1.0)"}
    if headers: base_headers.update(headers)
    for i in range(retries):
        try:
            req = request.Request(url, headers=base_headers)
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(0.5 * (i + 1))
    if last_err: raise last_err
    raise RuntimeError("http")


def fred_series(series_id, limit=300):
    if not FRED_KEY: return []
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
               f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
        j = _get_json(url)
        out = []
        for o in j.get("observations", []):
            v = o.get("value")
            if v in (None, ".", ""): continue
            try: out.append({"date": o.get("date"), "value": float(v)})
            except: pass
        return out
    except Exception as e:
        print(f"[fred] {series_id}: {e}")
        return []


def yahoo_quote_history(symbol, days=300):
    """Best-effort daily history for symbols FRED doesn't carry (^VVIX, ^SKEW).
    Returns [] silently on 429 / any failure — composite scoring degrades gracefully."""
    try:
        end = int(time.time())
        start = end - days * 86400
        url = (f"https://query1.finance.yahoo.com/v7/finance/chart/{symbol}"
               f"?period1={start}&period2={end}&interval=1d&includePrePost=false")
        j = _get_json(url, timeout=10, retries=2,
                      headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                                              "Chrome/124.0.0.0 Safari/537.36"})
        result = j.get("chart", {}).get("result", [{}])[0]
        ts = result.get("timestamp") or []
        closes = (result.get("indicators", {}).get("quote", [{}])[0] or {}).get("close") or []
        out = []
        for t, c in zip(ts, closes):
            if c is None: continue
            dt = datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat()
            try: out.append({"date": dt, "value": float(c)})
            except: pass
        out.reverse()
        return out
    except Exception as e:
        print(f"[yahoo-bestEffort] {symbol}: {type(e).__name__}: {str(e)[:100]}")
        return []


def percentile_rank(history, target):
    if target is None or not history: return None
    vals = sorted(v for v in history if v is not None)
    if not vals: return None
    return round(100 * sum(1 for v in vals if v <= target) / len(vals), 1)


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def get_history():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"snapshots": []}


def compute_term_structure(vix_30d, vix_3m):
    out = {"spot": vix_30d, "back": vix_3m}
    if vix_30d and vix_3m and vix_3m > 0:
        out["ratio_30d_3m"] = round(vix_30d / vix_3m, 3)
    if vix_30d and vix_3m and vix_30d > 0:
        out["slope_30d_3m_yearized"] = round((vix_3m - vix_30d) / (90 - 30) * 365 / 100, 4)
    inverted = bool(vix_30d and vix_3m and vix_30d > vix_3m * 1.05)
    out["inverted"] = inverted
    out["regime"] = "BACKWARDATED_PANIC" if inverted else (
        "STEEP_CONTANGO" if (vix_30d and vix_3m and vix_3m / max(vix_30d, 0.01) > 1.15) else "NORMAL_CONTANGO"
    )
    return out


def compute_cross_asset(spot_values):
    out = {"spots": spot_values}
    vix = spot_values.get("VIX_30D")
    ndx = spot_values.get("NDX_VOL")
    rut = spot_values.get("RUT_VOL")
    djia = spot_values.get("DJIA_VOL")
    eem = spot_values.get("EEM_VOL")
    ewz = spot_values.get("EWZ_VOL")
    oil = spot_values.get("OIL_VOL")
    gold = spot_values.get("GOLD_VOL")
    if vix and ndx and vix > 0: out["ndx_vix_ratio"] = round(ndx / vix, 3)
    if vix and djia and vix > 0: out["djia_vix_ratio"] = round(djia / vix, 3)
    if vix and rut and vix > 0:
        out["rut_vix_ratio"] = round(rut / vix, 3)
        out["smallcap_stress"] = out["rut_vix_ratio"] > 1.5
    if vix and eem and vix > 0: out["eem_vix_ratio"] = round(eem / vix, 3)
    if vix and ewz and vix > 0: out["ewz_vix_ratio"] = round(ewz / vix, 3)
    if vix and oil and vix > 0:
        out["oil_vix_ratio"] = round(oil / vix, 3)
        out["oil_dominant"] = out["oil_vix_ratio"] > 2.5
    if vix and gold and vix > 0: out["gold_vix_ratio"] = round(gold / vix, 3)
    return out


def compute_equity_vix_dispersion(spot_values):
    """Bloomberg OPV-class: dispersion across single-name equity VIX.
    HIGH dispersion = idiosyncratic risk (M&A, earnings, scandals)
    LOW dispersion = systemic move (everything correlated, beta-driven)"""
    sn = [(k, spot_values.get(k)) for k in ("AAPL_VOL", "GOOG_VOL", "AMZN_VOL", "GS_VOL", "IBM_VOL")]
    sn = [(k, v) for k, v in sn if v is not None]
    if len(sn) < 3: return {"available": False}
    vals = [v for _, v in sn]
    mean_v = sum(vals) / len(vals)
    var_v = sum((v - mean_v) ** 2 for v in vals) / len(vals)
    std_v = var_v ** 0.5
    cv = std_v / max(mean_v, 0.01)
    max_name, max_val = max(sn, key=lambda x: x[1])
    min_name, min_val = min(sn, key=lambda x: x[1])
    return {
        "available": True,
        "names": dict(sn),
        "mean": round(mean_v, 2),
        "std": round(std_v, 2),
        "coef_var": round(cv, 3),
        "spread": round(max_val - min_val, 2),
        "highest": {"name": max_name, "value": max_val},
        "lowest": {"name": min_name, "value": min_val},
        "regime": ("IDIOSYNCRATIC_RISK" if cv > 0.30 else
                   "BETA_DRIVEN" if cv < 0.12 else "MIXED"),
    }


def compute_skew_metrics(skew, skew_history):
    if skew is None: return {"value": None, "pctile_252d": None, "regime": "UNKNOWN", "source": None}
    pctile = percentile_rank(skew_history[-252:] if skew_history else [], skew)
    if skew >= 140: regime = "EXTREME_TAIL_HEDGING"
    elif skew >= 130: regime = "ELEVATED_PUT_BID"
    elif skew >= 120: regime = "MILD_PUT_BID"
    else: regime = "FLAT_OR_CALL_SKEW"
    tail_mult = round(math.exp((skew - 100) / 10), 3)
    return {"value": round(skew, 2), "pctile_252d": pctile, "regime": regime,
            "tail_mult": tail_mult, "source": "yahoo"}


def compute_vvix_metrics(vvix, vvix_history):
    if vvix is None: return {"value": None, "pctile_252d": None, "regime": "UNKNOWN", "source": None}
    pctile = percentile_rank(vvix_history[-252:] if vvix_history else [], vvix)
    if vvix >= 130: regime = "PANIC_VOL_OF_VOL"
    elif vvix >= 110: regime = "ELEVATED"
    elif vvix >= 90: regime = "NORMAL"
    else: regime = "COMPLACENT"
    return {"value": round(vvix, 2), "pctile_252d": pctile, "regime": regime, "source": "yahoo"}


def composite_stress_score(term, skew_data, vvix_data, cross, equity_dispersion):
    score = 0.0
    components = {}
    if term.get("inverted"):
        score += 35; components["term_inversion"] = 35
    elif term.get("ratio_30d_3m") and term.get("ratio_30d_3m") > 0.95:
        score += 15; components["term_flattening"] = 15
    sp = skew_data.get("pctile_252d")
    if sp is not None:
        c = round(sp * 0.20, 2); score += c; components["skew_pctile"] = c
    vp = vvix_data.get("pctile_252d")
    if vp is not None:
        c = round(vp * 0.12, 2); score += c; components["vvix_pctile"] = c
    if cross.get("smallcap_stress"):
        score += 10; components["smallcap_stress"] = 10
    if cross.get("oil_dominant"):
        score += 8; components["oil_dominant"] = 8
    if equity_dispersion.get("available") and equity_dispersion.get("regime") == "IDIOSYNCRATIC_RISK":
        score += 6; components["equity_dispersion"] = 6
    score = max(0.0, min(100.0, score))
    return round(score, 1), components


def classify_regime(score, term):
    if score >= 70: return "STRESS_RISK_OFF"
    if score >= 50: return "DEFENSIVE_BID"
    if score >= 35: return "MILDLY_ELEVATED"
    if term.get("regime") == "STEEP_CONTANGO" and score < 25: return "COMPLACENCY"
    return "NORMAL"


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[vol-surface v3.2] starting {datetime.now(timezone.utc).isoformat()}")
    if not FRED_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FRED_API_KEY not set"})}

    series_data = {}
    history_arrays = {}
    fred_ok, fred_failed = 0, []
    for label, meta in VOL_INDICES.items():
        obs = fred_series(meta["fred"], limit=300)
        series_data[label] = obs
        history_arrays[label] = [o["value"] for o in obs]
        if obs: fred_ok += 1
        else: fred_failed.append(label)
    print(f"[fred] {fred_ok}/{len(VOL_INDICES)} alive  failed={fred_failed}")

    vvix_obs = yahoo_quote_history("^VVIX", days=300)
    skew_obs = yahoo_quote_history("^SKEW", days=300)
    vvix_history = [o["value"] for o in vvix_obs]
    skew_history = [o["value"] for o in skew_obs]
    print(f"[yahoo] vvix={len(vvix_obs)} skew={len(skew_obs)}")

    spot = {label: (obs[0]["value"] if obs else None) for label, obs in series_data.items()}
    spot_dates = {label: (obs[0]["date"] if obs else None) for label, obs in series_data.items()}
    skew_now = skew_obs[0]["value"] if skew_obs else None
    skew_date = skew_obs[0]["date"] if skew_obs else None
    vvix_now = vvix_obs[0]["value"] if vvix_obs else None
    vvix_date = vvix_obs[0]["date"] if vvix_obs else None

    term = compute_term_structure(spot.get("VIX_30D"), spot.get("VIX_3M"))
    cross = compute_cross_asset(spot)
    equity_dispersion = compute_equity_vix_dispersion(spot)
    skew_data = compute_skew_metrics(skew_now, skew_history)
    vvix_data = compute_vvix_metrics(vvix_now, vvix_history)
    score, components = composite_stress_score(term, skew_data, vvix_data, cross, equity_dispersion)
    regime = classify_regime(score, term)

    underlyings = {}
    for label, meta in VOL_INDICES.items():
        v = spot.get(label)
        hist = history_arrays.get(label, [])[-252:]
        underlyings[label] = {
            "for": meta["for"], "tenor_d": meta["tenor_d"], "tier": meta["tier"],
            "value": round(v, 2) if v else None,
            "date": spot_dates.get(label),
            "pctile_252d": percentile_rank(hist, v),
        }

    history = get_history().get("snapshots", [])
    prior_regime = history[-1].get("regime") if history else None
    alerts = []
    if prior_regime and prior_regime != regime:
        alerts.append(f"Regime flip: {prior_regime} -> {regime}")
    if term.get("inverted"):
        alerts.append(f"VIX term-structure INVERTED (30d {spot.get('VIX_30D')} > 3m {spot.get('VIX_3M')})")
    if skew_data.get("pctile_252d") is not None and skew_data["pctile_252d"] >= 90:
        alerts.append(f"SKEW {skew_data['value']} = {skew_data['pctile_252d']}th pctile (extreme tail hedging)")
    if vvix_data.get("value") and vvix_data["value"] >= 110:
        alerts.append(f"VVIX {vvix_data['value']} ELEVATED (vol-of-vol risk)")
    if cross.get("smallcap_stress"):
        alerts.append(f"Small-cap stress: RVX/VIX = {cross.get('rut_vix_ratio')}")
    if cross.get("oil_dominant"):
        alerts.append(f"Oil-dominant vol regime: OVX/VIX = {cross.get('oil_vix_ratio')}")
    if equity_dispersion.get("regime") == "IDIOSYNCRATIC_RISK":
        h = equity_dispersion.get("highest", {})
        l = equity_dispersion.get("lowest", {})
        alerts.append(f"Equity-VIX dispersion HIGH: {h.get('name')}@{h.get('value')} vs {l.get('name')}@{l.get('value')}")

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "version": "v3.2",
        "data_source": "FRED VIX-family (14 series) + Yahoo best-effort VVIX/SKEW",
        "fred_alive": fred_ok,
        "fred_failed": fred_failed,
        "skew": skew_data,
        "vvix": vvix_data,
        "term_structure": term,
        "cross_asset": cross,
        "equity_dispersion": equity_dispersion,
        "underlyings": underlyings,
        "composite_stress_score": score,
        "stress_components": components,
        "regime": regime,
        "alerts": alerts,
        "data_freshness": {"latest_fred_date": spot_dates.get("VIX_30D"),
                            "latest_skew_date": skew_date,
                            "latest_vvix_date": vvix_date},
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=300")

    history.append({
        "generated_at": out["generated_at"],
        "regime": regime, "score": score,
        "skew_value": skew_data.get("value"),
        "skew_pctile": skew_data.get("pctile_252d"),
        "vvix": vvix_data.get("value"),
        "vix_30d": spot.get("VIX_30D"),
        "vix_3m": spot.get("VIX_3M"),
        "term_inverted": term.get("inverted"),
        "equity_dispersion_regime": equity_dispersion.get("regime"),
    })
    history = history[-HISTORY_MAX:]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps({"snapshots": history}, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=600")

    if alerts:
        emoji = {"STRESS_RISK_OFF": "[!]", "DEFENSIVE_BID": "[shield]",
                  "MILDLY_ELEVATED": "[up]", "COMPLACENCY": "[zzz]", "NORMAL": "[ok]"}
        msg = (f"{emoji.get(regime,'[vol]')} <b>VOL-SURFACE</b> [{regime}] score={score}\n"
               + "\n".join(f"- {a}" for a in alerts[:6]))
        maybe_telegram(msg)

    print(f"[vol-surface v3.2] done {out['elapsed_s']}s regime={regime} "
          f"score={score} alerts={len(alerts)} fred_alive={fred_ok}/{len(VOL_INDICES)}")

    return {"statusCode": 200, "body": json.dumps({"ok": True, "regime": regime,
                                                     "score": score, "alerts": len(alerts),
                                                     "fred_alive": fred_ok})}
