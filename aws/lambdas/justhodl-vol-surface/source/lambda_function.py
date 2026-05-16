"""justhodl-vol-surface v3 — Bloomberg OPV-class cross-asset volatility engine.

Prior approaches failed:
  v1 (Polygon /v3/snapshot/options/) — plan lacks options entitlement
  v2 (Yahoo Finance /v7/finance/options/) — 429 rate-limit from AWS IPs

This v3 uses the FRED VIX-family + CBOE SKEW Index. We lose per-strike 25-delta
skew on individual names, but get ~75% of OPV's signal:

  • CROSS-ASSET VOL — VIX (SPX), VXNCLS (NDX), RVXCLS (RUT/IWM), VXEEMCLS
                       (EEM), OVXCLS (oil/USO), GVZCLS (gold/GLD), TYVIXCLS
                       (10Y treasuries/TLT), EVZCLS (euro FX)
  • TERM STRUCTURE — VIX9DCLS (9-day) vs VIXCLS (30-day) vs VXVCLS (3-month):
                      contango (calm) vs backwardation (panic)
  • VOL OF VOL    — VVIXCLS (CBOE volatility of VIX)
  • PUT SKEW      — CBOE SKEW Index from FRED (SKEW). 100 = normal, 130+ = stress
                    (priced as exp((SKEW-100)/10) = lognormal-tail multiplier)
  • REGIME CLASSIFIER — across the above, scored 0-100 stress

Telegram alerts on:
  • Backwardation flip (VIX > VXV)
  • SKEW > 90th-pctile (extreme tail premium)
  • VVIX > 110 (panicked vol)
  • Cross-asset divergence (small-caps stressed but SPX calm = bottom-up rot)

Outputs:
  data/vol-surface.json           — current snapshot + regime
  data/vol-surface-history.json   — 168 hourly snapshots
"""
import json, os, time, math
from datetime import datetime, timezone
from urllib import request, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/vol-surface.json"
S3_HISTORY_KEY = "data/vol-surface-history.json"
HISTORY_MAX = 168

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

VOL_INDICES = {
    "VIX_30D":   {"fred": "VIXCLS",     "for": "SPX",  "tenor_d": 30},
    "VIX_9D":    {"fred": "VXST",       "for": "SPX",  "tenor_d": 9},
    "VIX_3M":    {"fred": "VXVCLS",     "for": "SPX",  "tenor_d": 90},
    "NDX_VOL":   {"fred": "VXNCLS",     "for": "NDX",  "tenor_d": 30},
    "RUT_VOL":   {"fred": "RVXCLS",     "for": "RUT",  "tenor_d": 30},
    "EEM_VOL":   {"fred": "VXEEMCLS",   "for": "EEM",  "tenor_d": 30},
    "OIL_VOL":   {"fred": "OVXCLS",     "for": "USO",  "tenor_d": 30},
    "GOLD_VOL":  {"fred": "GVZCLS",     "for": "GLD",  "tenor_d": 30},
    "TY_VOL":    {"fred": "TYVIXCLS",   "for": "TLT",  "tenor_d": 30},
    "EUR_VOL":   {"fred": "EVZCLS",     "for": "FXE",  "tenor_d": 30},
    "VVIX":      {"fred": "VVIXCLS",    "for": "VIX",  "tenor_d": 30},
}
SKEW_FRED_ID = "SKEWCLS"

s3 = boto3.client("s3", region_name="us-east-1")


def _get_json(url, timeout=15, retries=3):
    last_err = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last_err = e
            time.sleep(0.5 * (i + 1))
    if last_err: raise last_err
    raise RuntimeError("http")


def fred_series(series_id, limit=60):
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


def compute_term_structure(vix_30d, vix_9d, vix_3m):
    out = {"front": vix_9d, "spot": vix_30d, "back": vix_3m}
    if vix_9d and vix_30d and vix_30d > 0:
        out["ratio_9d_30d"] = round(vix_9d / vix_30d, 3)
    if vix_30d and vix_3m and vix_3m > 0:
        out["ratio_30d_3m"] = round(vix_30d / vix_3m, 3)
    if vix_30d and vix_3m and vix_30d > 0:
        out["slope_30d_3m_yearized"] = round((vix_3m - vix_30d) / (90 - 30) * 365 / 100, 4)
    inverted = False
    if vix_9d and vix_30d and vix_9d > vix_30d * 1.05: inverted = True
    if vix_30d and vix_3m and vix_30d > vix_3m * 1.05: inverted = True
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
    eem = spot_values.get("EEM_VOL")
    oil = spot_values.get("OIL_VOL")
    if vix and ndx and vix > 0: out["ndx_vix_ratio"] = round(ndx / vix, 3)
    if vix and rut and vix > 0:
        out["rut_vix_ratio"] = round(rut / vix, 3)
        out["smallcap_stress"] = out["rut_vix_ratio"] > 1.5
    if vix and eem and vix > 0: out["eem_vix_ratio"] = round(eem / vix, 3)
    if vix and oil and vix > 0:
        out["oil_vix_ratio"] = round(oil / vix, 3)
        out["oil_dominant"] = out["oil_vix_ratio"] > 2.5
    return out


def compute_skew_metrics(skew, skew_history):
    if skew is None: return {"value": None, "pctile_252d": None, "regime": "UNKNOWN"}
    pctile = percentile_rank(skew_history[-252:] if skew_history else [], skew)
    if skew >= 140: regime = "EXTREME_TAIL_HEDGING"
    elif skew >= 130: regime = "ELEVATED_PUT_BID"
    elif skew >= 120: regime = "MILD_PUT_BID"
    else: regime = "FLAT_OR_CALL_SKEW"
    tail_mult = round(math.exp((skew - 100) / 10), 3)
    return {"value": skew, "pctile_252d": pctile, "regime": regime, "tail_mult": tail_mult}


def compute_vvix_metrics(vvix, vvix_history):
    if vvix is None: return {"value": None, "pctile_252d": None, "regime": "UNKNOWN"}
    pctile = percentile_rank(vvix_history[-252:] if vvix_history else [], vvix)
    if vvix >= 130: regime = "PANIC_VOL_OF_VOL"
    elif vvix >= 110: regime = "ELEVATED"
    elif vvix >= 90: regime = "NORMAL"
    else: regime = "COMPLACENT"
    return {"value": vvix, "pctile_252d": pctile, "regime": regime}


def composite_stress_score(term, skew_data, vvix_data, cross):
    score = 0.0
    components = {}
    if term.get("inverted"):
        score += 35; components["term_inversion"] = 35
    elif term.get("ratio_30d_3m") and term.get("ratio_30d_3m") > 0.95:
        score += 15; components["term_flattening"] = 15
    sp = skew_data.get("pctile_252d")
    if sp is not None:
        c = round(sp * 0.25, 2); score += c; components["skew_pctile"] = c
    vp = vvix_data.get("pctile_252d")
    if vp is not None:
        c = round(vp * 0.15, 2); score += c; components["vvix_pctile"] = c
    if cross.get("smallcap_stress"):
        score += 10; components["smallcap_stress"] = 10
    if cross.get("oil_dominant"):
        score += 8; components["oil_dominant"] = 8
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
    print(f"[vol-surface v3 FRED] starting {datetime.now(timezone.utc).isoformat()}")
    if not FRED_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FRED_API_KEY not set"})}

    series_data = {}
    history_arrays = {}
    for label, meta in VOL_INDICES.items():
        obs = fred_series(meta["fred"], limit=300)
        series_data[label] = obs
        history_arrays[label] = [o["value"] for o in obs]
    skew_obs = fred_series(SKEW_FRED_ID, limit=300)
    skew_history = [o["value"] for o in skew_obs]

    spot = {label: (obs[0]["value"] if obs else None) for label, obs in series_data.items()}
    spot_dates = {label: (obs[0]["date"] if obs else None) for label, obs in series_data.items()}
    skew_now = skew_obs[0]["value"] if skew_obs else None
    skew_date = skew_obs[0]["date"] if skew_obs else None

    term = compute_term_structure(spot.get("VIX_30D"), spot.get("VIX_9D"), spot.get("VIX_3M"))
    cross = compute_cross_asset(spot)
    skew_data = compute_skew_metrics(skew_now, skew_history)
    vvix_data = compute_vvix_metrics(spot.get("VVIX"), history_arrays.get("VVIX", []))
    score, components = composite_stress_score(term, skew_data, vvix_data, cross)
    regime = classify_regime(score, term)

    underlying_pctiles = {}
    for label, meta in VOL_INDICES.items():
        v = spot.get(label)
        hist = history_arrays.get(label, [])[-252:]
        underlying_pctiles[label] = {
            "for": meta["for"], "tenor_d": meta["tenor_d"],
            "value": v, "date": spot_dates.get(label),
            "pctile_252d": percentile_rank(hist, v),
        }

    history = get_history().get("snapshots", [])
    prior_regime = history[-1].get("regime") if history else None
    alerts = []
    if prior_regime and prior_regime != regime:
        alerts.append(f"Regime flip: {prior_regime} → {regime}")
    if term.get("inverted"):
        alerts.append(f"VIX term-structure INVERTED (9d {spot.get('VIX_9D')} > 30d {spot.get('VIX_30D')} > 3m {spot.get('VIX_3M')})")
    if skew_data.get("pctile_252d") is not None and skew_data["pctile_252d"] >= 90:
        alerts.append(f"SKEW {skew_data['value']} = {skew_data['pctile_252d']}th pctile (extreme tail hedging)")
    if vvix_data.get("value") and vvix_data["value"] >= 110:
        alerts.append(f"VVIX {vvix_data['value']} ELEVATED (vol-of-vol risk)")
    if cross.get("smallcap_stress"):
        alerts.append(f"Small-cap stress: RVX/VIX = {cross.get('rut_vix_ratio')}")
    if cross.get("oil_dominant"):
        alerts.append(f"Oil-dominant vol regime: OVX/VIX = {cross.get('oil_vix_ratio')}")

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "data_source": "FRED VIX-family + CBOE SKEW (replaces broken Polygon/Yahoo options paths)",
        "skew": skew_data,
        "vvix": vvix_data,
        "term_structure": term,
        "cross_asset": cross,
        "underlyings": underlying_pctiles,
        "composite_stress_score": score,
        "stress_components": components,
        "regime": regime,
        "alerts": alerts,
        "data_freshness": {"latest_date": (skew_date or spot_dates.get("VIX_30D"))},
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
    })
    history = history[-HISTORY_MAX:]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps({"snapshots": history}, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=600")

    if alerts:
        emoji = {"STRESS_RISK_OFF": "🚨", "DEFENSIVE_BID": "🛡",
                  "MILDLY_ELEVATED": "↗️", "COMPLACENCY": "😴", "NORMAL": "✓"}
        msg = (f"{emoji.get(regime,'📊')} <b>VOL-SURFACE</b> [{regime}] score={score}\n"
               + "\n".join(f"• {a}" for a in alerts[:6]))
        maybe_telegram(msg)

    print(f"[vol-surface v3] done {out['elapsed_s']}s regime={regime} score={score} alerts={len(alerts)}")

    return {"statusCode": 200, "body": json.dumps({"ok": True, "regime": regime,
                                                     "score": score, "alerts": len(alerts)})}
