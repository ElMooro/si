"""
JUSTHODL STRESS INDEX (JSI) — unified cross-system financial-stress gauge.

DESIGN (rigorous continuity):
  • SPINE — a set of FRED components that each reach back to ~1990 (VIX, Chicago-Fed
    NFCI, KC-Fed KCFSI, St-Louis FSI, yield-curve inversion, HY credit OAS, TED-proxy).
    Each is z-scored on its OWN FULL HISTORY and mapped to a 0-100 stress sub-score, then
    blended into the JSI. The SAME computation runs from 1990-01 to today, so "current =
    Nth percentile since 1990" is a true, continuous statement — not a spliced series.
  • OVERLAY — the 12 live JustHodl stress engine feeds (global-stress, tail-risk,
    bank-stress, crisis-composite, eurodollar-stress/plumbing, CISS, risk-regime,
    fx-regime, crisis-canaries, vvix-vov, tail-hedge) normalized to 0-100 and shown as a
    contemporary component breakdown. They enrich the PRESENT reading and feed the
    forward-IC calibrator, but never reach into history (they don't exist pre-2025), so
    the 1990 comparison stays honest.

OUTPUTS:
  data/jsi.json           — live JSI score, components (spine + overlay), percentile-in-history,
                            regime, crisis markers, and the full 1990→today daily series.
  data/jsi-history.json   — compact {date, jsi} daily series (for lightweight chart loads).
"""
import math
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

VERSION = "1.5.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/jsi.json"
HIST_KEY = "data/jsi-history.json"

FRED_KEY = os.environ.get("FRED_KEY", "") or os.environ.get("FRED_API_KEY", "") or "2f057499936072679d8843d7fce99989"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
HISTORY_START = "1990-01-01"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def load_calibrated_spine_weights():
    """Read empirically-calibrated spine weights from SSM (written by the JSI calibrator).
    Falls back to the hardcoded SPINE weights if not yet calibrated."""
    try:
        p = ssm.get_parameter(Name="/justhodl/jsi/spine-weights")
        w = (json.loads(p["Parameter"]["Value"]) or {}).get("weights") or {}
        if w and all(sid in w for sid, *_ in SPINE):
            return {sid: float(w[sid]) for sid, *_ in SPINE}, "calibrated"
    except Exception:
        pass
    return {sid: wt for sid, _, _, wt, _ in SPINE}, "prior"

# ── SPINE: FRED components with real inception. Each tuple:
#    (series_id, label, polarity, weight, inception, mode)
#    polarity +1 = higher (transformed) value means MORE stress; -1 = inverse.
#    mode "level" = z-score the level (mean-reverting series: VIX, spreads, curve).
#    mode "chg"   = z-score the 3-month CHANGE (trending series: reserves, Fed BS —
#                   the stress is in the DRAINING, not the secular level). Falling
#                   reserves/BS = liquidity withdrawal = stress, so polarity -1 on the change.
SPINE = [
    ("VIXCLS",       "Equity volatility (VIX)",        +1, 0.20, "1990-01-02", "level"),
    ("NFCI",         "Chicago Fed NFCI",               +1, 0.18, "1971-01-08", "level"),
    ("KCFSI",        "KC Fed Financial Stress",        +1, 0.13, "1990-02-01", "level"),
    ("STLFSI4",      "St. Louis Fed Financial Stress", +1, 0.12, "1993-12-31", "level"),
    ("BAMLH0A0HYM2", "High-yield credit OAS",          +1, 0.14, "1996-12-31", "level"),
    ("T10Y2Y",       "Yield-curve (10Y-2Y)",           -1, 0.07, "1976-06-01", "level"),
    ("BAMLC0A0CM",   "Investment-grade OAS",           +1, 0.06, "1996-12-31", "level"),
    ("WRESBAL",      "Bank Reserves (draining)",       -1, 0.06, "1959-01-01", "chg"),
    ("WALCL",        "Fed Balance Sheet (QT)",         -1, 0.04, "2002-12-18", "chg"),
]

# ── OVERLAY: live JustHodl feeds (contemporary enrichment). Each tuple:
#    (s3_key, dotted_field, label, transform)  transform maps raw → 0-100 stress.
OVERLAY = [
    ("data/global-stress.json",      "global_stress_index",    "Global Stress Index",   "id"),
    ("data/tail-risk.json",          "system_tail_gauge",      "Tail Risk",             "id"),
    ("data/bank-stress.json",        "bank_stress_score",      "Bank Stress",           "id"),
    ("data/crisis-composite.json",   "master_crisis_score",    "Crisis Composite",      "id"),
    ("data/eurodollar-stress.json",  "composite_score",        "Eurodollar Stress",     "id"),
    ("data/eurodollar-plumbing.json","stress_score",           "Funding Plumbing",      "id"),
    ("data/ciss-stress.json",        "ea_composite",           "ECB CISS",              "x100"),
    ("data/crisis-canaries.json",    "composite_score",        "Crisis Canaries",       "id"),
    ("data/risk-regime.json",        "risk_regime_score",      "Risk Regime (RORO)",    "roro"),
    ("data/polygon-fx-regime.json",  "fx_roro.fx_roro_score",  "FX Risk-Off",           "roro"),
    ("data/vvix-vov-regime.json",    "signal_strength",        "Vol-of-Vol",            "x100"),
    ("data/tail-hedge.json",         "severity",               "Tail Hedge Severity",   "id"),
    ("data/euro-fragmentation.json", "fragmentation.score_0_100", "Euro Fragmentation (BTP-Bund)", "id"),
    ("data/carry-surface.json",      "unwind_overlay.cohort_fragility", "Carry-Unwind Fragility", "id"),
    ("data/global-tide.json",        "risk.global_risk_0_100", "Global Risk Tide",      "id"),
    ("data/risk-ratios.json",        "hyg_lqd.latest",         "HYG/LQD Credit Risk",   "ratio_inv"),
]

# Historical crisis windows for chart annotation.
CRISIS_MARKERS = [
    ("1990-08", "Gulf War / recession"),
    ("1994-02", "Bond massacre"),
    ("1997-07", "Asian crisis"),
    ("1998-08", "LTCM / Russia"),
    ("2000-03", "Dot-com peak"),
    ("2001-09", "9/11"),
    ("2007-08", "Quant quake"),
    ("2008-09", "Lehman / GFC"),
    ("2010-05", "Flash crash / EU debt"),
    ("2011-08", "US downgrade / EU crisis"),
    ("2015-08", "China deval / VIX spike"),
    ("2018-02", "Volmageddon"),
    ("2018-12", "Q4 selloff"),
    ("2020-03", "COVID crash"),
    ("2022-06", "Rate-shock / bear"),
    ("2023-03", "SVB / regional banks"),
]


# ──────────────────────────────────────────────────────────────────────
def http_json(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-jsi/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fred_full(series_id):
    """Full daily/weekly/monthly history from 1990. Returns [(date, value), ...] sorted."""
    if not FRED_KEY:
        return []
    url = ("%s?series_id=%s&api_key=%s&file_type=json&observation_start=%s"
           % (FRED_BASE, series_id, FRED_KEY, HISTORY_START))
    try:
        d = http_json(url)
    except Exception:
        return []
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v not in (".", "", None):
            try:
                out.append((o["date"], float(v)))
            except (ValueError, KeyError):
                pass
    out.sort(key=lambda x: x[0])
    return out


def _read_s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode())
    except Exception:
        return None


def _dig(o, path):
    cur = o
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return None
    return cur


def _mean(xs): return sum(xs) / len(xs) if xs else 0.0
def _std(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def z_to_stress(z):
    """Map a z-score to a 0-100 stress sub-score via a logistic squash.
    z=0 → 50 (normal), z=+2 → ~88, z=+3 → ~95, z=-2 → ~12. Bounded, monotone."""
    import math
    return round(100.0 / (1.0 + math.exp(-1.1 * z)), 2)


def build_spine_series():
    """For each spine component, pull full FRED history, forward-fill to a common daily
    grid, transform (level or 3-month change), z-score on its OWN full history, map to
    0-100, and blend into a daily JSI spine series.
    Returns (dates, jsi_series, per_component_latest, component_meta, weight_mode)."""
    cal_weights, weight_mode = load_calibrated_spine_weights()
    comps = {}
    for series_id, label, pol, wt, inception, mode in SPINE:
        obs = fred_full(series_id)
        if len(obs) < 50:
            continue
        comps[series_id] = {"label": label, "pol": pol,
                            "wt": cal_weights.get(series_id, wt),
                            "inception": inception, "mode": mode, "obs": obs}

    if not comps:
        return [], [], {}, [], weight_mode

    all_dates = sorted({d for c in comps.values() for d, _ in c["obs"]})
    CHG_LAG = 63  # ~3 trading months for the "chg" transform

    # Forward-fill onto the common grid, apply transform, then z-score on own history.
    for c in comps.values():
        m = dict(c["obs"])
        filled, last = [], None
        for d in all_dates:
            if d in m:
                last = m[d]
            filled.append(last)
        # transform: level (as-is) or chg (value now minus value CHG_LAG steps ago)
        if c["mode"] == "chg":
            xform = []
            for i, v in enumerate(filled):
                if v is None or i < CHG_LAG or filled[i - CHG_LAG] is None:
                    xform.append(None)
                else:
                    xform.append(v - filled[i - CHG_LAG])
            c["xform"] = xform
        else:
            c["xform"] = filled
        present = [v for v in c["xform"] if v is not None]
        c["mu"] = _mean(present)
        c["sd"] = _std(present) or 1.0

    jsi_series = []
    for i, d in enumerate(all_dates):
        num, wsum = 0.0, 0.0
        for c in comps.values():
            v = c["xform"][i]
            if v is None:
                continue
            z = (v - c["mu"]) / c["sd"] * c["pol"]
            sub = z_to_stress(z)
            num += sub * c["wt"]
            wsum += c["wt"]
        if wsum > 0:
            jsi_series.append((d, round(num / wsum, 2)))

    # Latest component readings for the breakdown.
    latest = {}
    for sid, c in comps.items():
        present = [(d, v) for d, v in zip(all_dates, c["xform"]) if v is not None]
        if present:
            ld, lv = present[-1]
            z = (lv - c["mu"]) / c["sd"] * c["pol"]
            latest[sid] = {"label": c["label"], "raw": round(lv, 3),
                           "stress": z_to_stress(z), "z": round(z, 2),
                           "as_of": ld, "weight": c["wt"], "inception": c["inception"],
                           "mode": c["mode"]}

    meta = [{"series": sid, "label": c["label"], "weight": round(c["wt"], 4),
             "inception": c["inception"], "mode": c["mode"]} for sid, c in comps.items()]
    dates = [d for d, _ in jsi_series]
    vals = [v for _, v in jsi_series]
    return dates, vals, latest, meta, weight_mode


def build_overlay():
    """Read the 12 live feeds, normalize each to 0-100 stress. Returns list of components
    and their mean (the contemporary enrichment score)."""
    out = []
    for key, field, label, tf in OVERLAY:
        o = _read_s3_json(key)
        if not o:
            out.append({"key": key, "label": label, "stress": None, "status": "unavailable"})
            continue
        raw = _dig(o, field)
        if not isinstance(raw, (int, float)):
            out.append({"key": key, "label": label, "stress": None, "status": "no_score"})
            continue
        if tf == "id":
            s = max(0.0, min(100.0, float(raw)))
        elif tf == "x100":
            s = max(0.0, min(100.0, float(raw) * 100.0))
        elif tf == "roro":
            # RORO score -100(risk-off/stress)..+100(risk-on) → invert to 0-100 stress
            s = max(0.0, min(100.0, (100.0 - float(raw)) / 2.0))
        elif tf == "ratio_inv":
            # A credit-risk ratio (e.g. HYG/LQD): FALLING = risk-off = stress. Z-score on
            # the feed's own history, INVERT the sign, logistic-map to 0-100.
            hist_path = field.rsplit(".", 1)[0] + ".history" if "." in field else "history"
            hist = _dig(o, hist_path) or _dig(o, "history")
            vals = []
            if isinstance(hist, list):
                for pt in hist:
                    if isinstance(pt, (list, tuple)) and len(pt) >= 2 and isinstance(pt[1], (int, float)):
                        vals.append(float(pt[1]))
                    elif isinstance(pt, (int, float)):
                        vals.append(float(pt))
            if len(vals) >= 20:
                mu, sd = _mean(vals), _std(vals) or 1e-9
                z = (float(raw) - mu) / sd * (-1.0)   # invert: low ratio → +z → stress
                s = max(0.0, min(100.0, 100.0 / (1.0 + math.exp(-1.1 * z))))
            else:
                s = 50.0  # insufficient history → neutral
        else:
            s = max(0.0, min(100.0, float(raw)))
        out.append({"key": key, "label": label, "stress": round(s, 1),
                    "raw": round(float(raw), 3), "status": "live"})

    # ── Brain-directed FRED-computed plumbing signals (RRP drain + SOFR spike) ──
    for comp in build_plumbing_signals():
        out.append(comp)

    live = [c["stress"] for c in out if c.get("stress") is not None]
    overlay_score = round(_mean(live), 1) if live else None
    return out, overlay_score, len(live)


def build_plumbing_signals():
    """Compute the operator's brain-directed funding-plumbing risk signals directly from
    FRED, with regime-shift-aware transforms:
      · RRP Drain    — RRPONTSYD is a POLICY level ($2.5T in 2021-23 was not stress), so we
                       z-score its 63-day CHANGE. Rising RRP = cash leaving markets into the
                       Fed = risk-off drain = stress.
      · SOFR Spike   — SOFR alone is just the policy rate; the STRESS is SOFR trading ABOVE
                       its floor (IORB). We z-score the SOFR-IORB spread on its own history;
                       a positive spike (Sept-2019-style) = acute funding stress.
    Both overlay-only (SOFR series starts 2018)."""
    out = []

    # RRP drain
    rrp = fred_full("RRPONTSYD")
    if len(rrp) > 100:
        vals = [v for _, v in rrp]
        chg = [vals[i] - vals[i - 63] for i in range(63, len(vals))]
        if len(chg) > 30:
            mu, sd = _mean(chg), _std(chg) or 1e-9
            z = (chg[-1] - mu) / sd            # rising change → +z → stress
            s = max(0.0, min(100.0, 100.0 / (1.0 + math.exp(-1.1 * z))))
            out.append({"key": "fred:RRPONTSYD", "label": "RRP Drain (liquidity)",
                        "stress": round(s, 1), "raw": round(chg[-1], 1), "status": "live"})

    # SOFR spike (SOFR - IORB spread)
    sofr = dict(fred_full("SOFR"))
    iorb = dict(fred_full("IORB"))
    if sofr and iorb:
        common = sorted(set(sofr) & set(iorb))
        spread = [(d, sofr[d] - iorb[d]) for d in common]
        if len(spread) > 60:
            sv = [v for _, v in spread]
            mu, sd = _mean(sv), _std(sv) or 1e-9
            z = (sv[-1] - mu) / sd             # positive spike → +z → stress
            s = max(0.0, min(100.0, 100.0 / (1.0 + math.exp(-1.1 * z))))
            out.append({"key": "fred:SOFR-IORB", "label": "SOFR Spike (funding)",
                        "stress": round(s, 1), "raw": round(sv[-1] * 100, 1),
                        "status": "live"})  # raw in bps
    return out


def percentile_of(value, series_vals):
    if not series_vals:
        return None
    below = sum(1 for v in series_vals if v <= value)
    return round(below / len(series_vals) * 100.0, 1)


def regime_from(score):
    if score >= 75:  return "CRISIS"
    if score >= 60:  return "STRESS"
    if score >= 45:  return "ELEVATED"
    if score >= 30:  return "NORMAL"
    return "CALM"


OVERLAY_HIST_KEY = "data/jsi-overlay-history.json"


def _spy_close_today():
    """Latest SPY close from FRED SP500 series (daily, free)."""
    obs = fred_full("SP500")
    if obs:
        return obs[-1][1]
    return None


def _write_overlay_snapshot(overlay_components, jsi_now, jsi_spine):
    """Append today's overlay feed scores + SPY close to jsi-overlay-history.json.
    One row per date (idempotent within a day). Capped at ~500 rows (~2y at daily)."""
    today = datetime.now(timezone.utc).date().isoformat()
    spy = _spy_close_today()
    feeds = {c["label"]: c["stress"] for c in overlay_components
             if c.get("stress") is not None}
    row = {"date": today, "spy_close": spy, "jsi": jsi_now,
           "jsi_spine": jsi_spine, "feeds": feeds}
    try:
        prior = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OVERLAY_HIST_KEY)["Body"].read())
        rows = prior.get("snapshots") or []
    except Exception:
        rows = []
    rows = [r for r in rows if r.get("date") != today]  # replace today's
    rows.append(row)
    rows = sorted(rows, key=lambda r: r.get("date") or "")[-500:]
    s3.put_object(Bucket=S3_BUCKET, Key=OVERLAY_HIST_KEY,
                  Body=json.dumps({"updated": datetime.now(timezone.utc).isoformat(),
                                   "snapshots": rows}, default=str).encode(),
                  ContentType="application/json")


def lambda_handler(event=None, context=None):
    t0 = time.time()

    # 1) Historical spine (1990 → today), identical method across all eras.
    dates, vals, spine_latest, spine_meta, weight_mode = build_spine_series()
    if not vals:
        payload = {"version": VERSION, "ok": False,
                   "error": "spine build failed — FRED unavailable",
                   "generated_at": datetime.now(timezone.utc).isoformat()}
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(payload).encode(), ContentType="application/json")
        return {"statusCode": 500, "body": json.dumps(payload)}

    jsi_spine = vals[-1]
    pctile = percentile_of(jsi_spine, vals)

    # 2) Live overlay (contemporary enrichment).
    overlay_components, overlay_score, n_overlay = build_overlay()

    # 3) Blended present reading: spine is the historical anchor; nudge modestly toward
    #    the live overlay when available (70% spine / 30% overlay), but the HISTORICAL
    #    percentile is always measured against the pure-spine series for continuity.
    if overlay_score is not None:
        jsi_now = round(0.70 * jsi_spine + 0.30 * overlay_score, 2)
    else:
        jsi_now = jsi_spine

    # Historical extremes for context.
    hi = max(vals); lo = min(vals)
    hi_date = dates[vals.index(hi)]; lo_date = dates[vals.index(lo)]

    # Downsample the full series for the chart payload (weekly), keep full in history file.
    weekly = [(dates[i], vals[i]) for i in range(0, len(vals), 5)]
    if weekly and weekly[-1][0] != dates[-1]:
        weekly.append((dates[-1], vals[-1]))

    generated = datetime.now(timezone.utc).isoformat()
    payload = {
        "version": VERSION, "ok": True, "generated_at": generated,
        "elapsed_s": round(time.time() - t0, 2),
        "jsi": jsi_now,
        "jsi_spine": jsi_spine,
        "overlay_score": overlay_score,
        "regime": regime_from(jsi_now),
        "percentile_since_1990": pctile,
        "history_span": {"start": dates[0], "end": dates[-1], "n": len(vals)},
        "historical_extremes": {
            "max": {"value": hi, "date": hi_date},
            "min": {"value": lo, "date": lo_date},
        },
        "spine_components": spine_latest,
        "spine_meta": spine_meta,
        "spine_weight_mode": weight_mode,
        "overlay_components": overlay_components,
        "n_overlay_live": n_overlay,
        "crisis_markers": [{"date": d, "label": l} for d, l in CRISIS_MARKERS],
        "series_weekly": [{"d": d, "v": v} for d, v in weekly],
        "methodology": {
            "spine": "FRED components z-scored on own full history since 1990, logistic-mapped to 0-100, weighted-blended. Same computation across all eras → continuous percentile.",
            "overlay": "12 live JustHodl stress feeds normalized to 0-100; contemporary enrichment only, never reaches into history.",
            "present_score": "0.70*spine + 0.30*overlay (spine-only when overlay unavailable); historical percentile always vs pure-spine series.",
        },
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=300, public")

    # Compact full-resolution history for lightweight chart loads.
    s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                  Body=json.dumps({"generated_at": generated,
                                   "series": [{"d": d, "v": v} for d, v in zip(dates, vals)]},
                                  default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600, public")

    # Append today's overlay snapshot for the forward-IC calibrator. Records the daily
    # per-feed overlay stress scores + the current SPY close, so the calibrator can pair
    # each day's feed readings with forward SPY drawdown once the window matures.
    try:
        _write_overlay_snapshot(overlay_components, jsi_now, jsi_spine)
    except Exception as e:
        print(f"[jsi] overlay snapshot write failed: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "jsi": jsi_now, "spine": jsi_spine, "overlay": overlay_score,
        "regime": payload["regime"], "pctile_since_1990": pctile,
        "n_hist": len(vals), "span": f"{dates[0]}→{dates[-1]}",
        "n_overlay": n_overlay, "elapsed_s": payload["elapsed_s"],
    })}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
