"""
justhodl-esi — Citi CESI-equivalent Economic Surprise Index composite.

Different from justhodl-macro-surprise (which logs individual releases):
this is the TIME-DECAYED COMPOSITE across all macro releases over rolling
60-day window. CESI predicts SPY 1-3 months out with documented correlation.

Reads the macro-surprise sidecar + adds:
  • 7d / 30d / 60d rolling Z-score composite
  • Time-decay: weight = exp(-days_since / 30)
  • Regime classification: STRONG_BEAT (>+1σ) / NORMAL (-1..+1σ) / WEAK_MISS (<-1σ)
  • SPY 1mo / 3mo conditional return historical lookup (placeholder for future)

If macro-surprise sidecar exists, reuse its events. Otherwise compute from FRED
release calendar (advanced).

Output: data/esi.json
  • composite_60d / composite_30d / composite_7d
  • regime, n_releases_60d
  • per-category breakdown: growth, inflation, labor, housing, manufacturing
  • z_score history (for chart)

Schedule: cron(30 12 ? * MON-FRI *) — daily 8:30 AM ET after morning releases.
"""
import json
import os
import time
import urllib.request
import math
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/esi.json"
S3_KEY_HIST = "data/esi-history.json"
MACRO_SURPRISE_KEY = "data/macro-surprise.json"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Category map for grouping
CATEGORY_KEYWORDS = {
    "growth":         ["gdp", "retail sales", "industrial production",
                          "income", "personal spending", "consumer"],
    "inflation":      ["cpi", "ppi", "pce", "import price", "wage growth"],
    "labor":          ["nfp", "nonfarm", "claims", "payroll", "unemployment",
                          "jolts", "employment"],
    "housing":        ["housing starts", "building permits", "existing home",
                          "new home", "case-shiller", "mortgage"],
    "manufacturing":  ["ism", "pmi", "durable goods", "factory orders",
                          "philly fed", "empire state", "kansas city fed"],
}

s3 = boto3.client("s3", region_name="us-east-1")


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}"); return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def categorize(release_name):
    name_l = (release_name or "").lower()
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in name_l for kw in kws):
            return cat
    return "other"


def days_ago(date_iso, now):
    try:
        if isinstance(date_iso, str):
            d = datetime.fromisoformat(date_iso[:10]).replace(tzinfo=timezone.utc)
        else:
            d = date_iso
        return (now - d).days
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    print("[esi] starting")

    prior = get_s3_json(S3_KEY, {}) or {}
    macro = get_s3_json(MACRO_SURPRISE_KEY) or {}

    # macro-surprise sidecar (current) has structure:
    #   by_indicator: {"CPI": {"z_score": 0.5, "release_date": "2026-05-12", ...}, ...}
    #   by_category:  {"inflation": {"composite_z": ..., "indicators": [...]}, ...}
    #   composite_z, regime, top_beats, top_misses
    # OR legacy: events: [...]
    events = []
    if isinstance(macro.get("by_indicator"), dict):
        for name, ind in macro["by_indicator"].items():
            if not isinstance(ind, dict): continue
            ev = dict(ind)
            ev["name"] = ev.get("name") or name
            events.append(ev)
    if not events:
        events = (macro.get("events") or macro.get("releases")
                    or macro.get("history") or [])
    if not events and isinstance(macro.get("recent_releases"), list):
        events = macro["recent_releases"]

    now = datetime.now(timezone.utc)
    cutoff_60 = now - timedelta(days=60)

    # Filter to last 60d
    recent = []
    for e in events:
        if not isinstance(e, dict): continue
        dt = (e.get("release_date") or e.get("date") or e.get("ts")
              or e.get("latest_release_date") or e.get("last_release")
              or e.get("latest_date"))
        d_ago = days_ago(dt, now)
        if d_ago is None or d_ago > 60 or d_ago < 0: continue
        # Get z-score (or compute from beat/miss if available)
        z = (e.get("z_score") or e.get("z") or e.get("surprise_z")
             or e.get("latest_z") or e.get("z_latest"))
        if z is None:
            actual = e.get("actual") or e.get("actual_value") or e.get("latest_value")
            cons = e.get("consensus") or e.get("forecast") or e.get("expected")
            if actual is not None and cons is not None and cons != 0:
                rel = (actual - cons) / abs(cons)
                z = max(-3, min(3, rel * 5))
        if z is None: continue
        # Use the pre-categorized field if present (macro-surprise pre-categorizes),
        # otherwise fall back to keyword categorization
        cat_raw = e.get("category")
        if cat_raw:
            cat_norm = {"INFLATION": "inflation", "GROWTH": "growth",
                         "CONSUMER": "growth", "LEADING": "growth",
                         "EMPLOYMENT": "labor", "HOUSING": "housing",
                         "EXTERNAL": "other"}.get(str(cat_raw).upper(),
                                                    str(cat_raw).lower())
            category = cat_norm
        else:
            category = categorize(e.get("name") or e.get("indicator"))
        recent.append({
            "name": e.get("name") or e.get("indicator") or "?",
            "date": dt[:10] if isinstance(dt, str) else str(dt),
            "z_score": float(z),
            "days_ago": d_ago,
            "category": category,
        })

    if not recent:
        # No data — write empty sidecar with explanatory note
        output = {
            "schema_version": "1.0",
            "method": "esi_v1",
            "generated_at": now.isoformat(),
            "err": "no macro-surprise data available",
            "n_events_60d": 0,
        }
        put_s3_json(S3_KEY, output)
        return {"statusCode": 200, "body": json.dumps({"ok": True, "n_events": 0})}

    # Compute time-decayed composite
    def composite(window_days):
        in_window = [e for e in recent if e["days_ago"] <= window_days]
        if not in_window: return None, 0
        # exp decay with 30-day half life equivalent
        total_w = sum(math.exp(-e["days_ago"] / 30) for e in in_window)
        weighted = sum(e["z_score"] * math.exp(-e["days_ago"] / 30) for e in in_window)
        if total_w == 0: return None, 0
        return round(weighted / total_w, 3), len(in_window)

    c7, n7 = composite(7)
    c30, n30 = composite(30)
    c60, n60 = composite(60)

    # Per-category
    by_cat = {}
    for cat in list(CATEGORY_KEYWORDS.keys()) + ["other"]:
        cat_events = [e for e in recent if e["category"] == cat]
        if not cat_events: continue
        avg = sum(e["z_score"] for e in cat_events) / len(cat_events)
        by_cat[cat] = {"composite": round(avg, 3), "n": len(cat_events),
                         "latest": cat_events[0] if cat_events else None}

    # Regime
    c30v = c30 or 0
    if c30v > 1: regime = "STRONG_BEATS"
    elif c30v > 0.5: regime = "BEATING"
    elif c30v > -0.5: regime = "NORMAL"
    elif c30v > -1: regime = "MISSING"
    else: regime = "WEAK_MISSES"

    output = {
        "schema_version": "1.0",
        "method": "esi_v1",
        "generated_at": now.isoformat(),
        "composite_7d": c7,
        "composite_30d": c30,
        "composite_60d": c60,
        "n_events_7d": n7,
        "n_events_30d": n30,
        "n_events_60d": n60,
        "regime": regime,
        "regime_interpretation": (
            "Economic data running above consensus — bullish for equity risk; "
            "rate cuts less likely." if c30v > 0.5 else
            "Economic data missing expectations — defensive positioning; "
            "Fed cuts more likely." if c30v < -0.5 else
            "Economic data roughly in line with consensus."
        ),
        "by_category": by_cat,
        "top_recent_beats": sorted(recent, key=lambda x: -x["z_score"])[:5],
        "top_recent_misses": sorted(recent, key=lambda x: x["z_score"])[:5],
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[esi] c30={c30} regime={regime} n_events={n30}")

    # History
    try:
        hist = get_s3_json(S3_KEY_HIST, {"snapshots": []}) or {"snapshots": []}
        hist["snapshots"].append({"ts": now.isoformat(), "c30": c30, "c7": c7, "regime": regime})
        hist["snapshots"] = hist["snapshots"][-180:]
        put_s3_json(S3_KEY_HIST, {**hist, "updated_at": now.isoformat()})
    except Exception as e:
        print(f"[history] err: {e}")

    # Alerts
    try:
        prior_regime = prior.get("regime")
        if prior_regime and prior_regime != regime:
            maybe_telegram(
                f"📊 <b>ESI REGIME CHANGE</b>\n"
                f"<b>{prior_regime} → {regime}</b>\n"
                f"30d composite: {c30}  · 7d: {c7}\n"
                f"{output['regime_interpretation']}"
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"ok": True, "c30": c30, "regime": regime, "n": n30}),
    }
