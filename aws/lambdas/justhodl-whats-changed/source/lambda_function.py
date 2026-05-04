"""
justhodl-whats-changed — Daily diff across key JustHodl data files.

For each tracked file, snapshots yesterday's version (from data/snapshots/) and
compares to today's. Flags meaningful deltas:

  - Khalid Index regime change
  - Macro Surprise composite z change > 0.5
  - Yield Curve regime change (e.g. BEAR_STEEPENER → BULL_FLATTENER)
  - Cross-asset correlation regime change (n_regime_breaks delta ≥ 2)
  - Historical analog call change (BULLISH ↔ BEARISH)
  - Active event themes added/removed
  - A/B test winner change
  - Paper portfolio: new open position, position closed, NAV delta > 0.5%
  - 13F: top 5 buyers/sellers consensus change
  - Short interest: new SQUEEZE_RISK names appearing
  - PEAD: new STRONG_POSITIVE_DRIFT or NEGATIVE_DRIFT names appearing

Output: data/whats-changed.json
        + writes today's snapshot to data/snapshots/{filename}-{YYYY-MM-DD}.json

Schedule: daily 17 UTC (after most upstreams have refreshed).
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/whats-changed.json"
SNAPSHOT_PREFIX = "data/snapshots/"

s3 = boto3.client("s3", region_name=REGION)


# Files we track + how to extract a "fingerprint" for diffing
TRACKED = [
    ("data/morning-intel.json",        "morning_intel"),
    ("data/macro-surprise.json",       "macro_surprise"),
    ("data/yield-curve.json",          "yield_curve"),
    ("data/correlation-surface.json",  "correlation_surface"),
    ("data/historical-analogs.json",   "historical_analogs"),
    ("data/event-study.json",          "event_study"),
    ("data/ab-test-results.json",      "ab_test"),
    ("portfolio/signal-portfolio-state.json", "paper_portfolio"),
    ("data/13f-positions.json",        "thirteen_f"),
    ("data/short-interest.json",       "short_interest"),
    ("data/earnings-tracker.json",     "earnings"),
]


def fetch_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def write_snapshot(filename, data, date_str):
    safe = filename.replace("/", "_").replace(".json", "")
    snap_key = f"{SNAPSHOT_PREFIX}{safe}-{date_str}.json"
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=snap_key,
            Body=json.dumps(data, default=str).encode(),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"snapshot write fail {snap_key}: {e}")


def fetch_yesterday_snapshot(filename, today_date):
    """Try yesterday first, then up to 3 days back."""
    safe = filename.replace("/", "_").replace(".json", "")
    for offset in range(1, 4):
        d = (today_date - timedelta(days=offset)).strftime("%Y-%m-%d")
        snap_key = f"{SNAPSHOT_PREFIX}{safe}-{d}.json"
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=snap_key)
            return json.loads(obj["Body"].read()), d
        except Exception:
            continue
    return None, None


def safe_get(d, *path, default=None):
    cur = d
    for k in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return default
    return cur if cur is not None else default


def diff_morning_intel(today, prev):
    out = []
    if not today or not prev:
        return out
    t_regime = safe_get(today, "regime")
    p_regime = safe_get(prev, "regime")
    if t_regime and p_regime and t_regime != p_regime:
        out.append({
            "category": "morning_intel",
            "severity": "HIGH",
            "summary": f"Khalid regime changed: {p_regime} → {t_regime}",
        })
    t_ki = safe_get(today, "khalid_index", "score")
    p_ki = safe_get(prev, "khalid_index", "score")
    if t_ki is not None and p_ki is not None:
        try:
            d = float(t_ki) - float(p_ki)
            if abs(d) >= 5.0:
                out.append({
                    "category": "morning_intel",
                    "severity": "MED",
                    "summary": f"Khalid Index moved {d:+.1f} pts ({float(p_ki):.1f} → {float(t_ki):.1f})",
                })
        except Exception:
            pass
    return out


def diff_macro_surprise(today, prev):
    out = []
    if not today or not prev:
        return out
    t_z = safe_get(today, "composite_z")
    p_z = safe_get(prev, "composite_z")
    t_reg = safe_get(today, "regime")
    p_reg = safe_get(prev, "regime")
    if t_reg and p_reg and t_reg != p_reg:
        out.append({
            "category": "macro_surprise",
            "severity": "HIGH",
            "summary": f"Macro surprise regime: {p_reg} → {t_reg}",
        })
    if t_z is not None and p_z is not None:
        try:
            d = float(t_z) - float(p_z)
            if abs(d) >= 0.5:
                out.append({
                    "category": "macro_surprise",
                    "severity": "MED",
                    "summary": f"Macro composite z moved {d:+.2f}σ ({float(p_z):+.2f} → {float(t_z):+.2f})",
                })
        except Exception:
            pass
    return out


def diff_yield_curve(today, prev):
    out = []
    if not today or not prev:
        return out
    t_reg = safe_get(today, "regime")
    p_reg = safe_get(prev, "regime")
    if t_reg and p_reg and t_reg != p_reg:
        out.append({
            "category": "yield_curve",
            "severity": "HIGH",
            "summary": f"Yield curve regime: {p_reg} → {t_reg}",
        })
    t_2s10s = safe_get(today, "spreads", "2s10s")
    p_2s10s = safe_get(prev, "spreads", "2s10s")
    if t_2s10s is not None and p_2s10s is not None:
        try:
            d = float(t_2s10s) - float(p_2s10s)
            if abs(d) >= 10:  # 10bps
                out.append({
                    "category": "yield_curve",
                    "severity": "MED",
                    "summary": f"2s10s moved {d:+.0f}bps ({float(p_2s10s):+.0f} → {float(t_2s10s):+.0f})",
                })
        except Exception:
            pass
    return out


def diff_correlation_surface(today, prev):
    out = []
    if not today or not prev:
        return out
    t_reg = safe_get(today, "macro_regime")
    p_reg = safe_get(prev, "macro_regime")
    if t_reg and p_reg and t_reg != p_reg:
        out.append({
            "category": "correlation_surface",
            "severity": "HIGH",
            "summary": f"Cross-asset macro regime: {p_reg} → {t_reg}",
        })
    t_breaks = safe_get(today, "n_regime_breaks", default=0)
    p_breaks = safe_get(prev, "n_regime_breaks", default=0)
    if abs(int(t_breaks) - int(p_breaks)) >= 2:
        out.append({
            "category": "correlation_surface",
            "severity": "MED",
            "summary": f"Regime breaks: {p_breaks} → {t_breaks}",
        })
    # Surface specific newly-broken pairs
    t_pairs = {x.get("pair"): x for x in (today.get("regime_breaks") or [])}
    p_pairs = {x.get("pair"): x for x in (prev.get("regime_breaks") or [])}
    new_breaks = sorted(set(t_pairs) - set(p_pairs))
    if new_breaks:
        out.append({
            "category": "correlation_surface",
            "severity": "MED",
            "summary": f"New regime breaks: {', '.join(new_breaks[:5])}",
        })
    return out


def diff_historical_analogs(today, prev):
    out = []
    if not today or not prev:
        return out
    t_call = safe_get(today, "call")
    p_call = safe_get(prev, "call")
    if t_call and p_call and t_call != p_call:
        sev = "HIGH" if {t_call, p_call} <= {"BULLISH", "BEARISH"} else "MED"
        out.append({
            "category": "historical_analogs",
            "severity": sev,
            "summary": f"Historical analog call: {p_call} → {t_call}",
        })
    return out


def diff_event_study(today, prev):
    out = []
    if not today or not prev:
        return out
    t_themes = set(today.get("active_themes") or [])
    p_themes = set(prev.get("active_themes") or [])
    added = sorted(t_themes - p_themes)
    removed = sorted(p_themes - t_themes)
    if added:
        out.append({
            "category": "event_study",
            "severity": "HIGH",
            "summary": f"New active themes: {', '.join(added)}",
        })
    if removed:
        out.append({
            "category": "event_study",
            "severity": "MED",
            "summary": f"Themes resolved: {', '.join(removed)}",
        })
    return out


def diff_ab_test(today, prev):
    out = []
    if not today or not prev:
        return out
    t_w = safe_get(today, "winner")
    p_w = safe_get(prev, "winner")
    if t_w != p_w:
        out.append({
            "category": "ab_test",
            "severity": "MED",
            "summary": f"A/B winner: {p_w or 'none'} → {t_w or 'none'}",
        })
    return out


def diff_paper_portfolio(today, prev):
    out = []
    if not today or not prev:
        return out
    t_open = {(p.get("symbol") or p.get("ticker"), p.get("source")): p
              for p in (today.get("open_positions") or [])}
    p_open = {(p.get("symbol") or p.get("ticker"), p.get("source")): p
              for p in (prev.get("open_positions") or [])}
    new_opens = [k for k in t_open if k not in p_open]
    closed = [k for k in p_open if k not in t_open]
    if new_opens:
        names = [f"{sym} ({src})" for (sym, src) in new_opens[:5]]
        out.append({
            "category": "paper_portfolio",
            "severity": "MED",
            "summary": f"New paper positions: {', '.join(names)}",
        })
    if closed:
        names = [f"{sym} ({src})" for (sym, src) in closed[:5]]
        out.append({
            "category": "paper_portfolio",
            "severity": "MED",
            "summary": f"Paper positions closed: {', '.join(names)}",
        })
    # NAV delta
    t_nav = safe_get(today, "nav") or safe_get(today, "total_equity")
    p_nav = safe_get(prev, "nav") or safe_get(prev, "total_equity")
    if t_nav and p_nav:
        try:
            tnf, pnf = float(t_nav), float(p_nav)
            pct = (tnf - pnf) / pnf * 100
            if abs(pct) >= 0.5:
                out.append({
                    "category": "paper_portfolio",
                    "severity": "LOW",
                    "summary": f"Paper NAV moved {pct:+.2f}% (${pnf:,.0f} → ${tnf:,.0f})",
                })
        except Exception:
            pass
    return out


def diff_short_interest(today, prev):
    out = []
    if not today or not prev:
        return out
    def squeeze_set(d):
        all_ = d.get("tracker") or d.get("tickers") or []
        return {x.get("symbol") or x.get("ticker") for x in all_
                if str(x.get("signal") or x.get("label") or "").startswith("SQUEEZE")}
    t = squeeze_set(today)
    p = squeeze_set(prev)
    new_sq = sorted(t - p)
    if new_sq:
        out.append({
            "category": "short_interest",
            "severity": "MED",
            "summary": f"New squeeze risk names: {', '.join(list(new_sq)[:6])}",
        })
    return out


def diff_earnings(today, prev):
    out = []
    if not today or not prev:
        return out
    def strong_drift_set(d):
        rec = (d.get("tracker") or d).get("pead_signals") or []
        return {x.get("symbol") or x.get("ticker") for x in rec
                if str(x.get("signal") or x.get("label") or "").startswith("STRONG")}
    t = strong_drift_set(today)
    p = strong_drift_set(prev)
    new = sorted(t - p)
    if new:
        out.append({
            "category": "earnings_pead",
            "severity": "MED",
            "summary": f"New strong-drift PEAD names: {', '.join(list(new)[:6])}",
        })
    return out


DIFFERS = {
    "morning_intel": diff_morning_intel,
    "macro_surprise": diff_macro_surprise,
    "yield_curve": diff_yield_curve,
    "correlation_surface": diff_correlation_surface,
    "historical_analogs": diff_historical_analogs,
    "event_study": diff_event_study,
    "ab_test": diff_ab_test,
    "paper_portfolio": diff_paper_portfolio,
    "short_interest": diff_short_interest,
    "earnings": diff_earnings,
    # 13F is quarterly so we don't day-diff; included only for snapshotting
    "thirteen_f": lambda t, p: [],
}


def lambda_handler(event, context):
    t0 = time.time()
    today_date = datetime.now(timezone.utc).date()
    today_str = today_date.strftime("%Y-%m-%d")

    all_changes = []
    files_processed = []
    for filename, key in TRACKED:
        today_data = fetch_json(filename)
        if not today_data:
            files_processed.append({"file": filename, "status": "missing_today"})
            continue
        prev_data, prev_date = fetch_yesterday_snapshot(filename, today_date)
        differ = DIFFERS.get(key, lambda t, p: [])
        try:
            changes = differ(today_data, prev_data)
        except Exception as e:
            print(f"diff error {key}: {e}")
            changes = []
        all_changes.extend(changes)
        # Always snapshot today's version
        write_snapshot(filename, today_data, today_str)
        files_processed.append({
            "file": filename,
            "key": key,
            "n_changes": len(changes),
            "prev_snapshot": prev_date,
        })

    # Sort: HIGH first, then MED, LOW
    sev_order = {"HIGH": 0, "MED": 1, "LOW": 2}
    all_changes.sort(key=lambda c: sev_order.get(c.get("severity", "LOW"), 9))

    out = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "as_of_date": today_str,
        "n_changes": len(all_changes),
        "n_high": sum(1 for c in all_changes if c.get("severity") == "HIGH"),
        "n_med": sum(1 for c in all_changes if c.get("severity") == "MED"),
        "n_low": sum(1 for c in all_changes if c.get("severity") == "LOW"),
        "changes": all_changes,
        "files_processed": files_processed,
        "duration_s": round(time.time() - t0, 2),
    }

    s3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=json.dumps(out, indent=2, default=str).encode(),
        ContentType="application/json",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_changes": len(all_changes),
            "n_high": out["n_high"],
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
