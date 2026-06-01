"""
justhodl-boj-detail — Bank of Japan Liquidity & Yen-Carry Detail Engine.

cb-injection scores the BOJ as one of four central banks at the balance-sheet
level. This engine goes deep on Japan, because the yen is THE funding currency
of the global carry trade — decades of zero/negative rates made borrowing yen
nearly free, and that cheap yen funds long positions in US equities, EM debt,
credit and crypto. The August 5, 2024 unwind (Nikkei -12% in a session, a
global VIX spike) is the reference event. So this engine answers one question
a global-macro desk lives by: HOW CLOSE IS THE NEXT YEN-CARRY UNWIND?

It fuses, on real FRED data (the practical official mirror of BOJ statistics):

  BALANCE SHEET   (JPNASSETS) — BOJ total assets. The BOJ owns ~half of all
      JGBs; whether the book is still growing or rolling off = QE vs QT.
  POLICY RATE     (call money / 3M interbank) — the post-NIRP normalization
      path. Every hike raises the cost of the carry.
  10Y JGB YIELD   (IRLTLT01JPM156N) — post-YCC, a rising JGB yield pulls
      Japanese capital home; Japan is the largest foreign holder of USTs.
  US-JP DIFFERENTIAL (DGS10 - JGB) — the gross profitability of the carry;
      compression erodes the trade's economics.
  USD/JPY         (DEXJPUS) — a very weak yen = a crowded, "loaded" carry;
      a sharp yen rally = the unwind detonating.

Outputs a YEN-CARRY-UNWIND RISK SCORE (0-100), a regime label, a -2..+2 BOJ
injection score and a eurodollar/carry read.

OUTPUT: data/boj-detail.json   SCHEDULE: daily 11:20 UTC
Real data only — FRED. Not investment advice.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/boj-detail.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# FRED carries the official BOJ balance sheet, Japanese rates and USD/JPY.
FRED_SERIES = {
    "balance_sheet": "JPNASSETS",        # BOJ total assets
    "call_rate":     "IRSTCI01JPM156N",  # immediate call money rate — policy
    "interbank_3m":  "IR3TIB01JPM156N",  # 3M interbank — policy-rate proxy
    "jgb_10y":       "IRLTLT01JPM156N",  # Japan 10Y govt bond yield
    "us_10y":        "DGS10",            # US 10Y Treasury — carry differential
    "usdjpy":        "DEXJPUS",          # yen per USD (up = weaker yen)
    "cpi":           "JPNCPIALLMINMEI",  # Japan CPI index — YoY computed
}


# ───────────────────────── data fetch ─────────────────────────
def _get(url, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-boj-detail/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    raise last or RuntimeError(f"fetch failed: {url}")


def fred(series_id, limit=900):
    """FRED observations -> newest-first [(date, float)]."""
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    d = json.loads(_get(url))
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((o["date"], float(v)))
        except (TypeError, ValueError):
            continue
    return out


# ───────────────────────── helpers ─────────────────────────
def val_days_ago(obs, days):
    if not obs:
        return None
    latest = datetime.fromisoformat(obs[0][0])
    target = latest - timedelta(days=days)
    return min(obs, key=lambda o: abs(datetime.fromisoformat(o[0])
                                      - target))[1]


def level_change(obs, days):
    base = val_days_ago(obs, days)
    if base is None or not obs:
        return None
    return round(obs[0][1] - base, 3)


def pct_change(obs, days):
    base = val_days_ago(obs, days)
    if base in (None, 0) or not obs:
        return None
    return round((obs[0][1] - base) / abs(base) * 100, 2)


def latest(obs):
    return obs[0][1] if obs else None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def to_tn_yen(sample):
    """Auto-range a BOJ balance-sheet value into trillions of yen.

    FRED reports JPNASSETS in unit conventions that have changed over time;
    the BOJ book is known to sit in the ~600-800 trillion yen range, so scale
    whatever raw value arrives until it lands in [100, 1000)."""
    a = abs(sample or 0)
    if a <= 0:
        return 1.0
    scale = 1.0
    for _ in range(20):
        if a * scale >= 1000:
            scale /= 10
        elif a * scale < 100:
            scale *= 10
        else:
            break
    return scale


def read_existing(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


# ───────────────────────── handler ─────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    errors, sources = [], []

    fr = {}
    for tag, sid in FRED_SERIES.items():
        try:
            fr[tag] = fred(sid)
            if not fr[tag]:
                errors.append(f"{sid}: empty")
        except Exception as e:
            errors.append(f"{sid}: {str(e)[:70]}")
            fr[tag] = []
    if any(fr.values()):
        sources.append("FRED — BOJ balance sheet, Japanese rates & USD/JPY")

    # ── 1. BALANCE SHEET — QE vs QT trajectory ──
    bs = fr["balance_sheet"]
    bs_scale = to_tn_yen(latest(bs))
    bs_tn = round(latest(bs) * bs_scale, 1) if bs else None
    bs_6m = pct_change(bs, 182)
    bs_12m = pct_change(bs, 365)
    if bs_6m is None:
        bs_traj = "UNKNOWN"
    elif bs_6m > 2.0:
        bs_traj = "EXPANDING"
    elif bs_6m > -1.0:
        bs_traj = "FLAT — reinvestment near full"
    elif bs_6m > -5.0:
        bs_traj = "TAPERING — measured runoff"
    else:
        bs_traj = "CONTRACTING — active QT"
    bs_read = (
        f"BOJ balance sheet \u00a5{bs_tn:,.0f}tn, {bs_6m:+.1f}% over 6m "
        f"({bs_12m:+.1f}% / 12m) — {bs_traj.lower()}. The BOJ has tapered "
        "its JGB purchases; the book is no longer the one-way liquidity "
        "engine it was under yield-curve control."
        if bs_tn is not None and bs_6m is not None
        else "BOJ balance-sheet data partial.")

    # ── 2. POLICY RATE — post-NIRP normalization ──
    call = fr["call_rate"]
    ib3m = fr["interbank_3m"]
    # prefer the call rate; fall back to the 3M interbank proxy
    if call and latest(call) is not None:
        pol, pol_src, pol_obs = latest(call), "call money rate", call
    elif ib3m:
        pol, pol_src, pol_obs = latest(ib3m), "3M interbank (proxy)", ib3m
    else:
        pol, pol_src, pol_obs = None, "unavailable", []
    rate_6m = level_change(pol_obs, 182)
    rate_12m = level_change(pol_obs, 365)
    if rate_6m is None:
        rate_stance = "UNKNOWN"
    elif rate_6m >= 0.20:
        rate_stance = "TIGHTENING"
    elif rate_6m >= 0.05:
        rate_stance = "NORMALIZING"
    elif rate_6m <= -0.10:
        rate_stance = "EASING"
    else:
        rate_stance = "ON HOLD"
    rate_read = (
        f"Policy rate {pol:.2f}% ({pol_src}), {rate_6m:+.2f}pp over 6m "
        f"({rate_12m:+.2f}pp / 12m) — {rate_stance.lower()}. Each step up "
        "from the zero/negative-rate era raises the cost of borrowing yen, "
        "the carry trade's funding leg."
        if pol is not None and rate_6m is not None
        else f"Policy rate {pol}% — rate detail partial.")

    # ── 3. 10Y JGB — the repatriation gauge ──
    jgb = fr["jgb_10y"]
    jgb_y = latest(jgb)
    jgb_6m = level_change(jgb, 182)
    jgb_12m = level_change(jgb, 365)
    jgb_read = (
        f"10Y JGB yield {jgb_y:.2f}% ({jgb_6m:+.2f}pp / 6m). Post-YCC the "
        "JGB yield floats freely; a higher domestic yield pulls Japanese "
        "capital home — and Japan is the largest foreign holder of US "
        "Treasuries — strengthening the yen and pressuring the carry."
        if jgb_y is not None and jgb_6m is not None
        else "10Y JGB data partial.")

    # ── 4. US-JP RATE DIFFERENTIAL — the carry's gross profitability ──
    us10 = latest(fr["us_10y"])
    diff = (round(us10 - jgb_y, 2)
            if us10 is not None and jgb_y is not None else None)
    us_6m = level_change(fr["us_10y"], 182)
    diff_6m = (round((us_6m or 0) - (jgb_6m or 0), 2)
               if us_6m is not None and jgb_6m is not None else None)
    diff_read = (
        f"US 10Y {us10:.2f}% vs JGB 10Y {jgb_y:.2f}% — a {diff:.2f}pp "
        f"differential ({diff_6m:+.2f}pp over 6m). "
        + ("A compressing differential erodes the gross carry and is the "
           "classic precursor to an unwind."
           if diff_6m is not None and diff_6m < 0 else
           "A wide, stable differential keeps the carry profitable — but "
           "also crowded.")
        if diff is not None and diff_6m is not None
        else "Rate-differential data partial.")

    # ── 5. USD/JPY — crowdedness and unwind momentum ──
    jpy = fr["usdjpy"]
    fx = latest(jpy)
    fx_1m = pct_change(jpy, 30)
    fx_3m = pct_change(jpy, 91)
    # negative pct_change = yen strengthening (USD/JPY falling)
    if fx_1m is None:
        fx_mom = "UNKNOWN"
    elif fx_1m <= -3.0:
        fx_mom = "SHARP YEN RALLY — unwind dynamics"
    elif fx_1m <= -1.0:
        fx_mom = "YEN FIRMING"
    elif fx_1m >= 1.0:
        fx_mom = "YEN WEAKENING — carry building"
    else:
        fx_mom = "RANGE-BOUND"
    if fx is None:
        fx_stretch = "UNKNOWN"
    elif fx >= 158:
        fx_stretch = "EXTREME — intervention zone"
    elif fx >= 150:
        fx_stretch = "STRETCHED — crowded carry"
    elif fx >= 140:
        fx_stretch = "ELEVATED"
    else:
        fx_stretch = "MODERATE"
    fx_read = (
        f"USD/JPY {fx:.1f} ({fx_1m:+.1f}% / 1m, {fx_3m:+.1f}% / 3m) — yen "
        f"{fx_stretch.lower()}. A very weak yen means the carry is maximally "
        "crowded with the most stored unwind energy; a sharp yen rally is "
        "that energy releasing."
        if fx is not None and fx_1m is not None
        else "USD/JPY data partial.")

    # ── 6. INFLATION — the BOJ reaction function ──
    cpi = fr["cpi"]
    cpi_yoy = pct_change(cpi, 365)
    cpi_read = (
        f"Japan CPI {cpi_yoy:+.1f}% YoY — "
        + ("sustained inflation above target keeps BOJ normalization live."
           if cpi_yoy is not None and cpi_yoy >= 2.0 else
           "inflation easing reduces pressure for further hikes.")
        if cpi_yoy is not None else "CPI data partial.")

    # ── 7. YEN-CARRY-UNWIND RISK SCORE (0-100) ──
    risk, comps = 0.0, {}
    # a. BOJ policy tightening: level (0-15) + 6m trend (0-10)
    lvl = clamp((pol or 0) / 1.0, 0, 1) * 15 if pol is not None else 0
    trd = (clamp(rate_6m / 0.50, 0, 1) * 10
           if rate_6m is not None and rate_6m > 0.05 else 0)
    comps["boj_tightening"] = round(lvl + trd, 1)
    risk += lvl + trd
    # b. JGB 10Y: level (0-16) + rising trend (0-4)
    jl = (clamp((jgb_y - 0.30) / 1.40, 0, 1) * 16
          if jgb_y is not None else 0)
    jt = (clamp(jgb_6m / 0.50, 0, 1) * 4
          if jgb_6m is not None and jgb_6m > 0.05 else 0)
    comps["jgb_yield"] = round(jl + jt, 1)
    risk += jl + jt
    # c. differential compression (0-20)
    dc = (clamp(abs(diff_6m) / 1.0, 0, 1) * 20
          if diff_6m is not None and diff_6m < 0 else 0)
    comps["differential_compression"] = round(dc, 1)
    risk += dc
    # d. USD/JPY stretch — crowded carry (0-20)
    st = clamp((fx - 125) / 40, 0, 1) * 20 if fx is not None else 0
    comps["usdjpy_stretch"] = round(st, 1)
    risk += st
    # e. yen-rally momentum — unwind in progress (0-15)
    mo = (clamp((abs(fx_1m) - 1.5) / 6.0, 0, 1) * 15
          if fx_1m is not None and fx_1m < -1.5 else 0)
    comps["yen_rally_momentum"] = round(mo, 1)
    risk += mo
    risk = round(clamp(risk, 0, 100), 1)
    unwind_in_progress = mo >= 7.0

    if unwind_in_progress:
        carry_regime = "UNWIND IN PROGRESS"
    elif risk >= 72:
        carry_regime = "UNWIND RISK ACUTE"
    elif risk >= 50:
        carry_regime = "UNWIND RISK ELEVATED"
    elif risk >= 25:
        carry_regime = "CARRY LOADED"
    else:
        carry_regime = "CARRY STABLE"

    # ── 8. BOJ INJECTION SCORE (-2..+2) — cb-injection's scale ──
    score = 0
    if bs_6m is not None:
        score += 1 if bs_6m > 2.0 else -1 if bs_6m < -1.0 else 0
    if rate_6m is not None:
        score += 1 if rate_6m < -0.10 else -1 if rate_6m > 0.10 else 0
    if jgb_6m is not None and jgb_6m > 0.20:
        score -= 1                       # a sharp JGB rise = passive tightening
    score = int(clamp(score, -2, 2))
    SCORE_LABEL = {2: "STRONG INJECTION", 1: "INJECTING", 0: "NEUTRAL",
                   -1: "DRAINING", -2: "STRONG DRAIN"}
    stance_label = SCORE_LABEL[score]

    # ── 9. CARRY / EURODOLLAR READ ──
    if unwind_in_progress:
        carry_read = (
            "The yen is rallying hard while the carry was crowded — the "
            "classic unwind sequence. Leveraged yen-funded longs across "
            "global risk assets get forced out; expect cross-asset "
            "volatility and USD funding strain to spike together.")
    elif risk >= 50:
        carry_read = (
            "Yen-carry unwind risk is elevated: the BOJ is normalizing, the "
            "rate differential is no longer working strongly in the carry's "
            "favour, and the yen is stretched. The trade is loaded — it does "
            "not take much of a yen rally to force a disorderly exit.")
    elif risk >= 25:
        carry_read = (
            "The carry is profitable but crowded — a weak yen and a still-"
            "wide differential keep yen-funded positions attractive. Stored "
            "energy is building; the risk is latent, not yet releasing.")
    else:
        carry_read = (
            "Yen-carry conditions are stable — BOJ policy and the rate "
            "differential are not pressuring the funding leg. Supportive, at "
            "the margin, for global risk assets funded in yen.")

    # ── 10. cross-reference (do not rebuild) ──
    cbi = read_existing("data/cb-injection.json") or {}
    eds = read_existing("data/eurodollar-stress.json") or {}
    cross = {
        "cb_injection_global_impulse":
            (cbi.get("global_injection_impulse") or {}).get("label"),
        "eurodollar_stress_score":
            eds.get("score") or eds.get("stress_score"),
    }

    headline = (
        f"BOJ: {stance_label}. Yen-carry unwind risk {risk:.0f}/100 "
        f"({carry_regime}). Balance sheet {bs_6m:+.1f}%/6m ({bs_traj.split(' ')[0].lower()}); "
        f"policy rate {pol:.2f}% ({rate_stance.lower()}); USD/JPY {fx:.0f}."
        if (risk is not None and bs_6m is not None and pol is not None
            and fx is not None)
        else f"BOJ liquidity & yen-carry detail: {stance_label} (partial data).")

    core_ok = bs_tn is not None and pol is not None and fx is not None
    out = {
        "schema_version": "1.0",
        "method": "boj_liquidity_yen_carry_detail",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": core_ok and len(errors) <= 3,
        "headline": headline,
        "boj_injection_score": score,
        "stance_label": stance_label,
        "carry_unwind_risk": {
            "score_0_100": risk,
            "regime": carry_regime,
            "unwind_in_progress": unwind_in_progress,
            "components": comps,
            "read": carry_read,
        },
        "balance_sheet": {
            "total_assets_jpy_tn": bs_tn,
            "change_6m_pct": bs_6m,
            "change_12m_pct": bs_12m,
            "trajectory": bs_traj,
            "read": bs_read,
        },
        "policy_rate": {
            "policy_rate_pct": round(pol, 3) if pol is not None else None,
            "rate_source": pol_src,
            "interbank_3m_pct": (round(latest(ib3m), 3)
                                 if latest(ib3m) is not None else None),
            "change_6m_pp": rate_6m,
            "change_12m_pp": rate_12m,
            "stance": rate_stance,
            "read": rate_read,
        },
        "jgb_10y": {
            "yield_pct": round(jgb_y, 3) if jgb_y is not None else None,
            "change_6m_pp": jgb_6m,
            "change_12m_pp": jgb_12m,
            "read": jgb_read,
        },
        "rate_differential": {
            "us_10y_pct": round(us10, 3) if us10 is not None else None,
            "jp_10y_pct": round(jgb_y, 3) if jgb_y is not None else None,
            "differential_pp": diff,
            "change_6m_pp": diff_6m,
            "read": diff_read,
        },
        "usdjpy": {
            "level": round(fx, 2) if fx is not None else None,
            "change_1m_pct": fx_1m,
            "change_3m_pct": fx_3m,
            "momentum": fx_mom,
            "stretch": fx_stretch,
            "read": fx_read,
        },
        "inflation": {
            "cpi_yoy_pct": cpi_yoy,
            "read": cpi_read,
        },
        "carry_read": carry_read,
        "cross_reference": cross,
        "sources": sources,
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[boj-detail] {stance_label} | carry unwind risk {risk}/100 "
          f"({carry_regime}) | USD/JPY {fx} | errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "stance": stance_label,
                                "carry_unwind_risk": risk,
                                "regime": carry_regime,
                                "errors": len(errors)})}
