"""
justhodl-snb-detail — Swiss National Bank Liquidity & Franc Detail Engine.

cb-injection scores the SNB on rate + franc alone. This engine goes deep on
Switzerland, the THIRD pillar of the carry / eurodollar picture after the BOJ
and ECB — because the Swiss franc is the world's second carry-funding
currency AND its premier safe-haven, and those two roles pull in opposite
directions.

  - As a FUNDING currency: the SNB has spent most of the last decade at or
    below zero. Cheap/negative CHF funds long positions in global risk just
    as cheap yen does.
  - As a SAFE-HAVEN: when global risk sells off, capital floods into the
    franc. A franc that is both cheap to borrow AND appreciating is a
    dangerous funding leg — the FX move eats the carry — and a sharp franc
    surge is itself a risk-off / carry-unwind signal.

Built on real FRED data (the practical official mirror of SNB statistics):

  MONETARY BASE   (SNBMONTBASE) — banknotes + sight deposits; the SNB's core
      liquidity aggregate. The SNB drained it hard from 2022; its direction
      is the cleanest read on whether the SNB is adding or withdrawing CHF.
  FX RESERVES     (SNBFORCURPOS) — the foreign-currency positions; the
      footprint of FX intervention (with the caveat that valuation moves it
      too, so it is read as a footprint, not a pure flow).
  POLICY RATE     (IR3TIB01CHM156N) — the SNB rate path; back toward / below
      zero means the franc carry is switched on.
  10Y YIELD       (IRLTLT01CHM156N) — a collapsing Swiss yield is
      flight-to-quality into Confederation bonds.
  FRANC CROSSES   (DEXSZUS + DEXUSEU -> USD/CHF, EUR/CHF) and the real
      effective rate (RBCHBIS) — how strong, and how fast-moving, the franc is.

Outputs a 0-100 FRANC SAFE-HAVEN PRESSURE score, a franc regime, a -2..+2
SNB injection score and a carry/eurodollar read.

OUTPUT: data/snb-detail.json   SCHEDULE: daily 11:40 UTC
Real data only — FRED. Not investment advice.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/snb-detail.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

FRED_SERIES = {
    "mon_base":    "SNBMONTBASE",      # Swiss monetary base — core SNB liquidity
    "fx_reserves": "SNBFORCURPOS",     # SNB foreign-currency positions
    "rate_3m":     "IR3TIB01CHM156N",  # Swiss 3M interbank — SNB policy proxy
    "yield_10y":   "IRLTLT01CHM156N",  # Swiss 10Y Confederation bond yield
    "usdchf":      "DEXSZUS",          # CHF per USD
    "eurusd":      "DEXUSEU",          # USD per EUR — for the EUR/CHF cross
    "reer":        "RBCHBIS",          # real broad effective exchange rate
    "cpi":         "CHECPIALLMINMEI",  # Swiss CPI index (lagged) — best-effort
}


# ───────────────────────── data fetch ─────────────────────────
def _get(url, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-snb-detail/1.0"})
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
    latest_d = datetime.fromisoformat(obs[0][0])
    target = latest_d - timedelta(days=days)
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


def to_bn(sample):
    """Auto-range a CHF aggregate into billions (lands the value in
    [100, 1000) — the SNB monetary base / FX reserves sit there)."""
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
        sources.append("FRED — SNB monetary base, FX reserves, Swiss "
                        "rates & franc crosses")

    # ── 1. MONETARY BASE — the SNB's core liquidity aggregate ──
    mb = fr["mon_base"]
    mb_scale = to_bn(latest(mb))
    mb_bn = round(latest(mb) * mb_scale, 1) if mb else None
    mb_6m = pct_change(mb, 182)
    mb_12m = pct_change(mb, 365)
    if mb_6m is None:
        mb_traj = "UNKNOWN"
    elif mb_6m > 2.0:
        mb_traj = "EXPANDING"
    elif mb_6m > -1.5:
        mb_traj = "FLAT"
    elif mb_6m > -6.0:
        mb_traj = "CONTRACTING — measured drain"
    else:
        mb_traj = "CONTRACTING — rapid drain"
    mb_read = (
        f"Swiss monetary base CHF {mb_bn:,.0f}bn, {mb_6m:+.1f}% over 6m "
        f"({mb_12m:+.1f}% / 12m) — {mb_traj.lower()}. The base is banknotes "
        "plus sight deposits; the SNB ran it down sharply after 2022 and its "
        "direction is the cleanest read on CHF liquidity provision."
        if mb_bn is not None and mb_6m is not None
        else "Swiss monetary-base data partial.")

    # ── 2. FX RESERVES — the intervention footprint ──
    fxr = fr["fx_reserves"]
    fxr_scale = to_bn(latest(fxr))
    fxr_bn = round(latest(fxr) * fxr_scale, 1) if fxr else None
    fxr_6m = pct_change(fxr, 182)
    fxr_12m = pct_change(fxr, 365)
    fxr_read = (
        f"SNB foreign-currency positions CHF {fxr_bn:,.0f}bn ({fxr_6m:+.1f}% "
        f"/ 6m). Rising reserves point to FX intervention — selling francs to "
        "cap their strength — though valuation moves the figure too, so it "
        "is read as a footprint, not a pure flow."
        if fxr_bn is not None and fxr_6m is not None
        else "SNB FX-reserve data partial.")

    # ── 3. POLICY RATE — at / below zero = franc carry switched on ──
    rate_obs = fr["rate_3m"]
    rate = latest(rate_obs)
    rate_6m = level_change(rate_obs, 182)
    rate_12m = level_change(rate_obs, 365)
    if rate is None:
        rate_zone = "UNKNOWN"
    elif rate < -0.05:
        rate_zone = "BELOW ZERO"
    elif rate < 0.30:
        rate_zone = "NEAR ZERO"
    elif rate < 1.0:
        rate_zone = "LOW-POSITIVE"
    else:
        rate_zone = "RESTRICTIVE"
    if rate_6m is None:
        rate_stance = "UNKNOWN"
    elif rate_6m <= -0.20:
        rate_stance = "CUTTING"
    elif rate_6m <= -0.05:
        rate_stance = "EASING AT MARGIN"
    elif rate_6m >= 0.20:
        rate_stance = "HIKING"
    elif rate_6m >= 0.05:
        rate_stance = "TIGHTENING AT MARGIN"
    else:
        rate_stance = "ON HOLD"
    rate_read = (
        f"Swiss short rate {rate:.2f}% ({rate_zone.lower()}), {rate_6m:+.2f}pp "
        f"over 6m — policy {rate_stance.lower()}. At or below zero the franc "
        "is once again a near-free funding currency; the SNB cuts toward zero "
        "primarily to lean against franc strength."
        if rate is not None and rate_6m is not None
        else f"Swiss short rate {rate}% — rate detail partial.")

    # ── 4. 10Y SWISS YIELD — flight-to-quality gauge ──
    y10_obs = fr["yield_10y"]
    y10 = latest(y10_obs)
    y10_6m = level_change(y10_obs, 182)
    y10_12m = level_change(y10_obs, 365)
    y10_read = (
        f"10Y Confederation bond yield {y10:.2f}% ({y10_6m:+.2f}pp / 6m). A "
        "very low or falling Swiss yield reflects flight-to-quality demand "
        "for Confederation paper — the safe-haven bid showing up in rates."
        if y10 is not None and y10_6m is not None
        else "10Y Swiss yield data partial.")

    # ── 5. FRANC CROSSES — strength & momentum ──
    usdchf = latest(fr["usdchf"])              # CHF per USD
    eurusd = latest(fr["eurusd"])              # USD per EUR
    eurchf = (round(usdchf * eurusd, 4)
              if usdchf is not None and eurusd is not None else None)
    usdchf_3m = pct_change(fr["usdchf"], 91)   # negative = franc stronger vs USD
    # EUR/CHF momentum: rebuild a short cross history from the two legs
    eurchf_3m = None
    su, se = fr["usdchf"], fr["eurusd"]
    if su and se:
        u0 = val_days_ago(su, 91)
        e0 = val_days_ago(se, 91)
        if u0 and e0 and eurchf:
            base = u0 * e0
            if base:
                eurchf_3m = round((eurchf - base) / base * 100, 2)
    if eurchf is None:
        fx_strength = "UNKNOWN"
    elif eurchf < 0.92:
        fx_strength = "VERY STRONG"
    elif eurchf < 0.97:
        fx_strength = "STRONG"
    elif eurchf < 1.02:
        fx_strength = "FIRM"
    else:
        fx_strength = "SOFT"
    franc_up_3m = eurchf_3m if eurchf_3m is not None else (
        -usdchf_3m if usdchf_3m is not None else None)
    if franc_up_3m is None:
        fx_mom = "UNKNOWN"
    elif franc_up_3m <= -2.5:
        fx_mom = "SHARP FRANC SURGE — safe-haven flight"
    elif franc_up_3m <= -0.8:
        fx_mom = "FRANC APPRECIATING"
    elif franc_up_3m >= 0.8:
        fx_mom = "FRANC SOFTENING"
    else:
        fx_mom = "RANGE-BOUND"
    reer = latest(fr["reer"])
    reer_6m = pct_change(fr["reer"], 182)
    fx_read = (
        f"EUR/CHF {eurchf} · USD/CHF {usdchf:.3f} — franc {fx_strength.lower()}"
        + (f", {fx_mom.lower()} ({franc_up_3m:+.1f}% / 3m)."
           if franc_up_3m is not None else ".")
        + " A cheap-to-borrow franc that is also appreciating is a costly "
        "funding leg — the currency move erodes the carry."
        if eurchf is not None and usdchf is not None
        else "Franc-cross data partial.")

    # ── 6. INFLATION — the SNB reaction function (lagged series) ──
    cpi = fr["cpi"]
    cpi_yoy = pct_change(cpi, 365)
    cpi_read = (
        f"Swiss CPI {cpi_yoy:+.1f}% YoY — "
        + ("inflation soft or negative, which is exactly why the SNB cuts "
           "toward zero and tolerates franc-driven disinflation."
           if cpi_yoy is not None and cpi_yoy < 1.0 else
           "inflation contained within the SNB's 0-2% objective.")
        if cpi_yoy is not None else "Swiss CPI data partial (series lags).")

    # ── 7. FRANC SAFE-HAVEN PRESSURE SCORE (0-100) ──
    pressure, comps = 0.0, {}
    # a. franc strength level (0-30): EUR/CHF + USD/CHF
    fs_eur = (clamp((1.00 - eurchf) / 0.15, 0, 1) * 18
              if eurchf is not None else 0)
    fs_usd = (clamp((0.95 - usdchf) / 0.20, 0, 1) * 12
              if usdchf is not None else 0)
    comps["franc_strength_level"] = round(fs_eur + fs_usd, 1)
    pressure += fs_eur + fs_usd
    # b. appreciation momentum (0-30)
    mom = (clamp(abs(franc_up_3m) / 4.0, 0, 1) * 30
           if franc_up_3m is not None and franc_up_3m < 0 else 0)
    comps["appreciation_momentum"] = round(mom, 1)
    pressure += mom
    # c. Swiss yield compression — flight-to-quality (0-20)
    yl = (clamp((1.0 - y10) / 1.0, 0, 1) * 14
          if y10 is not None else 0)
    yt = (clamp(abs(y10_6m) / 0.40, 0, 1) * 6
          if y10_6m is not None and y10_6m < 0 else 0)
    comps["yield_compression"] = round(yl + yt, 1)
    pressure += yl + yt
    # d. SNB rate defense — at/below zero & cutting (0-20)
    rd_lvl = (clamp((0.50 - rate) / 1.0, 0, 1) * 12
              if rate is not None else 0)
    rd_trd = (clamp(abs(rate_6m) / 0.50, 0, 1) * 8
              if rate_6m is not None and rate_6m < 0 else 0)
    comps["snb_rate_defense"] = round(rd_lvl + rd_trd, 1)
    pressure += rd_lvl + rd_trd
    pressure = round(clamp(pressure, 0, 100), 1)
    franc_surge = mom >= 20.0

    if franc_surge:
        franc_regime = "FRANC SURGE — active safe-haven flight"
    elif pressure >= 70:
        franc_regime = "STRONG SAFE-HAVEN BID — risk-off franc"
    elif pressure >= 50:
        franc_regime = "SAFE-HAVEN BID — elevated franc pressure"
    elif pressure >= 25:
        franc_regime = "FRANC FIRM — funding currency, appreciation risk"
    else:
        franc_regime = "FRANC SOFT — clean carry funding"

    # ── 8. SNB INJECTION SCORE (-2..+2) — cb-injection's scale ──
    score = 0
    if mb_6m is not None:
        score += 1 if mb_6m > 2.0 else -1 if mb_6m < -1.5 else 0
    if fxr_6m is not None:
        score += 1 if fxr_6m > 2.0 else -1 if fxr_6m < -2.0 else 0
    if rate_6m is not None:
        score += 1 if rate_6m < -0.10 else -1 if rate_6m > 0.10 else 0
    score = int(clamp(score, -2, 2))
    SCORE_LABEL = {2: "STRONG INJECTION", 1: "INJECTING", 0: "NEUTRAL",
                   -1: "DRAINING", -2: "STRONG DRAIN"}
    stance_label = SCORE_LABEL[score]

    # ── 9. CARRY / EURODOLLAR READ ──
    if franc_surge:
        carry_read = (
            "The franc is surging — the safe-haven bid is live. That is a "
            "risk-off tell in its own right, and it forces losses on anyone "
            "funding global longs in cheap CHF: the currency leg is moving "
            "hard against the trade. Franc carry and yen carry tend to "
            "unwind together.")
    elif pressure >= 50:
        carry_read = (
            "Franc safe-haven pressure is elevated. CHF is cheap to borrow "
            "but strong and firm — a treacherous funding currency, because a "
            "further risk-off leg would both strengthen the franc and squeeze "
            "franc-funded carry. The SNB is leaning against it via near-zero "
            "rates and, on the evidence of reserves, intervention.")
    elif pressure >= 25:
        carry_read = (
            "The franc is a working funding currency — rates at or near zero "
            "make CHF cheap to borrow — but it is firm, so franc-funded "
            "positions carry latent currency risk if global risk sentiment "
            "turns.")
    else:
        carry_read = (
            "The franc is soft and Swiss rates are low — clean, low-cost "
            "carry funding with little appreciation drag. Benign for "
            "CHF-funded risk positions.")

    # ── 10. cross-reference (do not rebuild) ──
    cbi = read_existing("data/cb-injection.json") or {}
    eds = read_existing("data/eurodollar-stress.json") or {}
    boj = read_existing("data/boj-detail.json") or {}
    cross = {
        "cb_injection_global_impulse":
            (cbi.get("global_injection_impulse") or {}).get("label"),
        "eurodollar_stress_score":
            eds.get("score") or eds.get("stress_score"),
        "boj_yen_carry_unwind_risk":
            (boj.get("carry_unwind_risk") or {}).get("score_0_100"),
    }

    headline = (
        f"SNB: {stance_label}. Franc safe-haven pressure {pressure:.0f}/100 "
        f"({franc_regime}). Monetary base {mb_6m:+.1f}%/6m "
        f"({mb_traj.split(' ')[0].lower()}); short rate {rate:.2f}% "
        f"({rate_zone.lower()}); EUR/CHF {eurchf}."
        if (pressure is not None and mb_6m is not None and rate is not None
            and eurchf is not None)
        else f"SNB liquidity & franc detail: {stance_label} (partial data).")

    core_ok = mb_bn is not None and rate is not None and eurchf is not None
    out = {
        "schema_version": "1.0",
        "method": "snb_liquidity_franc_detail",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": core_ok and len(errors) <= 3,
        "headline": headline,
        "snb_injection_score": score,
        "stance_label": stance_label,
        "franc_pressure": {
            "score_0_100": pressure,
            "regime": franc_regime,
            "franc_surge": franc_surge,
            "components": comps,
            "read": carry_read,
        },
        "monetary_base": {
            "total_chf_bn": mb_bn,
            "change_6m_pct": mb_6m,
            "change_12m_pct": mb_12m,
            "trajectory": mb_traj,
            "read": mb_read,
        },
        "fx_reserves": {
            "total_chf_bn": fxr_bn,
            "change_6m_pct": fxr_6m,
            "change_12m_pct": fxr_12m,
            "read": fxr_read,
        },
        "policy_rate": {
            "short_rate_pct": round(rate, 3) if rate is not None else None,
            "rate_zone": rate_zone,
            "change_6m_pp": rate_6m,
            "change_12m_pp": rate_12m,
            "stance": rate_stance,
            "read": rate_read,
        },
        "yield_10y": {
            "yield_pct": round(y10, 3) if y10 is not None else None,
            "change_6m_pp": y10_6m,
            "change_12m_pp": y10_12m,
            "read": y10_read,
        },
        "franc_crosses": {
            "eur_chf": eurchf,
            "usd_chf": round(usdchf, 4) if usdchf is not None else None,
            "franc_3m_change_pct": franc_up_3m,
            "reer_index": round(reer, 2) if reer is not None else None,
            "reer_change_6m_pct": reer_6m,
            "strength": fx_strength,
            "momentum": fx_mom,
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
    print(f"[snb-detail] {stance_label} | franc pressure {pressure}/100 "
          f"({franc_regime}) | EUR/CHF {eurchf} | errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "stance": stance_label,
                                "franc_pressure": pressure,
                                "regime": franc_regime,
                                "errors": len(errors)})}
