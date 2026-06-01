"""
justhodl-yen-carry — Yen Carry Trade & Bank of Japan Liquidity Engine.

cb-injection scores the BOJ at the balance-sheet-total level — one summary
line. But the yen carry trade is THE carry trade, and a global-macro desk
does not watch "is the BOJ balance sheet bigger." The carry is picking up
pennies in front of a steamroller; what matters is five things, and this
engine fuses all of them:

  1. FUNDING LEG   — BOJ balance-sheet trajectory + the short rate. The cost
                     of borrowing yen. A hiking BOJ raises the funding cost.
  2. CARRY WIDTH   — the US-Japan rate differential (front-end and 10y). Wide
                     differential = the carry pays = leverage builds.
  3. THE DETONATOR — USD/JPY level, appreciation momentum and realised vol.
                     A sharp yen rally is what forces a leveraged unwind
                     (the August 2024 unwind is the reference event).
  4. CROWDEDNESS   — CFTC non-commercial JPY positioning. A large net short
                     yen = the carry is crowded = more fuel for a squeeze.
  5. JGB STRESS    — the 10y JGB yield. A sharp rise pressures the BOJ and
                     drives Japanese repatriation — yen-positive, carry-bad.

OUTPUT: a yen-carry REGIME (carry-on -> carry-unwind), a 0-100 UNWIND-RISK
score with explicit triggers, a carry-attractiveness read, a -2..+2 BOJ
injection score and a eurodollar read.

OUTPUT KEY: data/yen-carry.json   SCHEDULE: daily 12:00 UTC
Real data only — FRED (the official mirror of BOJ statistics) + the
in-system CFTC positioning cache. Not investment advice.
"""
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/yen-carry.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# FRED carries the official BOJ statistics and the US side of the carry.
FRED_SERIES = {
    "boj_assets":  "JPNASSETS",         # BOJ total assets (100m yen)
    "jp_rate_3m":  "IR3TIB01JPM156N",   # Japan 3M interbank — BOJ policy proxy
    "jgb_10y":     "IRLTLT01JPM156N",   # Japan 10y government bond yield, %
    "usdjpy":      "DEXJPUS",           # USD/JPY spot, daily
    "us_2y":       "DGS2",              # US 2y treasury, %
    "us_10y":      "DGS10",             # US 10y treasury, %
    "fed_funds":   "DFF",               # US fed funds effective, %
}


# ───────────────────────── data fetchers ─────────────────────────
def _get(url, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-yen-carry/1.0"})
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


def read_existing(key):
    """Defensively read an existing S3 JSON output (for cross-reference)."""
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


# ───────────────────────── helpers ─────────────────────────
def _d(s):
    return datetime.strptime(s, "%Y-%m-%d")


def latest(obs):
    return obs[0][1] if obs else None


def val_days_ago(obs, days):
    if not obs:
        return None
    target = _d(obs[0][0]) - timedelta(days=days)
    for dt, v in obs:
        if _d(dt) <= target:
            return v
    return obs[-1][1]


def pct_change(obs, days):
    now_v, then_v = latest(obs), val_days_ago(obs, days)
    if now_v is None or then_v in (None, 0):
        return None
    return (now_v / then_v - 1.0) * 100.0


def level_change(obs, days):
    now_v, then_v = latest(obs), val_days_ago(obs, days)
    if now_v is None or then_v is None:
        return None
    return now_v - then_v


def realized_vol(obs, window=20):
    """Annualised realised vol (%) from a daily price series, newest-first."""
    vals = [v for _, v in obs[:window + 1]]
    if len(vals) < max(6, window // 2 + 2):
        return None
    vals = vals[::-1]
    rets = []
    for i in range(1, len(vals)):
        if vals[i - 1] > 0 and vals[i] > 0:
            rets.append(math.log(vals[i] / vals[i - 1]))
    if len(rets) < 5:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var) * math.sqrt(252.0) * 100.0


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def r1(x):
    return round(x, 1) if isinstance(x, (int, float)) else x


def r2(x):
    return round(x, 2) if isinstance(x, (int, float)) else x


# ───────────────── CFTC JPY positioning (cross-reference) ─────────────────
def _stdev(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def jpy_positioning():
    """JPY non-commercial positioning from the in-system CFTC cache.

    Reads data/cftc-all-cache.json (the agent's cached /cot/all response),
    locates the Japanese Yen entry (6J / CFTC code 097741) and z-scores the
    current net speculator position against its own recent history — a large,
    historically-extreme net short yen = a crowded carry. Returns a block
    or None.
    """
    cache = read_existing("data/cftc-all-cache.json")
    if not isinstance(cache, (dict, list)):
        return None

    entry = None

    def walk(node):
        nonlocal entry
        if entry is not None:
            return
        if isinstance(node, dict):
            cc = str(node.get("cftc_code", "")).strip()
            ct = str(node.get("contract", "")).strip().upper()
            nm = str(node.get("name", "")).lower()
            if (cc == "097741" or ct == "6J" or "japanese yen" in nm) and (
                    "weekly_reports" in node or "current" in node
                    or "net_speculator" in node):
                entry = node
                return
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)

    try:
        walk(cache)
    except Exception:
        return None
    if not isinstance(entry, dict):
        return None

    cur = (entry.get("current") if isinstance(entry.get("current"), dict)
           else entry)
    net = cur.get("net_speculator")
    if not isinstance(net, (int, float)):
        net = cur.get("net_managed_money")
    if not isinstance(net, (int, float)):
        return None

    hist = [w["net_speculator"] for w in (entry.get("weekly_reports") or [])
            if isinstance(w, dict)
            and isinstance(w.get("net_speculator"), (int, float))]
    z = None
    if len(hist) >= 8:
        shortness = [-x for x in hist]
        m = sum(shortness) / len(shortness)
        sd = _stdev(shortness)
        if sd > 0:
            z = ((-net) - m) / sd

    sig = entry.get("signals") if isinstance(
        entry.get("signals"), dict) else {}
    side = "short" if net < 0 else "long"
    return {
        "net_speculator": net,
        "net_managed_money": cur.get("net_managed_money"),
        "report_date": cur.get("report_date"),
        "open_interest": cur.get("open_interest"),
        "net_zscore_vs_history": round(z, 2) if z is not None else None,
        "crowded_short": net < 0,
        "extreme": bool(sig.get("extreme")),
        "reversal_risk": bool(sig.get("reversal_risk")),
        "history_weeks": len(hist),
        "source": "cftc-all-cache (6J / Japanese Yen)",
        "read": (
            f"Speculators are net {side} {abs(int(net)):,} JPY contracts "
            f"(as of {cur.get('report_date')})"
            + (f", {z:+.1f}sd vs the last {len(hist)}w"
               if z is not None else "")
            + (" — a net short yen IS the carry trade; the more extreme the "
               "short, the more fuel for a squeeze."
               if net < 0 else
               " — speculators net long yen means the carry is not crowded.")),
    }


# ───────────────────────── synthesis ─────────────────────────
def boj_injection_score(bs_chg_6m, rate_chg_6m):
    """-2..+2 — is the BOJ adding (QE/cuts) or draining (QT/hikes) yen."""
    score = 0.0
    if bs_chg_6m is not None:
        if bs_chg_6m > 2:
            score += 1
        elif bs_chg_6m > 0.3:
            score += 0.5
        elif bs_chg_6m < -2:
            score -= 1
        elif bs_chg_6m < -0.3:
            score -= 0.5
    if rate_chg_6m is not None:
        if rate_chg_6m > 0.15:
            score -= 1
        elif rate_chg_6m > 0.03:
            score -= 0.5
        elif rate_chg_6m < -0.15:
            score += 1
        elif rate_chg_6m < -0.03:
            score += 0.5
    return int(clamp(round(score), -2, 2))


STANCE = {2: "INJECTING", 1: "MILDLY INJECTING", 0: "NEUTRAL",
          -1: "MILDLY DRAINING", -2: "DRAINING"}


def lambda_handler(event, context):
    t0 = time.time()
    errors, sources = [], []
    series = {}

    for name, sid in FRED_SERIES.items():
        try:
            obs = fred(sid)
            if obs:
                series[name] = obs
            else:
                errors.append(f"{name}({sid}): empty")
        except Exception as e:
            errors.append(f"{name}({sid}): {type(e).__name__}")
    if any(k in series for k in ("boj_assets", "jp_rate_3m", "usdjpy")):
        sources.append("FRED — BOJ / JGB / FX / US rates")

    # ── 1. BOJ funding leg ──────────────────────────────────────────
    boj = series.get("boj_assets", [])
    jp3m = series.get("jp_rate_3m", [])
    bs_6m = pct_change(boj, 182)
    bs_12m = pct_change(boj, 365)
    rate_now = latest(jp3m)
    rate_6m = level_change(jp3m, 182)
    rate_12m = level_change(jp3m, 365)
    qt_pace = ("EXPANDING" if (bs_6m or 0) > 1.5
               else "CONTRACTING" if (bs_6m or 0) < -1.5
               else "FLAT")
    boj_score = boj_injection_score(bs_6m, rate_6m)
    funding = {
        "boj_balance_sheet_chg_6m_pct": r2(bs_6m),
        "boj_balance_sheet_chg_12m_pct": r2(bs_12m),
        "boj_qt_pace": qt_pace,
        "jp_short_rate_pct": r2(rate_now),
        "jp_short_rate_chg_6m_pp": r2(rate_6m),
        "jp_short_rate_chg_12m_pp": r2(rate_12m),
        "policy_direction": ("HIKING" if (rate_6m or 0) > 0.05
                             else "CUTTING" if (rate_6m or 0) < -0.05
                             else "ON HOLD"),
        "read": (
            f"BOJ balance sheet {bs_6m:+.1f}% / 6m ({qt_pace.lower()}); "
            f"short rate {rate_now:.2f}% ({rate_6m:+.2f}pp / 6m). "
            "Every basis point of BOJ tightening raises the cost of the yen "
            "funding leg and compresses the carry."
            if bs_6m is not None and rate_now is not None and rate_6m is not None
            else "BOJ funding data partial."),
    }

    # ── 2. carry width — the US-Japan rate differential ─────────────
    ff = latest(series.get("fed_funds", []))
    us2 = latest(series.get("us_2y", []))
    us10 = latest(series.get("us_10y", []))
    jgb10 = latest(series.get("jgb_10y", []))
    front_carry = (ff - rate_now) if (ff is not None
                                      and rate_now is not None) else None
    dur_carry = (us10 - jgb10) if (us10 is not None
                                   and jgb10 is not None) else None
    front_6m_ago = None
    if series.get("fed_funds") and jp3m:
        ff_then = val_days_ago(series["fed_funds"], 182)
        jp_then = val_days_ago(jp3m, 182)
        if ff_then is not None and jp_then is not None:
            front_6m_ago = ff_then - jp_then
    front_chg = (front_carry - front_6m_ago) if (
        front_carry is not None and front_6m_ago is not None) else None
    width = {
        "front_end_carry_pp": r2(front_carry),
        "front_end_carry_chg_6m_pp": r2(front_chg),
        "duration_carry_pp": r2(dur_carry),
        "us_fed_funds_pct": r2(ff),
        "us_2y_pct": r2(us2),
        "us_10y_pct": r2(us10),
        "trend": ("WIDENING" if (front_chg or 0) > 0.15
                  else "COMPRESSING" if (front_chg or 0) < -0.15
                  else "STABLE"),
        "read": (
            f"US-Japan front-end carry {front_carry:+.2f}pp "
            f"({'widening' if (front_chg or 0) > 0.15 else 'compressing' if (front_chg or 0) < -0.15 else 'stable'}), "
            f"10y duration carry {dur_carry:+.2f}pp — the gross spread a "
            "leveraged yen-funded position earns before currency moves."
            if front_carry is not None and dur_carry is not None
            else "Carry-width data partial."),
    }

    # ── 3. the detonator — USD/JPY level, momentum, realised vol ────
    fx = series.get("usdjpy", [])
    usdjpy = latest(fx)
    chg_1m = pct_change(fx, 30)
    chg_3m = pct_change(fx, 91)
    chg_6m = pct_change(fx, 182)
    rv20 = realized_vol(fx, 20)
    rv60 = realized_vol(fx, 60)
    vol_regime = ("CALM" if (rv20 or 0) < 10.5
                  else "ELEVATED" if (rv20 or 0) < 14
                  else "STRESSED" if (rv20 or 0) < 20 else "SPIKING")
    # yen direction: USD/JPY falling = yen strengthening = carry pain
    yen_dir = ("YEN STRENGTHENING" if (chg_1m or 0) < -1.5
               else "YEN WEAKENING" if (chg_1m or 0) > 1.5
               else "RANGE-BOUND")
    fx_block = {
        "usdjpy": r2(usdjpy),
        "usdjpy_chg_1m_pct": r2(chg_1m),
        "usdjpy_chg_3m_pct": r2(chg_3m),
        "usdjpy_chg_6m_pct": r2(chg_6m),
        "realized_vol_20d_pct": r1(rv20),
        "realized_vol_60d_pct": r1(rv60),
        "vol_regime": vol_regime,
        "yen_direction": yen_dir,
        "read": (
            f"USD/JPY {usdjpy:.1f} ({chg_1m:+.1f}% / 1m, {yen_dir.lower()}); "
            f"20d realised vol {rv20:.0f}% ({vol_regime.lower()}). A fast yen "
            "rally with a vol spike is the steamroller — it forces leveraged "
            "carry to delever all at once."
            if usdjpy is not None and chg_1m is not None and rv20 is not None
            else "FX detonator data partial."),
    }

    # ── 4. JGB long-end stress ──────────────────────────────────────
    jgb = series.get("jgb_10y", [])
    jgb_6m = level_change(jgb, 182)
    jgb_12m = level_change(jgb, 365)
    jgb_block = {
        "jgb_10y_pct": r2(jgb10),
        "jgb_10y_chg_6m_pp": r2(jgb_6m),
        "jgb_10y_chg_12m_pp": r2(jgb_12m),
        "stress": ("RISING SHARPLY" if (jgb_6m or 0) > 0.3
                   else "RISING" if (jgb_6m or 0) > 0.1
                   else "STABLE" if abs(jgb_6m or 0) <= 0.1 else "FALLING"),
        "read": (
            f"10y JGB {jgb10:.2f}% ({jgb_6m:+.2f}pp / 6m). A rising long end "
            "pressures the BOJ and pulls Japanese capital home — yen-positive, "
            "and a slow squeeze on the carry."
            if jgb10 is not None and jgb_6m is not None
            else "JGB data partial."),
    }

    # ── 5. positioning (CFTC cross-reference) ───────────────────────
    pos = jpy_positioning()
    if pos:
        sources.append("CFTC — JPY non-commercial positioning")

    # ── unwind-risk score (0-100) ───────────────────────────────────
    comps, weights_avail = {}, 0.0

    # FX realised vol elevation — weight 30
    if rv20 is not None:
        comps["fx_vol"] = clamp((rv20 - 8.0) / (22.0 - 8.0) * 30.0, 0, 30)
        weights_avail += 30
    # yen appreciation momentum — weight 25 (only a strengthening yen scores)
    if chg_1m is not None:
        mom = 0.0
        if chg_1m < 0:
            mom = max(mom, clamp(-chg_1m / 8.0 * 25.0, 0, 25))
        if chg_3m is not None and chg_3m < 0:
            mom = max(mom, clamp(-chg_3m / 14.0 * 25.0, 0, 25))
        comps["yen_momentum"] = mom
        weights_avail += 25
    # crowded positioning — weight 20
    if pos is not None:
        net = pos.get("net_speculator") or 0
        z = pos.get("net_zscore_vs_history")
        if net < 0:                       # net short yen — the carry is on
            crowd = (clamp(10.0 + z * 5.0, 2, 20) if z is not None else 11.0)
        else:                             # net long yen — carry not crowded
            crowd = 4.0
        if pos.get("extreme"):
            crowd = max(crowd, 16.0)
        if pos.get("reversal_risk"):
            crowd = max(crowd, 14.0)
        comps["crowded_positioning"] = crowd
        weights_avail += 20
    # BOJ hawkish shift — weight 15
    if rate_6m is not None:
        comps["boj_hawkish"] = clamp(rate_6m / 0.40 * 15.0, 0, 15)
        weights_avail += 15
    # JGB long-end stress — weight 10
    if jgb_6m is not None:
        comps["jgb_stress"] = clamp(jgb_6m / 0.60 * 10.0, 0, 10)
        weights_avail += 10

    raw = sum(comps.values())
    unwind_risk = round(raw / weights_avail * 100.0, 1) if weights_avail else None
    risk_label = (None if unwind_risk is None
                  else "HIGH" if unwind_risk >= 75
                  else "ELEVATED" if unwind_risk >= 50
                  else "MODERATE" if unwind_risk >= 25 else "LOW")

    # ── carry regime ────────────────────────────────────────────────
    active_unwind = ((chg_1m or 0) < -5 and (rv20 or 0) > 16)
    wide_carry = (front_carry or 0) > 2.5
    if active_unwind:
        regime = "CARRY-UNWIND"
    elif unwind_risk is not None and unwind_risk >= 55:
        regime = "CARRY-AT-RISK"
    elif (unwind_risk is not None and unwind_risk <= 32 and wide_carry
          and vol_regime == "CALM"):
        regime = "CARRY-ON"
    else:
        regime = "NEUTRAL"

    # carry attractiveness — width vs the vol cost of holding it
    if front_carry is not None and rv20 is not None:
        if front_carry > 2.5 and rv20 < 11:
            attractiveness = "ATTRACTIVE"
        elif front_carry > 1.5 and rv20 < 16:
            attractiveness = "MODERATE"
        else:
            attractiveness = "UNATTRACTIVE"
    else:
        attractiveness = "UNKNOWN"

    # ── triggers ────────────────────────────────────────────────────
    triggers = []
    if usdjpy is not None:
        triggers.append(
            f"USD/JPY breaking below ~{usdjpy * 0.95:.0f} (a ~5% yen rally) "
            "would flip the regime toward CARRY-UNWIND.")
    if rv20 is not None:
        triggers.append(
            "20d realised vol sustained above ~16-18% signals the steamroller "
            "is moving — leveraged carry starts to delever.")
    triggers.append(
        "A BOJ rate hike (or a hawkish surprise) lifts the funding cost and "
        "is the classic carry-unwind catalyst.")
    if jgb10 is not None:
        triggers.append(
            "A disorderly rise in the 10y JGB forces Japanese repatriation — "
            "yen-positive and carry-negative.")

    # ── eurodollar read + decisive call ─────────────────────────────
    cb = read_existing("data/cb-injection.json") or {}
    cb_carry = (cb.get("carry_trade") or {})
    edx = (
        f"The BOJ is the carry funding central bank. With the balance sheet "
        f"{qt_pace.lower()} and the policy rate {funding['policy_direction'].lower()}, "
        "the yen funding leg of the global carry / eurodollar system is "
        + ("getting more expensive — a structural, persistent headwind for "
           "leveraged risk."
           if boj_score < 0 else
           "broadly stable.")
        + (" Unwind risk is currently "
           + (risk_label.lower() if risk_label else "indeterminate")
           + f" ({unwind_risk}/100)." if unwind_risk is not None else ""))

    if regime == "CARRY-UNWIND":
        call = ("DECISIVE: a yen-carry unwind is in motion. Expect correlated "
                "global de-risking — equities, EM and high-carry FX down, yen "
                "and quality bonds bid. Cut leverage; do not fade the yen.")
    elif regime == "CARRY-AT-RISK":
        call = ("DECISIVE: the carry is crowded and the detonators are warming "
                "up. Reduce yen-funded leverage and hedge tail risk now — by "
                "the time vol spikes, the exit is already crowded.")
    elif regime == "CARRY-ON":
        call = ("DECISIVE: carry conditions are constructive — wide differential, "
                "calm vol, stable yen. The carry pays, but it always pays right "
                "up until it doesn't; watch USD/JPY vol and the BOJ.")
    else:
        call = ("DECISIVE: no clean carry signal. The differential still pays, "
                "but watch the BOJ path and USD/JPY realised vol for the turn.")

    headline = (
        f"YEN CARRY: {regime}. Unwind risk "
        + (f"{unwind_risk:.0f}/100 ({risk_label})" if unwind_risk is not None
           else "n/a")
        + (f"; USD/JPY {usdjpy:.0f}, vol {rv20:.0f}% ({vol_regime.lower()})"
           if usdjpy is not None and rv20 is not None else "")
        + (f"; BOJ {STANCE[boj_score].lower()}." if True else "."))

    out = {
        "schema_version": "1.0",
        "method": "yen_carry_and_boj_liquidity",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": len(series) >= 3,
        "headline": headline,
        "carry_regime": regime,
        "boj_injection_score": boj_score,
        "boj_stance_label": STANCE[boj_score],
        "unwind_risk_score": unwind_risk,
        "unwind_risk_label": risk_label,
        "unwind_risk_components": {k: round(v, 1)
                                   for k, v in comps.items()},
        "carry_attractiveness": attractiveness,
        "boj_funding_leg": funding,
        "carry_width": width,
        "fx_detonator": fx_block,
        "jgb_long_end": jgb_block,
        "positioning": pos or {"note": "CFTC JPY positioning unavailable — "
                               "unwind score scaled across available factors"},
        "triggers": triggers,
        "eurodollar_read": edx,
        "decisive_call": call,
        "cross_reference": {
            "cb_injection_global_impulse": (cb.get("global_injection_impulse")
                                            or {}).get("label"),
            "cb_injection_carry_conditions": cb_carry.get("carry_conditions"),
        },
        "sources": sources,
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return {"ok": out["ok"], "carry_regime": regime,
            "unwind_risk": unwind_risk, "errors": len(errors)}
