"""
justhodl-ecb-detail — ECB / Eurosystem Liquidity Detail Engine.

cb-injection scores the ECB at the balance-sheet-*total* level. This engine
goes to the source — the ECB Data Portal SDMX API — for the granular
capital-injection data a global-macro desk actually watches:

  EXCESS LIQUIDITY  (ILM.D.U2.C.EXLIQ.U2.EUR, daily) — the single most
      eurodollar-relevant ECB number: surplus reserves the banking system
      holds above requirements. Draining excess liquidity = the euro leg of
      the offshore-USD system tightening.
  DEPOSIT FACILITY RECOURSE  (ILM.D.U2.C.L020200) — cash parked overnight
      at the ECB; the mirror of excess liquidity.
  CURRENT ACCOUNTS  (ILM.D.U2.C.L020100) — minimum-reserve balances.
  NET LIQUIDITY EFFECT  (ILM.D.U2.C.NLIQ) — autonomous factors + monetary
      policy portfolios; the structural liquidity drift.

Combined with the Eurosystem balance sheet (FRED ECBASSETSW — the APP/PEPP
runoff = passive QT) and the three key rates (deposit facility / main
refinancing / marginal lending), it builds:

  - an ECB LIQUIDITY REGIME (abundant -> approaching the structural floor)
  - the QT pace and an illustrative runway toward the ample-reserves floor
  - the rate corridor and policy stance
  - an ECB INJECTION SCORE (-2..+2) and a eurodollar read.

OUTPUT: data/ecb-detail.json   SCHEDULE: daily 11:00 UTC
Real data only — ECB Data Portal + FRED. Not investment advice.
"""
import csv
import io
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ecb-detail.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

ECB_API = "https://data-api.ecb.europa.eu/service/data"

# ECB Data Portal — Internal Liquidity Management (ILM) daily series.
# key = the dot-path after the dataflow; dataflow is ILM.
ILM_SERIES = {
    "excess_liquidity":   "D.U2.C.EXLIQ.U2.EUR",
    "deposit_facility":   "D.U2.C.L020200.U2.EUR",
    "current_accounts":   "D.U2.C.L020100.U2.EUR",
    "net_liquidity_eff":  "D.U2.C.NLIQ.U2.EUR",
}

# FRED carries the Eurosystem balance sheet and the three ECB key rates.
FRED_SERIES = {
    "balance_sheet":     "ECBASSETSW",   # Eurosystem total assets, weekly, EUR mn
    "rate_deposit":      "ECBDFR",       # deposit facility rate, %
    "rate_main_refi":    "ECBMRRFR",     # main refinancing operations rate, %
    "rate_marginal":     "ECBMLFR",      # marginal lending facility rate, %
}


# ───────────────────────── data fetchers ─────────────────────────
def _get(url, ua, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": ua})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    raise last or RuntimeError(f"fetch failed: {url}")


def fred(series_id, limit=600):
    """FRED observations -> newest-first [(date, float)]."""
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    d = json.loads(_get(url, "justhodl-ecb-detail/1.0"))
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


def ecb_series(key, last_n=420):
    """ECB Data Portal ILM series -> newest-first [(date, float)]."""
    url = f"{ECB_API}/ILM/{key}?format=csvdata&lastNObservations={last_n}"
    raw = _get(url, "justhodl-ecb-detail/1.0").decode("utf-8", "ignore")
    rdr = csv.reader(io.StringIO(raw))
    rows = list(rdr)
    if len(rows) < 2:
        return []
    header = rows[0]
    try:
        ti = header.index("TIME_PERIOD")
        vi = header.index("OBS_VALUE")
    except ValueError:
        return []
    out = []
    for row in rows[1:]:
        if len(row) <= max(ti, vi):
            continue
        d, v = row[ti].strip(), row[vi].strip()
        if not d or v in ("", "NaN", "."):
            continue
        try:
            out.append((d, float(v)))
        except ValueError:
            continue
    out.sort(key=lambda x: x[0], reverse=True)
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
    return round(obs[0][1] - base, 2)


def pct_change(obs, days):
    base = val_days_ago(obs, days)
    if base in (None, 0) or not obs:
        return None
    return round((obs[0][1] - base) / abs(base) * 100, 2)


def to_bn_scale(sample):
    """Detect the unit of an ECB EUR series from a representative value."""
    a = abs(sample or 0)
    if a >= 5e8:           # raw euro
        return 1e-9
    if a >= 5e4:           # euro millions (ECB ILM convention)
        return 1e-3
    return 1.0             # already ~billions


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


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

    # ── ECB Data Portal: ILM liquidity series ──
    ilm = {}
    for tag, key in ILM_SERIES.items():
        try:
            ilm[tag] = ecb_series(key)
            if not ilm[tag]:
                errors.append(f"ILM/{key}: empty")
        except Exception as e:
            errors.append(f"ILM/{key}: {str(e)[:70]}")
            ilm[tag] = []
    if any(ilm.values()):
        sources.append("ECB Data Portal — Internal Liquidity Management (ILM)")

    # ── FRED: balance sheet + key rates ──
    fr = {}
    for tag, sid in FRED_SERIES.items():
        try:
            fr[tag] = fred(sid)
        except Exception as e:
            errors.append(f"{sid}: {str(e)[:70]}")
            fr[tag] = []
    if any(fr.values()):
        sources.append("FRED — Eurosystem balance sheet & ECB key rates")

    el = ilm["excess_liquidity"]
    scale = to_bn_scale(el[0][1] if el else None)

    def bn(obs):
        return round(obs[0][1] * scale, 1) if obs else None

    # ── 1. EXCESS LIQUIDITY — the core eurodollar signal ──
    el_bn = bn(el)
    el_1m = level_change(el, 30)
    el_3m = level_change(el, 91)
    el_6m = level_change(el, 182)
    el_1m_bn = round(el_1m * scale, 1) if el_1m is not None else None
    el_3m_bn = round(el_3m * scale, 1) if el_3m is not None else None
    el_6m_bn = round(el_6m * scale, 1) if el_6m is not None else None
    drain_per_month = (round(el_3m_bn / 3, 1)
                       if el_3m_bn is not None else None)

    if el_bn is None:
        liq_level = "UNKNOWN"
    elif el_bn > 2000:
        liq_level = "ABUNDANT"
    elif el_bn > 1200:
        liq_level = "AMPLE"
    elif el_bn > 700:
        liq_level = "APPROACHING FLOOR"
    else:
        liq_level = "AT/BELOW FLOOR"

    if drain_per_month is None:
        liq_trend = "UNKNOWN"
    elif drain_per_month < -20:
        liq_trend = "DRAINING"
    elif drain_per_month > 20:
        liq_trend = "RISING"
    else:
        liq_trend = "STABLE"
    liq_regime = (f"{liq_level} — {liq_trend}"
                  if liq_level != "UNKNOWN" else "UNKNOWN")

    # illustrative runway toward the ~EUR 1,500bn ample-reserves zone
    runway_months = None
    if (el_bn is not None and drain_per_month is not None
            and drain_per_month < -5 and el_bn > 1500):
        runway_months = int((el_bn - 1500) / abs(drain_per_month))

    liq_read = (
        f"Excess liquidity EUR {el_bn:,.0f}bn ({el_3m_bn:+,.0f}bn over 3m, "
        f"~{drain_per_month:+,.0f}bn/month) — {liq_regime.lower()}."
        + (f" At this pace ~{runway_months} months to the EUR 1,500bn "
           "ample-reserves zone where money-market rates turn sensitive."
           if runway_months else "")
        if el_bn is not None and el_3m_bn is not None
        else "Excess-liquidity data unavailable.")

    # ── 2. BALANCE SHEET — passive QT via APP/PEPP runoff ──
    bs = fr["balance_sheet"]
    bs_bn = round(bs[0][1] / 1000, 1) if bs else None   # EUR mn -> bn
    bs_6m = pct_change(bs, 182)
    bs_12m = pct_change(bs, 365)
    if bs_6m is None:
        qt_pace = "UNKNOWN"
    elif bs_6m < -6:
        qt_pace = "RAPID QT"
    elif bs_6m < -1.5:
        qt_pace = "PASSIVE QT — measured runoff"
    elif bs_6m > 1.5:
        qt_pace = "EXPANDING"
    else:
        qt_pace = "FLAT"
    bs_read = (
        f"Eurosystem balance sheet EUR {bs_bn:,.0f}bn, {bs_6m:+.1f}% over 6m "
        f"({bs_12m:+.1f}% / 12m) — {qt_pace.lower()}; the APP and PEPP "
        "portfolios are rolling off without reinvestment."
        if bs_bn is not None and bs_6m is not None
        else "Balance-sheet data partial.")

    # ── 3. POLICY RATES & CORRIDOR ──
    def latest(obs):
        return obs[0][1] if obs else None

    r_dep = latest(fr["rate_deposit"])
    r_mro = latest(fr["rate_main_refi"])
    r_mlf = latest(fr["rate_marginal"])
    dep_6m = level_change(fr["rate_deposit"], 182)
    dep_12m = level_change(fr["rate_deposit"], 365)
    corridor_bp = (round((r_mlf - r_dep) * 100)
                   if r_dep is not None and r_mlf is not None else None)
    if dep_6m is None:
        rate_stance = "UNKNOWN"
    elif dep_6m <= -0.35:
        rate_stance = "EASING"
    elif dep_6m >= 0.35:
        rate_stance = "TIGHTENING"
    elif dep_6m <= -0.10:
        rate_stance = "EASING AT MARGIN"
    elif dep_6m >= 0.10:
        rate_stance = "TIGHTENING AT MARGIN"
    else:
        rate_stance = "ON HOLD"
    rate_read = (
        f"Deposit facility rate {r_dep}% (the floor that prices euro "
        f"liquidity), main refinancing {r_mro}%, marginal lending {r_mlf}% "
        f"— corridor {corridor_bp}bp; policy {rate_stance.lower()} "
        f"({dep_6m:+.2f}pp over 6m)."
        if r_dep is not None and dep_6m is not None
        else f"Deposit facility rate {r_dep}% — rate detail partial.")

    # ── 4. ECB INJECTION SCORE (-2..+2) ──
    score = 0
    if bs_6m is not None:
        score += 1 if bs_6m > 1.5 else -1 if bs_6m < -1.5 else 0
    if dep_6m is not None:
        score += 1 if dep_6m < -0.10 else -1 if dep_6m > 0.10 else 0
    if drain_per_month is not None:
        score += (1 if drain_per_month > 20 else
                  -1 if drain_per_month < -20 else 0)
    score = int(clamp(score, -2, 2))
    SCORE_LABEL = {2: "STRONG INJECTION", 1: "INJECTING", 0: "NEUTRAL",
                   -1: "DRAINING", -2: "STRONG DRAIN"}
    stance_label = SCORE_LABEL[score]

    # ── 5. EURODOLLAR READ ──
    draining = score < 0
    if draining and liq_level in ("APPROACHING FLOOR", "AT/BELOW FLOOR"):
        edollar = ("ECB draining with excess liquidity nearing the structural "
                   "floor — the euro leg of the offshore-USD system is "
                   "tightening; watch EUR cross-currency basis and €STR-DFR.")
    elif draining:
        edollar = ("ECB in passive QT — euro liquidity still abundant but "
                   "trending down; a slow, persistent tightening of the "
                   "eurodollar system rather than an acute squeeze.")
    elif score > 0:
        edollar = ("ECB adding liquidity at the margin — supportive for euro "
                   "funding and the eurodollar system.")
    else:
        edollar = ("ECB on hold with liquidity ample — neutral for euro "
                   "funding conditions.")

    # ── 6. cross-reference (do not rebuild) ──
    cbi = read_existing("data/cb-injection.json") or {}
    eds = read_existing("data/eurodollar-stress.json") or {}
    cross = {
        "cb_injection_global_impulse":
            (cbi.get("global_injection_impulse") or {}).get("label"),
        "eurodollar_stress_score":
            eds.get("score") or eds.get("stress_score"),
    }

    headline = (
        f"ECB: {stance_label}. Excess liquidity EUR "
        f"{el_bn:,.0f}bn ({liq_regime.lower()}); balance sheet {bs_6m:+.1f}%/6m "
        f"({qt_pace.lower()}); deposit rate {r_dep}% — policy "
        f"{rate_stance.lower()}."
        if el_bn is not None and bs_6m is not None and r_dep is not None
        else f"ECB liquidity detail: {stance_label} (partial data).")

    core_ok = el_bn is not None and bs_bn is not None
    out = {
        "schema_version": "1.0",
        "method": "ecb_eurosystem_liquidity_detail",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": core_ok and len(errors) <= 3,
        "headline": headline,
        "ecb_injection_score": score,
        "stance_label": stance_label,
        "liquidity": {
            "excess_liquidity_eur_bn": el_bn,
            "change_1m_bn": el_1m_bn,
            "change_3m_bn": el_3m_bn,
            "change_6m_bn": el_6m_bn,
            "drain_pace_bn_per_month": drain_per_month,
            "deposit_facility_recourse_eur_bn": bn(ilm["deposit_facility"]),
            "current_accounts_eur_bn": bn(ilm["current_accounts"]),
            "net_liquidity_effect_eur_bn": bn(ilm["net_liquidity_eff"]),
            "level": liq_level,
            "trend": liq_trend,
            "regime": liq_regime,
            "runway_to_floor_months": runway_months,
            "read": liq_read,
        },
        "balance_sheet": {
            "total_assets_eur_bn": bs_bn,
            "change_6m_pct": bs_6m,
            "change_12m_pct": bs_12m,
            "qt_pace": qt_pace,
            "read": bs_read,
        },
        "policy_rates": {
            "deposit_facility_pct": r_dep,
            "main_refinancing_pct": r_mro,
            "marginal_lending_pct": r_mlf,
            "corridor_width_bp": corridor_bp,
            "deposit_rate_change_6m_pp": dep_6m,
            "deposit_rate_change_12m_pp": dep_12m,
            "stance": rate_stance,
            "read": rate_read,
        },
        "eurodollar_read": edollar,
        "cross_reference": cross,
        "sources": sources,
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[ecb-detail] {stance_label} | excess liq EUR {el_bn}bn "
          f"({liq_regime}) | errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "stance": stance_label,
                                "excess_liquidity_bn": el_bn,
                                "errors": len(errors)})}
