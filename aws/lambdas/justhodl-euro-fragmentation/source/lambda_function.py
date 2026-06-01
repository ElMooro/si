"""
justhodl-euro-fragmentation — Euro-Area Sovereign Fragmentation Engine.

systemic-stress carries SovCISS as one sub-block. This engine is the
dedicated deep dive on the single question a euro rates desk lives by:
is the monetary union's bond market pricing every member as one currency,
or is it splintering — pricing redenomination / default risk into the
spreads of France, Belgium and the periphery?

Fragmentation is the euro's structural fault line. The 2011-12 crisis took
the BTP-Bund spread to ~550bp before Draghi's "whatever it takes"; the 2024
French snap election drove the OAT-Bund spread to records and, briefly,
near peripheral levels. France is now the locus — and France fragmenting
is more dangerous than Italy fragmenting, because France is a CORE economy,
too large for a small-scale backstop, and its access to the ECB's TPI is
ambiguous while it sits under an excessive-deficit procedure.

The engine fuses two real sources:
  SovCISS  (ECB Data Portal, DAILY) — the ECB's official per-country
      sovereign-stress index; its level, percentile, momentum and the
      cross-country dispersion are the fast fragmentation signal.
  10Y YIELDS  (FRED OECD, monthly) — the concrete tradeable spreads over
      the Bund benchmark in basis points: OAT-Bund, BTP-Bund, Bonos-Bund.

-> a 0-100 fragmentation score, a regime, a core-vs-periphery split, a
dedicated France deep-dive and the ECB-backstop (TPI) context.

OUTPUT: data/euro-fragmentation.json   SCHEDULE: daily 12:20 UTC
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
OUT_KEY = "data/euro-fragmentation.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

ECB_API = "https://data-api.ecb.europa.eu/service/data"

# euro-area sovereigns — Germany is the risk-free benchmark.
BENCH = "DE"
COUNTRIES = ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "PT", "IE", "GR"]
CORE = {"FR", "NL", "BE", "AT"}          # core / semi-core (ex-Germany)
PERIPHERY = {"IT", "ES", "PT", "GR", "IE"}
NAME = {"DE": "Germany", "FR": "France", "IT": "Italy", "ES": "Spain",
        "NL": "Netherlands", "BE": "Belgium", "AT": "Austria",
        "PT": "Portugal", "IE": "Ireland", "GR": "Greece"}
# FRED OECD 10Y government bond yields, per country.
FRED_10Y = {cc: f"IRLTLT01{cc}M156N" for cc in COUNTRIES}


# ───────────────────────── data fetch ─────────────────────────
def _get(url, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-euro-fragmentation/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    raise last or RuntimeError(f"fetch failed: {url}")


def ciss_series(key, last_n=2900):
    """ECB CISS dataflow series -> newest-first [(date, float)]."""
    url = f"{ECB_API}/CISS/{key}?format=csvdata&lastNObservations={last_n}"
    raw = _get(url).decode("utf-8", "ignore")
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


def fred(series_id, limit=400):
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
    return obs[0][1] - base


def percentile(obs):
    if not obs or len(obs) < 30:
        return None
    latest_v = obs[0][1]
    vals = [v for _, v in obs]
    return round(sum(1 for v in vals if v <= latest_v) / len(vals) * 100, 1)


def latest(obs):
    return obs[0][1] if obs else None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def stdev(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def sovciss_regime(pct, level):
    if pct is None:
        return "UNKNOWN"
    if pct >= 95 and (level or 0) >= 0.20:
        return "STRESSED"
    if pct >= 85:
        return "ELEVATED"
    if pct >= 60:
        return "FIRM"
    return "CALM"


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
    errors = []

    # ── 1. SovCISS (daily) + 10Y yields (monthly) per country ──
    sov, y10 = {}, {}
    for cc in COUNTRIES:
        try:
            sov[cc] = ciss_series(f"D.{cc}.Z0Z.4F.EC.SOV_CIN.IDX")
            if not sov[cc]:
                errors.append(f"SovCISS/{cc}: empty")
        except Exception as e:
            errors.append(f"SovCISS/{cc}: {str(e)[:55]}")
            sov[cc] = []
        try:
            y10[cc] = fred(FRED_10Y[cc])
            if not y10[cc]:
                errors.append(f"10Y/{cc}: empty")
        except Exception as e:
            errors.append(f"10Y/{cc}: {str(e)[:55]}")
            y10[cc] = []

    bund = latest(y10.get(BENCH))

    # ── 2. per-country fragmentation block ──
    countries = {}
    for cc in COUNTRIES:
        s = sov[cc]
        sc = latest(s)
        pct = percentile(s)
        y = latest(y10.get(cc))
        spread_bp = (round((y - bund) * 100, 1)
                     if y is not None and bund is not None else None)

        def sp_chg(days):
            yc = level_change(y10.get(cc), days)
            yb = level_change(y10.get(BENCH), days)
            if yc is None or yb is None:
                return None
            return round((yc - yb) * 100, 1)

        countries[cc] = {
            "name": NAME[cc],
            "group": "benchmark" if cc == BENCH else (
                "core" if cc in CORE else "periphery"),
            "sovciss": round(sc, 5) if sc is not None else None,
            "sovciss_percentile": pct,
            "sovciss_change_1w": (round(level_change(s, 7), 5)
                                  if level_change(s, 7) is not None else None),
            "sovciss_change_1m": (round(level_change(s, 30), 5)
                                  if level_change(s, 30) is not None
                                  else None),
            "sovciss_regime": sovciss_regime(pct, sc),
            "yield_10y_pct": round(y, 3) if y is not None else None,
            "spread_vs_bund_bp": spread_bp,
            "spread_change_1m_bp": sp_chg(30),
            "spread_change_3m_bp": sp_chg(91),
            "spread_change_6m_bp": sp_chg(182),
        }

    # ── 3. core vs periphery decomposition ──
    core_sp = {c: countries[c]["spread_vs_bund_bp"] for c in CORE
               if countries[c]["spread_vs_bund_bp"] is not None}
    peri_sp = {c: countries[c]["spread_vs_bund_bp"] for c in PERIPHERY
               if countries[c]["spread_vs_bund_bp"] is not None}
    widest_core = max(core_sp, key=core_sp.get) if core_sp else None
    widest_peri = max(peri_sp, key=peri_sp.get) if peri_sp else None
    # which countries are the 3 most-stressed by SovCISS
    sov_pct = {c: countries[c]["sovciss_percentile"] for c in COUNTRIES
               if c != BENCH and countries[c]["sovciss_percentile"]
               is not None}
    top3 = sorted(sov_pct, key=sov_pct.get, reverse=True)[:3]
    core_stress_flag = any(c in CORE for c in top3)
    cvp_read = (
        f"Core spreads average {mean(list(core_sp.values())):.0f}bp over the "
        f"Bund (widest: {NAME.get(widest_core, '?')} "
        f"{core_sp.get(widest_core, 0):.0f}bp); periphery averages "
        f"{mean(list(peri_sp.values())):.0f}bp (widest: "
        f"{NAME.get(widest_peri, '?')} {peri_sp.get(widest_peri, 0):.0f}bp). "
        + ("A core economy sits among the most-stressed sovereigns — core "
           "fragmentation is structurally more dangerous than peripheral "
           "stress." if core_stress_flag else
           "Stress is concentrated in the periphery, the historically "
           "normal pattern.")
        if core_sp and peri_sp
        else "Core/periphery spread data partial.")

    # ── 4. FRAGMENTATION SCORE (0-100) ──
    all_sp = {c: countries[c]["spread_vs_bund_bp"] for c in COUNTRIES
              if c != BENCH and countries[c]["spread_vs_bund_bp"]
              is not None}
    widest_bp = max(all_sp.values()) if all_sp else None
    sp_moms = [countries[c]["spread_change_1m_bp"] for c in COUNTRIES
               if countries[c]["spread_change_1m_bp"] is not None]
    fastest_widening = max(sp_moms) if sp_moms else None
    sov_levels = [countries[c]["sovciss"] for c in COUNTRIES
                  if c != BENCH and countries[c]["sovciss"] is not None]
    dispersion = stdev(sov_levels)
    sov_moms = [countries[c]["sovciss_change_1m"] for c in COUNTRIES
                if countries[c]["sovciss_change_1m"] is not None]
    fastest_sov = max(sov_moms) if sov_moms else None

    score, comps = 0.0, {}
    sc1 = clamp((widest_bp or 0) / 300, 0, 1) * 30
    comps["widest_spread"] = round(sc1, 1); score += sc1
    sc2 = clamp((fastest_widening or 0) / 40, 0, 1) * 25
    comps["spread_widening_pace"] = round(sc2, 1); score += sc2
    sc3 = clamp((dispersion or 0) / 0.06, 0, 1) * 20
    comps["sovciss_dispersion"] = round(sc3, 1); score += sc3
    sc4 = clamp((fastest_sov or 0) / 0.04, 0, 1) * 15
    comps["sovciss_momentum"] = round(sc4, 1); score += sc4
    sc5 = 10.0 if core_stress_flag else 0.0
    comps["core_fragmentation_flag"] = sc5; score += sc5
    score = round(clamp(score, 0, 100), 1)

    if score >= 70:
        regime = "ACUTE FRAGMENTATION"
    elif score >= 48:
        regime = "FRAGMENTATION RISK"
    elif score >= 25:
        regime = "DISPERSION"
    else:
        regime = "COHESIVE"
    frag_read = (
        f"Euro-area fragmentation {score:.0f}/100 — {regime.lower()}. Widest "
        f"sovereign spread {widest_bp:.0f}bp; fastest 1m widening "
        f"{fastest_widening:+.0f}bp. "
        + ("Spreads are widening with core involvement — the union's bond "
           "market is pricing meaningful divergence; watch the ECB's "
           "response function." if score >= 48 else
           "The bloc's bond markets are pricing broadly as one currency; "
           "divergence is contained.")
        if widest_bp is not None and fastest_widening is not None
        else "Fragmentation score partial.")

    # ── 5. FRANCE DEEP-DIVE ──
    fr = countries["FR"]
    fr_read = (
        f"France OAT-Bund spread {fr['spread_vs_bund_bp']:.0f}bp "
        f"({fr['spread_change_3m_bp']:+.0f}bp / 3m); SovCISS at its "
        f"{fr['sovciss_percentile']:.0f}th percentile ({fr['sovciss_regime'].lower()}). "
        "France is the fragmentation locus that matters most: a core "
        "economy with a large deficit and fragile politics. Core "
        "fragmentation is harder to backstop than peripheral stress — "
        "France is too big for a Greece-style programme, and its access to "
        "the ECB's TPI is ambiguous while it runs an excessive deficit."
        if fr["spread_vs_bund_bp"] is not None
        and fr["sovciss_percentile"] is not None
        else "France detail partial.")
    france_focus = {
        "oat_bund_spread_bp": fr["spread_vs_bund_bp"],
        "spread_change_1m_bp": fr["spread_change_1m_bp"],
        "spread_change_3m_bp": fr["spread_change_3m_bp"],
        "sovciss": fr["sovciss"],
        "sovciss_percentile": fr["sovciss_percentile"],
        "sovciss_regime": fr["sovciss_regime"],
        "is_most_stressed_core": widest_core == "FR",
        "read": fr_read,
    }

    # ── 6. ECB backstop context ──
    ecb_backstop = {
        "instrument": "Transmission Protection Instrument (TPI)",
        "read": ("The ECB's TPI (created July 2022) can buy a member state's "
                 "bonds without limit to counter 'unwarranted, disorderly' "
                 "fragmentation — a powerful implicit cap on spreads. But "
                 "eligibility is conditional on sound fiscal policy, so the "
                 "backstop is most reliable exactly where it is least "
                 "needed; a large core sovereign under fiscal strain is the "
                 "uncomfortable edge case."),
    }

    # ── 7. cross-reference (do not rebuild) ──
    ssj = read_existing("data/systemic-stress.json") or {}
    cbi = read_existing("data/cb-injection.json") or {}
    ecd = read_existing("data/ecb-detail.json") or {}
    cross = {
        "systemic_stress_composite": (ssj.get("composite") or {}).get(
            "score_0_100"),
        "systemic_stress_regime": (ssj.get("composite") or {}).get("regime"),
        "cb_injection_global_impulse": (cbi.get("global_injection_impulse")
                                        or {}).get("label"),
        "ecb_stance": ecd.get("stance_label"),
    }

    headline = (
        f"Euro fragmentation {regime}. Score {score:.0f}/100 — widest "
        f"spread {widest_bp:.0f}bp ({NAME.get(max(all_sp, key=all_sp.get))}); "
        f"France OAT-Bund {fr['spread_vs_bund_bp']:.0f}bp."
        if widest_bp is not None and all_sp
        and fr["spread_vs_bund_bp"] is not None
        else f"Euro-area sovereign fragmentation: {regime} (partial data).")

    core_ok = (sum(1 for c in COUNTRIES if sov.get(c)) >= 6
               and bund is not None)
    out = {
        "schema_version": "1.0",
        "method": "euro_area_sovereign_fragmentation",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": core_ok and len(errors) <= 8,
        "headline": headline,
        "fragmentation": {
            "score_0_100": score,
            "regime": regime,
            "components": comps,
            "widest_spread_bp": widest_bp,
            "fastest_widening_1m_bp": fastest_widening,
            "read": frag_read,
        },
        "bund_benchmark_10y_pct": round(bund, 3) if bund is not None else None,
        "countries": countries,
        "core_vs_periphery": {
            "core_avg_spread_bp": (round(mean(list(core_sp.values())), 1)
                                   if core_sp else None),
            "periphery_avg_spread_bp": (round(mean(list(peri_sp.values())), 1)
                                        if peri_sp else None),
            "widest_core": widest_core,
            "widest_periphery": widest_peri,
            "most_stressed_top3": top3,
            "core_stress_flag": core_stress_flag,
            "read": cvp_read,
        },
        "france_focus": france_focus,
        "ecb_backstop": ecb_backstop,
        "cross_reference": cross,
        "sources": ["ECB Data Portal — SovCISS (sovereign stress, daily)",
                    "FRED — OECD 10Y government bond yields"],
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[euro-fragmentation] {regime} {score}/100 | widest "
          f"{widest_bp}bp | France OAT-Bund {fr['spread_vs_bund_bp']}bp | "
          f"errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "score": score,
                                "regime": regime,
                                "france_oat_bund_bp":
                                    fr["spread_vs_bund_bp"],
                                "errors": len(errors)})}
