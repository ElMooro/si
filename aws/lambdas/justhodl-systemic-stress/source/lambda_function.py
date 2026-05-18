"""
justhodl-systemic-stress — ECB Systemic & Sovereign Stress Engine.

The platform already proxies sovereign credit risk from bond yields
(cds-proxy) and bond/credit stress from ICE BofA spreads (credit-stress).
This engine adds the thing those proxies cannot: the ECB's OFFICIAL,
purpose-built stress indices, straight from the ECB Data Portal —

  CISS — Composite Indicator of Systemic Stress. A real-time index of
      financial-system stress fusing money-market, bond-market, equity,
      financial-intermediary and FX sub-indices, with cross-correlation
      weighting so the index spikes only when stress is BROAD. Published
      for the euro area and, country-by-country, for the US, UK, China,
      Germany, France, Italy, Spain and more. The reference peaks: GFC
      ~0.9, euro crisis ~0.5, COVID ~0.6.

  SovCISS — Composite Indicator of Systemic SOVEREIGN Stress. The same
      methodology applied to the government bond market, country by
      country across the euro area (DE, FR, IT, ES, NL, BE, AT, FI, IE,
      GR, PT). The dispersion of SovCISS across countries — and the gap
      of the worst sovereign over Germany — is a direct read on euro-area
      FRAGMENTATION risk.

It computes percentile ranks against ~11y of history (the CISS level is
small in absolute terms, so the percentile is what carries the signal), a
0-100 composite stress score, a fragmentation gauge, and cross-references
the proxy engines so the page is one consolidated stress view.

OUTPUT: data/systemic-stress.json   SCHEDULE: daily 12:00 UTC
Real data only — ECB Data Portal. Not investment advice.
"""
import csv
import io
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/systemic-stress.json"

ECB_API = "https://data-api.ecb.europa.eu/service/data"

# CISS — systemic financial stress (euro area + the majors Khalid named).
CISS_COUNTRIES = ["U2", "US", "GB", "CN", "DE", "FR", "IT", "ES"]
# SovCISS — sovereign bond-market stress, per euro-area country.
SOV_COUNTRIES = ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "FI", "IE",
                 "GR", "PT"]

NAME = {
    "U2": "Euro Area", "US": "United States", "GB": "United Kingdom",
    "CN": "China", "DE": "Germany", "FR": "France", "IT": "Italy",
    "ES": "Spain", "NL": "Netherlands", "BE": "Belgium", "AT": "Austria",
    "FI": "Finland", "IE": "Ireland", "GR": "Greece", "PT": "Portugal",
}


# ───────────────────────── data fetch ─────────────────────────
def _get(url, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-systemic-stress/1.0"})
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
    return round(obs[0][1] - base, 4)


def percentile(obs):
    """Percentile rank (0-100) of the latest value vs full history."""
    if not obs or len(obs) < 30:
        return None
    latest_v = obs[0][1]
    vals = [v for _, v in obs]
    below = sum(1 for v in vals if v <= latest_v)
    return round(below / len(vals) * 100, 1)


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


def regime_from(pct, level):
    """Stress regime — percentile-led, with absolute-level guardrails."""
    if pct is None:
        return "UNKNOWN"
    if pct >= 97:
        r = "CRISIS"
    elif pct >= 90:
        r = "STRESSED"
    elif pct >= 75:
        r = "ELEVATED"
    elif pct >= 50:
        r = "NORMAL"
    else:
        r = "CALM"
    # the CISS is bounded 0-1; do not call a low absolute level a crisis
    if level is not None:
        if r == "CRISIS" and level < 0.30:
            r = "STRESSED"
        if r == "STRESSED" and level < 0.15:
            r = "ELEVATED"
    return r


def read_existing(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def block(obs):
    """Build a per-series stress block from an observation list."""
    if not obs:
        return None
    lvl = obs[0][1]
    pct = percentile(obs)
    return {
        "value": round(lvl, 5),
        "percentile": pct,
        "change_1m": level_change(obs, 30),
        "change_3m": level_change(obs, 91),
        "as_of": obs[0][0],
        "regime": regime_from(pct, lvl),
        "history_n": len(obs),
    }


# ───────────────────────── handler ─────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    errors = []

    # ── 1. CISS — systemic financial stress ──
    ciss = {}
    for cc in CISS_COUNTRIES:
        try:
            obs = ciss_series(f"D.{cc}.Z0Z.4F.EC.SS_CIN.IDX")
            b = block(obs)
            if b:
                b["name"] = NAME.get(cc, cc)
                ciss[cc] = b
            else:
                errors.append(f"CISS/{cc}: empty")
        except Exception as e:
            errors.append(f"CISS/{cc}: {str(e)[:60]}")

    # ── 2. SovCISS — sovereign bond-market stress ──
    sov = {}
    for cc in SOV_COUNTRIES:
        try:
            obs = ciss_series(f"D.{cc}.Z0Z.4F.EC.SOV_CIN.IDX")
            b = block(obs)
            if b:
                b["name"] = NAME.get(cc, cc)
                sov[cc] = b
            else:
                errors.append(f"SovCISS/{cc}: empty")
        except Exception as e:
            errors.append(f"SovCISS/{cc}: {str(e)[:60]}")

    # ── 3. SYSTEMIC STRESS read ──
    ea = ciss.get("U2") or {}
    ea_pct = ea.get("percentile")
    worst_ciss = None
    if ciss:
        worst_ciss = max(
            (c for c in ciss if c != "U2" and ciss[c].get("percentile")
             is not None),
            key=lambda c: ciss[c]["percentile"], default=None)
    sys_read = (
        f"Euro-area CISS at its {ea_pct:.0f}th percentile of ~11y of history "
        f"({ea.get('regime', 'n/a').lower()}); "
        + (f"the most-stressed major economy is {ciss[worst_ciss]['name']} "
           f"({ciss[worst_ciss]['percentile']:.0f}th pct, "
           f"{ciss[worst_ciss]['regime'].lower()})."
           if worst_ciss else "country detail partial.")
        if ea_pct is not None
        else "Systemic-stress data partial.")

    # ── 4. SOVEREIGN STRESS + FRAGMENTATION ──
    de_sov = (sov.get("DE") or {}).get("value")
    sov_levels = {c: sov[c]["value"] for c in sov
                  if sov[c].get("value") is not None}
    worst_sov = (max(sov_levels, key=sov_levels.get) if sov_levels else None)
    ea_sov_avg = mean(list(sov_levels.values()))
    sov_dispersion = stdev(list(sov_levels.values()))
    # fragmentation: gap of the worst sovereign over the German benchmark,
    # scaled, plus cross-country dispersion
    frag_gap = (round(sov_levels[worst_sov] - de_sov, 4)
                if worst_sov and de_sov is not None else None)
    frag_score = 0.0
    if frag_gap is not None:
        frag_score += clamp(frag_gap / 0.20, 0, 1) * 60
    if sov_dispersion is not None:
        frag_score += clamp(sov_dispersion / 0.08, 0, 1) * 40
    frag_score = round(clamp(frag_score, 0, 100), 1)
    if frag_score >= 70:
        frag_label = "ACUTE FRAGMENTATION"
    elif frag_score >= 45:
        frag_label = "FRAGMENTATION BUILDING"
    elif frag_score >= 22:
        frag_label = "MILD DISPERSION"
    else:
        frag_label = "COHESIVE"
    sov_read = (
        f"Sovereign stress is widest in {sov[worst_sov]['name']} "
        f"(SovCISS {sov_levels[worst_sov]:.3f}, {sov[worst_sov]['regime'].lower()}), "
        f"vs Germany {de_sov:.3f} — a {frag_gap:+.3f} gap. Euro-area "
        f"fragmentation: {frag_label.lower()} ({frag_score:.0f}/100). The "
        "dispersion of SovCISS across member states is the direct read on "
        "whether the bloc's bond markets are pricing as one or splintering."
        if worst_sov and de_sov is not None and frag_gap is not None
        else "Sovereign-stress data partial.")

    # ── 5. COMPOSITE STRESS SCORE (0-100) ──
    parts, wts = [], []
    if ea_pct is not None:
        parts.append(ea_pct); wts.append(0.30)
    if worst_ciss:
        parts.append(ciss[worst_ciss]["percentile"]); wts.append(0.25)
    worst_sov_pct = None
    if sov:
        cand = [c for c in sov if sov[c].get("percentile") is not None]
        if cand:
            worst_sov_pct = max(sov[c]["percentile"] for c in cand)
            parts.append(worst_sov_pct); wts.append(0.25)
    parts.append(frag_score); wts.append(0.20)
    composite = (round(sum(p * w for p, w in zip(parts, wts)) / sum(wts), 1)
                 if parts else None)
    if composite is None:
        comp_regime = "UNKNOWN"
    elif composite >= 90:
        comp_regime = "CRISIS"
    elif composite >= 75:
        comp_regime = "STRESSED"
    elif composite >= 55:
        comp_regime = "ELEVATED"
    elif composite >= 35:
        comp_regime = "WATCH"
    else:
        comp_regime = "CALM"
    comp_read = (
        f"Composite systemic-&-sovereign stress {composite:.0f}/100 — "
        f"{comp_regime.lower()}. "
        + ("Stress is broad and elevated; treat risk exposure defensively "
           "and watch for cross-asset contagion."
           if composite is not None and composite >= 55 else
           "Stress is contained; the ECB's official indices are not "
           "corroborating an acute systemic event."
           if composite is not None else "Composite unavailable.")
        if composite is not None else "Composite unavailable.")

    # ── 6. cross-reference the proxy engines (do not rebuild) ──
    cdsp = read_existing("data/cds-proxy.json") or {}
    crst = read_existing("data/credit-stress.json") or {}
    vixc = read_existing("data/vix-curve.json") or {}
    cgrid = read_existing("data/canary-grid.json") or {}
    cross = {
        "cds_proxy_regime": cdsp.get("regime"),
        "cds_proxy_composite": cdsp.get("composite_credit_risk"),
        "credit_stress_regime": crst.get("regime") or crst.get("status"),
        "vix_level": (vixc.get("vix") or vixc.get("spot")
                      or (vixc.get("front") or {}).get("value")),
        "canary_grid_state": (cgrid.get("state") or cgrid.get("regime")
                              or cgrid.get("posture")),
    }

    headline = (
        f"Systemic stress {comp_regime}. Composite {composite:.0f}/100 — "
        f"euro-area CISS {ea.get('regime', 'n/a').lower()} ({ea_pct:.0f}th "
        f"pct); euro sovereign fragmentation {frag_label.lower()}."
        if composite is not None and ea_pct is not None
        else "ECB systemic & sovereign stress: partial data.")

    core_ok = len(ciss) >= 4 and len(sov) >= 6
    out = {
        "schema_version": "1.0",
        "method": "ecb_ciss_sovciss_systemic_stress",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": core_ok and len(errors) <= 5,
        "headline": headline,
        "composite": {
            "score_0_100": composite,
            "regime": comp_regime,
            "read": comp_read,
        },
        "systemic_stress": {
            "euro_area": ea or None,
            "countries": {c: ciss[c] for c in ciss if c != "U2"},
            "most_stressed": worst_ciss,
            "read": sys_read,
        },
        "sovereign_stress": {
            "countries": sov,
            "euro_area_avg_sovciss": (round(ea_sov_avg, 5)
                                      if ea_sov_avg is not None else None),
            "dispersion": (round(sov_dispersion, 5)
                           if sov_dispersion is not None else None),
            "most_stressed": worst_sov,
            "germany_benchmark": (round(de_sov, 5)
                                  if de_sov is not None else None),
            "fragmentation_gap": frag_gap,
            "fragmentation_score": frag_score,
            "fragmentation_label": frag_label,
            "read": sov_read,
        },
        "cross_reference": cross,
        "sources": ["ECB Data Portal — CISS / SovCISS "
                    "(Composite Indicators of Systemic & Sovereign Stress)"],
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[systemic-stress] composite {composite}/100 ({comp_regime}) | "
          f"euro CISS pct {ea_pct} | frag {frag_score} ({frag_label}) | "
          f"ciss={len(ciss)} sov={len(sov)} errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "composite": composite,
                                "regime": comp_regime,
                                "fragmentation": frag_label,
                                "errors": len(errors)})}
