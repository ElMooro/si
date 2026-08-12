"""justhodl-physical-econ v2.0.2 (ops 4614)

The Physical/Real Economy signal, institutional restructure: five
weighted SUB-PILLARS computed purely from evidence landed by
justhodl-real-economy-collector (data/warm/real-economy/*) plus the
fleet's own surfaces. Ingestion and computation are separated; every
leg carries an evidence tier, a staleness gate, and a per-leg status
row — absences and staleness are reported, never papered over.

Sub-pillars (weights):
  energy .28 · trade_transport .27 · materials .15 · labor .15 ·
  construction .15
Tier discipline: tier1 legs are load-bearing; tier2 contribute at
half weight; tier3 are OBSERVED ONLY (shown, never scored).
Output: data/physical-economy.json (schema 2.0) + history.
History of this engine: v1.0.x was the flat 5-leg join (ops 4610-13);
v2.0.0 is the full real-economy build (ops 4614).
v2.0.1: copper basis bug fixed (FRED fallback is $/tonne MONTHLY -
the daily window mislabeled it and pinned the leg at 100); rail +
Destatis toll promoted to scored trade legs.
"""
import json
import math
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("S3_KEY_OUT", "data/physical-economy.json")
HIST_KEY = "data/physical-economy-history.json"
WARM = "data/warm/real-economy/"
s3 = boto3.client("s3")

SUB_WEIGHTS = {"energy": 0.28, "trade_transport": 0.27,
               "materials": 0.15, "labor": 0.15,
               "construction": 0.15}


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def age_days(env):
    try:
        dt = datetime.fromisoformat(env["fetched_at"])
        return (datetime.now(timezone.utc) - dt).total_seconds() / 86400
    except Exception:
        return 999


def series_last_date_age(env):
    se = env.get("series") or []
    if not se:
        return 999
    try:
        d = datetime.fromisoformat(se[-1]["date"]).replace(
            tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - d).total_seconds() / 86400
    except Exception:
        return 999


def mom_pct(se, recent_n, base_n):
    """Mean of last recent_n vs mean of the base_n before it, %."""
    if len(se) < recent_n + base_n:
        return None
    vals = [o["value"] for o in se]
    r = sum(vals[-recent_n:]) / recent_n
    b = sum(vals[-recent_n - base_n:-recent_n]) / base_n
    return round((r / b - 1) * 100, 2) if b else None


def yoy_pct(se):
    if len(se) < 2:
        return None
    last = se[-1]
    target = last["date"][:4]
    try:
        prev_yr = str(int(target) - 1) + last["date"][4:]
    except Exception:
        return None
    prev = min(se, key=lambda o: abs(
        (datetime.fromisoformat(o["date"])
         - datetime.fromisoformat(prev_yr)).days))
    gap = abs((datetime.fromisoformat(prev["date"])
               - datetime.fromisoformat(prev_yr)).days)
    if gap > 20 or not prev["value"]:
        return None
    return round((last["value"] / prev["value"] - 1) * 100, 2)


def leg_row(leg_id, sub, name, support, detail, tier, status,
            data_age):
    return {"leg_id": leg_id, "sub_pillar": sub, "name": name,
            "expansion_0_100": support, "detail": detail,
            "tier": tier, "status": status,
            "data_age_days": round(data_age, 1) if data_age < 900
            else None}


def warm_leg(leg_id, sub, name, transform, stale_days, tier_hint=None):
    """Load a collector envelope, gate on staleness, run transform ->
    (support, detail) on the envelope. Transform may return None."""
    env = s3_json(WARM + leg_id + ".json")
    if env is None:
        return leg_row(leg_id, sub, name, None, "not landed",
                       tier_hint or "?", "ABSENT", 999)
    tier = env.get("tier", tier_hint or "?")
    st = env.get("status")
    dage = min(series_last_date_age(env), age_days(env) + 0)
    if st != "OK":
        return leg_row(leg_id, sub, name, None,
                       str(env.get("detail"))[:140], tier, st, dage)
    if dage > stale_days:
        return leg_row(leg_id, sub, name, None,
                       "stale: data %.1fd old (gate %dd)"
                       % (dage, stale_days), tier, "STALE", dage)
    try:
        sup, detail = transform(env)
    except Exception as e:
        return leg_row(leg_id, sub, name, None,
                       "transform: %s" % str(e)[:120], tier,
                       "FAILED", dage)
    if sup is None:
        return leg_row(leg_id, sub, name, None, detail, tier,
                       "NO_SIGNAL", dage)
    return leg_row(leg_id, sub, name, round(clamp(sup, 0, 100), 1),
                   detail, tier, "OK", dage)


# ── transforms (support scale: 100 = physical expansion) ─────────────
def t_weekly_mom(env, k=4.0, r=4, b=12, label="4w vs prior 12w"):
    se = env["series"]
    m = mom_pct(se, r, b)
    y = yoy_pct(se)
    if m is None:
        return None, "insufficient history"
    d = "%+.1f%% (%s)" % (m, label)
    if y is not None:
        d += " · YoY %+.1f%%" % y
    d += " · latest %s" % se[-1]["value"]
    return 50 + m * k, d


def t_daily_mom(env, k=8.0, r=7, b=28):
    se = env["series"]
    m = mom_pct(se, r, b)
    if m is None:
        return None, "insufficient history"
    return 50 + m * k, "%+.2f%% (7d vs 28d) · latest %s" % (
        m, se[-1]["value"])


def t_claims(env):
    se = env["series"]
    m = mom_pct(se, 4, 26)
    if m is None:
        return None, "insufficient history"
    return 50 - m * 2.0, ("claims 4w avg %+.1f%% vs 26w — inverted · "
                          "latest %s" % (m, int(se[-1]["value"])))


def t_wti_term(env):
    sp = (env.get("metrics") or {}).get("latest_spread")
    if sp is None:
        return None, "no spread"
    state = (env.get("metrics") or {}).get("state")
    # backwardation = physical tightness; mild is fine, extreme is the
    # doctrine crisis precursor -> hump-shaped support
    sup = 60 + sp * 4 if sp <= 1.5 else 66 - (sp - 1.5) * 12
    if sp < 0:
        sup = 50 + sp * 5  # deep contango = demand weakness
    return sup, "c1-c4 $%+.2f (%s) · c1 $%s" % (
        sp, state, (env.get("metrics") or {}).get("c1"))


def t_aisi(env):
    u = (env.get("metrics") or {}).get("capacity_utilization_pct")
    if u is None:
        return None, "no utilization"
    return (u - 65.0) * 2.4 + 30, "capacity utilization %.1f%%" % u


def t_copper(env):
    se = env["series"]
    monthly = "PCOPPUSDM" in str(env.get("source", ""))
    if monthly:
        m = mom_pct(se, 3, 12)
        if m is None:
            return None, "insufficient history"
        return 50 + m * 1.2, ("%+.1f%% (3m vs prior 12m, IMF "
                              "monthly, k=1.2) · $%.0f/tonne"
                              % (m, se[-1]["value"]))
    m = mom_pct(se, 5, 21)
    if m is None:
        return None, "insufficient history"
    return 50 + m * 2.5, "%+.1f%% (1w vs 1m) · $%.3f/lb" % (
        m, se[-1]["value"])


def t_indeed(env):
    se = env["series"]
    m = mom_pct(se, 7, 30)
    if m is None:
        return None, "insufficient history"
    return 50 + m * 6, "postings %+.2f%% (7d vs 30d) · index %.1f" % (
        m, se[-1]["value"])


def t_rail(env):
    se = env.get("series")
    if se:
        return t_monthly_mom(env, k=5.0)
    mx = env.get("metrics") or {}
    if mx.get("weekly_carloads"):
        return None, ("weekly print only: %.0f carloads"
                      % mx["weekly_carloads"])
    return None, "no data"


def t_monthly_mom(env, k=6.0):
    se = env["series"]
    if len(se) < 15:
        return None, "insufficient history"
    m = mom_pct(se, 3, 12)
    y = yoy_pct(se)
    d = "%+.1f%% (3m vs prior 12m)" % m if m is not None else ""
    if y is not None:
        d += " · YoY %+.1f%%" % y
    if m is None:
        return None, "no momentum"
    return 50 + m * k, d + " · latest %s" % se[-1]["value"]


# ── fleet-surface legs (computed engines, joined not refetched) ─────
def fleet_legs():
    out = []
    pjm = s3_json("data/pjm-grid.json")
    if pjm:
        mom = ((pjm.get("load") or {}).get("momentum_8d_pct"))
        if isinstance(mom, (int, float)):
            out.append(leg_row("pjm_momentum", "energy",
                               "PJM demand momentum (8d)",
                               round(clamp(50 + mom * 10, 0, 100), 1),
                               "%+.2f%% · %s GW"
                               % (mom, (pjm.get("load") or {})
                                  .get("current_gw")),
                               "tier1", "OK", 0))
        sh = ((pjm.get("lmp") or {}).get("shock_state")
              or ((pjm.get("canaries") or {}).get("lmp_spike")
                  or {}).get("state"))
        if sh in ("CALM", "AMBER", "RED"):
            out.append(leg_row("pjm_lmp_shock", "energy",
                               "PJM power-price shock canary",
                               {"CALM": 65, "AMBER": 35, "RED": 10}[sh],
                               "LMP $%s DoD %s%% · %s"
                               % ((pjm.get("lmp") or {}).get(
                                   "daily_avg"),
                                  (pjm.get("lmp") or {}).get(
                                   "daily_avg_dod_pct"), sh),
                               "tier1", "OK", 0))
    gq = s3_json("data/grid-queue.json")
    if gq:
        nat = gq.get("national") or gq
        ex, hl = nat.get("mw_with_executed_ia"), nat.get(
            "headline_queue_mw")
        if isinstance(ex, (int, float)) and hl:
            out.append(leg_row("grid_buildout", "construction",
                               "Grid buildout quality "
                               "(executed-IA share)",
                               round(clamp(ex / hl * 100, 0, 100), 1),
                               "%.0f of %.0f MW executed" % (ex, hl),
                               "tier1", "OK", 0))
    fp = s3_json("data/freight-pulse.json")
    if fp:
        v = None
        for k in ("pulse", "composite", "score", "z", "z_score"):
            x = fp.get(k)
            if isinstance(x, (int, float)):
                v = x
                break
        if v is None:
            def wf(o, d=0):
                if d > 5 or o is None:
                    return None
                if isinstance(o, dict):
                    for kk, vv in o.items():
                        if isinstance(vv, (int, float)) and kk in (
                                "pulse", "composite", "score", "z"):
                            return float(vv)
                    for vv in o.values():
                        r0 = wf(vv, d + 1)
                        if r0 is not None:
                            return r0
                if isinstance(o, list):
                    for vv in o[:20]:
                        r0 = wf(vv, d + 1)
                        if r0 is not None:
                            return r0
                return None
            v = wf(fp)
        if v is not None:
            sup = (clamp(v, 0, 100) if 1.5 < abs(v) <= 100
                   else 50 + 25 * math.tanh(v))
            out.append(leg_row("freight_pulse", "trade_transport",
                               "Freight pulse", round(sup, 1),
                               "engine reading %s" % v, "tier1",
                               "OK", 0))
    pc = s3_json("data/port-cargo.json")
    if pc and pc.get("fetch_status") == "OK":
        seas = pc.get("seasonal_baseline") or {}
        v = (seas.get("seasonal_chg_pct")
             if seas.get("status") == "OK" else None)
        basis = "same-week vs 1-3y prior"
        if v is None:
            v = (pc.get("global_pulse") or {}).get("total_chg_pct")
            basis = "7d vs 28d"
        if isinstance(v, (int, float)):
            out.append(leg_row("port_tonnage", "trade_transport",
                               "Port cargo tonnage momentum",
                               round(clamp(50 + v * 2.5, 0, 100), 1),
                               "%+.1f%% (%s) · %s ports"
                               % (v, basis,
                                  pc.get("n_ports_with_data")),
                               "tier1", "OK",
                               pc.get("data_age_days") or 0))
    al = s3_json("data/asia-leads.json")
    if al:
        def wff(o, names, d=0):
            if d > 6 or o is None:
                return None
            if isinstance(o, dict):
                for kk, vv in o.items():
                    if (isinstance(vv, (int, float))
                            and any(n in kk.lower() for n in names)):
                        return float(vv)
                for vv in o.values():
                    r0 = wff(vv, names, d + 1)
                    if r0 is not None:
                        return r0
            if isinstance(o, list):
                for vv in o[:24]:
                    r0 = wff(vv, names, d + 1)
                    if r0 is not None:
                        return r0
            return None
        v = wff(al, ("kr_flash", "kr_exports_yoy", "flash_yoy",
                     "exports_yoy"))
        if v is not None:
            out.append(leg_row("kr_exports", "trade_transport",
                               "Korea first-20-days exports (YoY)",
                               round(clamp(50 + v * 1.2, 0, 100), 1),
                               "KR flash %+.1f%% YoY — fastest hard "
                               "trade print" % v, "tier1", "OK", 0))
    ac = s3_json("data/air-cargo.json")
    if ac:
        v = None
        for k in ("yoy_pct", "momentum_pct", "composite", "score"):
            x = ac.get(k)
            if isinstance(x, (int, float)):
                v = x
                break
        if v is not None:
            out.append(leg_row("air_cargo", "trade_transport",
                               "Air cargo",
                               round(clamp(50 + v * 2, 0, 100), 1),
                               "engine reading %+.1f" % v, "tier1",
                               "OK", 0))
    return out


REGISTRY = [
    # (leg_id, sub, name, transform, stale_days)
    ("eia930_us48", "energy", "US-48 electricity demand (daily)",
     lambda e: t_daily_mom(e, k=8), 5),
    ("eia930_ercot", "energy", "ERCOT demand — 2nd AI hub (daily)",
     lambda e: t_daily_mom(e, k=8), 5),
    ("eia_distillate", "energy",
     "Distillate supplied — diesel IS freight",
     lambda e: t_weekly_mom(e, k=4), 14),
    ("eia_gasoline", "energy", "Gasoline supplied — miles driven",
     lambda e: t_weekly_mom(e, k=4), 14),
    ("eia_jet", "energy", "Jet fuel supplied",
     lambda e: t_weekly_mom(e, k=3), 14),
    ("eia_refinery", "energy", "Refinery crude inputs",
     lambda e: t_weekly_mom(e, k=3), 14),
    ("wti_term", "energy", "WTI term structure (doctrine leg)",
     t_wti_term, 6),
    ("chokepoints", "trade_transport",
     "Global chokepoint transits (daily)",
     lambda e: t_daily_mom(e, k=6), 8),
    ("tsa", "trade_transport", "TSA checkpoint throughput (daily)",
     lambda e: t_daily_mom(e, k=5), 6),
    ("aisi_steel", "materials", "Raw steel capacity utilization",
     t_aisi, 12),
    ("copper", "materials", "Dr. Copper (priced, tier-2)",
     t_copper, 6),
    ("aar_rail", "trade_transport", "Rail freight carloads",
     t_rail, 45),
    ("destatis_toll", "trade_transport",
     "DE truck-toll mileage (daily)",
     lambda e: t_daily_mom(e, k=6), 15),
    ("fred_claims", "labor", "Initial claims (inverted)",
     t_claims, 14),
    ("indeed_postings", "labor", "Indeed job postings (daily)",
     t_indeed, 8),
    ("fred_hours", "labor", "Aggregate weekly hours (monthly)",
     lambda e: t_monthly_mom(e, k=10), 60),
    ("fred_houst", "construction", "Housing starts (monthly)",
     lambda e: t_monthly_mom(e, k=3), 60),
    ("fred_permit", "construction", "Building permits (monthly)",
     lambda e: t_monthly_mom(e, k=3), 60),
]

OBSERVED_ONLY = ["noaa_degree_days", "acc_cab"]


def label_for(score):
    if score is None:
        return "NO DATA"
    if score >= 62:
        return "EXPANSION"
    if score >= 45:
        return "NEUTRAL"
    return "CONTRACTION"


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    legs = []
    for leg_id, sub, name, tr, gate in REGISTRY:
        legs.append(warm_leg(leg_id, sub, name, tr, gate))
    legs.extend(fleet_legs())

    observed = []
    for oid in OBSERVED_ONLY:
        env = s3_json(WARM + oid + ".json")
        if env:
            observed.append({"leg_id": oid, "tier": env.get("tier"),
                             "status": env.get("status"),
                             "metrics": env.get("metrics"),
                             "detail": str(env.get("detail"))[:120]})

    subs = {}
    for sp in SUB_WEIGHTS:
        rows = [x for x in legs if x["sub_pillar"] == sp]
        num = den = 0.0
        live = 0
        for x in rows:
            if x["expansion_0_100"] is None:
                continue
            w = 1.0 if x["tier"] == "tier1" else 0.5
            num += w * x["expansion_0_100"]
            den += w
            live += 1
        subs[sp] = {"score": round(num / den, 1) if den else None,
                    "label": label_for(num / den if den else None),
                    "n_live": live, "n_total": len(rows)}

    scored = {p: d["score"] for p, d in subs.items()
              if d["score"] is not None}
    composite = None
    if scored:
        wsum = sum(SUB_WEIGHTS[p] for p in scored)
        composite = round(sum(SUB_WEIGHTS[p] * scored[p]
                              for p in scored) / wsum, 1)
    n_live = sum(d["n_live"] for d in subs.values())
    conf = ("HIGH" if n_live >= 14 else
            "MEDIUM" if n_live >= 9 else "LOW")
    lab = label_for(composite)
    if lab == "EXPANSION":
        tilt = ("physical economy accelerating — supports the "
                "cyclical / power-equipment sleeve (GEV, EME, NVT, "
                "transports)")
    elif lab == "CONTRACTION":
        tilt = ("physical economy rolling over — fade cyclicals; "
                "watch diesel, chokepoints and power demand for "
                "confirmation")
    else:
        tilt = "physical economy mixed — no cyclical edge either way"

    # doctrine canaries
    canaries = {}
    wt = next((x for x in legs if x["leg_id"] == "wti_term"), None)
    if wt and wt["status"] == "OK":
        env = s3_json(WARM + "wti_term.json") or {}
        sp = (env.get("metrics") or {}).get("latest_spread")
        if sp is not None:
            canaries["oil_backwardation"] = {
                "state": ("RED" if sp >= 4 else
                          "AMBER" if sp >= 2 else "CALM"),
                "c1_minus_c4": sp,
                "doctrine": "extreme backwardation = physical "
                            "scarcity, the crisis precursor"}
    ck = s3_json(WARM + "chokepoints.json")
    if ck and ck.get("status") == "OK":
        se = ck.get("series") or []
        if len(se) >= 2 and se[-2]["value"]:
            dod = (se[-1]["value"] / se[-2]["value"] - 1) * 100
            canaries["chokepoint_shock"] = {
                "state": ("RED" if dod <= -25 else
                          "AMBER" if dod <= -12 else "CALM"),
                "dod_pct": round(dod, 1),
                "doctrine": "a one-day collapse in chokepoint "
                            "transits = trade-route disruption"}
    cl = s3_json(WARM + "fred_claims.json")
    if cl and cl.get("status") == "OK":
        se = cl.get("series") or []
        if len(se) >= 5:
            wow = ((se[-1]["value"] / se[-2]["value"]) - 1) * 100 \
                if se[-2]["value"] else 0
            canaries["claims_spike"] = {
                "state": ("RED" if wow >= 15 else
                          "AMBER" if wow >= 8 else "CALM"),
                "wow_pct": round(wow, 1),
                "latest": int(se[-1]["value"])}

    payload = {
        "schema_version": "2.0",
        "engine": "justhodl-physical-econ",
        "as_of": now.isoformat(timespec="seconds"),
        "architecture": "collector(ingest) -> signal(compute); "
                        "tier1 load-bearing, tier2 half-weight, "
                        "tier3 observed-only",
        "sub_pillar_weights": SUB_WEIGHTS,
        "sub_pillars": subs,
        "legs": legs,
        "observed_only": observed,
        "composite_score": composite,
        "composite_label": lab,
        "n_live_legs": n_live,
        "trade_signal": {"signal": lab, "confidence": conf,
                         "tilt": tilt},
        "canaries": canaries,
        "declared_not_public": ["MBA purchase applications",
                                "AIA Architecture Billings Index"],
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    try:
        hist = s3_json(HIST_KEY) or {"points": []}
        hist["points"].append(
            {"t": now.isoformat(timespec="seconds"), "c": composite,
             "n": n_live,
             **{p: subs[p]["score"] for p in subs}})
        hist["points"] = hist["points"][-400:]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                      Body=json.dumps(hist).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
    except Exception as e:
        print(f"[phys] history: {e}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": composite is not None
                                and n_live >= 8,
                                "composite": composite,
                                "label": lab, "n_live": n_live,
                                "confidence": conf})}
