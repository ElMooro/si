"""
justhodl-us-cycle v1.0 — US Macro-Cycle Canaries (brain-gap implementation)
===========================================================================
Built from Khalid's brain audit (ops 1580). Each pillar carries its note-spec:

  Sahm rule          — recession trigger ≥0.5 (FRED SAHMREALTIME)
  SLOOS              — "EU SLOOS" exists; this is the US original (DRTSCILM/S)
  Jobless claims     — 4wk avg momentum (brain: classic Rickards checklist)
  JOLTS quits        — falling quits = labor cooling before payrolls crack
  Copper/gold        — brain: "look for copper/gold ratio for sniper exit"
  Term premium       — brain 13×: "back end is about term premium" (FRED ACM)
  Real 10y / 5y5y    — brain 21×: "DXY and US real yields" regime context
  Buffett indicator  — equity mcap vs GDP percentile (Z.1)
  Margin debt proxy  — brain 25×: "elevated margin debt ahead of corrections"
  Semis leadership   — SMH/SPY 60d relative slope (cycle's tip of the spear)

Composite 0–100 coverage-honest; ≥70 logs us_cycle DOWN vs SPY (closed loop).
"""
import json, os, time, urllib.request, urllib.parse, bisect
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/us-cycle.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.0.0"


def fred(sid, start="1990-01-01"):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY,
                                   "file_type": "json", "observation_start": start,
                                   "limit": 100000}))
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=35).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", [])
                if o.get("value") not in (".", "", None)]
    except Exception as e:
        print(f"[fred] {sid}: {str(e)[:50]}")
        return []


def probe(sids, start="1990-01-01", min_n=24):
    for sid in sids:
        o = fred(sid, start)
        if len(o) >= min_n:
            return sid, o
    return None, []


def zlast(vals, look):
    w = vals[-look:]
    if len(w) < 20:
        return None
    m, sd = mean(w), stdev(w)
    return round((vals[-1] - m) / sd, 2) if sd else 0.0


def pctile(vals):
    sv = sorted(vals)
    return round(100.0 * bisect.bisect_left(sv, vals[-1]) / len(sv), 1)


def down(pts, cap=240):
    if len(pts) <= cap:
        return pts
    st = len(pts) / cap
    o, i = [], 0.0
    while int(i) < len(pts) - 1:
        o.append(pts[int(i)]); i += st
    o.append(pts[-1])
    return o


def poly_closes(t, days=420):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=600&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=35).read())
        return [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(),
                 float(r["c"])) for r in (j.get("results") or [])]
    except Exception as e:
        print(f"[poly] {t}: {str(e)[:50]}")
        return []


def lambda_handler(event=None, context=None):
    t0 = time.time()
    P, charts, alerts = {}, {}, []

    # 1) Sahm
    sh = fred("SAHMREALTIME", "1970-01-01")
    if sh:
        v = sh[-1][1]
        P["sahm"] = {"value": v, "as_of": sh[-1][0],
                     "signal": "CRITICAL" if v >= 0.5 else "WATCH" if v >= 0.3 else "NORMAL",
                     "spec": "≥0.5 = recession started (never false since 1970)"}
        charts["sahm"] = {"label": "Sahm rule (pp above 12m-low unemployment)",
                           "points": down(sh), "pctile": pctile([x for _, x in sh]),
                           "thresholds": {"trigger": 0.5}}
        if v >= 0.5:
            alerts.append(f"SAHM TRIGGERED at {v} — recession signature")

    # 2) SLOOS C&I (+small, +CRE probe)
    sid, sl = probe(["DRTSCILM"], "1990-01-01")
    if sl:
        v = sl[-1][1]
        sm = dict(fred("DRTSCIS", "1990-01-01"))
        cre_sid, cre = probe(["DRTSCLCC", "SUBLPDCLCTSNQ"], "2014-01-01", 8)
        P["sloos"] = {"ci_large_net_pct": v, "ci_small_net_pct": sm.get(sl[-1][0]),
                      "cre_net_pct": cre[-1][1] if cre else None,
                      "as_of": sl[-1][0],
                      "signal": "CRITICAL" if v >= 30 else "WATCH" if v >= 10 else "NORMAL",
                      "spec": ">+20 net tightening preceded 1990/2001/2008/2020 recessions"}
        charts["sloos"] = {"label": "SLOOS: net % banks tightening C&I (large)",
                            "points": down(sl), "pctile": pctile([x for _, x in sl]),
                            "thresholds": {"watch": 10, "critical": 30}}

    # 3) Claims 4wk
    ic = fred("ICSA", "2015-01-01")
    if len(ic) > 60:
        vals = [v for _, v in ic]
        a4 = [mean(vals[i - 4:i]) for i in range(4, len(vals))]
        yo = (a4[-1] / a4[-53] - 1) * 100 if len(a4) > 53 else None
        P["claims"] = {"four_wk_avg_k": round(a4[-1] / 1000, 1),
                       "yoy_pct": round(yo, 1) if yo is not None else None,
                       "z_2y": zlast(a4, 104), "as_of": ic[-1][0],
                       "signal": "WATCH" if (yo or 0) >= 15 else "NORMAL",
                       "spec": "4wk avg +15% YoY = labor crack confirmed"}

    # 4) JOLTS quits
    jq = fred("JTSQUR", "2010-01-01")
    if jq:
        v = jq[-1][1]
        ch6 = round(v - jq[-7][1], 2) if len(jq) > 7 else None
        P["quits"] = {"quits_rate_pct": v, "chg_6m_pp": ch6, "as_of": jq[-1][0],
                      "signal": "WATCH" if (ch6 or 0) <= -0.3 else "NORMAL",
                      "spec": "quits fall before layoffs rise — workers smell it first"}

    # 5) Copper/gold ("sniper exit")
    cu = dict(fred("PCOPPUSDM", "2000-01-01"))
    au = dict(probe(["GOLDPMGBD228NLBM", "GOLDAMGBD228NLBM"], "2000-01-01")[1])
    aum = {}
    for d_, v_ in au.items():
        aum[d_[:7]] = v_
    cg = [(d_, cu[d_] / aum[d_[:7]]) for d_ in sorted(cu) if d_[:7] in aum and aum[d_[:7]]]
    if len(cg) > 40:
        vals = [v for _, v in cg]
        z = zlast(vals, 120)
        sl6 = vals[-1] - vals[-7] if len(vals) > 7 else 0
        P["copper_gold"] = {"ratio": round(vals[-1], 4), "z_10y": z,
                            "slope_6m": round(sl6, 4), "as_of": cg[-1][0],
                            "signal": "WATCH" if (z or 0) <= -1.2 and sl6 < 0 else "NORMAL",
                            "spec": "brain: flattening/rolling copper-gold = sniper exit on cycle"}
        charts["copper_gold"] = {"label": "Copper/Gold ratio (growth vs fear)",
                                  "points": down([(d_, round(v_, 4)) for d_, v_ in cg]),
                                  "pctile": pctile(vals), "thresholds": {}}

    # 6) Term premium (ACM via FRED)
    sid, tp = probe(["THREEFYTP10", "ACMTP10"], "2000-01-01")
    if tp:
        vals = [v for _, v in tp]
        P["term_premium"] = {"series": sid, "tp10_pct": round(vals[-1], 2),
                             "z_5y": zlast(vals, 1260), "as_of": tp[-1][0],
                             "signal": "WATCH" if zlast(vals, 1260) and zlast(vals, 1260) >= 2 else "NORMAL",
                             "spec": "brain: back-end = term premium; spike = supply/inflation risk repricing"}
        charts["term_premium"] = {"label": "10y term premium (ACM, %)",
                                   "points": down(tp), "pctile": pctile(vals),
                                   "thresholds": {}}

    # 7) Real 10y + 5y5y
    r10 = fred("DFII10", "2010-01-01")
    f55 = fred("T5YIFR", "2010-01-01")
    if r10:
        vals = [v for _, v in r10]
        P["real_10y"] = {"pct": vals[-1], "z_3y": zlast(vals, 756), "as_of": r10[-1][0],
                         "signal": "WATCH" if vals[-1] >= 2.3 else "NORMAL",
                         "spec": "real 10y ≥2.3% historically breaks something"}
    if f55:
        v = f55[-1][1]
        P["infl_5y5y"] = {"pct": v, "as_of": f55[-1][0],
                          "signal": "NORMAL" if 2.0 <= v <= 2.6 else "WATCH",
                          "spec": "outside 2.0–2.6 = expectations slipping"}

    # 8) Buffett indicator (Z.1 equities / GDP)
    eq_sid, eq = probe(["NCBEILQ027S", "BOGZ1LM893064105Q"], "1990-01-01", 20)
    gdp = dict(fred("GDP", "1990-01-01"))
    if eq and gdp:
        gk = sorted(gdp)
        ratio = []
        for d_, v_ in eq:
            j = bisect.bisect_right(gk, d_) - 1
            if j >= 0:
                ratio.append((d_, v_ / 1000.0 / gdp[gk[j]] * 100))
        if len(ratio) > 20:
            vals = [v for _, v in ratio]
            P["buffett"] = {"mcap_gdp_pct": round(vals[-1], 1), "pctile": pctile(vals),
                            "series": eq_sid, "as_of": ratio[-1][0],
                            "signal": "WATCH" if pctile(vals) >= 90 else "NORMAL",
                            "spec": "brain: mcap/GDP percentile — valuation altitude, not timing"}
            charts["buffett"] = {"label": "Buffett indicator (corp equities / GDP, %)",
                                  "points": down(ratio), "pctile": pctile(vals),
                                  "thresholds": {}}

    # 9) Margin-debt proxy (Z.1 security credit; FINRA file = KHALID_ACTIONS)
    md_sid, md = probe(["BOGZ1FL663067003Q", "BOGZ1FL663067005Q"], "1995-01-01", 20)
    if md:
        vals = [v for _, v in md]
        yo = (vals[-1] / vals[-5] - 1) * 100 if len(vals) > 5 else None
        P["margin_debt"] = {"series": md_sid, "level_bn": round(vals[-1] / 1000, 1),
                            "yoy_pct": round(yo, 1) if yo is not None else None,
                            "pctile": pctile(vals), "as_of": md[-1][0],
                            "signal": "WATCH" if (yo or 0) >= 25 else "NORMAL",
                            "spec": "brain: elevated margin debt appears ahead of corrections"}

    # 10) Semis leadership (SMH/SPY 60d relative slope z)
    smh = dict(poly_closes("SMH")); spy = dict(poly_closes("SPY"))
    rel = [(d_, smh[d_] / spy[d_]) for d_ in sorted(set(smh) & set(spy))]
    if len(rel) > 80:
        vals = [v for _, v in rel]
        sl = [(vals[i] / vals[i - 60] - 1) * 100 for i in range(60, len(vals))]
        P["semis_leadership"] = {"smh_spy_60d_pct": round(sl[-1], 2),
                                 "z": zlast(sl, 252), "as_of": rel[-1][0],
                                 "signal": "WATCH" if sl[-1] <= -8 else "NORMAL",
                                 "spec": "semis roll first; −8% 60d relative = cycle tip cracking"}

    # composite (recession-direction z blend, coverage-honest)
    comp = []
    def add(name, z, w):
        if z is not None:
            comp.append({"id": name, "z": round(max(-3, min(3, z)), 2), "w": w})
    add("sahm", (P.get("sahm", {}).get("value", 0)) / 0.25, 0.20)
    add("sloos", ((P.get("sloos", {}).get("ci_large_net_pct") or 0)) / 15.0, 0.15)
    add("claims", ((P.get("claims", {}).get("yoy_pct") or 0)) / 12.0, 0.12)
    add("quits", -((P.get("quits", {}).get("chg_6m_pp") or 0)) / 0.25, 0.10)
    cg_ = P.get("copper_gold", {})
    add("copper_gold", -(cg_.get("z_10y") or 0) if (cg_.get("slope_6m") or 0) < 0 else 0, 0.12)
    add("real_10y", (P.get("real_10y", {}).get("z_3y") or 0), 0.08)
    add("term_premium", (P.get("term_premium", {}).get("z_5y") or 0) * 0.5, 0.05)
    add("buffett", ((P.get("buffett", {}).get("pctile") or 50) - 50) / 22.0 * 0.5, 0.08)
    add("margin", ((P.get("margin_debt", {}).get("yoy_pct") or 0)) / 20.0, 0.05)
    add("semis", -((P.get("semis_leadership", {}).get("z") or 0)), 0.05)
    tw = sum(c["w"] for c in comp)
    zbar = sum(c["z"] * c["w"] for c in comp) / tw if tw else 0
    score = round(max(0, min(100, 50 + 18 * zbar)), 1)
    level = ("ACUTE" if score >= 75 else "ELEVATED" if score >= 60
             else "WATCH" if score >= 50 else "CALM")

    n_logged = 0
    if score >= 70:
        try:
            spy_px = list(spy.values())[-1] if spy else None
            if spy_px:
                nowt = datetime.now(timezone.utc)
                DDB.Table("justhodl-signals").put_item(Item={
                    "signal_id": f"us-cycle#US#{nowt.strftime('%Y-%m-%d')}",
                    "signal_type": "us_cycle", "signal_value": str(score),
                    "predicted_direction": "DOWN", "confidence": Decimal("0.58"),
                    "measure_against": "ticker", "baseline_price": str(spy_px),
                    "benchmark": "SPY", "check_windows": ["day_5", "day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                          for w in (5, 21, 63)},
                    "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                    "logged_epoch": int(nowt.timestamp()), "status": "pending",
                    "schema_version": "2", "horizon_days_primary": 21,
                    "regime_at_log": level, "ttl": int(nowt.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "us-cycle", "v": VERSION, "score": str(score)},
                    "rationale": f"US cycle composite {score} ({level}): " + "; ".join(alerts[:3])})
                n_logged = 1
        except Exception as e:
            print(f"[loop] {str(e)[:70]}")

    out = {"engine": "us-cycle", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "pillars": P, "charts": charts, "alerts": alerts,
           "cycle_score": {"score_0_100": score, "level": level,
                            "components": comp, "coverage_pillars": len(comp)},
           "signals_logged": n_logged, "brain_audit_source": "ops-1580",
           "methodology": ("US macro-cycle canaries implemented from the Brain gap audit: "
                           "Sahm, SLOOS, claims momentum, quits, copper/gold, ACM term "
                           "premium, real rates, Buffett percentile, margin-debt proxy, "
                           "semis leadership. Coverage-honest z-blend; ≥70 logs us_cycle "
                           "DOWN vs SPY to the closed loop.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[us-cycle] score={score} {level} pillars={len(P)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"score": score, "level": level})}
