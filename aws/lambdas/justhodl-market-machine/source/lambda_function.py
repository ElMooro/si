"""justhodl-market-machine v1.2.0 (ops 4610)

Khalid's doctrine, verbatim: "The stock market is a machine that relies
on four things: 1) future profits, 2) interest rates, 3) where money is
flowing, 4) what traders are being forced to do right now."

This engine IS that machine-read. It does not refetch the world — it
joins the fleet's already-computed surfaces (accumulation cluster,
plumbing v2.1, vol-target unwind, margin radar, SPX-MA breadth,
risk-gate, estimate revisions) plus a small direct FRED block for the
rates pillar, and publishes one composite: is the machine a tailwind or
a headwind for stocks RIGHT NOW, and which pillar is driving it.

Convention: every contributor is mapped onto a 0-100 SUPPORT scale
(100 = maximally supportive for equities, 0 = maximally hostile).
Stress-style inputs are inverted at the join. Pillar = weighted mean of
found contributors; composite = weighted mean of pillars. All real
data; contributors that can't be discovered are reported absent, never
faked.

Output: data/market-machine.json (+ history, 400 pts).
v1.1.0: multi-key discovery for the pillar-4 joins + two direct
FRED forced-flow reads (VIX term structure, SPX vs 200dma) so the
forced pillar never rides on a single series.
v1.2.0: Physical Economy pulse joined into the profits pillar —
real activity (power, ports, freight, grid buildout) leads
earnings.
"""
import json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("S3_KEY_OUT", "data/market-machine.json")
HIST_KEY = "data/market-machine-history.json"
FRED_KEY = os.environ.get("FRED_API_KEY",
                          "2f057499936072679d8843d7fce99989")
s3 = boto3.client("s3")

PILLAR_WEIGHTS = {"profits": 0.25, "rates": 0.25, "flow": 0.25,
                  "forced": 0.25}


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent":
                                               "justhodl-market-machine"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as h:
            return h.read()
    except Exception as e:
        print(f"[machine] GET {url[:80]}: {e}")
        return None


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception as e:
        print(f"[machine] s3 {key}: {e}")
        return None


def s3_json_multi(keys):
    for k in keys:
        d = s3_json(k)
        if d is not None:
            return d, k
    return None, None


def walk_find(obj, names, depth=0):
    """First numeric found under any of the candidate key names,
    breadth-ish recursive walk (house discovery pattern)."""
    if depth > 6 or obj is None:
        return None
    if isinstance(obj, dict):
        for n in names:
            if n in obj and isinstance(obj[n], (int, float)):
                return float(obj[n])
        for v in obj.values():
            r = walk_find(v, names, depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj[:24]:
            r = walk_find(v, names, depth + 1)
            if r is not None:
                return r
    return None


def walk_find_str(obj, names, depth=0):
    if depth > 6 or obj is None:
        return None
    if isinstance(obj, dict):
        for n in names:
            if n in obj and isinstance(obj[n], str) and obj[n]:
                return obj[n]
        for v in obj.values():
            r = walk_find_str(v, names, depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj[:24]:
            r = walk_find_str(v, names, depth + 1)
            if r is not None:
                return r
    return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def squash_z(z, per_unit=25.0, sign=+1):
    """Signed metric → 0-100 support via tanh; sign=+1 means bigger is
    supportive."""
    return round(clamp(50.0 + sign * per_unit * math.tanh(z), 0, 100), 1)


def fred_series(sid, n=400):
    qs = urllib.parse.urlencode({
        "series_id": sid, "api_key": FRED_KEY, "file_type": "json",
        "sort_order": "desc", "limit": n})
    b = http_get("https://api.stlouisfed.org/fred/series/"
                 "observations?" + qs)
    if not b:
        return []
    out = []
    try:
        for o in json.loads(b).get("observations", []):
            if o.get("value") not in (None, ".", ""):
                out.append({"date": o["date"], "value": float(o["value"])})
    except Exception as e:
        print(f"[machine] fred {sid}: {e}")
    out.reverse()
    return out


def contrib(name, source, support, detail, found=True):
    return {"name": name, "source": source,
            "support_0_100": support if found else None,
            "detail": detail, "found": found}


# ── Pillar 1 — FUTURE PROFITS ────────────────────────────────────────
def pillar_profits():
    c = []
    er = s3_json("data/estimate-revisions.json")
    if er is not None:
        v = walk_find(er, ["net_revision_breadth", "revision_breadth",
                           "net_breadth", "breadth", "composite_score",
                           "score", "net_pct"])
        if v is not None:
            sup = (round(clamp(v, 0, 100), 1) if 1.5 < abs(v) <= 100
                   else squash_z(v))
            c.append(contrib("Analyst estimate revisions",
                             "estimate-revisions.json", sup,
                             f"engine reading {v}"))
    aa = s3_json("data/analyst-actions.json")
    if aa is not None:
        ups = walk_find(aa, ["upgrades", "n_upgrades", "up"])
        dns = walk_find(aa, ["downgrades", "n_downgrades", "down"])
        if ups is not None and dns is not None and (ups + dns) > 0:
            ratio = ups / (ups + dns)
            c.append(contrib("Upgrade/downgrade balance",
                             "analyst-actions.json",
                             round(ratio * 100, 1),
                             f"{int(ups)} up vs {int(dns)} down"))
    pe = s3_json("data/physical-economy.json")
    if pe is not None:
        v = pe.get("composite_score")
        if isinstance(v, (int, float)):
            sig = (pe.get("trade_signal") or {})
            c.append(contrib("Physical economy pulse (leads earnings)",
                             "physical-economy.json",
                             round(clamp(v, 0, 100), 1),
                             f"{pe.get('composite_label')} · "
                             f"{sig.get('confidence')} confidence · "
                             f"{pe.get('n_components')} engines"))
    rt = s3_json("data/readthrough.json")
    if rt is not None:
        v = walk_find(rt, ["forward_orders_z", "backlog_z", "composite",
                           "score"])
        if v is not None:
            sup = (round(clamp(v, 0, 100), 1) if 1.5 < abs(v) <= 100
                   else squash_z(v))
            c.append(contrib("Backlog / forward orders",
                             "readthrough.json", sup,
                             f"engine reading {v}"))
    return c


# ── Pillar 2 — INTEREST RATES ────────────────────────────────────────
def pillar_rates():
    c = []
    d10 = fred_series("DGS10", 90)
    if len(d10) > 63:
        chg = (d10[-1]["value"] - d10[-64]["value"]) * 100
        c.append(contrib("10Y yield 3-month move",
                         "FRED DGS10",
                         round(clamp(50 - chg / 3.0, 0, 100), 1),
                         f"{chg:+.0f}bp over ~3m "
                         f"(now {d10[-1]['value']:.2f}%)"))
    cur = fred_series("T10Y2Y", 30)
    if cur:
        bp = cur[-1]["value"] * 100
        c.append(contrib("Curve 10s-2s", "FRED T10Y2Y",
                         round(clamp(50 + bp / 3.0, 0, 100), 1),
                         f"{bp:+.0f}bp"))
    rr = fred_series("DFII10", 90)
    if len(rr) > 63:
        chg = (rr[-1]["value"] - rr[-64]["value"]) * 100
        c.append(contrib("10Y REAL yield 3-month move", "FRED DFII10",
                         round(clamp(50 - chg / 3.0, 0, 100), 1),
                         f"{chg:+.0f}bp (now {rr[-1]['value']:.2f}%)"))
    hy = fred_series("BAMLH0A0HYM2", 45)
    if len(hy) > 21:
        chg = (hy[-1]["value"] - hy[-22]["value"]) * 100
        c.append(contrib("HY OAS 1-month change", "FRED BAMLH0A0HYM2",
                         round(clamp(50 - chg / 4.0, 0, 100), 1),
                         f"{chg:+.0f}bp (now "
                         f"{hy[-1]['value'] * 100:.0f}bp)"))
    pj = s3_json("data/plumbing-stress.json")
    if pj is not None:
        comp = pj.get("composite_score")
        if isinstance(comp, (int, float)):
            c.append(contrib("Funding plumbing (inverted stress)",
                             "plumbing-stress.json v2.1",
                             round(clamp(100 - comp, 0, 100), 1),
                             f"plumbing composite {comp} "
                             f"({pj.get('composite_label')})"))
    return c


# ── Pillar 3 — WHERE MONEY IS FLOWING ────────────────────────────────
def pillar_flow():
    c = []
    ac = s3_json("data/accum-composite.json")
    if ac is not None:
        v = walk_find(ac, ["composite_score", "composite", "score"])
        if v is not None:
            sup = (round(clamp(v, 0, 100), 1) if 1.5 < abs(v) <= 100
                   else squash_z(v))
            c.append(contrib("Institutional accumulation composite",
                             "accum-composite.json", sup,
                             f"engine reading {v}"))
    tf = s3_json("data/etf-true-flows.json")
    if tf is not None:
        v = walk_find(tf, ["net_flow_z", "aggregate_z", "net_flow_bn",
                           "composite", "score"])
        if v is not None:
            sup = (round(clamp(v, 0, 100), 1) if 1.5 < abs(v) <= 100
                   else squash_z(v))
            c.append(contrib("ETF true creations/redemptions",
                             "etf-true-flows.json", sup,
                             f"engine reading {v}"))
    dx = s3_json("data/dix.json")
    if dx is not None:
        v = walk_find(dx, ["dix", "DIX", "value"])
        if v is not None and 0 < v < 1:
            c.append(contrib("Dark-pool buying intensity (DIX)",
                             "dix.json",
                             round(clamp((v - 0.37) / 0.10 * 50 + 50,
                                         0, 100), 1),
                             f"DIX {v:.3f} (0.37 = neutral)"))
    rd = s3_json("data/rotation-dashboard.json")
    if rd is not None:
        st = walk_find_str(rd, ["regime", "nowcast", "state", "label"])
        if st:
            m = {"RISK_ON": 78, "EXPANSION": 72, "REFLATION": 66,
                 "NEUTRAL": 50, "MIXED": 50, "SLOWDOWN": 36,
                 "RISK_OFF": 22, "CONTRACTION": 25}
            sup = m.get(st.upper().replace(" ", "_").replace("-", "_"))
            if sup is not None:
                c.append(contrib("Cross-asset rotation regime",
                                 "rotation-dashboard.json", sup,
                                 f"regime {st}"))
    return c


# ── Pillar 4 — WHAT TRADERS ARE FORCED TO DO ─────────────────────────
def pillar_forced():
    c = []
    vt, vtk = s3_json_multi(["data/vol-target-unwind.json",
                             "data/vol-target.json",
                             "data/vol-unwind.json"])
    if vt is not None:
        v = walk_find(vt, ["unwind_score", "unwind_risk", "score",
                           "composite"])
        if v is not None:
            sup = (round(clamp(100 - v, 0, 100), 1)
                   if 1.5 < abs(v) <= 100 else squash_z(v, sign=-1))
            c.append(contrib("Vol-target fund unwind pressure "
                             "(inverted)", "vol-target-unwind.json",
                             sup, f"engine reading {v}"))
    ml, mlk = s3_json_multi(["data/capital-flow-radar.json",
                             "data/margin-lending.json",
                             "data/margin-debt.json",
                             "data/margin.json"])
    if ml is not None:
        v = walk_find(ml, ["margin_yoy_pct", "margin_debt_yoy",
                           "margin_z", "composite", "score"])
        if v is not None:
            sup = (round(clamp(v, 0, 100), 1) if 1.5 < abs(v) <= 100
                   else squash_z(v))
            c.append(contrib("Margin / leverage impulse",
                             "capital-flow-radar.json", sup,
                             f"engine reading {v}"))
    sm, smk = s3_json_multi(["data/spx-ma.json",
                             "data/spx-ma-command.json",
                             "data/spx-breadth.json"])
    if sm is not None:
        v = walk_find(sm, ["pct_above_200dma", "pct_above_200",
                           "breadth_200", "above_200_pct",
                           "pct200", "pctAbove200",
                           "pct_above_50dma", "pct_above_50"])
        if v is not None:
            v = v * 100 if 0 < v <= 1 else v
            c.append(contrib("CTA trend proxy — members above 200dma",
                             "spx-ma.json", round(clamp(v, 0, 100), 1),
                             f"{v:.0f}% of index above trend"))
    rg, rgk = s3_json_multi(["data/risk-gate.json",
                             "data/riskgate.json",
                             "data/risk_gate.json"])
    if rg is not None:
        st = walk_find_str(rg, ["state", "gate", "regime", "label",
                                "verdict", "posture", "risk_state",
                                "mode", "signal"])
        v = walk_find(rg, ["score", "composite", "gate_score",
                           "risk_score", "net"])
        if st:
            m = {"RISK_ON": 75, "NEUTRAL": 50, "CAUTION": 38,
                 "RISK_OFF": 20}
            sup = m.get(st.upper().replace("-", "_"))
            if sup is None and v is not None:
                sup = squash_z(v)
            if sup is not None:
                c.append(contrib("Risk-gate (6-leg brain-cited)",
                                 "risk-gate.json", sup,
                                 f"gate {st} (score {v})"))
    vx = fred_series("VIXCLS", 30)
    v3 = fred_series("VXVCLS", 30)
    if vx and v3:
        ratio = vx[-1]["value"] / v3[-1]["value"] if v3[-1]["value"] \
            else None
        if ratio:
            c.append(contrib(
                "VIX term structure (backwardation = forced zone)",
                "FRED VIXCLS/VXVCLS",
                round(clamp((1.02 - ratio) * 500, 0, 100), 1),
                f"1m/3m ratio {ratio:.3f} "
                f"({'BACKWARDATION' if ratio >= 1 else 'contango'})"))
    spx = fred_series("SP500", 320)
    if len(spx) > 200:
        ma = sum(o["value"] for o in spx[-200:]) / 200.0
        dist = (spx[-1]["value"] / ma - 1) * 100
        c.append(contrib(
            "SPX vs 200dma (CTA trend trigger)", "FRED SP500",
            round(clamp(50 + dist * 5, 0, 100), 1),
            f"{dist:+.1f}% vs 200dma (cross below = systematic "
            f"selling trigger)"))
    if vx:
        lvl = vx[-1]["value"]
        c.append(contrib("VIX level (forced-deleverage zone >28)",
                         "FRED VIXCLS",
                         round(clamp(100 - (lvl - 12) * (100 / 28.0),
                                     0, 100), 1),
                         f"VIX {lvl:.1f}"))
    return c


def pillar_score(contribs):
    vals = [x["support_0_100"] for x in contribs
            if x.get("support_0_100") is not None]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 1)


def label_for(score):
    if score is None:
        return "NO DATA"
    if score >= 70:
        return "STRONG TAILWIND"
    if score >= 55:
        return "TAILWIND"
    if score >= 45:
        return "NEUTRAL"
    if score >= 30:
        return "HEADWIND"
    return "FORCED SELLING RISK"


VERDICT_CLAUSE = {
    "profits": {"hi": "profit expectations rising",
                "mid": "profit expectations flat",
                "lo": "profit expectations being cut"},
    "rates": {"hi": "rates a tailwind",
              "mid": "rates neutral",
              "lo": "rates squeezing valuations"},
    "flow": {"hi": "money flowing IN",
             "mid": "flows mixed",
             "lo": "money flowing OUT"},
    "forced": {"hi": "no one is forced to sell",
               "mid": "positioning calm",
               "lo": "traders being FORCED to de-risk"},
}


def clause(pid, score):
    if score is None:
        return f"{pid}: no data"
    band = "hi" if score >= 55 else ("mid" if score >= 45 else "lo")
    return VERDICT_CLAUSE[pid][band]


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    pillars = {}
    builders = {"profits": pillar_profits, "rates": pillar_rates,
                "flow": pillar_flow, "forced": pillar_forced}
    for pid, fn in builders.items():
        try:
            cs = fn()
        except Exception as e:
            print(f"[machine] pillar {pid}: {e}")
            cs = []
        sc = pillar_score(cs)
        pillars[pid] = {"score": sc, "label": label_for(sc),
                        "n_contributors": len(cs), "contributors": cs}

    scored = {p: d["score"] for p, d in pillars.items()
              if d["score"] is not None}
    composite = None
    if scored:
        wsum = sum(PILLAR_WEIGHTS[p] for p in scored)
        composite = round(sum(PILLAR_WEIGHTS[p] * scored[p]
                              for p in scored) / wsum, 1)
    verdict = " · ".join(clause(p, pillars[p]["score"])
                         for p in ("profits", "rates", "flow", "forced"))

    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-market-machine",
        "as_of": now.isoformat(timespec="seconds"),
        "doctrine": ("The stock market is a machine that relies on four "
                     "things: future profits, interest rates, where "
                     "money is flowing, and what traders are being "
                     "forced to do right now."),
        "pillar_weights": PILLAR_WEIGHTS,
        "pillars": pillars,
        "composite_score": composite,
        "composite_label": label_for(composite),
        "machine_verdict": verdict,
        "convention": "0-100 SUPPORT scale; 100 = maximally supportive "
                      "for equities",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")

    try:
        hist = s3_json(HIST_KEY) or {"points": []}
        hist["points"].append(
            {"t": now.isoformat(timespec="seconds"), "c": composite,
             **{p: pillars[p]["score"] for p in pillars}})
        hist["points"] = hist["points"][-400:]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                      Body=json.dumps(hist).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
    except Exception as e:
        print(f"[machine] history: {e}")

    n_found = sum(p["n_contributors"] for p in pillars.values())
    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "composite": composite,
                                "label": label_for(composite),
                                "n_contributors": n_found})}
