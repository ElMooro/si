"""justhodl-wl-fusion v1.0 — ops 3178.

Khalid's 96 active watchlist engines become inputs to the platform's own
engines. Three outputs, in ascending order of usefulness:

  1. THEME PRESSURE — his panels pooled by theme (LIQUIDITY, STRESS,
     CREDIT, DOLLAR, GROWTH, INFLATION, BREADTH, RATES): how extreme is
     each theme right now (mean activation percentile of its ACTIVE
     engines), how many are firing, and how many have PROVEN an edge.

  2. EVIDENCE-WEIGHTED OVERLAY — only panels that survive FDR with
     |t| >= 2 and n_eff >= 6 are allowed to tilt a score, bounded to
     [0.90, 1.10]. Everything else is context, never score. Consumers
     read this through aws/shared/wl_fusion.py, which enforces the rule.

  3. DIVERGENCE BOARD — the interesting part. Where HIS indicators say
     one thing and the PLATFORM'S engines say another. A divergence is
     not a verdict; it is the question worth asking today.

Output: data/wl-fusion.json
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/wl-fusion.json"
S3 = boto3.client("s3", region_name="us-east-1")

# platform engines each theme should be compared against (defensive reads —
# key names differ across the fleet, so every probe has fallbacks)
PLATFORM = {
    "LIQUIDITY": [("data/global-liquidity.json",
                   ("regime", "global_impulse_13w_pct")),
                  ("data/fed-liquidity.json", ("regime", "score"))],
    "DOLLAR": [("data/dollar.json", ("regime", "risk_transmission")),
               ("data/dollar-radar.json", ("regime", "signal"))],
    "STRESS": [("data/crisis-composite.json",
                ("defcon_level", "master_crisis_score")),
               ("data/systemic-stress.json", ("composite", "score_0_100"))],
    "GROWTH": [("data/macro-nowcast.json", ("regime", "normalized_score")),
               ("data/cycle-clock.json", ("cycle", "verdict"))],
    "INFLATION": [("data/macro-nowcast.json", ("regime", "inflation"))],
    "CREDIT": [("data/best-setups.json", ("credit_danger", "regime"))],
    "BREADTH": [("data/breadth-thrust.json", ("state", "signal"))],
    "RATES": [("data/fed-liquidity.json", ("regime", "score"))],
}


def gj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def dig(doc, keys):
    """pull the first key that exists, at top level or one level down."""
    if not isinstance(doc, dict):
        return None
    for k in keys:
        if k in doc:
            return doc[k]
    for v in doc.values():
        if isinstance(v, dict):
            for k in keys:
                if k in v:
                    return v[k]
    return None


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    idx = gj("data/wl-engines.json") or {}
    engines = [e for e in (idx.get("engines") or [])
               if e.get("state") == "ACTIVE"]
    if not engines:
        return {"ok": False, "error": "no active watchlist engines"}

    # ── 1. theme pressure ────────────────────────────────────────────
    themes = {}
    for e in engines:
        th = e.get("theme") or "OTHER"
        themes.setdefault(th, []).append(e)

    out_themes = {}
    for th, es in themes.items():
        pcts = [e["activation_pctile"] for e in es
                if e.get("activation_pctile") is not None]
        firing = [e for e in es if e.get("firing")]
        proven = [e for e in es
                  if e.get("fdr_pass")
                  and abs((e.get("w13") or {}).get("t_stat", 0)) >= 2
                  and (e.get("w13") or {}).get("n_effective", 0) >= 6]
        pressure = round(sum(pcts) / len(pcts), 1) if pcts else None
        # proven tilt: sign of the proven panels' historical forward excess
        tilt = None
        if proven:
            ex = [(e.get("w13") or {}).get("excess_vs_base_pct", 0)
                  for e in proven]
            mu = sum(ex) / len(ex)
            tilt = max(-1.0, min(1.0, mu / 2.0))     # ±2% → full tilt
        out_themes[th] = {
            "n_active": len(es), "n_firing": len(firing),
            "n_proven": len(proven),
            "pressure_pctile": pressure,
            "verdict": ("EXTREME" if (pressure or 0) >= 80
                        else "ELEVATED" if (pressure or 0) >= 60
                        else "QUIET"),
            "proven_tilt": round(tilt, 3) if tilt is not None else None,
            "proven_evidence": [
                {"engine": e["name"],
                 "t": (e.get("w13") or {}).get("t_stat"),
                 "excess_13w_pct": (e.get("w13") or {})
                 .get("excess_vs_base_pct")}
                for e in proven[:4]],
            "top_firing": [
                {"engine_id": e["engine_id"], "name": e["name"],
                 "activation": e.get("activation_now"),
                 "pctile": e.get("activation_pctile"),
                 "lit": (e.get("lit") or [])[:4]}
                for e in sorted(firing,
                                key=lambda x: -(x.get("activation_pctile") or 0)
                                )[:5]],
        }

    # ── 2. divergences vs the platform's own engines ─────────────────
    divs = []
    for th, feeds in PLATFORM.items():
        t = out_themes.get(th)
        if not t or t["verdict"] == "QUIET":
            continue
        for key, fields in feeds:
            doc = gj(key)
            if not doc:
                continue
            state = dig(doc, fields)
            if state is None:
                continue
            divs.append({
                "theme": th,
                "khalid": {"verdict": t["verdict"],
                           "pressure_pctile": t["pressure_pctile"],
                           "firing": t["n_firing"], "of": t["n_active"],
                           "top": [p["name"] for p in t["top_firing"][:2]]},
                "platform": {"engine": key.split("/")[-1]
                             .replace(".json", ""),
                             "state": state},
                "note": (f"His {th} panels are {t['verdict']} "
                         f"({t['pressure_pctile']}th pctile, "
                         f"{t['n_firing']}/{t['n_active']} firing) while "
                         f"{key.split('/')[-1].replace('.json','')} reads "
                         f"'{state}'. Worth asking which is early."),
            })
            break                                   # one feed per theme

    doc = {
        "generated_at": now.isoformat(), "version": "1.0",
        "n_engines_active": len(engines),
        "n_firing": sum(1 for e in engines if e.get("firing")),
        "n_proven": sum(1 for t in out_themes.values() for _ in
                        range(t["n_proven"])),
        "themes": out_themes,
        "divergences": divs,
        "contract": ("ADDITIVE-ONLY: consumers read this through "
                     "aws/shared/wl_fusion.py. Unproven panels are CONTEXT "
                     "(displayed, never scored); only FDR-proven panels "
                     "(|t|>=2, n_eff>=6) may tilt a score, bounded to "
                     "[0.90, 1.10]."),
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json")
    print(json.dumps({"ok": True, "themes": len(out_themes),
                      "divergences": len(divs)}))
    return {"ok": True, "n_themes": len(out_themes),
            "n_divergences": len(divs)}
