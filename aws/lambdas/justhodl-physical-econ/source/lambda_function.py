"""justhodl-physical-econ v1.0.0 (ops 4610)

The Physical Economy TRADE SIGNAL — the cross-engine wiring job flagged
in the ops-4559 strategic review, now with PJM as the fourth leg.

Joins (never refetches):
  data/pjm-grid.json      PJM load momentum + LMP shock canary
  data/port-cargo.json    port call / cargo momentum
  data/grid-queue.json    interconnection queue velocity
  data/freight-pulse.json freight activity pulse

Each component maps to a 0-100 PHYSICAL EXPANSION scale (100 = real
economy accelerating). Composite → a trade signal for the cyclical
complex (industrials / transports / power equipment — the GEV/EME/NVT
sleeve): EXPANSION / NEUTRAL / CONTRACTION, with confidence = found
legs. Discovery-based joins report absences honestly; nothing faked.

Output: data/physical-economy.json (+ history 400).
"""
import json
import math
import os
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("S3_KEY_OUT", "data/physical-economy.json")
HIST_KEY = "data/physical-economy-history.json"
s3 = boto3.client("s3")


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception as e:
        print(f"[phys] {key}: {e}")
        return None


def walk_find(obj, names, depth=0):
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
    return round(clamp(50.0 + sign * per_unit * math.tanh(z), 0, 100), 1)


def to_support(v):
    """Engine reading → 0-100: pass through if already 0-100-ish,
    squash if z-like."""
    return (round(clamp(v, 0, 100), 1) if 1.5 < abs(v) <= 100
            else squash_z(v))


def comp(name, source, support, detail, found=True):
    return {"name": name, "source": source,
            "expansion_0_100": support if found else None,
            "detail": detail, "found": found}


def build_components():
    c = []
    # ── PJM (shape known — I wrote it) ───────────────────────────────
    pjm = s3_json("data/pjm-grid.json")
    if pjm is not None:
        mom = ((pjm.get("load") or {}).get("momentum_8d_pct"))
        if isinstance(mom, (int, float)):
            c.append(comp("PJM demand momentum (8d)", "pjm-grid.json",
                          round(clamp(50 + mom * 10, 0, 100), 1),
                          f"RTO load momentum {mom:+.2f}% · "
                          f"{(pjm.get('load') or {}).get('current_gw')}"
                          f" GW now"))
        shock = ((pjm.get("lmp") or {}).get("shock_state"))
        if shock in ("CALM", "AMBER", "RED"):
            m = {"CALM": 65, "AMBER": 35, "RED": 10}
            lm = pjm.get("lmp") or {}
            c.append(comp("PJM power-price shock canary",
                          "pjm-grid.json", m[shock],
                          f"LMP ${lm.get('daily_avg')}/MWh · DoD "
                          f"{lm.get('daily_avg_dod_pct')}% · {shock}"))
    # ── Port cargo ───────────────────────────────────────────────────
    pc = s3_json("data/port-cargo.json")
    if pc is not None:
        v = walk_find(pc, ["momentum_pct", "momentum", "yoy_pct",
                           "composite_score", "composite", "score",
                           "z", "z_score", "pulse"])
        st = walk_find_str(pc, ["state", "regime", "label", "signal"])
        if v is not None:
            c.append(comp("Port cargo momentum", "port-cargo.json",
                          to_support(v),
                          f"engine reading {v}"
                          + (f" · {st}" if st else "")))
    # ── Grid interconnection queue ───────────────────────────────────
    gq = s3_json("data/grid-queue.json")
    if gq is not None:
        v = walk_find(gq, ["queue_velocity", "velocity",
                           "momentum_pct", "momentum", "growth_pct",
                           "composite_score", "composite", "score",
                           "z", "z_score"])
        st = walk_find_str(gq, ["state", "regime", "label", "signal"])
        if v is not None:
            c.append(comp("Grid interconnection queue velocity",
                          "grid-queue.json", to_support(v),
                          f"engine reading {v}"
                          + (f" · {st}" if st else "")))
    # ── Freight pulse ────────────────────────────────────────────────
    fp = s3_json("data/freight-pulse.json")
    if fp is not None:
        v = walk_find(fp, ["pulse", "momentum_pct", "momentum",
                           "yoy_pct", "composite_score", "composite",
                           "score", "z", "z_score"])
        st = walk_find_str(fp, ["state", "regime", "label", "signal"])
        if v is not None:
            c.append(comp("Freight pulse", "freight-pulse.json",
                          to_support(v),
                          f"engine reading {v}"
                          + (f" · {st}" if st else "")))
    return c


def label_for(score):
    if score is None:
        return "NO DATA"
    if score >= 62:
        return "EXPANSION"
    if score >= 45:
        return "NEUTRAL"
    return "CONTRACTION"


def trade_signal(score, n_found):
    lab = label_for(score)
    conf = ("HIGH" if n_found >= 4 else
            "MEDIUM" if n_found == 3 else "LOW")
    if lab == "EXPANSION":
        tilt = ("physical economy accelerating — supports the "
                "cyclical / power-equipment sleeve (GEV, EME, NVT, "
                "transports)")
    elif lab == "CONTRACTION":
        tilt = ("physical economy rolling over — fade cyclicals, "
                "watch freight and power demand for confirmation")
    else:
        tilt = "physical economy mixed — no cyclical edge either way"
    return {"signal": lab, "confidence": conf, "tilt": tilt}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    comps = []
    try:
        comps = build_components()
    except Exception as e:
        print(f"[phys] build: {e}")
    vals = [x["expansion_0_100"] for x in comps
            if x.get("expansion_0_100") is not None]
    score = round(sum(vals) / len(vals), 1) if vals else None
    sig = trade_signal(score, len(vals))
    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-physical-econ",
        "as_of": now.isoformat(timespec="seconds"),
        "composite_score": score,
        "composite_label": label_for(score),
        "n_components": len(comps),
        "components": comps,
        "trade_signal": sig,
        "convention": "0-100 physical-expansion scale; joins the four "
                      "Physical Economy engines, never refetches",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    try:
        hist = {}
        try:
            hist = json.loads(s3.get_object(
                Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"points": []}
        hist["points"].append({"t": now.isoformat(timespec="seconds"),
                               "c": score, "n": len(vals)})
        hist["points"] = hist["points"][-400:]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                      Body=json.dumps(hist).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
    except Exception as e:
        print(f"[phys] history: {e}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": score is not None,
                                "composite": score,
                                "label": label_for(score),
                                "n_components": len(comps),
                                "signal": sig.get("signal"),
                                "confidence": sig.get("confidence")})}
