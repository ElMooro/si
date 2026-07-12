"""aws/shared/wl_fusion.py — Khalid's watchlist engines → the fleet.

Every platform engine that fuses his panels does it through THIS module,
so the rules are enforced in one place:

  1. ADDITIVE ONLY. If data/wl-fusion.json is missing or stale, every
     consumer behaves exactly as it did before. No engine may depend on
     his panels to function.
  2. EVIDENCE-WEIGHTED. A panel that has NOT proven an edge (FDR pass,
     |t| >= 2, n_eff >= 6) contributes CONTEXT — it is displayed, never
     scored. Only proven panels are allowed to move a number, and even
     then the multiplier is bounded to [0.90, 1.10]: his research tilts
     a ranking, it never hijacks one.
  3. AUDITABLE. Every consumer records WHICH panels moved it and by how
     much, so a decision can always be traced back to the evidence.
"""

import json
import os

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FUSION_KEY = "data/wl-fusion.json"
_S3 = boto3.client("s3", region_name="us-east-1")

MULT_FLOOR, MULT_CEIL = 0.90, 1.10


def load(bucket=None):
    """→ the fusion doc, or {} (never raises — additive-only contract)."""
    try:
        b = _S3.get_object(Bucket=bucket or BUCKET,
                           Key=FUSION_KEY)["Body"].read()
        return json.loads(b)
    except Exception:
        return {}


def theme(doc, name):
    return (doc.get("themes") or {}).get(name) or {}


def context(doc, themes=None):
    """A compact, human-readable block for engine outputs: what HIS
    research says right now, with the evidence attached."""
    out = {}
    for name, t in (doc.get("themes") or {}).items():
        if themes and name not in themes:
            continue
        if not t.get("n_active"):
            continue
        out[name] = {
            "pressure_pctile": t.get("pressure_pctile"),
            "firing": t.get("n_firing"),
            "of": t.get("n_active"),
            "proven": t.get("n_proven"),
            "top": [p.get("name") for p in (t.get("top_firing") or [])][:3],
            "verdict": t.get("verdict"),
        }
    return out or None


def multiplier(doc, name, bullish_is_up=True):
    """Score tilt from a theme — ONLY from panels that have proven an
    edge. Unproven pressure returns exactly 1.0 (no effect)."""
    t = theme(doc, name)
    proven = t.get("proven_tilt")          # signed: +ve = risk-ON history
    if not proven or not t.get("n_proven"):
        return 1.0, None
    firing = (t.get("pressure_pctile") or 0) >= 80
    if not firing:
        return 1.0, None
    # a proven risk-OFF panel firing → shade DOWN; risk-ON → shade UP
    tilt = max(-1.0, min(1.0, float(proven)))
    m = 1.0 + 0.10 * tilt * (1 if bullish_is_up else -1)
    m = max(MULT_FLOOR, min(MULT_CEIL, m))
    return round(m, 3), {
        "theme": name, "multiplier": round(m, 3),
        "proven_panels": t.get("n_proven"),
        "evidence": t.get("proven_evidence"),
    }


def divergences(doc):
    """Where HIS research disagrees with the platform's own engines."""
    return doc.get("divergences") or []
