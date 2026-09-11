"""Fleet helper: load the public Brain constitution (no note bodies).

Engines that already import public_brain_projection can switch to this.
Raw data/brain.json stays private. Fail-soft if the constitution is missing.
"""
CONSTITUTION_KEY = "data/brain-constitution.json"


def load_constitution(s3, bucket="justhodl-dashboard-live"):
    try:
        doc = __import__("json").loads(
            s3.get_object(Bucket=bucket, Key=CONSTITUTION_KEY)["Body"].read()
        )
    except Exception:
        return {"ok": False, "hard_rules": [], "themes": [], "risk_posture": None}
    # A partial write, an empty body or a foreign document must not count as a
    # constitution: `ok` requires the producer stamp AND a content hash, so a
    # consumer falls back to its own private directive read instead of "balanced".
    if not isinstance(doc, dict) or doc.get("engine") != "brain-sync" or not doc.get("content_hash"):
        return {"ok": False, "hard_rules": [], "themes": [], "risk_posture": None}
    return {
        "ok": True,
        "generated_at": doc.get("generated_at"),
        "content_hash": doc.get("content_hash"),
        "n_notes": doc.get("n_notes"),
        "investor_profile": doc.get("investor_profile"),
        "hard_rules": doc.get("hard_rules") or [],
        "themes": doc.get("themes") or [],
        "sector_tilts": doc.get("sector_tilts") or {},
        "risk_posture": doc.get("risk_posture"),
        "signal_emphasis": doc.get("signal_emphasis") or [],
        "avoid": doc.get("avoid") or [],
        "regime_read": doc.get("regime_read"),
    }


def posture_enum(value):
    """Map the directive's posture (free text distilled from private notes) to a public enum."""
    p = str(value or "").lower().strip()
    if not p:
        return None
    return "aggressive" if "aggressive" in p else "defensive" if "defensive" in p else "balanced"


def overlay_payload(payload, constitution):
    """Attach a constitution RECEIPT to a public payload: counts, hash and an enum posture only.
    Never the directive text -- most consumers' outputs are public (risk-gate, position-sizing,
    domain-barometers) and the Sep-8 audit boundary forbids Brain prose there."""
    if not isinstance(payload, dict):
        return payload
    payload = dict(payload)
    payload["brain_constitution"] = {
        "consumed": bool(constitution and constitution.get("ok")),
        "content_hash": (constitution or {}).get("content_hash"),
        "risk_posture": posture_enum((constitution or {}).get("risk_posture")),
        "n_hard_rules": len((constitution or {}).get("hard_rules") or []),
        "n_themes": len((constitution or {}).get("themes") or []),
    }
    return payload
