#!/usr/bin/env python3
"""Publish a public brain constitution (no note bodies) after each private mirror."""
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "lambdas/justhodl-brain-sync/source/lambda_function.py"
OLD = '''    publish_private("brain-history", {"history": read_history(), "last_checked": out["generated_at"]})
    print(f"[brain-sync] DONE'''
NEW = '''    publish_private("brain-history", {"history": read_history(), "last_checked": out["generated_at"]})
    direc = directive if isinstance(directive, dict) else {}
    constitution = {
        "engine": "brain-sync",
        "schema_version": "1.0",
        "version": "2.1",
        "generated_at": out["generated_at"],
        "content_hash": content_hash,
        "n_notes": len(notes),
        "n_pinned": len(pinned),
        "investor_profile": direc.get("investor_profile"),
        "hard_rules": direc.get("hard_rules") or [],
        "themes": direc.get("themes") or [],
        "sector_tilts": direc.get("sector_tilts") or {},
        "risk_posture": direc.get("risk_posture"),
        "signal_emphasis": direc.get("signal_emphasis") or [],
        "avoid": direc.get("avoid") or [],
        "regime_read": regime_read if isinstance(regime_read, dict) else None,
        "distill_cadence": out.get("distill_cadence"),
        "note": "Public constitution for fleet scoring. Raw notes stay private.",
    }
    s3.put_object(Bucket=BUCKET, Key="data/brain-constitution.json",
                  Body=json.dumps(constitution, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=60")
    print(f"[brain-sync] DONE'''
VER_OLD = '"version": "2.0"'
VER_NEW = '"version": "2.1"'

def main():
    text = TARGET.read_text()
    if "data/brain-constitution.json" in text:
        print("already publishes constitution")
        return 0
    if OLD not in text:
        raise SystemExit("brain-sync anchor miss")
    text = text.replace(OLD, NEW, 1).replace(VER_OLD, VER_NEW, 1)
    TARGET.write_text(text)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
