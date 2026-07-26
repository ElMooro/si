"""
ops_3911 — PROBE: read data/brain.json in full — Khalid's own notes on how
he intends to set up macro/risk grading, which he has explicitly asked to be
read before amending how the system grades macro and risk. Dumps the
structure, then prints every note in full (his instruction is to read ALL
the notes), flagging the macro/risk/liquidity/grading-relevant ones.
Writes no code.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
KEYWORDS = ("macro", "risk", "liquid", "rrp", "repo", "stress", "carry", "yen",
            "grade", "grading", "weight", "regime", "gate", "risk-off", "riskoff")


def main():
    with report("3911_read_brain_notes") as rep:
        rep.heading("ops 3911 — read ALL of Khalid's brain notes (his explicit instruction)")
        try:
            o = s3.get_object(Bucket=BUCKET, Key="data/brain.json")
            doc = json.loads(o["Body"].read())
            age_h = round((datetime.now(timezone.utc) - o["LastModified"]).total_seconds()/3600, 1)
        except Exception as e:
            rep.fail(f"  data/brain.json unreadable: {str(e)[:200]}")
            sys.exit(1)

        rep.section("1. structure")
        rep.kv(age_h=age_h, top_level_type=type(doc).__name__)
        if isinstance(doc, dict):
            rep.log(f"  top-level keys: {sorted(doc.keys())}")
            for k, v in doc.items():
                if isinstance(v, list):
                    rep.log(f"  {k}: list of {len(v)}")
                elif isinstance(v, dict):
                    rep.log(f"  {k}: dict with keys {sorted(v.keys())[:15]}")
                else:
                    rep.log(f"  {k}: {type(v).__name__} = {str(v)[:120]}")

        rep.section("2. EVERY note, in full")
        # find the notes container wherever it is
        notes = None
        if isinstance(doc, dict):
            for k in ("notes", "entries", "items", "brain", "cards", "thoughts"):
                if isinstance(doc.get(k), list):
                    notes = doc[k]
                    break
            if notes is None:
                lists = [(k, v) for k, v in doc.items() if isinstance(v, list)]
                if lists:
                    notes = max(lists, key=lambda kv: len(kv[1]))[1]
        elif isinstance(doc, list):
            notes = doc
        if notes is None:
            rep.fail("  could not locate a notes list — full raw doc follows")
            rep.log(json.dumps(doc, default=str)[:8000])
            sys.exit(1)

        rep.kv(n_notes=len(notes))
        for i, n in enumerate(notes):
            blob = json.dumps(n, default=str) if not isinstance(n, str) else n
            relevant = any(kw in blob.lower() for kw in KEYWORDS)
            tag = ">>> MACRO/RISK-RELEVANT <<<" if relevant else ""
            rep.log(f"--- note {i+1}/{len(notes)} {tag} ---")
            rep.log(f"  {blob[:2500]}")

        rep.ok(f"PROBE COMPLETE — {len(notes)} notes read in full")


if __name__ == "__main__":
    main()
