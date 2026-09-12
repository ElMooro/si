"""ops_5424 -- plumbing brief + verdict projection.

S3 only. Does not call FRED/Massive/Finviz. Does not invent weights.
If plumbing-stress is EXPIRED, brief status=HELD and is not written as LIVE.
verdict.json writer is always jh-fusion-projection.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402
from brief_contract import (  # noqa: E402
    BRIEF_SCHEMA, TTL_HOURS, freshness, project_verdict,
    validate_brief, validate_verdict,
)

B = "justhodl-dashboard-live"
PLUMB_KEY = "data/plumbing-stress.json"
FUSION_KEY = "data/jh-fusion.json"
NFCI_KEY = "data/nfci.json"


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def main():
    with report("ops_5424_brief_plumbing_verdict") as R:
        R.heading("ops 5424 -- brief v3 plumbing + verdict projection")
        s3 = boto3.client("s3", region_name="us-east-1")
        stress, stress_lm, stress_err = _load(s3, PLUMB_KEY)
        fusion, fusion_lm, fusion_err = _load(s3, FUSION_KEY)
        nfci, nfci_lm, nfci_err = _load(s3, NFCI_KEY)

        as_of = None
        if isinstance(stress, dict):
            as_of = stress.get("as_of") or stress_lm
        fr = freshness(as_of, TTL_HOURS["plumbing"])
        required_ok = stress is not None and fr != "EXPIRED" and not stress_err
        inputs = {
            PLUMB_KEY: {
                "required": True,
                "last_modified": stress_lm,
                "as_of": as_of,
                "freshness": fr if stress else "EXPIRED",
                "error": stress_err,
            },
            NFCI_KEY: {
                "required": False,
                "last_modified": nfci_lm,
                "freshness": freshness(nfci_lm, TTL_HOURS["plumbing"]) if nfci_lm else "EXPIRED",
                "error": nfci_err,
            },
        }
        fields = {}
        if isinstance(stress, dict):
            fields = {
                "composite_score": stress.get("composite_score"),
                "composite_label": stress.get("composite_label"),
                "n_indicators": stress.get("n_indicators"),
                "n_with_data": stress.get("n_with_data"),
                "as_of": stress.get("as_of"),
            }
            layers = stress.get("layers") or {}
            fields["layer_scores"] = {k: (v or {}).get("score") for k, v in layers.items() if isinstance(v, dict)}
        if isinstance(nfci, dict):
            fields["nfci"] = {k: nfci.get(k) for k in ("value", "as_of", "level", "nfci") if k in nfci or True}
            fields["nfci"] = {"present": True, "keys": list(nfci.keys())[:12]}

        brief = {
            "schema": BRIEF_SCHEMA,
            "mode": "plumbing",
            "status": "LIVE" if required_ok else "HELD",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "ops_5424",
            "inputs": inputs,
            "fields": fields,
            "why": "plumbing-stress composite_label=%s score=%s" % (
                fields.get("composite_label"), fields.get("composite_score")),
        }
        berr = validate_brief(brief)
        if brief["status"] == "LIVE" and berr:
            R.ok("validator blocked LIVE: %s -- publishing HELD" % berr)
            brief["status"] = "HELD"
            brief["held_reason"] = berr
        s3.put_object(
            Bucket=B, Key="data/plumbing-brief.json",
            Body=json.dumps(brief, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("plumbing-brief status=%s freshness=%s score=%s" % (
            brief["status"], fr, fields.get("composite_score")))

        fusion_doc = fusion if isinstance(fusion, dict) else {"_error": fusion_err or "missing"}
        verdict = project_verdict(fusion_doc)
        verr = validate_verdict(verdict)
        if verr:
            R.fail("verdict invalid %s" % verr)
            sys.exit(1)
        s3.put_object(
            Bucket=B, Key="data/verdict.json",
            Body=json.dumps(verdict, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("verdict bias=%s score=%s horizon=%s shadow=%s missing=%s" % (
            verdict.get("bias"), verdict.get("score"), verdict.get("horizon"),
            verdict.get("shadow_mode"), verdict.get("missing_families"),
        ))
        if fusion_err:
            R.ok("fusion read error (missing vote, not fabricated): %s" % fusion_err)


if __name__ == "__main__":
    main()
