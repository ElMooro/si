"""ops_5428 -- positioning brief. S3 only. Required: 13f-positions."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
from brief_contract import BRIEF_SCHEMA, TTL_HOURS, freshness, validate_brief  # noqa: E402

B = "justhodl-dashboard-live"
F13 = "data/13f-positions.json"
INST = "data/finviz-inst-flow.json"


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def main():
    with report("ops_5428_positioning_brief") as R:
        R.heading("ops 5428 positioning-brief")
        s3 = boto3.client("s3", region_name="us-east-1")
        ttl = TTL_HOURS["positioning"]
        a, alm, aerr = _load(s3, F13)
        b, blm, berr = _load(s3, INST)
        a_asof = (a or {}).get("generated_at") or alm
        inputs = {
            F13: {
                "required": True,
                "last_modified": alm,
                "as_of": a_asof,
                "freshness": freshness(a_asof, ttl) if a_asof else "EXPIRED",
                "error": aerr,
            },
            INST: {
                "required": False,
                "last_modified": blm,
                "as_of": (b or {}).get("generated_at") or blm,
                "freshness": freshness((b or {}).get("generated_at") or blm, ttl) if (blm or b) else "EXPIRED",
                "error": berr,
            },
        }
        required_ok = inputs[F13]["freshness"] != "EXPIRED" and not aerr and a
        fields = {}
        if isinstance(a, dict):
            fields["as_of_quarter"] = a.get("as_of_quarter")
            fields["funds_total"] = a.get("funds_total")
            fields["funds_parsed"] = a.get("funds_parsed")
            fields["stale_funds"] = [x.get("fund_key") for x in (a.get("stale_funds") or [])]
        if isinstance(b, dict):
            fields["accumulating"] = b.get("n_accumulating")
            fields["distributing"] = b.get("n_distributing")
        brief = {
            "schema": BRIEF_SCHEMA,
            "mode": "positioning",
            "status": "LIVE" if required_ok else "HELD",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "ops_5428",
            "inputs": inputs,
            "fields": fields,
            "why": "13F quarter %s funds=%s stale=%s inst buy/sell %s/%s" % (
                fields.get("as_of_quarter"), fields.get("funds_total"),
                ",".join(fields.get("stale_funds") or []) or "none",
                fields.get("accumulating"), fields.get("distributing")),
        }
        berr2 = validate_brief(brief)
        if brief["status"] == "LIVE" and berr2:
            brief["status"] = "HELD"
            brief["held_reason"] = berr2
        s3.put_object(
            Bucket=B, Key="data/positioning-brief.json",
            Body=json.dumps(brief, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("positioning-brief status=%s %s" % (brief["status"], brief["why"]))


if __name__ == "__main__":
    main()
