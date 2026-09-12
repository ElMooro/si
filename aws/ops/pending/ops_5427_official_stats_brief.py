"""ops_5427 -- official-stats brief. S3 only. Required: fed-nowcast-join."""
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
NOWCAST = "data/fed-nowcast-join.json"


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def main():
    with report("ops_5427_official_stats_brief") as R:
        R.heading("ops 5427 official-stats-brief")
        s3 = boto3.client("s3", region_name="us-east-1")
        ttl = TTL_HOURS["official_stats"]
        doc, lm, err = _load(s3, NOWCAST)
        as_of = (doc or {}).get("generated_at") or lm
        fr = freshness(as_of, ttl) if as_of else "EXPIRED"
        required_ok = doc is not None and fr != "EXPIRED" and not err
        series = (doc or {}).get("series") or {}
        atl = ((series.get("atlantafed") or {}).get("last") or {})
        cle = ((series.get("clevelandfed") or {}).get("last") or {})
        fields = {
            "gdpnow": atl.get("GDPNOW"),
            "gdpnow_date": atl.get("observation_date"),
            "t10y3m": cle.get("T10Y3M"),
            "t10y3m_date": cle.get("observation_date"),
            "nowcast_status": (doc or {}).get("status"),
        }
        brief = {
            "schema": BRIEF_SCHEMA,
            "mode": "official_stats",
            "status": "LIVE" if required_ok else "HELD",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "ops_5427",
            "inputs": {
                NOWCAST: {
                    "required": True,
                    "last_modified": lm,
                    "as_of": as_of,
                    "freshness": fr,
                    "error": err,
                }
            },
            "fields": fields,
            "why": "GDPNow %s on %s; T10Y3M %s on %s" % (
                fields.get("gdpnow"), fields.get("gdpnow_date"),
                fields.get("t10y3m"), fields.get("t10y3m_date")),
        }
        berr = validate_brief(brief)
        if brief["status"] == "LIVE" and berr:
            brief["status"] = "HELD"
            brief["held_reason"] = berr
        s3.put_object(
            Bucket=B, Key="data/official-stats-brief.json",
            Body=json.dumps(brief, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("official-stats-brief status=%s %s" % (brief["status"], brief["why"]))


if __name__ == "__main__":
    main()
