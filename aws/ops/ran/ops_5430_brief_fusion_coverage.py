"""ops_5430 -- read-only coverage map. No fusion write. No registry edit."""
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

B = "justhodl-dashboard-live"
BRIEFS = {
    "MACRO": "data/official-stats-brief.json",
    "RISK": "data/plumbing-brief.json",
    "FLOW": "data/positioning-brief.json",
    "MARKET": "data/market-tape-brief.json",
    "CATALYST": "data/event-brief.json",
}


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def main():
    with report("ops_5430_brief_fusion_coverage") as R:
        R.heading("ops 5430 coverage map (read-only)")
        s3 = boto3.client("s3", region_name="us-east-1")
        fusion, flm, ferr = _load(s3, "data/jh-fusion.json")
        verdict, vlm, verr = _load(s3, "data/verdict.json")
        ent = ((fusion or {}).get("entities") or {}).get("market:US_EQUITY") or {}
        best = ent.get("best_horizon") or "INTERMEDIATE"
        hz = ((ent.get("horizons") or {}).get(best) or {})
        missing = list(hz.get("missing_families") or [])
        rows = {}
        for fam, key in BRIEFS.items():
            doc, lm, err = _load(s3, key)
            rows[fam] = {
                "brief_key": key,
                "status": (doc or {}).get("status"),
                "source": (doc or {}).get("source"),
                "why": (doc or {}).get("why"),
                "last_modified": lm,
                "error": err,
                "fills_missing_family": fam in missing,
                "adapter_exists": False,
                "note": "brief LIVE does not imply fusion ingest; adapter still missing",
            }
        out = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "ops_5430",
            "fusion_run_id": (fusion or {}).get("run_id"),
            "fusion_error": ferr,
            "verdict_writer": (verdict or {}).get("writer"),
            "horizon": best,
            "missing_families": missing,
            "coverage": hz.get("fusion_coverage"),
            "briefs": rows,
            "next": "add jh_adapters + registry rows only after this map stays stable 24h",
        }
        s3.put_object(
            Bucket=B, Key="data/brief-fusion-coverage.json",
            Body=json.dumps(out, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("missing=%s" % missing)
        for fam, r in rows.items():
            R.ok("%s brief=%s fills=%s" % (fam, r["status"], r["fills_missing_family"]))


if __name__ == "__main__":
    main()
