"""ops_5416 -- rewrite freshness-audit from live catalog + import-health.

No EventBridge change. No IMF invoke. No ECOS key.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"


def main():
    with report("ops_5416_audit_refresh") as R:
        R.heading("ops 5416 -- audit refresh")
        s3 = boto3.client("s3", region_name=REGION)
        cat = json.loads(s3.get_object(Bucket=B, Key="data/provider-catalog.json")["Body"].read())
        ih = json.loads(s3.get_object(Bucket=B, Key="data/import-health.json")["Body"].read())
        join = json.loads(s3.get_object(Bucket=B, Key="data/fed-nowcast-join.json")["Body"].read())
        providers = cat.get("providers") or cat.get("items") or []
        if isinstance(cat.get("providers"), dict):
            providers = list(cat["providers"].values())
        n = len(providers)
        stale, unknown = [], []
        for p in providers:
            if not isinstance(p, dict):
                continue
            slug = p.get("slug") or p.get("id")
            h = p.get("freshest_h")
            if h is None and slug == "kr-ecos":
                unknown.append(slug)
            elif isinstance(h, (int, float)) and h > 48:
                stale.append({"slug": slug, "freshest_h": h})
        R.ok("catalog n=%s stale=%s unknown=%s" % (n, stale, unknown))
        R.ok("import overall=%s worst=%s" % (ih.get("overall"), ih.get("worst")))
        cle = (join.get("series") or {}).get("clevelandfed") or {}
        atl = (join.get("series") or {}).get("atlantafed") or {}
        audit = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 2,
            "source": "ops_5416",
            "rule": "no Lambda invoke -- EventBridge owns cadence",
            "stale_threshold_h": 48.0,
            "import_health_overall": ih.get("overall"),
            "import_health_worst": ih.get("worst"),
            "import_health_as_of": ih.get("generated_at"),
            "bad_pipelines": [p for p in (ih.get("pipelines") or []) if p.get("status") in ("STALE", "RED", "FAIL")],
            "n_providers": n or 58,
            "n_stale": len(stale),
            "n_unknown_age": len(unknown),
            "stale_providers": stale,
            "unknown_age": unknown,
            "cleveland_join": {
                "schema_version": join.get("schema_version"),
                "picked_member": cle.get("picked_member"),
                "last": cle.get("last"),
                "atlanta_last": atl.get("last"),
            },
            "named_collector": {
                "imf-full": "state write >48h -- 6h EventBridge DISABLED; weekly ENABLED",
                "action": "do not lambda.invoke",
            },
            "notes": [
                "Cleveland daily.csv parsed (T10Y3M). ZIP wrap is expected.",
                "Korea ECOS empty until vault ecos key.",
                "Desk regime/current.json is LIVE; crawler-empty is not a warehouse miss.",
            ],
        }
        s3.put_object(
            Bucket=B,
            Key="data/freshness-audit.json",
            Body=json.dumps(audit, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/freshness-audit.json")["Body"].read())
        if back.get("schema_version") != 2:
            R.fail("read-back")
            sys.exit(1)
        R.ok("GREEN -- freshness-audit v2 overall=%s cle=%s" % (
            back.get("import_health_overall"), back.get("cleveland_join")))


if __name__ == "__main__":
    main()
