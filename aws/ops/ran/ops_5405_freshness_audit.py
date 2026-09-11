"""ops_5405 -- freshness audit. Does not invoke engines.

Doctrine: Event invokes only. This op reads import-health + provider-catalog
and writes data/freshness-audit.json so the desk can see STALE vs Friday-cadence.
The only named collector STALE in 5403/5404 was imf-full 103h.
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
STALE_H = 48.0


def _get(s3, key):
    obj = s3.get_object(Bucket=B, Key=key)
    return json.loads(obj["Body"].read()), obj["LastModified"].isoformat()


def main():
    with report("ops_5405_freshness_audit") as R:
        R.heading("ops 5405 -- freshness audit (no invokes)")
        s3 = boto3.client("s3", region_name=REGION)
        now = datetime.now(timezone.utc).isoformat()

        cat, cat_lm = _get(s3, "data/provider-catalog.json")
        ih, ih_lm = _get(s3, "data/import-health.json")

        stale_providers = []
        fresh_providers = []
        unknown = []
        for p in cat.get("providers") or []:
            fh = p.get("freshest_h")
            row = {
                "slug": p.get("slug"),
                "name": p.get("name"),
                "freshest_h": fh,
                "mb": p.get("total_mb"),
                "n_keys": p.get("n_keys"),
            }
            if fh is None:
                unknown.append(row)
            elif float(fh) > STALE_H:
                stale_providers.append(row)
            else:
                fresh_providers.append(row)
        stale_providers.sort(key=lambda x: -(x["freshest_h"] or 0))

        pipes = ih.get("pipelines") or []
        bad_pipes = [p for p in pipes if str(p.get("status") or "").upper() not in
                     ("OK", "COMPLETE", "RUNNING", "HEALTHY")]

        out = {
            "generated_at": now,
            "schema_version": 1,
            "source": "ops_5405",
            "rule": "no Lambda invoke -- EventBridge owns cadence",
            "stale_threshold_h": STALE_H,
            "import_health_overall": ih.get("overall"),
            "import_health_worst": ih.get("worst"),
            "import_health_as_of": ih.get("generated_at"),
            "bad_pipelines": bad_pipes,
            "n_providers": len(cat.get("providers") or []),
            "n_fresh": len(fresh_providers),
            "n_stale": len(stale_providers),
            "n_unknown_age": len(unknown),
            "stale_providers": stale_providers,
            "unknown_age": unknown,
            "named_collector": {
                "imf-full": "103h+ no state write -- only dead-lanes STALE in sentinel",
                "action": "do not lambda.invoke; EventBridge / existing imf engine owns it",
            },
            "notes": [
                "FRED pipeline is COMPLETE (282141 imported, cursor==queue).",
                "CFTC Friday weekly is cadence, not stale.",
                "Cleveland canary object is a ZIP (PK header), not gzip CSV -- parse still dirty.",
            ],
        }
        s3.put_object(
            Bucket=B,
            Key="data/freshness-audit.json",
            Body=json.dumps(out, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/freshness-audit.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        R.log("overall=%s stale_providers=%d bad_pipes=%s" % (
            ih.get("overall"), len(stale_providers),
            [p.get("name") for p in bad_pipes]))
        for row in stale_providers[:15]:
            R.warn("%s freshest_h=%s" % (row["slug"], row["freshest_h"]))
        R.ok("GREEN -- data/freshness-audit.json")


if __name__ == "__main__":
    main()
