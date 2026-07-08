#!/usr/bin/env python3
"""ops 2986 -- U9 phase A final: status histogram over the 668-engine
dict, orphan extraction on any ORPHAN-bearing status (fresh/stale/dead
split by status text, falling back to age_h), plus the full
allocator_compass_bridge object. Always-PASS; samples guarantee
convergence."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def main():
    out = {"ops": 2986, "fails": [], "warns": [],
           "ts": datetime.now(timezone.utc).isoformat(),
           "verdict": "PASS"}
    with report("2986_orphan_dump3") as rep:
        audit = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/fleet-audit.json")["Body"].read())
        eng = audit.get("engines") or {}
        hist = {}
        for nm, r in eng.items():
            s = str((r or {}).get("status"))
            hist[s] = hist.get(s, 0) + 1
        out["status_histogram"] = hist
        rep.kv(histogram=json.dumps(hist))

        orphan_statuses = [s for s in hist if "ORPHAN" in s.upper()]
        samples = {}
        for s in orphan_statuses:
            for nm, r in eng.items():
                if str(r.get("status")) == s:
                    samples[s] = {nm: r}
                    break
        out["orphan_status_samples"] = {
            k: json.dumps(v)[:500] for k, v in samples.items()}

        fresh, stale, dead = [], [], []
        for nm, r in eng.items():
            s = str(r.get("status")).upper()
            if "ORPHAN" not in s:
                continue
            row = {"engine": nm, "family": r.get("family") or "?",
                   "outs": (r.get("outs") or [])[:6],
                   "age_h": r.get("age_h") or r.get("freshest_age_h"),
                   "status": r.get("status")}
            if "FRESH" in s:
                fresh.append(row)
            elif "STALE" in s:
                stale.append(row)
            elif "DEAD" in s:
                dead.append(row)
            else:
                a = row["age_h"]
                (fresh if (a is not None and a <= 60) else stale
                 ).append(row)
        out["orphan_fresh"], out["orphan_stale"], out["orphan_dead"] = \
            fresh, stale, dead
        out["counts"] = {"fresh": len(fresh), "stale": len(stale),
                         "dead": len(dead)}
        out["allocator_compass_bridge"] = audit.get(
            "allocator_compass_bridge")
        out["u9_recount"] = audit.get("u9_orphans_fresh_recount")
        rep.kv(**out["counts"])
        (AWS_DIR / "ops" / "reports" / "2986.json").write_text(
            json.dumps(out, indent=1))


main()
sys.exit(0)
