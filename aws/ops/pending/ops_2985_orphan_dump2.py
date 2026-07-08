#!/usr/bin/env python3
"""ops 2985 -- U9 phase A retry: self-describing dump. Writes raw
structure samples of fleet-audit.json and engine-wiring.json into the
report VERBATIM (truncated), then extracts orphan tables with adaptive
strategies. Never exits nonzero on extraction shortfall -- the samples
themselves are the deliverable that guarantees convergence.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    out = {"ops": 2985, "fails": [], "warns": [],
           "ts": datetime.now(timezone.utc).isoformat()}
    with report("2985_orphan_dump2") as rep:
        audit = s3_json("data/fleet-audit.json")
        out["audit_top_keys"] = {
            k: (type(v).__name__ + (":%d" % len(v)
                if isinstance(v, (list, dict)) else ""))
            for k, v in audit.items()}
        for k, v in audit.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                out["sample__" + k] = json.dumps(v[0])[:900]
            if isinstance(v, dict) and v:
                fk = next(iter(v))
                out["sample__" + k] = (
                    fk + " => " + json.dumps(v[fk])[:800])

        try:
            wd = s3_json("data/engine-wiring.json")
            out["wiring_top_keys"] = {
                k: (type(v).__name__ + (":%d" % len(v)
                    if isinstance(v, (list, dict)) else ""))
                for k, v in wd.items()}
            for k, v in wd.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    out["wsample__" + k] = json.dumps(v[0])[:800]
                    break
        except Exception as e:
            out["warns"].append("wiring: %s" % str(e)[:80])
            wd = {}

        # adaptive extraction
        fresh, stale, dead = [], [], []

        def push(bucket, nm, fam, outs, age):
            if isinstance(outs, str):
                outs = [outs]
            bucket.append({"engine": nm, "family": fam or "?",
                           "outs": (outs or [])[:6], "age_h": age})

        def scan_rows(rows):
            for r in rows:
                nm = r.get("engine") or r.get("name") or r.get("fn")
                if not nm:
                    continue
                blob = json.dumps(r).upper()
                fam = (r.get("family") or r.get("fam")
                       or r.get("category") or r.get("class"))
                outs = (r.get("outs") or r.get("out_keys")
                        or r.get("keys") or r.get("feeds"))
                age = (r.get("age_h") or r.get("freshest_age_h")
                       or r.get("min_age_h") or r.get("age"))
                if "ORPHAN_FRESH" in blob or "ORPHAN-FRESH" in blob:
                    push(fresh, nm, fam, outs, age)
                elif "ORPHAN_STALE" in blob or "ORPHAN-STALE" in blob:
                    push(stale, nm, fam, outs, age)
                elif "ORPHAN_DEAD" in blob or "ORPHAN-DEAD" in blob \
                        or ("ORPHAN" in blob and "DEAD" in blob):
                    push(dead, nm, fam, outs, age)

        for k, v in audit.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                scan_rows(v)
            if isinstance(v, dict):
                rows = [dict(vv, engine=kk) if isinstance(vv, dict)
                        else {"engine": kk} for kk, vv in v.items()]
                scan_rows(rows)
        if not fresh and isinstance(wd, dict):
            for k, v in wd.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    scan_rows(v)

        out["orphan_fresh"], out["orphan_stale"], out["orphan_dead"] = \
            fresh, stale, dead
        out["counts"] = {"fresh": len(fresh), "stale": len(stale),
                         "dead": len(dead)}
        rep.kv(**out["counts"])
        out["verdict"] = "PASS"
        rp = AWS_DIR / "ops" / "reports" / "2985.json"
        rp.write_text(json.dumps(out, indent=1))
        rep.log("dumped; fresh=%d" % len(fresh))


main()
sys.exit(0)
