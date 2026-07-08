#!/usr/bin/env python3
"""ops 2991 -- bridge certainty: read data/master-allocation.json (the
REAL allocator out key; 2990 short-circuited on a stale data/allocator
doc) and assert compass_bridge used:true with nonempty tilts_pp, gate
flag coherent with spy_tlt corr, and tilt caps respected. Also dump the
full sifma module verbatim for the record.
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
    fails, warns = [], []
    out = {"ops": 2991, "ts": datetime.now(timezone.utc).isoformat()}
    with report("2991_bridge_certainty") as rep:
        doc = s3_json("data/master-allocation.json")
        br = doc.get("compass_bridge") or {}
        out["bridge"] = br
        rep.kv(bridge=json.dumps(br)[:400])
        if not br.get("used"):
            fails.append("compass_bridge.used != true on "
                         "master-allocation.json: %s"
                         % json.dumps(br)[:200])
        tilts = br.get("tilts_pp") or {}
        if br.get("used") and not tilts:
            fails.append("used but tilts_pp empty")
        bad = {k: v for k, v in tilts.items()
               if not isinstance(v, (int, float)) or abs(v) > 2.01}
        if bad:
            fails.append("per-sleeve cap breached: %s"
                         % json.dumps(bad))
        tot = sum(abs(v) for v in tilts.values()
                  if isinstance(v, (int, float)))
        out["abs_tilt_sum_pp"] = round(tot, 2)
        if tot > 6.01:
            fails.append("layer cap breached: %.2f" % tot)
        corr = br.get("spy_tlt_corr_90d")
        gated = br.get("duration_hedge_gated")
        if corr is not None and corr > 0.30 and gated is not True:
            fails.append("corr %.2f > 0.30 but gate not applied" % corr)

        sif = (s3_json("data/gap-metrics.json").get("modules")
               or {}).get("sifma")
        out["sifma_full"] = sif
        rep.kv(sifma=json.dumps(sif)[:300])

        out["fails"], out["warns"] = fails, warns
        out["verdict"] = "PASS" if not fails else "FAIL"
        (AWS_DIR / "ops" / "reports" / "2991.json").write_text(
            json.dumps(out, indent=1))
        rep.log("FAILS=%d" % len(fails))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
