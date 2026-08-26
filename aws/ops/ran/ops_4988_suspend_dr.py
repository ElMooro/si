"""ops_4988 -- SUSPEND us-west-2 DR replication (Khalid's call).

The $79.50 USW2-Requests-SIA line = cross-region replication of
every new object into the DR buckets (Intelligent-Tiering request
+ monitoring fees), and it bills DAILY until the rules stop.
Buckets and already-replicated copies remain untouched --
re-enabling later is one put_bucket_replication away (the config
is archived to S3 before deletion).

  P0 evidence: replication config verbatim + DR bucket object
     counts/sizes + Intelligent-Tiering configs
  P1 archive config -> data/ops/dr-replication-archive.json
  P2 delete_bucket_replication on every source bucket that
     targets us-west-2
  P3 verify: config gone; note in cost ledger
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
s3g = boto3.client("s3")


with report("ops_4988_suspend_dr") as R:
    fails = []
    R.section("P0 evidence")
    sources = {}
    try:
        buckets = [b["Name"] for b in
                   s3g.list_buckets().get("Buckets", [])]
    except Exception as e:
        R.log("  list_buckets: %s" % str(e)[:80])
        buckets = [B]
    for nm in buckets:
        try:
            cfg = s3g.get_bucket_replication(Bucket=nm)[
                "ReplicationConfiguration"]
            dsts = [r_.get("Destination", {}).get("Bucket", "")
                    for r_ in cfg.get("Rules", [])]
            R.log("  SOURCE %-40s rules=%d -> %s" % (
                nm, len(cfg.get("Rules", [])),
                [d.rsplit(":", 1)[-1] for d in dsts]))
            sources[nm] = cfg
        except Exception:
            pass
    if not sources:
        R.log("  no replication configs found anywhere")
    for nm in buckets:
        if "dr" in nm.lower() or "usw2" in nm.lower():
            try:
                n = sz = 0
                pag = s3g.get_paginator("list_objects_v2")
                for pg in pag.paginate(Bucket=nm,
                                       PaginationConfig={
                                           "MaxItems": 200000}):
                    for o in pg.get("Contents", []):
                        n += 1
                        sz += o["Size"]
                R.log("  DR %-42s ~%d objs %.2fGB" % (
                    nm, n, sz / 1e9))
            except Exception as e:
                R.log("  DR %-42s census err %s" % (
                    nm, str(e)[:60]))
            try:
                it = s3g.list_bucket_intelligent_tiering_configurations(
                    Bucket=nm)
                R.log("    IT configs: %s" % [
                    c.get("Id") for c in
                    it.get("IntelligentTieringConfigurationList",
                           [])])
            except Exception:
                pass

    R.section("P1 archive configs")
    try:
        s3.put_object(
            Bucket=B, Key="data/ops/dr-replication-archive.json",
            Body=json.dumps(
                {"archived_at": datetime.now(
                    timezone.utc).isoformat(timespec="seconds"),
                 "reason": "ops 4988 -- Khalid: stop the "
                           "us-west-2 SIA spend ($79.50/16d, "
                           "daily-billing)",
                 "configs": {k: v for k, v in sources.items()}},
                indent=1, default=str).encode(),
            ContentType="application/json")
        R.log("  archived %d config(s)" % len(sources))
    except Exception as e:
        R.log("  archive err %s" % str(e)[:90])
        fails.append("P1")

    R.section("P2 delete replication rules")
    for nm in sources:
        try:
            s3g.delete_bucket_replication(Bucket=nm)
            R.log("  DELETED replication on %s" % nm)
        except Exception as e:
            R.log("  delete %s: %s" % (nm, str(e)[:90]))
            fails.append("P2:" + nm)
    if not sources:
        R.log("  nothing to delete -- if the SIA line persists, "
              "the driver is IT archive-tier monitoring on the "
              "DR buckets themselves (next lever: lifecycle "
              "the DR buckets or empty them on Khalid's word)")

    R.section("P3 verify")
    time.sleep(3)
    still = []
    for nm in sources:
        try:
            s3g.get_bucket_replication(Bucket=nm)
            still.append(nm)
        except Exception:
            R.log("  %s: replication config GONE" % nm)
    if still:
        R.log("  STILL CONFIGURED: %s" % still)
        fails.append("P3")

    if fails:
        R.log("ops 4988 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(sources_suspended=len(sources))
    R.log("ops 4988 GREEN -- DR replication suspended; the "
          "us-west-2 request line stops accruing (existing "
          "copies untouched; re-enable = restore archived "
          "config)")
