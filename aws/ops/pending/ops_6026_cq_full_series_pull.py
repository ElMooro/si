#!/usr/bin/env python3
"""ops_6026 -- fire justhodl-cryptoquant (already deployed v2.1, 410-metric
spec) so the 1y Professional series bank lands in S3.

Pages already fuse 303 cq-feed live prints as searchable indicators.
This pull is what makes armed names CQ: chartable. Does not create a
new lambda. Does not invent pre-harvest history. Event-invoke (lambda
timeout 900s) + poll data/cryptoquant-series.json until n>=200.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-cryptoquant"
SERIES_KEY = "data/cryptoquant-series.json"
SPEC_KEY = "data/config/cryptoquant-spec.json"
ONCHAIN_KEY = "data/cryptoquant-onchain.json"

lam = boto3.client(
    "lambda", region_name=REGION,
    config=Config(read_timeout=920, retries={"max_attempts": 0}),
)
s3 = boto3.client("s3", region_name=REGION)


def _j(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_err": str(e)[:160]}


fails = []
with report("6026_cq_full_series_pull") as r:
    r.heading("ops 6026 -- justhodl-cryptoquant full Professional series pull")
    start = datetime.now(timezone.utc)
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
    before = _j(SERIES_KEY)
    n_before = len((before.get("series") or {}))
    gen_before = str(before.get("generated_at") or "")
    cfg = lam.get_function_configuration(FunctionName=FN)
    r.kv(
        fn=FN,
        version=cfg.get("Version"),
        timeout=cfg.get("Timeout"),
        memory=cfg.get("MemorySize"),
        last_mod=cfg.get("LastModified"),
        desc=(cfg.get("Description") or "")[:80],
        n_series_before=n_before,
        generated_at_before=gen_before,
        start_iso=start_iso,
    )
    if (cfg.get("Timeout") or 0) < 600:
        r.warn("timeout %s < 600s -- long-tail harvest may truncate"
               % cfg.get("Timeout"))
    r.section("1. Event invoke")
    try:
        resp = lam.invoke(
            FunctionName=FN, InvocationType="Event", Payload=b"{}",
        )
        r.kv(
            status=resp.get("StatusCode"),
            request_id=(resp.get("ResponseMetadata") or {}).get("RequestId"),
        )
        if resp.get("StatusCode") not in (202, 200):
            fails.append("invoke status %s" % resp.get("StatusCode"))
        else:
            r.ok("Event invoke accepted")
    except Exception as e:
        fails.append("invoke: %s" % e)
        r.fail(str(e))

    r.section("2. Poll series bank (n>=200, generated_at after start)")
    landed = None
    deadline = time.time() + 18 * 60
    while time.time() < deadline and not fails:
        time.sleep(20)
        doc = _j(SERIES_KEY)
        n = len((doc.get("series") or {}))
        gen = str(doc.get("generated_at") or "")
        r.log("n=%d generated_at=%s" % (n, gen))
        if n >= 200 and gen and (gen >= start_iso or gen > gen_before):
            landed = doc
            break

    if not landed:
        after = _j(SERIES_KEY)
        fails.append(
            "series bank still n=%d generated_at=%s (want n>=200 after %s)"
            % (len((after.get("series") or {})),
               after.get("generated_at"), start_iso)
        )
    else:
        spec = _j(SPEC_KEY)
        onch = _j(ONCHAIN_KEY)
        n = len((landed.get("series") or {}))
        n_spec = len((spec.get("metrics") or []))
        n_on = len((onch.get("metrics") or {}))
        r.ok("series n=%d spec=%d onchain=%d generated_at=%s"
             % (n, n_spec, n_on, landed.get("generated_at")))
        sample = sorted((landed.get("series") or {}))[:16]
        r.log("sample: " + ", ".join(sample))
        if n_spec < 200:
            r.warn("S3 spec still %d metrics (bundled 410 should land on write)"
                   % n_spec)
        if "btc_mvrv" not in (landed.get("series") or {}):
            fails.append("core btc_mvrv missing after expand")
        twins = landed.get("twins") or {}
        r.kv(n_twins=len(twins), n_series=n, n_spec=n_spec, n_onchain=n_on)

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail(f)
    else:
        r.ok("OPS 6026 PASS -- Professional series bank live (n>=200)")
if fails:
    sys.exit(1)
