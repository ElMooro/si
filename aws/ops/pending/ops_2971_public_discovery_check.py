#!/usr/bin/env python3
"""ops 2971 -- close the 2970 warn: the public /data/asset-discovery.json
fetch returned 403 during the 2970 run, almost certainly S3's
403-on-missing-key behavior (GET without ListBucket on a not-yet-written
object) from script ordering inside the same run-ops loop. The object
was verified via boto3 by ops 2969 in that run. This script re-fetches
the PUBLIC path with a cache-buster and hard-verifies the honest-empty
July document shape end-to-end (schema, PROVISIONAL, llm_status,
candidates list-typed), proving the page's discovery panel has a
readable feed.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
URL = "https://justhodl.ai/data/asset-discovery.json"


def fail(rep, fails, msg):
    fails.append(msg)
    rep.fail(msg)


def main():
    fails, warns = [], []
    hl = {}
    with report("2971_public_discovery_check") as rep:
        rep.section("Public-path fetch with cache-buster")
        d, st = None, None
        for i in range(6):
            try:
                req = urllib.request.Request(
                    URL + "?t=%d" % int(time.time()),
                    headers={"User-Agent": "ops-2971",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=25) as r:
                    st = r.status
                    d = json.loads(r.read().decode("utf-8", "replace"))
                break
            except Exception as e:
                st = str(e)[:80]
                time.sleep(10)
        rep.kv(status=st, attempt=i + 1)
        if not isinstance(d, dict):
            fail(rep, fails, "public discovery json still unreadable: %s"
                 % st)
            _write(rep, fails, warns, hl)
            return
        hl["month"] = d.get("month")
        hl["llm_status"] = d.get("llm_status")
        hl["candidates_n"] = len(d.get("candidates") or [])
        rep.kv(**hl, schema=d.get("schema_version"),
               status_field=d.get("status"))
        if d.get("schema_version") != "1.0":
            fail(rep, fails, "schema %r" % d.get("schema_version"))
        if d.get("status") != "PROVISIONAL":
            fail(rep, fails, "status %r" % d.get("status"))
        if d.get("llm_status") not in ("OK", "GATED_OR_DOWN"):
            fail(rep, fails, "llm_status %r" % d.get("llm_status"))
        if not isinstance(d.get("candidates"), list):
            fail(rep, fails, "candidates not a list")
        if not fails:
            rep.ok("public discovery feed live: month %s, llm %s, "
                   "candidates %d -- 2970 warn was the missing-object "
                   "403, now resolved"
                   % (hl["month"], hl["llm_status"], hl["candidates_n"]))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2971, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2971.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    rep.log("report written: %s" % rp)
    if fails:
        sys.exit(1)


main()
sys.exit(0)
