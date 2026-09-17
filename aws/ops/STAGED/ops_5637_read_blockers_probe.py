"""ops 5637 -- why the owned read makes no dated calls (Claude, 2026-09-17). READ-ONLY, direct lane.

ops 5622 (attempt 3) settled the first owned-voice read (stances, 3 opportunities) but calls=0: decision_status
ADVISORY_ONLY with 4 release blockers, so the engine mutes dated calls and nothing reaches the ledger to be graded.
This prints the blockers, each board source's freshness, and the fleet registry summary, so the next fix is exact.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

PRI, PUB = "justhodl-ai-857687956942", "justhodl-dashboard-live"


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5637_read_blockers_probe") as r:
        r.heading("ops 5637 -- the owned read's release blockers")
        doc = json.loads(s3.get_object(Bucket=PRI, Key="ai/market-read/latest.json")["Body"].read())
        rd, board = doc.get("read") or {}, doc.get("board") or {}
        r.section("1. Read state")
        r.kv(read_id=doc.get("read_id"), voice=rd.get("voice"), decision_status=rd.get("decision_status"), calls=len(rd.get("calls") or []),
             coercions=json.dumps((rd.get("owned_voice") or {}).get("coercions"))[:300], repaired=(rd.get("owned_voice") or {}).get("repaired"))
        r.section("2. Release blockers (why calls are muted)")
        for b in rd.get("release_blockers") or []:
            r.warn(str(b)[:300])
        r.section("3. Board sources")
        for name, src in sorted((board.get("sources") or {}).items()):
            r.log("%-22s %-8s age_h=%-7s %s" % (name, src.get("status"), src.get("age_h"), str(src.get("key") or src.get("note") or "")[:80]))
        r.section("4. Fleet registry summary (fleet_inputs)")
        fc = board.get("fleet_coverage") or {}
        r.kv(status=fc.get("status"), summary=json.dumps(fc.get("summary"), default=str)[:600])
        ai = json.loads(s3.get_object(Bucket=PUB, Key="data/ai.json")["Body"].read())
        fi = ai.get("fleet_inputs") or {}
        r.kv(fleet_inputs_status=fi.get("status"), fleet_summary=json.dumps(fi.get("summary"), default=str)[:600],
             release_blockers=json.dumps(fi.get("release_blockers"), default=str)[:800])
        notready = [x for x in (fi.get("feeds") or fi.get("inputs") or []) if isinstance(x, dict) and str(x.get("status") or "").upper() not in ("FRESH", "OK", "READY")][:25]
        for x in notready:
            r.log("   %s" % json.dumps({k: x.get(k) for k in ("id", "name", "key", "status", "age_h", "reason", "engine") if k in x}, default=str)[:200])
        r.ok("probe complete")


if __name__ == "__main__":
    main()
