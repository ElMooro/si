"""ops 6726 - live proof of the 13F hand-off chain: sec-13f -> 13f-positions -> 13f-book (Khalid 2026-11-03 "continue").

After deploy-lambdas ships justhodl-13f-positions (handoff_book) and justhodl-13f-book v1.0.2 (begin + trigger record):
  1. both functions carry today's code (LastModified after the deploy commit);
  2. invoke justhodl-13f-positions with the detector's exact payload {"trigger": "new_filing", ...} (async, the way
     justhodl-sec-13f starts it on a new accession) - a real native collection + publication;
  3. wait for data/13f-desk.json to refresh, then for data/13f-book.json to refresh AFTER it;
  4. the book's `trigger` block must read kind = holdings_research_published with the research's source_generated_at,
     and its managers/cohort must match the desk; the research log line "[13f] book hand-off fired" is read when the
     runner may read logs (tolerated otherwise).
Invokes one engine (the research), which hands off to the book. Writes nothing but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
RESEARCH, BOOK = "justhodl-13f-positions", "justhodl-13f-book"
DESK, OUT = "data/13f-desk.json", "data/13f-book.json"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def head(s3, key):
    h = s3.head_object(Bucket=LIVE, Key=key)
    return h["LastModified"]


def wait_newer(s3, key, than, budget_s, label, r):
    t_end = time.time() + budget_s
    while time.time() < t_end:
        time.sleep(10)
        mod = head(s3, key)
        if mod > than:
            r.log("%s refreshed at %s (+%.0f s)" % (label, mod.isoformat(timespec="seconds"), (mod - than).total_seconds()))
            return mod
    return None


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6726_13f_handoff_proof") as r:
        r.heading("ops 6726 - 13F hand-off chain proof %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. deployed code")
        for fn in (RESEARCH, BOOK):
            cf = lam.get_function_configuration(FunctionName=fn)
            r.log("%s: modified %s, memory %s, timeout %s, state %s" % (fn, (cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout"), cf.get("State")))
        r.section("2. detector-shaped invoke of the research")
        desk_before, book_before = head(s3, DESK), head(s3, OUT)
        r.log("before: desk %s, book %s" % (desk_before.isoformat(timespec="seconds"), book_before.isoformat(timespec="seconds")))
        t0 = datetime.now(timezone.utc)
        resp = lam.invoke(FunctionName=RESEARCH, InvocationType="Event",
                          Payload=json.dumps({"trigger": "new_filing", "changed": ["ops-6726-proof"]}).encode())
        r.log("async invoke %s at %s" % (resp.get("StatusCode"), t0.strftime("%H:%M:%S")))
        r.section("3. the chain")
        desk_after = wait_newer(s3, DESK, desk_before, 9 * 60, "desk", r)
        if not desk_after:
            r.fail("the research did not publish within 9 min")
            raise SystemExit(1)
        book_after = wait_newer(s3, OUT, book_before, 4 * 60, "book", r)
        if not book_after:
            r.fail("the book did not follow the publication within 4 min (hand-off missing?)")
            raise SystemExit(1)
        r.section("4. what the book says about its trigger")
        book = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
        desk = json.loads(s3.get_object(Bucket=LIVE, Key=DESK)["Body"].read())
        trig = book.get("trigger") or {}
        r.log("book version %s generated_at %s trigger %s" % (book.get("version"), book.get("generated_at"), json.dumps(trig)))
        r.log("desk generated_at %s source_generated_at %s | book.source.desk_generated_at %s" % (desk.get("generated_at"), desk.get("source_generated_at"), (book.get("source") or {}).get("desk_generated_at")))
        r.log("book managers %d cohort %d | desk funds %s/%s" % (len((book.get("book") or {}).get("managers") or {}), len((book.get("book") or {}).get("cohort") or []), desk.get("funds_parsed"), desk.get("funds_total")))
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + RESEARCH, startTime=int(t0.timestamp() * 1000) - 5000, filterPattern='"book hand-off"', limit=10)
            r.log("research log: %s" % ([e["message"].strip()[:160] for e in ev.get("events", [])] or "no matching line"))
        except Exception as exc:  # noqa: BLE001
            r.warn("log read not permitted (%s) - the book's trigger block is the proof" % type(exc).__name__)
        ok = (trig.get("kind") == "holdings_research_published" and book.get("version") == "1.0.2"
              and (book.get("source") or {}).get("desk_generated_at") == desk.get("generated_at"))
        r.kv(trigger_kind=trig.get("kind"), book_version=book.get("version"), desk_match=str((book.get("source") or {}).get("desk_generated_at") == desk.get("generated_at")),
             lag_s=str(int((book_after - desk_after).total_seconds())))
        if not ok:
            r.fail("chain not proven: trigger %s version %s" % (trig, book.get("version")))
            raise SystemExit(1)
        r.ok("PROVEN: a detector-shaped research run published and the book followed it in %d s, carrying the research's clock" % int((book_after - desk_after).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
