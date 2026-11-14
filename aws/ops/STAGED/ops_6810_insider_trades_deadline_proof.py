"""ops 6810 - justhodl-insider-trades wall-clock deadline proof (daily health run 2026-11-14; verify-only, the deploy is
deploy-lambdas.yml's).

Finding (ops 6809): every 21:10 UTC run since Nov 12 died `Status: timeout` at 300 s (3 invocations / 3 errors a day,
data/insider-trades.json 62 h old at the Nov 14 read): the atom phase ended at ~60 s with fetch_errors=59 of 100, then
backfill_from_daily_index fetched up to 350 EDGAR submissions one by one, 15 s socket timeout each, no wall clock - the
AUTONOMY.md 'slow-drip livelock' class. The engine commit gives both fetch phases a deadline derived from the function's
real remaining time (soft_deadline = started + min(840, remaining - 75); the atom pool gets 60 % of it, the backfill the
rest, the backfill cursor keeps the day's progress) so the merge, aggregate and the two S3 writes always run.

This op climbs the three-rung ladder (repo has the marker, artifact has it, function is READY), invokes the engine once
as an Event (its own scheduled path: no payload), waits for data/insider-trades.json to be rewritten, and reads the
run's own stats (fetch_duration_s, fetch_errors, atom_phase_cut, stopped_at_deadline, backfill coverage) and its REPORT
line (Duration, no `Status: timeout`). Real data on the engine's own path; nothing else is changed. Gate: the output
was rewritten by this invocation and the REPORT line carries no timeout.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import re
import sys
import time
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
FN = "justhodl-insider-trades"
LIVE = "justhodl-dashboard-live"
OUT_KEY = "data/insider-trades.json"
FULL_KEY = "data/insider-trades-full.json"
CURSOR_KEY = "data/_insider/backfill-cursor.json"
MARKER = "def soft_deadline(started: float, context"   # the deadline commit's new function
SECRET_KV = re.compile(r"(?i)(apiKey|api[_-]?key|token|secret|user[_-]?agent)(\s*[=:]\s*)[^\s&\"',}]+")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def redact(line):
    line = SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", str(line))
    return EMAIL.sub("***@***", line)


def report_line_ok(msg):
    """The invocation's REPORT line proves the run ended inside its wall: no 'Status: timeout'. Pure, for the selftest."""
    return msg.startswith("REPORT RequestId") and "Status: timeout" not in msg


def selftest():
    assert report_line_ok("REPORT RequestId: x Duration: 120000.00 ms Billed Duration: 120001 ms Memory Size: 512 MB")
    assert not report_line_ok("REPORT RequestId: x Duration: 300000.00 ms Billed Duration: 300351 ms Status: timeout")
    assert not report_line_ok("END RequestId: x")
    assert redact("contact someone@example.com here") == "contact ***@*** here"
    assert redact("apikey=abc rest") == "apikey=*** rest"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6810_insider_trades_deadline_proof") as r:
        r.heading("ops 6810 - justhodl-insider-trades wall-clock deadline proof %s (ladder, then one scheduled-path Event invocation)" % now.date())

        r.section("1. the three-rung ladder")
        src = (ROOT / "aws/lambdas/justhodl-insider-trades/source/lambda_function.py").read_text(encoding="utf-8")
        if MARKER not in src:
            r.fail("rung 1: the repo source does not carry the marker - wrong checkout")
            sys.exit(1)
        r.log("rung 1: repo has the marker")
        ready = False
        conf = {}
        for attempt in range(40):   # deploy-lambdas.yml runs on the engine push; allow ~10 min
            conf = lam.get_function_configuration(FunctionName=FN)
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    deployed = z.read("lambda_function.py").decode("utf-8", "replace")
                has_marker = MARKER in deployed
            except Exception as exc:  # noqa: BLE001
                has_marker = False
                r.log("  artifact read: %s" % type(exc).__name__)
            state_ok = conf.get("State") == "Active" and conf.get("LastUpdateStatus") == "Successful"
            if has_marker and state_ok:
                ready = True
                r.log("rung 2+3 at attempt %d: artifact has the marker, State %s / LastUpdateStatus %s, LastModified %s, timeout %s s, memory %s MB" % (
                    attempt + 1, conf.get("State"), conf.get("LastUpdateStatus"), conf.get("LastModified"), conf.get("Timeout"), conf.get("MemorySize")))
                break
            r.log("  attempt %d: marker %s, State %s / %s - waiting 15 s" % (attempt + 1, has_marker, conf.get("State"), conf.get("LastUpdateStatus")))
            time.sleep(15)
        if not ready:
            r.fail("the deployed artifact never carried the marker (or never settled) within 10 min - deploy-lambdas.yml for the engine commit did not land")
            sys.exit(1)
        time.sleep(5)
        facts["deployed_last_modified"] = conf.get("LastModified")
        facts["timeout_s"] = conf.get("Timeout")

        r.section("2. the scheduled path, once (Event invocation; result read back from S3 and the log)")
        try:
            before = s3.head_object(Bucket=LIVE, Key=OUT_KEY)["LastModified"]
            r.log("before: %s LastModified %s (%.1f h old)" % (OUT_KEY, before.isoformat(), (now - before).total_seconds() / 3600))
            facts["output_age_before_h"] = round((now - before).total_seconds() / 3600, 1)
        except Exception as exc:  # noqa: BLE001
            before = None
            r.warn("before: %s unreadable: %s" % (OUT_KEY, type(exc).__name__))
        try:
            cur = json.loads(s3.get_object(Bucket=LIVE, Key=CURSOR_KEY)["Body"].read() or b"{}")
            done = cur.get("done") or {}
            r.log("backfill cursor before: %d days tracked, %d complete, updated %s" % (len(done), sum(1 for v in done.values() if v.get("complete")), cur.get("updated")))
        except Exception as exc:  # noqa: BLE001
            r.log("backfill cursor before: unreadable (%s)" % type(exc).__name__)
        t0 = datetime.now(timezone.utc)
        resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("invoked at %s: HTTP %s" % (t0.isoformat(timespec="seconds"), resp.get("StatusCode")))
        if resp.get("StatusCode") not in (202,):
            r.fail("Event invoke not accepted: %s" % resp.get("StatusCode"))
            sys.exit(1)
        after = None
        for _ in range(42):   # the wall is 300 s; the deadline leaves 75 s for the writes
            time.sleep(10)
            try:
                h = s3.head_object(Bucket=LIVE, Key=OUT_KEY)
                if before is None or h["LastModified"] > t0:
                    after = h["LastModified"]
                    break
            except Exception as exc:  # noqa: BLE001
                r.log("  head: %s" % type(exc).__name__)
        if after is None:
            r.fail("the engine did not rewrite %s within 7 min of the invoke (the wall is 300 s) - the run still dies before the write" % OUT_KEY)
            r.kv(**{k: str(v) for k, v in facts.items()})
            sys.exit(1)
        r.log("after: %s LastModified %s (%.0f s after the invoke)" % (OUT_KEY, after.isoformat(), (after - t0).total_seconds()))
        facts["written_after_s"] = round((after - t0).total_seconds())
        doc = json.loads(s3.get_object(Bucket=LIVE, Key=OUT_KEY)["Body"].read() or b"{}")
        stats = doc.get("stats") or {}
        cov = doc.get("coverage") or {}
        diag = stats.get("diagnostics") or {}
        r.log("stats: generated_at %s version %s | fetch_duration_s %s fetch_errors %s atom_feed_count %s | diagnostics %s" % (
            doc.get("generated_at"), doc.get("version"), stats.get("fetch_duration_s"), stats.get("fetch_errors"), stats.get("atom_feed_count"),
            redact(json.dumps(diag, sort_keys=True))[:500]))
        r.log("fetch errlog (per error type): %s" % redact(json.dumps(stats.get("fetch_errlog") or {}, sort_keys=True))[:400])
        r.log("coverage: %s" % redact(json.dumps({k: v for k, v in cov.items() if k in (
            "filings_scanned_this_run", "backfill_days_complete", "backfill_days_target", "stopped_at_deadline", "sector_mapped", "universe_size",
            "total_leaves", "backfill_error")}, sort_keys=True))[:500])
        r.log("counts: buys %d | sells %d | clusters %s | big_buys %s | total_buys %s | fleet ledger %s" % (
            len([t for t in doc.get("transactions") or [] if isinstance(t, dict) and "ticker" in t]),
            len([t for t in doc.get("sell_transactions") or [] if isinstance(t, dict) and "ticker" in t]),
            stats.get("cluster_count"), stats.get("big_buy_count"), stats.get("total_buys"),
            json.dumps((doc.get("fleet") or {}).get("ledger"), default=str)[:300]))
        facts.update(fetch_duration_s=stats.get("fetch_duration_s"), fetch_errors=stats.get("fetch_errors"), atom_phase_cut=diag.get("atom_phase_cut"),
                     stopped_at_deadline=cov.get("stopped_at_deadline"), filings_scanned=cov.get("filings_scanned_this_run"),
                     backfill_days_complete=cov.get("backfill_days_complete"), cluster_count=stats.get("cluster_count"), total_buys=stats.get("total_buys"))
        if doc.get("v3_error"):
            r.warn("v3 surface error carried in the output: %s" % redact(str(doc.get("v3_error")))[:200])
        try:
            full = s3.head_object(Bucket=LIVE, Key=FULL_KEY)
            r.log("%s LastModified %s (%s)" % (FULL_KEY, full["LastModified"].isoformat(), "rewritten" if full["LastModified"] > t0 else "NOT rewritten"))
        except Exception as exc:  # noqa: BLE001
            r.log("%s: %s" % (FULL_KEY, type(exc).__name__))

        time.sleep(15)   # CloudWatch Logs delivery lag
        rep, lines = None, []
        for _ in range(6):
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime", descending=True, limit=3).get("logStreams", [])
            for st in streams:
                evs = logs.get_log_events(logGroupName="/aws/lambda/" + FN, logStreamName=st["logStreamName"], startTime=int(t0.timestamp() * 1000),
                                          limit=500, startFromHead=True).get("events", [])
                for e in evs:
                    m = " ".join(e["message"].split())
                    if m.startswith("REPORT RequestId"):
                        rep = m
                    elif any(s in m for s in ("fetch deadline", "atom phase cut", "Parsed", "backfill:", "Wrote", "Atom feed", "FATAL", "Task timed out")):
                        lines.append(m)
                if rep:
                    break
            if rep:
                break
            time.sleep(15)
        for m in lines[:12]:
            r.log("  log: %s" % redact(m)[:300])
        if not rep:
            r.warn("no REPORT line for this invocation in the three newest log streams yet (the output itself was rewritten - the write is the proof)")
            facts["report_line"] = "not found"
        else:
            r.log("  %s" % rep[:300])
            dur = re.search(r"Duration: ([\d.]+) ms", rep)
            facts["duration_s"] = round(float(dur.group(1)) / 1000, 1) if dur else None
            facts["report_line"] = "ok" if report_line_ok(rep) else "TIMEOUT"
            if not report_line_ok(rep):
                r.fail("the invocation still timed out: %s" % rep[:200])
                r.kv(**{k: str(v) for k, v in facts.items()})
                sys.exit(1)
        r.ok("%s rewritten %d s after the invoke; fetch %s s, fetch_errors %s, atom cut %s, backfill stopped at deadline %s (%s filings scanned, %s/%s days complete); REPORT %s" % (
            OUT_KEY, facts["written_after_s"], stats.get("fetch_duration_s"), stats.get("fetch_errors"), diag.get("atom_phase_cut"),
            cov.get("stopped_at_deadline"), cov.get("filings_scanned_this_run"), cov.get("backfill_days_complete"), cov.get("backfill_days_target"), facts["report_line"]))
        r.kv(**{k: str(v) for k, v in facts.items()})
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
