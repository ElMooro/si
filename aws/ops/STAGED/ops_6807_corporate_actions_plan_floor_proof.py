"""ops 6807 - justhodl-corporate-actions plan-history-floor proof (daily health run 2026-11-13; verify-only, the deploy is
deploy-lambdas.yml's).

Commit 5e60545b: the engine learns the Polygon plan's history floor once per invocation (the first 403 for a session at
least a year old) and answers every session at or older than it without a call, for every ticker of the run; a ticker's
dividends are resolved newest-first so its own first 403 floors the rest. Yesterday's and today's 06:30 UTC sweeps each
processed ~24 of ~180 due tickers inside the 250 s budget, one 403 per pre-2021 dividend (184 on Nov 13).

This op climbs the three-rung ladder (repo has it, artifact has it, function is READY), then invokes the engine's own
DAILY path once (no tickers: it resumes from the saved cursor exactly as the 06:30 UTC schedule would), as an Event
invocation, and reads the result back from S3 (_state.json / the index) and the invocation's '[corpact] done' line:
processed vs due, floored_403, plan_floor_403, how many 403 lines the run still printed. Real data on the engine's own
path; nothing else is changed. Gate: processed >= 80% of due, or the window finished (stopped_early false).
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
FN = "justhodl-corporate-actions"
LIVE = "justhodl-dashboard-live"
STATE_KEY = "data/corporate-actions/_state.json"
INDEX_KEY = "data/corporate-actions-index.json"
MARKER = 'stats["floored_403"] = _FLOORED_N'   # 5e60545b, last-edited region of the file
SECRET_KV = re.compile(r"(?i)(apiKey|api[_-]?key|token|secret)(\s*[=:]\s*)[^\s&\"',}]+")


def redact(line):
    return SECRET_KV.sub(lambda m: m.group(1) + m.group(2) + "***", str(line))


def window_cleared(due, processed, stopped_early):
    """The daily window counts as cleared when at least 80% of the due tickers were processed or the run finished its
    window on its own. Pure, for the selftest."""
    due, processed = int(due or 0), int(processed or 0)
    if not stopped_early:
        return True
    return due > 0 and processed >= 0.8 * due


def selftest():
    assert redact("GET x?ticker=AAPL&apiKey=abc&limit=5") == "GET x?ticker=AAPL&apiKey=***&limit=5"
    assert window_cleared(179, 24, True) is False
    assert window_cleared(179, 150, True) is True
    assert window_cleared(179, 60, False) is True
    assert window_cleared(0, 0, True) is False
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
    with report("ops_6807_corporate_actions_plan_floor_proof") as r:
        r.heading("ops 6807 - justhodl-corporate-actions plan-history-floor proof %s (ladder, then one daily-path Event invocation from the saved cursor)" % now.date())

        r.section("1. the three-rung ladder")
        src = (ROOT / "aws/lambdas/justhodl-corporate-actions/source/lambda_function.py").read_text(encoding="utf-8")
        if MARKER not in src:
            r.fail("rung 1: the repo source does not carry the marker - wrong checkout")
            sys.exit(1)
        r.log("rung 1: repo has the marker")
        ready = False
        conf = {}
        for attempt in range(40):   # deploy-lambdas.yml runs on the same push; allow ~10 min
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
                r.log("rung 2+3 at attempt %d: artifact has the marker, State %s / LastUpdateStatus %s, LastModified %s" % (
                    attempt + 1, conf.get("State"), conf.get("LastUpdateStatus"), conf.get("LastModified")))
                break
            r.log("  attempt %d: marker %s, State %s / %s - waiting 15 s" % (attempt + 1, has_marker, conf.get("State"), conf.get("LastUpdateStatus")))
            time.sleep(15)
        if not ready:
            r.fail("the deployed artifact never carried the marker (or never settled) within 10 min - deploy-lambdas.yml for 5e60545b did not land")
            sys.exit(1)
        time.sleep(5)
        facts["deployed_last_modified"] = conf.get("LastModified")

        r.section("2. the daily path, once, from the saved cursor (Event invocation; result read back from S3 and the log)")
        before_state = json.loads(s3.get_object(Bucket=LIVE, Key=STATE_KEY)["Body"].read() or b"{}")
        before_idx = s3.head_object(Bucket=LIVE, Key=INDEX_KEY)["LastModified"]
        r.log("before: cursor %s (state updated_at %s); index LastModified %s" % (before_state.get("cursor"), before_state.get("updated_at"), before_idx.isoformat()))
        facts["cursor_before"] = before_state.get("cursor")
        t0 = datetime.now(timezone.utc)
        resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        r.log("invoked at %s: HTTP %s" % (t0.isoformat(timespec="seconds"), resp.get("StatusCode")))
        if resp.get("StatusCode") not in (202,):
            r.fail("Event invoke not accepted: %s" % resp.get("StatusCode"))
            sys.exit(1)
        after_state = None
        for _ in range(48):   # the engine's soft budget is 250 s inside a 300 s wall
            time.sleep(10)
            try:
                obj = s3.get_object(Bucket=LIVE, Key=STATE_KEY)
                if obj["LastModified"] > t0:
                    after_state = json.loads(obj["Body"].read() or b"{}")
                    break
            except Exception as exc:  # noqa: BLE001
                r.log("  state read: %s" % type(exc).__name__)
        if after_state is None:
            r.fail("the engine did not write %s within 8 min of the invoke (the wall is 300 s) - the run died before saving" % STATE_KEY)
            sys.exit(1)
        after_idx = s3.head_object(Bucket=LIVE, Key=INDEX_KEY)["LastModified"]
        r.log("after: cursor %s (state updated_at %s); index LastModified %s (%s)" % (
            after_state.get("cursor"), after_state.get("updated_at"), after_idx.isoformat(), "rewritten" if after_idx > before_idx else "NOT rewritten"))
        facts["cursor_after"] = after_state.get("cursor")
        if after_idx <= before_idx:
            r.warn("the index was not rewritten by this run")

        time.sleep(15)   # CloudWatch Logs delivery lag
        done, n403, floor_line = None, 0, None
        for _ in range(6):
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime", descending=True, limit=3).get("logStreams", [])
            for st in streams:
                evs = logs.get_log_events(logGroupName="/aws/lambda/" + FN, logStreamName=st["logStreamName"], startTime=int(t0.timestamp() * 1000),
                                          limit=2000, startFromHead=True).get("events", [])
                for e in evs:
                    m = e["message"]
                    if "[corpact] done:" in m:
                        done = m
                    elif "HTTPError 403" in m:
                        n403 += 1
                    elif "plan history floor learned" in m:
                        floor_line = m.strip()
                if done:
                    break
            if done:
                break
            time.sleep(15)
        if not done:
            r.fail("no '[corpact] done:' line for this invocation in the three newest log streams")
            sys.exit(1)
        st_ = json.loads(done.split("done:", 1)[1].strip())
        r.log("done: %s" % redact(json.dumps(st_, sort_keys=True)))
        r.log("403 lines printed by this run: %d (yesterday's sweep: 184) | %s" % (n403, redact(floor_line or "no floor line")))
        facts.update(due=st_.get("due"), processed=st_.get("processed"), written=st_.get("written"), failed=st_.get("failed"),
                     stopped_early=st_.get("stopped_early"), duration_s=st_.get("duration_s"), plan_floor_403=st_.get("plan_floor_403"),
                     floored_403=st_.get("floored_403"), http_403_lines=n403, factor_gaps=st_.get("factor_gaps"))
        if st_.get("plan_floor_403") is None and n403:
            r.warn("the run printed %d 403 lines but learned no floor - the 403s were for sessions younger than a year (not history)" % n403)
        if not window_cleared(st_.get("due"), st_.get("processed"), st_.get("stopped_early")):
            r.fail("window not cleared: processed %s of %s due, stopped_early %s, duration %s s" % (
                st_.get("processed"), st_.get("due"), st_.get("stopped_early"), st_.get("duration_s")))
            r.kv(**{k: str(v) for k, v in facts.items()})
            sys.exit(1)
        r.ok("window cleared: processed %s of %s due in %s s (stopped_early %s), floored_403 %s, plan floor %s, %d x 403 printed" % (
            st_.get("processed"), st_.get("due"), st_.get("duration_s"), st_.get("stopped_early"), st_.get("floored_403"), st_.get("plan_floor_403"), n403))
        r.kv(**{k: str(v) for k, v in facts.items()})
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
