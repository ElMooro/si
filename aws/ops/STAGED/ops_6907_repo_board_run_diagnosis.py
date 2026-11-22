"""ops 6907 - READ-ONLY: why the board's 06:10 UTC run on 2026-11-22 published 766 series instead of 1,800.

ops 6904 found the board at engine 3.1 without its FRED group (DFEDTARU, IORB, WALCL, SOFR averages ...) and so
without the derived series built on it.  Read here: the board's own diag, its groups, and the run's log lines.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE, FN = "justhodl-dashboard-live", "justhodl-repo"


def clip(v, n=700):
    s = json.dumps(v, default=str)
    return s if len(s) <= n else s[:n] + " ...(%d chars)" % len(s)


def selftest():
    assert clip({"a": 1}) == '{"a": 1}' and clip("x" * 900).endswith("(902 chars)")


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    logs = boto3.client("logs", region_name="us-east-1")
    with report("ops_6907_repo_board_run_diagnosis") as r:
        r.heading("ops 6907 - the board's 06:10 UTC run, diagnosed (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        board = json.loads(s3.get_object(Bucket=LIVE, Key="data/repo.json")["Body"].read())
        r.section("1. The board")
        r.log("engine %s as of %s | counts %s" % (board.get("engine_v"), board.get("as_of"), json.dumps(board.get("counts"))))
        r.log("groups: %s" % ", ".join("%s %d" % (g["name"], len(g["series"])) for g in board.get("groups") or []))
        for k, v in (board.get("diag") or {}).items():
            r.log("diag.%s: %s" % (k, clip(v)))
        r.section("2. The run's log")
        start = int((datetime.now(timezone.utc) - timedelta(minutes=75)).timestamp() * 1000)
        seen, token, n = [], None, 0
        while n < 12:
            kw = {"logGroupName": "/aws/lambda/" + FN, "startTime": start, "limit": 400}
            if token:
                kw["nextToken"] = token
            page = logs.filter_log_events(**kw)
            seen.extend(page.get("events", []))
            token = page.get("nextToken")
            n += 1
            if not token:
                break
        r.log("%d log events in the last 75 minutes" % len(seen))
        shown = 0
        for e in seen:
            m = e["message"].strip()
            if m.startswith(("START", "END", "INIT_START")):
                continue
            low = m.lower()
            if m.startswith("REPORT") or any(k in low for k in ("error", "fred", "exception", "traceback", "own sources", "timed out", "skip", "fail", "429", "dollar", "budget", "rhythm", "asked")) or shown < 25:
                r.log("  %s %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%H:%M:%S"), m[:420]))
                shown += 1
            if shown > 140:
                break
        r.ok("read-only")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
