"""ops 6669 - read-only trace of ONE justhodl-equity-research request (the why.html desk) end to end.

08:04 UTC it answered AAPL (refresh=1) with the full 84 KB report; 08:39 UTC it answered 404 "No data for ticker AAPL".
The 404 fires only when BOTH /profile and /quote came back empty; /quote is NEVER answered from the bank (a quote is
live) and live FMP is bandwidth-blocked, so the verdict rests on /profile from profile-bulk through the bank-first shim.
This probe: (1) runs the shim's own decision for profile / quote / an income-statement ask on the runner (shaped? fresh?
bank rows?), (2) invokes the function once for AAPL with refresh=1, keeps the invocation RequestId, waits, and pulls
EVERY log line of that one request (the engine's "[research] N/M endpoints returned data", the shim's lines, the REPORT
line) so the failing step is named, not guessed.  Nothing is written.
"""
from pathlib import Path
import json
import sys
import time
import urllib.parse

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-equity-research"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=300)


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    import fmp_bank
    import fmp_bankfirst as fb
    with report("ops_6669_equity_research_trace") as r:
        r.heading("ops 6669 - equity-research: one request traced end to end (read-only)")
        r.section("1. the shim's decisions on the runner (same code the function runs)")
        for path, qs in (("profile", "symbol=AAPL"), ("quote", "symbol=AAPL"), ("income-statement", "symbol=AAPL&period=annual&limit=20"),
                         ("ratios", "symbol=AAPL&period=annual&limit=15"), ("key-metrics-ttm", "symbol=AAPL"), ("historical-price-eod/light", "symbol=AAPL&from=2016-10-26")):
            q = {k: v[0] for k, v in urllib.parse.parse_qs(qs).items()}
            head = path
            try:
                shaped = fb._shaped(head, path, q)
            except Exception as e:  # noqa: BLE001
                shaped = "error %s" % type(e).__name__
            try:
                t0 = time.time()
                rows = fmp_bank.try_bank(path, qs)
                dt = time.time() - t0
                n = len(rows) if isinstance(rows, list) else (1 if rows else 0)
            except Exception as e:  # noqa: BLE001
                rows, dt, n = None, 0, "error %s" % type(e).__name__
            fresh = None
            if isinstance(rows, list) and rows:
                try:
                    fresh = fb._fresh_enough(head, rows, q)
                except Exception as e:  # noqa: BLE001
                    fresh = "error %s" % type(e).__name__
            r.log("%s?%s -> shaped %s | bank rows %s in %.2fs | fresh_enough %s | never %s" % (path, qs, shaped, n, dt, fresh, head in fb.NEVER_HEADS))
        r.log("fmp_bank STATS %s | history book phase %s" % (json.dumps(fmp_bank.STATS), (fmp_bank.history_manifest() or {}).get("phase")))
        man = fmp_bank.manifest() or {}
        r.log("fmp-bank manifest updated_at %s endpoints_served %d" % (man.get("updated_at"), len(man.get("endpoints_served") or [])))
        r.section("2. one invocation, traced")
        cf = lam.get_function_configuration(FunctionName=FN)
        r.log("deployed: modified %s, %s MB, %s s" % ((cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout")))
        t_inv = int(time.time() * 1000) - 5000
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"queryStringParameters": {"ticker": "AAPL", "refresh": "1"}, "rawQueryString": "ticker=AAPL&refresh=1"}).encode())
        rid = resp.get("ResponseMetadata", {}).get("RequestId")
        body = resp["Payload"].read()
        r.log("invoke RequestId %s: status %s, %d bytes, FunctionError %s, head %s" % (rid, resp.get("StatusCode"), len(body), resp.get("FunctionError"), body[:200]))
        time.sleep(25)
        lines = []
        for attempt in range(4):
            try:
                ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=t_inv, filterPattern='"%s"' % rid, limit=200).get("events", [])
            except Exception as e:  # noqa: BLE001
                r.warn("logs: %s" % type(e).__name__)
                ev = []
            lines = [e.get("message", "").rstrip() for e in ev]
            if any("REPORT RequestId" in x for x in lines):
                break
            time.sleep(15)
        r.log("%d log lines for this request:" % len(lines))
        for x in lines[:120]:
            r.log("   " + x.replace("\n", " ⏎ ")[:400])
        # the engine prints its own lines without the request id; pull the window around the request too
        try:
            ev2 = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=t_inv, filterPattern='?"[research]" ?"[fmp-bankfirst]" ?"[fmp-bank]" ?Traceback ?"[ERROR]"', limit=80).get("events", [])
            r.log("engine / shim lines since the invoke (%d):" % len(ev2))
            for e in ev2[:80]:
                r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:400]))
        except Exception as e:  # noqa: BLE001
            r.warn("logs window: %s" % type(e).__name__)
        r.ok("ops 6669 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
