"""ops 6685 - the why.html desk after the fmp_bank point-scan rule (lane ER): six requests on one warm container, in
order, mixing names the books hold (AAPL, MSFT) with names they do not (DOCMF - an OTC foreign ordinary, ALPN - acquired
2024, VINO, INNV - the names that died out-of-memory in ops 6683).  For each: status, bytes, "[research] N/43 endpoints"
and the REPORT line's Max Memory Used - the proof is a heap that stays far below 1 GB on the misses instead of pinning
at 976-1024 MB.  Reserved concurrency is 2, so the requests run one at a time.  Nothing is written.  No key involved.
"""
from pathlib import Path
import json
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-equity-research"
CFG = Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=320)
TICKERS = (os.environ.get("TICKERS") or "AAPL DOCMF ALPN MSFT VINO INNV").split()


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6685_desk_book_only_light") as r:
        r.heading("ops 6685 - equity-research after the point-scan rule: six requests, one container (read-only)")
        c = lam.get_function_configuration(FunctionName=FN)
        r.log("deployed: %s MB / %s s, modified %s" % (c.get("MemorySize"), c.get("Timeout"), (c.get("LastModified") or "")[:19]))
        since_ms = int(time.time() * 1000) - 5000
        rids = []
        for t in TICKERS:
            t0 = time.time()
            for attempt in range(4):
                try:
                    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"queryStringParameters": {"ticker": t, "refresh": "1"}, "rawQueryString": "ticker=%s&refresh=1" % t}).encode())
                    break
                except Exception as e:  # noqa: BLE001
                    if "TooManyRequests" in type(e).__name__ and attempt < 3:
                        time.sleep(20)
                        continue
                    r.warn("%s: invoke %s" % (t, type(e).__name__))
                    resp = None
                    break
            if resp is None:
                continue
            body = resp["Payload"].read()
            rid = resp.get("ResponseMetadata", {}).get("RequestId")
            rids.append((t, rid))
            status = None
            try:
                status = json.loads(body).get("statusCode")
            except Exception:  # noqa: BLE001
                pass
            r.log("%s: status %s (%s), %d bytes in %.1fs, FunctionError %s" % (t, status, resp.get("StatusCode"), len(body), time.time() - t0, resp.get("FunctionError")))
        time.sleep(20)
        r.section("REPORT lines (memory is the proof)")
        for t, rid in rids:
            line = "(not yet)"
            for _ in range(5):
                try:
                    ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=since_ms, filterPattern='"REPORT RequestId: %s"' % rid, limit=3).get("events", [])
                except Exception:  # noqa: BLE001
                    ev = []
                if ev:
                    m = ev[-1].get("message", "")
                    line = " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:", "Init Duration")))
                    break
                time.sleep(10)
            r.log("%s: %s" % (t, line))
        r.section("engine / shim lines")
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=since_ms, limit=120,
                                        filterPattern='?"[research]" ?"[fmp-bankfirst]" ?"stepped aside" ?Traceback ?OutOfMemory').get("events", [])
            for e in ev[-40:]:
                r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:260]))
        except Exception as e:  # noqa: BLE001
            r.warn("logs: %s" % type(e).__name__)
        r.ok("ops 6685 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
