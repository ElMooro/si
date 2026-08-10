"""ops 4587 — N-PORT evidence gate.

rev-F publishes diag (map sizes + unresolved tickers) whenever the fund
index comes back empty. This op invokes etf-true-flows and gates on
EVIDENCE: either funds index (>=1) or the diag block is on the record so
the next session opens with facts. ISO-NE stays on the ledger with its
own precise next step (parse the export link out of the cookie-gated
HTML).
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def main():
    with report("4587_nport_evidence") as r:
        r.heading("ops 4587 — N-PORT evidence gate")
        key = "data/etf-true-flows.json"
        # settle the rev-F deploy
        t0 = time.time()
        while time.time() - t0 < 240:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-etf-true-flows")["Configuration"]
                if c.get("LastUpdateStatus") == "Successful" \
                        and c.get("State") == "Active":
                    break
            except Exception:
                pass
            time.sleep(6)
        before = (get_json(key) or {}).get("generated_at") or ""
        lam.invoke(FunctionName="justhodl-etf-true-flows",
                   InvocationType="Event")
        j, t0 = None, time.time()
        while time.time() - t0 < 420:
            time.sleep(10)
            cur = get_json(key)
            ts = (cur or {}).get("generated_at") or ""
            if cur is not None and ts and ts != before:
                j = cur
                r.log("refreshed (%ss)" % int(time.time() - t0))
                break
        j = j or get_json(key) or {}
        gt = j.get("ground_truth") or {}
        n = len(gt.get("per_etf") or [])
        diag = gt.get("diag") or {}
        r.log("status=%s per_etf=%d" % (gt.get("status"), n))
        if diag:
            r.log("diag: mf_map_n=%s op_map_n=%s" % (diag.get("mf_map_n"),
                                                     diag.get("op_map_n")))
            r.log("tops=%s" % diag.get("tops"))
            r.log("unresolved=%s" % diag.get("unresolved"))
        if n >= 1:
            r.ok("N-PORT WIRED_INDEX — %d funds; wo4585 fully closed" % n)
            for e in (gt.get("per_etf") or [])[:8]:
                r.log("  %s cik=%s latest %s" % (e.get("etf"), e.get("cik"),
                                                 e.get("latest_nport_date")))
        elif diag:
            r.ok("evidence on record — mf_map_n tells whether the MF fetch "
                 "itself fails (0) or symbols mismatch (>0 with unresolved "
                 "list); next session fixes from facts")
        else:
            r.fail("neither funds nor diag — rev-F not live on this run")
            sys.exit(1)


if __name__ == "__main__":
    main()
