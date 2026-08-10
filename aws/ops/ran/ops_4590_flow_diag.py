"""ops 4590 — unified flow reader diag (wo4585 close-out).

Both parallel-lane readers produced rows=0; rev-I unified them and
publishes diag {records_read, with_industry, votes_cast, field}. This op
invokes impact-graph, polls the CONVERGENCE key (the 4589 lesson), and
gates on the diag being on the record — with_industry tells us whether
the zero is an fc-name/graph-industry coverage gap (accrues via the
nightly FMP backfill) or a reader bug. Rows>0 is logged, not gated: a
starving >=2 rule with diag published is an honest, self-explaining
state.
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
    with report("4590_flow_diag") as r:
        r.heading("ops 4590 — unified flow reader diag")
        t0 = time.time()
        while time.time() - t0 < 240:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-impact-graph")["Configuration"]
                if c.get("LastUpdateStatus") == "Successful" \
                        and c.get("State") == "Active":
                    break
            except Exception:
                pass
            time.sleep(6)
        key = "data/impact/convergence.json"
        before = (get_json(key) or {}).get("generated_at") or ""
        lam.invoke(FunctionName="justhodl-impact-graph",
                   InvocationType="Event")
        j, t0 = None, time.time()
        while time.time() - t0 < 420:
            time.sleep(10)
            cur = get_json(key)
            ts = (cur or {}).get("generated_at") or ""
            if cur is not None and ts and ts != before:
                j = cur
                r.log("convergence refreshed (%ss)" % int(time.time() - t0))
                break
        if j is None:
            r.fail("convergence did not refresh in 420s")
            sys.exit(1)
        fl = j.get("flow_convergence") or {}
        diag = fl.get("diag") or {}
        misses = 0
        if j.get("version") != "1.1":
            r.fail("version %s != 1.1" % j.get("version"))
            misses += 1
        if "records_read" not in diag:
            r.fail("diag absent — rev-I not live")
            misses += 1
        else:
            r.ok("diag: field=%s records=%s industry_mapped=%s votes=%s"
                 % (diag.get("field"), diag.get("records_read"),
                    diag.get("with_industry"), diag.get("votes_cast")))
        r.log("source: %s" % str(fl.get("source"))[:160])
        rows = fl.get("rows") or []
        r.log("rows=%d%s" % (len(rows),
                             "" if rows else
                             " — >=2-underlying-engine gate starving; diag "
                             "above says whether coverage or evidence"))
        for row in rows[:5]:
            r.log("  %s %s score=%s sources=%s"
                  % (row.get("industry"), row.get("direction"),
                     row.get("score"), sorted(row.get("sources") or {})))
        if misses:
            sys.exit(1)
        r.ok("wo4585 closed — unified reader live, evidence self-explaining")


if __name__ == "__main__":
    main()
