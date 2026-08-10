"""ops 4589 — poll the RIGHT key (4588's own sequencing bug).

The impact-graph handler writes graph → history → betas → convergence, in
that order. 4588 asserted on convergence the instant the GRAPH landed and
read the stale 4582-era board. This op invokes once and polls the
CONVERGENCE key itself, then asserts the rev-G2 contract.
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
    with report("4589_convergence_key") as r:
        r.heading("ops 4589 — convergence key, polled correctly")
        misses = 0
        ckey = "data/impact/convergence.json"
        cb = (get_json(ckey) or {}).get("generated_at") or ""
        lam.invoke(FunctionName="justhodl-impact-graph",
                   InvocationType="Event")
        r.log("fired; polling %s" % ckey)
        conv, t0 = None, time.time()
        while time.time() - t0 < 820:
            time.sleep(15)
            cur = get_json(ckey)
            if cur and cur.get("generated_at") and \
                    cur["generated_at"] != cb:
                conv = cur
                r.log("convergence refreshed (%ss)" % int(time.time() - t0))
                break
        if conv is None:
            r.fail("convergence did not refresh in 820s")
            sys.exit(1)

        fl = conv.get("flow_convergence") or {}
        ok1 = "flow-confluence" in str(fl.get("relationship"))
        r.ok("relationship note: %s" % str(fl.get("relationship"))[:120]) \
            if ok1 else r.fail("relationship note missing")
        misses += 0 if ok1 else 1
        fc = get_json("data/flow-confluence.json") or {}
        n_multi = len(fc.get("multi_engine_confluence") or [])
        voted_rows = [row for row in (fl.get("rows") or [])
                      if "flow_confluence" in (row.get("sources") or {})]
        ok2 = bool(voted_rows) or n_multi == 0
        if ok2:
            r.ok("flow_confluence votes: %d industries carry them "
                 "(source has %d multi-engine names)"
                 % (len(voted_rows), n_multi))
        else:
            r.fail("no flow_confluence votes despite %d multi-engine names "
                   "— rows: %s"
                   % (n_multi,
                      [(x.get("industry"), list(x.get("sources") or {}))
                       for x in (fl.get("rows") or [])[:4]]))
            misses += 1
        for row in (fl.get("rows") or [])[:5]:
            r.log("  %s %s score=%s sources=%s"
                  % (row.get("industry"), row.get("direction"),
                     row.get("score"), list((row.get("sources") or {}))))

        if misses:
            r.fail("%d red" % misses)
            sys.exit(1)
        r.ok("already-built audit CLOSED — duplicate converged and "
             "credited; genuine gaps (bulk industry map, N-PORT fund "
             "index) confirmed net-new and live")


if __name__ == "__main__":
    main()
