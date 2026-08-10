"""ops 4588 — already-built audit convergence verification (wo4585 tail).

Khalid's directive: check nothing built rides beside prior art. Audit
found ONE conceptual duplicate (flow-confluence pre-existed the flow-
convergence idea) and ONE genuine gap (no bulk ticker→industry map).
rev-G/G2/H converge both. This op invokes impact-graph and asserts:

  1. convergence board consumes flow-confluence (vote source present when
     that engine has multi-engine names) + relationship note published
  2. industry coverage moved above the census ceiling (502) via the
     backfill, with the audit-trail block on the payload
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


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4588_audit_verify") as r:
        r.heading("ops 4588 — already-built audit convergence")
        misses = 0

        # settle rev-G2/H deploy, apply timeout from config
        t0 = time.time()
        while time.time() - t0 < 300:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-impact-graph")["Configuration"]
                if c.get("LastUpdateStatus") == "Successful" \
                        and c.get("State") == "Active":
                    break
            except Exception:
                pass
            time.sleep(6)
        c = lam.get_function(FunctionName="justhodl-impact-graph")[
            "Configuration"]
        if c.get("Timeout") != 780:
            lam.update_function_configuration(
                FunctionName="justhodl-impact-graph", Timeout=780)
            time.sleep(8)
            r.log("  Timeout applied → 780")

        gkey = "data/impact/exposure-graph.json"
        ckey = "data/impact/convergence.json"
        gb = (get_json(gkey) or {}).get("generated_at") or ""
        lam.invoke(FunctionName="justhodl-impact-graph",
                   InvocationType="Event")
        r.log("  fired impact-graph")
        g, t0 = None, time.time()
        while time.time() - t0 < 820:
            time.sleep(15)
            cur = get_json(gkey)
            if cur and cur.get("generated_at") and \
                    cur["generated_at"] != gb:
                g = cur
                r.log("  graph refreshed (%ss)" % int(time.time() - t0))
                break
        if g is None:
            r.fail("graph did not refresh in 820s")
            sys.exit(1)

        r.section("1. Coverage above the census ceiling")
        cov = g.get("field_coverage") or {}
        bf = g.get("industry_backfill") or {}
        misses += contract(r, "graph",
                           (cov.get("industry") or 0) > 502,
                           "industry coverage %s (was 502; reused=%s "
                           "filled_tonight=%s still_missing_top_adv=%s)"
                           % (cov.get("industry"),
                              bf.get("reused_from_prior_graph"),
                              bf.get("fmp_filled_tonight"),
                              bf.get("top_adv_still_missing")))
        misses += contract(r, "graph", "prior_art_note" in bf,
                           "audit trail on the payload")

        r.section("2. Convergence consumes the prior-art engine")
        conv = get_json(ckey) or {}
        fl = conv.get("flow_convergence") or {}
        misses += contract(r, "convergence",
                           "flow-confluence" in str(fl.get("relationship")),
                           "relationship note names the authority")
        fc = get_json("data/flow-confluence.json") or {}
        n_multi = len(fc.get("multi_engine_confluence") or [])
        voted = any("flow_confluence" in (row.get("sources") or {})
                    for row in (fl.get("rows") or []))
        misses += contract(r, "convergence",
                           voted or n_multi == 0,
                           "flow_confluence votes present (source engine "
                           "has %d multi-engine names; voted=%s)"
                           % (n_multi, voted))
        for row in (fl.get("rows") or [])[:5]:
            r.log("    %s %s score=%s sources=%s"
                  % (row.get("industry"), row.get("direction"),
                     row.get("score"), list((row.get("sources") or {}))))

        r.section("verdict")
        if misses:
            r.fail("audit convergence: %d red" % misses)
            sys.exit(1)
        r.ok("already-built audit CLOSED — one duplicate converged "
             "(flow-confluence consumed + credited), one genuine gap "
             "filled (bulk industry map), N-PORT/fund-CIK confirmed "
             "net-new")


if __name__ == "__main__":
    main()
