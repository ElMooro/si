#!/usr/bin/env python3
"""ops 3800 — verify dependency_pct is no longer a thin-coverage artifact.

3799 raised dependency coverage 153 -> 181 by fixing a dead node read (0 of 183
nodes ever had a "centrality" key; they carry ticker/degree/n_suppliers). But it
also exposed a worse problem the gates happily passed: ELEVEN names printed
exactly 100.0% — AMZN, WMT, AAPL, NEE, JCI, UAL, MP and others. Amazon is not
100% of Specialty Retail's supply-chain dependency. The denominator sums
centrality across SCORED names in the industry, so a company that is the ONLY
mapped name in its industry takes the whole share by construction.

A number that is wrong but plausible is worse than a blank. v4.3.1 therefore
requires >=3 mapped peers in an industry before publishing a share, carries
dependency_mapped_peers on every row, and states the reason when suppressed.

GATE: no row may print >=99.9% unless its industry has several mapped peers,
and the count of exact-100% rows must fall to zero.
"""
import sys, json, time, zipfile, io, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

FN = "justhodl-chokepoint"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3800_verify_dependency_sanity") as rep:
        rep.heading("ops 3800 — dependency must not be a coverage artifact")

        rep.section("Settle")
        settled = False
        for i in range(16):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                u = lam.get_function(FunctionName=FN)["Code"]["Location"]
                with urllib.request.urlopen(u, timeout=90) as r:
                    blob = r.read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    if "ops 3800: the denominator is centrality summed" in z.read(
                            "lambda_function.py").decode("utf-8", "replace"):
                        settled = True; rep.ok("v4.3.1 live (attempt %d)" % (i + 1)); break
            time.sleep(15)
        gate(rep, "DEPLOY.settled", settled, "peer-floor patch in deployed zip")
        if FAILED:
            sys.exit(1)

        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"mode": "full"}).encode())
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        mem = [m for b in (cap.get("by_industry") or []) for m in (b.get("members") or []) if m]
        st = cap.get("stats") or {}

        gate(rep, "LIVE.v431", d.get("version") == "4.3.1", "version=%s" % d.get("version"))
        got = [x for x in rows if x.get("dependency_pct") is not None]
        hundred = [x for x in got if (x.get("dependency_pct") or 0) >= 99.9]
        rep.kv(dependency_published=len(got), exactly_100=len(hundred),
               graph_nodes=st.get("graph_nodes"), graph_edges=st.get("graph_edges"),
               was_153_then_181=True)

        gate(rep, "SANITY.no_lone_100", len(hundred) == 0,
             "%d rows still print >=99.9%% (was 11)" % len(hundred))
        for x in hundred[:6]:
            rep.log("  STILL 100%%: %-6s %s peers=%s" % (
                x.get("ticker"), x.get("industry"), x.get("dependency_mapped_peers")))

        gate(rep, "FEED.peer_count", any(x.get("dependency_mapped_peers") is not None
                                         for x in rows), "mapped-peer count shipped")
        gate(rep, "FEED.suppress_reason", any(x.get("dependency_suppressed") for x in rows),
             "suppression reason shipped")

        rep.section("Distribution now")
        if got:
            vs = sorted(x["dependency_pct"] for x in got)
            rep.kv(dep_min=vs[0], dep_median=vs[len(vs) // 2], dep_max=vs[-1])
        for x in sorted(got, key=lambda z: -(z.get("dependency_pct") or 0))[:12]:
            rep.log("  %-6s %-30s dep=%5.1f%%  mapped_peers=%-3s crit=%s" % (
                x.get("ticker"), (x.get("industry") or "")[:30],
                x.get("dependency_pct") or 0, x.get("dependency_mapped_peers"),
                x.get("criticality")))

        dm = sum(1 for m in mem if m.get("dependency_pct") is not None)
        rep.kv(dependency_on_member_rows=dm)

        rep.section("Additive")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct", "criticality_pctile"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — dependency shares now require real peer coverage")


if __name__ == "__main__":
    main()
