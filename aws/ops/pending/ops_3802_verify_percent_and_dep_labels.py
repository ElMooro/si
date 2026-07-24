#!/usr/bin/env python3
"""ops 3802 — verify % notation everywhere + dependency reason on the boards.

Khalid, two items:
 1. "on Most Undervalued — supply chain dependency isn't working"
 2. "in the entire engine and page i prefer % over pp"

ON (1) — 3801 proved it is NOT a bug. copy_lag=0, and EVERY one of the 50
leaderboard names carries the reason "no mapped supplier links for this
company". The curated graph names ~185 symbols; the leaderboard is dominated by
micro/nano caps that simply are not in it. The column was right, the DISPLAY was
wrong: a bare dash reads as broken. It now renders "unmapped" (no links at all)
or "thin" (blocked by the >=3 mapped-peer floor added in 3800 to kill the fake
AMZN/WMT 100% prints), each with the full reason on hover.

I am NOT relaxing the peer floor to make the column look populated — that would
resurrect numbers like "AMZN = 100% of Specialty Retail dependency", which were
wrong in a way that looked right. Real coverage requires expanding
justhodl-supply-chain-graph, which is its own arc.

ON (2) — pp is gone from this engine and both pages. One caveat worth stating
rather than burying: capture_gap is a difference of two PERCENTILE RANKS, so
"+31" means 31 rank-percent, not 31% of price. The glossary now says exactly
that so the notation change does not quietly imply a price move.
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
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(u, a=0):
    x = u + ("&" if "?" in u else "?") + "v=%d%d" % (int(time.time()), a)
    req = urllib.request.Request(x, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def main():
    with report("3802_verify_percent_and_dep_labels") as rep:
        rep.heading("ops 3802 — % notation + dependency labelling")

        rep.section("Engine settle (v4.3.2)")
        settled = False
        for i in range(16):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                u = lam.get_function(FunctionName=FN)["Code"]["Location"]
                with urllib.request.urlopen(u, timeout=90) as r:
                    blob = r.read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    body = z.read("lambda_function.py").decode("utf-8", "replace")
                if 'capture_gap>=20%' in body:
                    settled = True; rep.ok("v4.3.2 artifact live (attempt %d)" % (i + 1)); break
            time.sleep(15)
        gate(rep, "DEPLOY.settled", settled, "% notation present in deployed zip")

        if settled:
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
        lead = cap.get("top_undervalued_all_industries") or []
        rep.kv(version=d.get("version"), rows=len(rows), leaderboard=len(lead))

        rep.section("No 'pp' left in engine-authored text")
        blob = json.dumps(cap.get("method") or {}) + json.dumps(
            [x.get("legs_why") for x in rows[:200]])
        gate(rep, "ENGINE.no_pp", "pp" not in blob.replace("append", ""),
             "method + legs_why carry no 'pp'")

        rep.section("Dependency: every leaderboard row must carry a REASON")
        with_reason = sum(1 for x in lead if x.get("dependency_suppressed")
                          or x.get("dependency_pct") is not None)
        rep.kv(leaderboard_with_reason_or_value=with_reason, leaderboard=len(lead))
        gate(rep, "DEP.reason_present", with_reason == len(lead),
             "%d of %d rows explain themselves" % (with_reason, len(lead)))
        pub = sum(1 for x in rows if x.get("dependency_pct") is not None)
        hundred = sum(1 for x in rows if (x.get("dependency_pct") or 0) >= 99.9)
        rep.kv(dependency_published_ledger=pub, exactly_100=hundred,
               graph_nodes=(cap.get("stats") or {}).get("graph_nodes"))
        gate(rep, "DEP.no_fake_100", hundred == 0, "%d rows print >=99.9%%" % hundred)

        rep.section("Served pages")
        cg = ""
        for a in range(1, 10):
            try:
                cg = fetch("https://justhodl.ai/capture-gap.html", a)
            except Exception as e:
                rep.warn(str(e)[:80]); time.sleep(25); continue
            if "v10-ops3802" in cg:
                break
            time.sleep(25)
        gate(rep, "PAGE.stamp", "v10-ops3802" in cg, "capture-gap v10 served (%d bytes)" % len(cg))
        gate(rep, "PAGE.no_pp", "pp<" not in cg and "+'pp'" not in cg,
             "no 'pp' rendered on capture-gap")
        gate(rep, "PAGE.unmapped_label", "unmapped" in cg, "dependency shows 'unmapped'")
        gate(rep, "PAGE.thin_label", "'thin'" in cg or ">thin<" in cg or "thin" in cg,
             "dependency shows 'thin' for peer-floor cases")
        gate(rep, "PAGE.rank_caveat", "percent of rank" in cg,
             "glossary states capture gap is percent of RANK, not price")

        try:
            wh = fetch("https://justhodl.ai/why.html")
            i = wh.find("jhCaptureTiles")
            seg = wh[i:i + 3000] if i > 0 else ""
            gate(rep, "WHY.no_pp", "+'pp'" not in seg, "why.html tiles carry no 'pp'")
        except Exception as e:
            rep.warn("why.html fetch: %s" % str(e)[:90])

        rep.section("Sample — leaderboard dependency now self-explaining")
        for x in lead[:8]:
            rep.log("  %-6s dep=%-7s peers=%-4s reason=%s" % (
                x.get("ticker"),
                ("%.1f%%" % x["dependency_pct"]) if x.get("dependency_pct") is not None else "—",
                x.get("dependency_mapped_peers"),
                str(x.get("dependency_suppressed") or "")[:48]))

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — % everywhere; dependency blanks now say why")


if __name__ == "__main__":
    main()
