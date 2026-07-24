#!/usr/bin/env python3
"""ops 3797 — verify rev share + dependency now reach the boards (v4.2.2).

3794 confirmed the cause: revenue_share_pct 0/50 and dependency_pct 0/50 on the
leaderboard, 0/3411 on industry members, while all_rows carried 1,675 and 154.
The keys were PRESENT but None — snapshotted before the v4.1 percentage block
ran. 3790's refresh only re-copied six growth fields.

FIX (v4.2.2): refresh the copies WHOLESALE from the source row instead of from
a curated field list. That list had now lagged the engine three separate times
(growth_tier/in_sp500, then revenue_share_pct/dependency_pct/criticality_basis/
revenue_currency/revenue_share_suppressed). Copying every key the source row
carries makes the class of bug structurally impossible rather than fixed once
more.

SEPARATE, HONEST FINDING carried to the UI: dependency_pct exists for only 154
of 3,411 ledger names because the supply-chain graph maps selected sectors, and
the leaderboard is dominated by small/micro caps it does not cover. Even after
this fix the column will be mostly dashes. The page now says WHY on hover and
prints live coverage counts in Method, so a dash reads as "not measurable"
rather than "broken".
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


def main():
    with report("3797_verify_revshare_ordering") as rep:
        rep.heading("ops 3797 — rev share + dependency on the boards (v4.2.2)")

        rep.section("Zip settle")
        settled = False
        for i in range(16):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                u = lam.get_function(FunctionName=FN)["Code"]["Location"]
                with urllib.request.urlopen(u, timeout=90) as r:
                    blob = r.read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    body = z.read("lambda_function.py").decode("utf-8", "replace")
                if body.find("# ops 3794: refresh the copies WHOLESALE") > body.find('_c["revenue_share_pct"] = (round'):
                    settled = True
                    rep.ok("v4.2.2 artifact live (attempt %d)" % (i + 1))
                    break
            time.sleep(15)
        gate(rep, "DEPLOY.settled", settled, "wholesale refresh present in deployed zip")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke")
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
        mem = [m for b in (cap.get("by_industry") or []) for m in (b.get("members") or []) if m]
        st = cap.get("stats") or {}
        gate(rep, "LIVE.v423", d.get("version") == "4.2.3", "version=%s" % d.get("version"))

        rep.section("THE FIX — copies must now carry the percentage fields")
        for f in ("revenue_share_pct", "criticality_pctile", "criticality_basis"):
            n = sum(1 for x in lead if x.get(f) is not None)
            gate(rep, f"LEAD.{f}", n > 0, "%d of %d leaderboard rows (was 0/50)" % (n, len(lead)))
        for f in ("revenue_share_pct", "criticality_pctile"):
            n = sum(1 for m in mem if m.get(f) is not None)
            gate(rep, f"MEMBERS.{f}", n > 0, "%d of %d member rows (was 0)" % (n, len(mem)))

        rep.section("Dependency — expected to stay sparse (graph coverage)")
        dl = sum(1 for x in lead if x.get("dependency_pct") is not None)
        dm = sum(1 for m in mem if m.get("dependency_pct") is not None)
        da = sum(1 for x in rows if x.get("dependency_pct") is not None)
        rep.kv(dependency_ledger=da, dependency_leaderboard=dl, dependency_members=dm)
        if dl == 0:
            rep.warn("0 leaderboard names have mapped supplier links — the curated "
                     "graph does not cover the small/micro caps that dominate the "
                     "board. Honest sparsity, now labelled on the page.")
        gate(rep, "DEP.members_reached", dm > 0 or da == 0,
             "%d member rows carry dependency (ledger has %d)" % (dm, da))

        rep.section("Sample")
        for x in lead[:10]:
            rep.log("  %-6s rev_share=%-9s dep=%-8s crit%%ile=%-6s basis=%s" % (
                x.get("ticker"),
                ("%.2f%%" % x["revenue_share_pct"]) if x.get("revenue_share_pct") is not None else "—",
                ("%.1f%%" % x["dependency_pct"]) if x.get("dependency_pct") is not None else "—",
                x.get("criticality_pctile"), (x.get("criticality_basis") or "")[:28]))

        rep.section("Served page v9")
        M = {"stamp": "v9-ops3795", "dep_tooltip": "missing coverage, not zero",
             "coverage_note": "names carry a revenue share", "rsh": "function rsh("}
        body = ""
        for a in range(1, 10):
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/capture-gap.html?v=%d%d" % (int(time.time()), a),
                    headers={"User-Agent": UA, "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=60) as rr:
                    body = rr.read().decode("utf-8", "replace")
            except Exception as e:
                rep.warn("attempt %d: %s" % (a, str(e)[:90])); time.sleep(25); continue
            h = sum(1 for m in M.values() if m in body)
            rep.log("attempt %d: %d bytes · %d/%d" % (a, len(body), h, len(M)))
            if h == len(M):
                break
            time.sleep(25)
        for k, m in M.items():
            gate(rep, f"SERVED.{k}", m in body, "present")

        rep.section("Additive")
        for k in ("capture_gap", "catchup_pct", "growth_tier", "in_sp500"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in lead),
                 "leaderboard still carries it")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — rev share live on the boards; dependency sparsity labelled honestly")


if __name__ == "__main__":
    main()
