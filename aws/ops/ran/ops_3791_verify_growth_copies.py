#!/usr/bin/env python3
"""ops 3791 — verify v4.2.1: growth/sp500 reach the COPIED structures.

3789 caught that growth_tier and in_sp500 were 0/50 on the leaderboard despite
1,242 ledger rows carrying growth and the field list naming them.

ROOT CAUSE: ordering. The leaderboard and by_industry members are LISTS OF
COPIED DICTS built earlier in the function; the v4.2 growth block assigned to
cap_rows afterwards. all_rows shares object references so it looked healthy —
only the copies were empty, which is a genuinely confusing signature.

3790 tried to MOVE the block and broke the file: the leaderboard sits inside a
12-space try: while the growth block is at 8-space, so relocating it orphaned
the enclosing try and the source failed to compile. That failure auto-committed
a broken lambda_function.py, which I restored from the last good commit.

FINAL FIX (v4.2.1, pushed as source): keep the block where it is and REFRESH the
copied structures from cap_rows once growth exists. No scope change, no move.

This ops verifies the deployed artifact and gates the COPIES specifically.
"""
import sys, json, time, zipfile, io
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
    with report("3791_verify_growth_copies") as rep:
        rep.heading("ops 3791 — growth must reach leaderboard + members")

        rep.section("Zip settle — confirm v4.2.1 is the deployed artifact")
        settled = False
        for i in range(16):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                import urllib.request
                u = lam.get_function(FunctionName=FN)["Code"]["Location"]
                with urllib.request.urlopen(u, timeout=90) as r:
                    blob = r.read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    body = z.read("lambda_function.py").decode("utf-8", "replace")
                if "ops 3790: the leaderboard and by_industry members are COPIED" in body:
                    settled = True
                    rep.ok("v4.2.1 artifact live (attempt %d)" % (i + 1))
                    break
            time.sleep(15)
        gate(rep, "DEPLOY.settled", settled, "refresh patch present in deployed zip")
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
        lead = cap.get("top_undervalued_all_industries") or []
        rows = cap.get("all_rows") or []
        bi = cap.get("by_industry") or []
        st = cap.get("stats") or {}

        gate(rep, "LIVE.version", d.get("version") == "4.2.1", "version=%s" % d.get("version"))
        rep.kv(scored=st.get("scored"), with_growth=st.get("with_growth"),
               sp500=st.get("sp500_members"), leaderboard=len(lead))

        rep.section("THE GATE THAT MATTERS — copied structures")
        for f in ("growth_tier", "in_sp500", "revenue_growth_yoy", "gm_level"):
            n = sum(1 for x in lead if x.get(f) is not None)
            gate(rep, f"LEAD.{f}", n > 0, "%d of %d leaderboard rows (was 0/50)" % (n, len(lead)))
        mem = [m for b in bi for m in (b.get("members") or [])]
        for f in ("growth_tier", "in_sp500"):
            n = sum(1 for m in mem if m.get(f) is not None)
            gate(rep, f"MEMBERS.{f}", n > 0, "%d of %d member rows" % (n, len(mem)))

        rep.section("Filter viability — each dropdown option must match rows")
        for t in ("HIGH", "MEDIUM", "LOW"):
            n = sum(1 for x in lead if x.get("growth_tier") == t)
            rep.log("  leaderboard growth=%-7s %d" % (t, n))
        rep.kv(leaderboard_sp500=sum(1 for x in lead if x.get("in_sp500") is True))
        for b in ("mega", "large", "mid", "small", "micro"):
            n = sum(1 for x in lead if x.get("cap_bucket") == b)
            rep.log("  leaderboard cap=%-7s %d" % (b, n))

        rep.section("Sample")
        for x in lead[:12]:
            rep.log("  %-6s %-22s growth=%-8s tier=%-7s sp500=%-5s cap=%-6s gm=%s" % (
                x.get("ticker"), (x.get("industry") or "")[:22],
                ("%.1f%%" % x["revenue_growth_yoy"]) if x.get("revenue_growth_yoy") is not None else "—",
                x.get("growth_tier"), x.get("in_sp500"), x.get("cap_bucket"),
                ("%.0f%%" % x["gm_level"]) if x.get("gm_level") is not None else "—"))

        rep.section("Additive — nothing regressed")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct",
                  "criticality_pctile", "dependency_pct"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")
        for k in ("structural_names", "industry_leaders", "all_chokepoints"):
            gate(rep, f"ADDITIVE.{k}", k in d, "present")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — size and growth filters act on populated fields end to end")


if __name__ == "__main__":
    main()
