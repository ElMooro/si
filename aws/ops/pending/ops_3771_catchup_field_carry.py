#!/usr/bin/env python3
"""ops 3771 — activate the catch-up % leg (cap_rows field drop).

3770 shipped v4.0: pool widened 877 -> 1,189, by_industry (127) and the
leaderboard (50) both live. One gate failed: with_catchup = 0.

ROOT CAUSE (verified by reading the deployed source, not guessed):
evaluate() DOES compute ev_sales and pe correctly (lines 187-207) and DOES
return them (line 214). But cap_rows — the structure the v4 catch-up block
consumes — is assembled from an explicit field list at line 584-604 that never
copies them. So `_c.get("ev_sales")` was None for all 1,189 names and every
catch-up number silently became None.

This is the SAME defect class as the backlog leg in 3766/3768: the producer
emits the field, the consumer's hand-written field list drops it, and the gap
shows up as a plausible-looking zero rather than an error. The countermeasure
is the same one that caught it — gate on the field being populated downstream,
not merely on the producing code existing.

FIX: carry ev_sales/pe into cap_rows. One line. Then re-verify that catch-up is
two-sided (both cheap and rich names) so the leg cannot read as a one-way
"everything is undervalued" artifact.
"""
import sys, json, time, zipfile, io
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

FN = "justhodl-chokepoint"
SRC = ROOT / "lambdas" / FN / "source"
LAMBDA_FILE = SRC / "lambda_function.py"
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
    with report("3771_catchup_field_carry") as rep:
        rep.heading("ops 3771 — carry ev_sales/pe into cap_rows (catch-up leg)")

        src = LAMBDA_FILE.read_text()

        rep.section("G0 — prove producer emits, consumer drops")
        gate(rep, "G0.producer_computes", "ev_sales = round(mcap / _rev_ttm, 3)" in src,
             "evaluate() computes ev_sales")
        gate(rep, "G0.producer_returns", '"ev_sales": ev_sales, "pe": pe_ratio,' in src,
             "evaluate() returns ev_sales/pe")
        gate(rep, "G0.consumer_drops",
             '"ev_sales": _r.get("ev_sales")' not in src,
             "cap_rows does NOT carry ev_sales — the confirmed root cause")
        gate(rep, "G0.consumer_reads", '_c.get("ev_sales")' in src,
             "v4 catch-up block reads ev_sales from cap_rows")
        if FAILED:
            sys.exit(1)

        rep.section("Fix — carry the fields")
        anchor = '''                    "cap_bucket": _r.get("cap_bucket"),
                    "is_chokepoint": _r.get("is_chokepoint"),
                })'''
        gate(rep, "FIX.anchor", src.count(anchor) == 1, "cap_rows tail anchor unique")
        if FAILED:
            sys.exit(1)
        new = '''                    "cap_bucket": _r.get("cap_bucket"),
                    "is_chokepoint": _r.get("is_chokepoint"),
                    # ops 3771: evaluate() emits these; the hand-written field list
                    # above dropped them, so every catch-up number was silently None.
                    "ev_sales": _r.get("ev_sales"),
                    "pe": _r.get("pe"),
                })'''
        src = src.replace(anchor, new, 1)
        src = src.replace('VERSION = "4.0"', 'VERSION = "4.0.1"', 1)
        LAMBDA_FILE.write_text(src)
        import py_compile
        py_compile.compile(str(LAMBDA_FILE), doraise=True)
        rep.ok("ev_sales/pe now carried into cap_rows (v4.0.1)")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.0.1 (ledger-widened pool, by-industry, industry-median catch-up %, leaderboard).",
                      create_function_url=False, smoke=False)

        settled = False
        for i in range(14):
            time.sleep(15)
            c0 = lam.get_function_configuration(FunctionName=FN)
            if c0.get("State") != "Active" or c0.get("LastUpdateStatus") != "Successful":
                continue
            import urllib.request
            u = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(u, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if '"ev_sales": _r.get("ev_sales")' in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True
                    rep.ok("settled attempt %d" % (i + 1))
                    break
        gate(rep, "DEPLOY.settled", settled, "v4.0.1 live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke + verify the leg is alive")
        from botocore.config import Config
        ll = boto3.client("lambda", region_name="us-east-1",
                          config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        r = ll.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"mode": "full"}).encode())
        rep.kv(invoke_status=r.get("StatusCode"), invoke_seconds=round(time.time() - t0, 1))

        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        st = cap.get("stats") or {}
        rows = cap.get("all_rows") or []

        rep.kv(version=d.get("version"), scored=st.get("scored"),
               with_catchup=st.get("with_catchup"),
               industries_grouped=st.get("industries_grouped"),
               ledger_note="accretes each run")

        gate(rep, "LIVE.v401", d.get("version") == "4.0.1", "version=%s" % d.get("version"))
        gate(rep, "FIX.catchup_alive", (st.get("with_catchup") or 0) > 0,
             "catchup populated on %s names (was 0)" % st.get("with_catchup"))

        cus = [x.get("catchup_pct") for x in rows if x.get("catchup_pct") is not None]
        if cus:
            neg = sum(1 for x in cus if x < 0)
            pos = sum(1 for x in cus if x > 0)
            rep.kv(catchup_min=round(min(cus), 1), catchup_max=round(max(cus), 1),
                   catchup_negative=neg, catchup_positive=pos)
            gate(rep, "SANITY.two_sided", neg > 0 and pos > 0,
                 "%d below / %d above industry median — not a one-way artifact" % (pos, neg))
            gate(rep, "SANITY.median_sane", abs(sorted(cus)[len(cus) // 2]) < 200,
                 "median catchup %.1f%% is plausible" % sorted(cus)[len(cus) // 2])

        rep.section("TOP UNDERVALUED — all industries (with catch-up)")
        for x in (cap.get("top_undervalued_all_industries") or [])[:15]:
            rep.log("  %-6s %-22s %-24s score=%-6.1f gap=%+5.1f catchup=%7s%% (%s) legs=%d %s" % (
                x.get("ticker"), (x.get("name") or "")[:22], (x.get("industry") or "")[:24],
                x.get("undervaluation_score") or 0, x.get("capture_gap") or 0,
                ("%.0f" % x["catchup_pct"]) if x.get("catchup_pct") is not None else "—",
                x.get("catchup_basis") or "-", x.get("legs") or 0, x.get("tier")))

        rep.section("BY INDUSTRY — median catch-up now populated")
        for b in (cap.get("by_industry") or [])[:14]:
            rep.log("  %-32s n=%-3d %-6s med_gap=%+6.1f med_catchup=%8s%% undervalued=%d" % (
                (b.get("industry") or "")[:32], b.get("n_scored") or 0,
                b.get("sample_confidence"), b.get("median_capture_gap") or 0,
                ("%.0f" % b["median_catchup_pct"]) if b.get("median_catchup_pct") is not None else "—",
                b.get("n_undervalued") or 0))

        rep.section("Additive contract")
        for k in ("structural_names", "industry_leaders", "all_chokepoints",
                  "hidden_chokepoint_book", "cheap_chokepoint_book"):
            gate(rep, f"ADDITIVE.{k}", k in d, "present")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — catch-up leg alive; all four asks now live in the feed")
        rep.log("NEXT: page rewrite — leaderboard on top, industry-first layout, catch-up column.")


if __name__ == "__main__":
    main()
