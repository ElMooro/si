#!/usr/bin/env python3
"""ops 3790 — fix ordering: growth assigned AFTER the leaderboard is built.

3789 caught it: growth_tier and in_sp500 were 0/50 on the leaderboard even
though the field list names them and 1,242 rows carry growth.

ROOT CAUSE — not a missing key this time, an ORDERING bug. My 3788 splice put
the v4.2 growth block immediately before the v4.1 percentage block, which sits
LATER in the function than the leaderboard build:
    line 879  top_undervalued_all_industries = [...]   <- snapshot taken here
    line 924  _c["growth_tier"] = ...                  <- assigned here
The leaderboard is a LIST OF COPIED DICTS ({k: x.get(k) ...}), so it snapshots
values at build time. Fields added to cap_rows afterwards never appear in it.
The full ledger shares object references, which is why all_rows looked fine and
only the copied leaderboard was empty — a genuinely confusing signature.

FIX: move the growth/sp500 computation ABOVE the leaderboard build. Same code,
correct position. Gate on the LEADERBOARD specifically, not just on all_rows —
3789 only caught this because it checked the copied structure separately.
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
LF = SRC / "lambda_function.py"
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
    with report("3790_fix_growth_ordering") as rep:
        rep.heading("ops 3790 — move growth block above the leaderboard build")

        src = LF.read_text()
        start = src.find('        # ── v4.2 growth tiers + S&P 500 membership ──')
        end = src.find('        # ── v4.1 "% critical to industry" — three DISTINCT percentages ──')
        lead_at = src.find('            capture["top_undervalued_all_industries"] = [')

        rep.section("G0 — prove the ordering defect")
        gate(rep, "G0.block_found", start != -1, "v4.2 block located")
        gate(rep, "G0.pct_found", end != -1, "v4.1 block located")
        gate(rep, "G0.lead_found", lead_at != -1, "leaderboard build located")
        if FAILED:
            sys.exit(1)
        rep.kv(growth_block_offset=start, leaderboard_offset=lead_at)
        gate(rep, "G0.is_after", start > lead_at,
             "growth assigned AFTER the leaderboard snapshot — the confirmed bug")
        if FAILED:
            rep.warn("ordering already correct — nothing to move")
            sys.exit(1)

        block = src[start:end]
        gate(rep, "G0.block_sane", 'growth_tier' in block and 'in_sp500' in block
             and len(block) > 800, "extracted block is %d chars" % len(block))
        if FAILED:
            sys.exit(1)

        # remove from the wrong place, insert above the leaderboard build
        src2 = src[:start] + src[end:]
        anchor = '            # ── [4] cross-industry leaderboard ─────────────────────────────'
        if src2.count(anchor) != 1:
            anchor = '            capture["top_undervalued_all_industries"] = ['
        gate(rep, "FIX.anchor", src2.count(anchor) == 1, "insert anchor unique")
        if FAILED:
            sys.exit(1)
        src2 = src2.replace(anchor, block + anchor, 1)
        src2 = src2.replace('VERSION = "4.2"', 'VERSION = "4.2.1"', 1)

        LF.write_text(src2)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("block relocated + compile clean (v4.2.1)")

        s3chk = LF.read_text()
        ns, nl = s3chk.find('        # ── v4.2 growth tiers'), s3chk.find('            capture["top_undervalued_all_industries"] = [')
        gate(rep, "FIX.now_before", ns < nl, "growth now computed BEFORE the leaderboard (%d < %d)" % (ns, nl))
        if FAILED:
            sys.exit(1)

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.2.1 (growth/S&P500 computed before leaderboard snapshot).",
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
                bod = z.read("lambda_function.py").decode("utf-8", "replace")
            if bod.find('# ── v4.2 growth tiers') < bod.find('capture["top_undervalued_all_industries"]'):
                settled = True; rep.ok("settled attempt %d" % (i + 1)); break
        gate(rep, "DEPLOY.settled", settled, "reordered artifact live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke + gate the COPIED structure, not just all_rows")
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

        gate(rep, "LIVE.v421", d.get("version") == "4.2.1", "version=%s" % d.get("version"))
        for f in ("growth_tier", "in_sp500", "revenue_growth_yoy", "gm_level"):
            n = sum(1 for x in lead if x.get(f) is not None)
            gate(rep, f"LEAD.{f}", n > 0, "%d of %d leaderboard rows (was 0)" % (n, len(lead)))
        mem = [m for b in bi for m in (b.get("members") or [])]
        n_gt = sum(1 for m in mem if m.get("growth_tier") is not None)
        gate(rep, "MEMBERS.growth_tier", n_gt > 0,
             "%d of %d industry-member rows carry growth_tier" % (n_gt, len(mem)))
        n_all = sum(1 for x in rows if x.get("growth_tier") is not None)
        gate(rep, "LEDGER.growth_tier", n_all > 500, "%d ledger rows" % n_all)

        rep.section("HIGH-growth names now visible on the leaderboard")
        hi = [x for x in lead if x.get("growth_tier") == "HIGH"]
        rep.kv(high_growth_on_leaderboard=len(hi), sp500_on_leaderboard=
               sum(1 for x in lead if x.get("in_sp500") is True))
        for x in hi[:10]:
            rep.log("  %-6s %-22s growth=%-7s tier=%-6s sp500=%-5s cap=%-6s gap=%+.1fpp" % (
                x.get("ticker"), (x.get("industry") or "")[:22],
                ("%.1f%%" % x["revenue_growth_yoy"]) if x.get("revenue_growth_yoy") is not None else "—",
                x.get("growth_tier"), x.get("in_sp500"), x.get("cap_bucket"),
                x.get("capture_gap") or 0))

        rep.section("Additive")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct", "criticality_pctile"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — filters now act on populated fields end to end")


if __name__ == "__main__":
    main()
