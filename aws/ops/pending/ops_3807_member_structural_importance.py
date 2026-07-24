#!/usr/bin/env python3
"""ops 3807 — structural_importance must reach the industry drill-downs.

3806 shipped "how crucial to industry" with 1,847/1,847 ledger coverage and
50/50 on the leaderboard, but its own gate caught COPY.members = 0 of 1,847.

CAUSE: member rows are refreshed through an explicit allow-list (_mem_extra)
that never included the new field. Ordering was already correct — the refresh
runs at offset 68,641, well after structural_importance is assigned at 64,936 —
so this is purely the allow-list lagging the engine.

That list is now the recurring weak point in this arc: it hid growth_tier/
in_sp500 (3790), then revenue_share_pct/dependency_pct/criticality_basis (3794),
now structural_importance. So v4.4.1 adds the headline score AND its supporting
legs (revenue_rank_in_industry, margin_premium_vs_industry, rd_premium_vs_
industry, structural_basis, centrality_mapped) rather than just the one field
that was reported — otherwise the hover explanation would be blank even once the
number appeared.

GATE: member rows must carry the score AND the basis string, and the leaderboard
must not regress.
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
    with report("3807_member_structural_importance") as rep:
        rep.heading("ops 3807 — industry drill-downs must match the leaderboard")

        rep.section("Settle v4.4.1")
        settled = False
        for i in range(16):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                u = lam.get_function(FunctionName=FN)["Code"]["Location"]
                with urllib.request.urlopen(u, timeout=90) as r:
                    blob = r.read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    body = z.read("lambda_function.py").decode("utf-8", "replace")
                if '"structural_importance", "structural_basis"' in body:
                    settled = True
                    rep.ok("v4.4.1 artifact live (attempt %d)" % (i + 1))
                    break
            time.sleep(15)
        gate(rep, "DEPLOY.settled", settled, "extended allow-list in deployed zip")
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
        bi = cap.get("by_industry") or []
        mem = [m for b in bi for m in (b.get("members") or []) if m]
        gate(rep, "LIVE.v441", d.get("version") == "4.4.1", "version=%s" % d.get("version"))

        rep.section("THE FIX — member rows")
        for f in ("structural_importance", "structural_basis",
                  "revenue_rank_in_industry", "margin_premium_vs_industry"):
            n = sum(1 for m in mem if m.get(f) is not None)
            gate(rep, f"MEMBER.{f}", n > 0, "%d of %d member rows (was 0)" % (n, len(mem)))

        rep.section("No regression on the leaderboard or ledger")
        nl = sum(1 for x in lead if x.get("structural_importance") is not None)
        na = sum(1 for x in rows if x.get("structural_importance") is not None)
        gate(rep, "NOREG.leaderboard", nl > 0, "%d of %d" % (nl, len(lead)))
        gate(rep, "NOREG.ledger", na > len(rows) * 0.9, "%d of %d" % (na, len(rows)))

        rep.section("Drill-down parity — same ticker, same number everywhere")
        src = {x.get("ticker"): x for x in rows}
        checked = mismatch = 0
        for m in mem[:400]:
            t = m.get("ticker")
            s = src.get(t)
            if not s or s.get("structural_importance") is None:
                continue
            checked += 1
            if m.get("structural_importance") != s.get("structural_importance"):
                mismatch += 1
                if mismatch <= 5:
                    rep.log("  MISMATCH %-6s member=%s ledger=%s" % (
                        t, m.get("structural_importance"), s.get("structural_importance")))
        rep.kv(parity_checked=checked, mismatches=mismatch)
        gate(rep, "PARITY.consistent", mismatch == 0,
             "%d of %d member rows match the ledger exactly" % (checked - mismatch, checked))

        rep.section("Sample drill-down")
        for b in bi[:3]:
            ms = [m for m in (b.get("members") or []) if m and m.get("structural_importance") is not None]
            if not ms:
                continue
            rep.log("  %s (%d scored)" % (b.get("industry"), b.get("n_scored") or 0))
            for m in sorted(ms, key=lambda z: -(z.get("structural_importance") or 0))[:4]:
                rep.log("     %-6s crucial=%5.1f  %s" % (
                    m.get("ticker"), m.get("structural_importance") or 0,
                    (m.get("structural_basis") or "")[:52]))

        rep.section("Additive")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct", "growth_tier"):
            gate(rep, f"ADDITIVE.{k}", any(m.get(k) is not None for m in mem),
                 "member rows still carry it")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — drill-downs now match the leaderboard")


if __name__ == "__main__":
    main()
