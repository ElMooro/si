#!/usr/bin/env python3
"""ops 3812 — verify the v5.0 mispricing verdict actually populates (v5.0.1).

3810 deployed v5.0 but the block raised 'int object has no attribute get', so
every one of 2,393 rows got no verdict. 3811 found the cause and it was NOT my
guess: dark_map is ticker -> RAW OFF-EXCHANGE SHARE COUNT (MLM -> 1532511), not
a per-name record. finra-short's `tickers` was the well-formed one.

v5.0.1 rebuilds the dark-pool lookup from the `board` / `top_accumulation` lists
(dicts carrying dark_pool_pct / dark_accel / state) and TYPE-GUARDS all six map
dereferences, so one unexpected value type can never again take down the whole
verdict for every name.

GATES: every leg must join non-zero, verdicts must discriminate (not all one
class), MISPRICED must be a minority, and the disqualifier legs must actually
fire — a classifier that never rejects anything is not a classifier.
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


def main():
    with report("3812_verify_verdict_populates") as rep:
        rep.heading("ops 3812 — mispricing verdict must populate for real")

        rep.section("Settle v5.0.1")
        settled = False
        for i in range(16):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                u = lam.get_function(FunctionName=FN)["Code"]["Location"]
                with urllib.request.urlopen(u, timeout=90) as r:
                    blob = r.read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    body = z.read("lambda_function.py").decode("utf-8", "replace")
                if "ops 3811: dark_map is ticker -> RAW SHARE COUNT" in body:
                    settled = True
                    rep.ok("v5.0.1 artifact live (attempt %d)" % (i + 1))
                    break
            time.sleep(15)
        gate(rep, "DEPLOY.settled", settled, "type-guard patch in deployed zip")
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
        st = cap.get("stats") or {}
        gate(rep, "LIVE.v501", d.get("version") == "5.0.1", "version=%s" % d.get("version"))
        gate(rep, "LIVE.no_err", "verdict_error" not in cap,
             "err=%s" % cap.get("verdict_error"))
        if "verdict_error" in cap:
            sys.exit(1)

        rep.section("Every leg must join")
        joins = st.get("joins") or {}
        for k in ("revisions", "dark_pool", "short", "pead", "industry_boom"):
            v = joins.get(k) or 0
            gate(rep, f"JOIN.{k}", v > 0, "%d rows" % v)

        rep.section("Verdict distribution")
        vc = st.get("verdict_counts") or {}
        tot = sum(v for v in vc.values() if v)
        for k, v in sorted(vc.items(), key=lambda z: -(z[1] or 0)):
            rep.log("  %-16s %5d  (%.1f%%)" % (k, v, 100.0 * v / max(tot, 1)))
        gate(rep, "VERDICT.populated", tot > 0, "%d rows classified" % tot)
        classes = [k for k, v in vc.items() if k and v]
        gate(rep, "VERDICT.discriminates", len(classes) >= 3,
             "%d classes present: %s" % (len(classes), classes))
        gate(rep, "VERDICT.mispriced_minority",
             0 < (vc.get("MISPRICED") or 0) < len(rows) * 0.15,
             "MISPRICED=%s of %d" % (vc.get("MISPRICED"), len(rows)))
        gate(rep, "VERDICT.rejects_something", (vc.get("VALUE_TRAP") or 0) > 0,
             "VALUE_TRAP=%s — a classifier that never rejects is not a classifier"
             % vc.get("VALUE_TRAP"))

        rep.section("Disqualifier legs must actually fire")
        falling = sum(1 for x in rows if x.get("estimates_falling"))
        decaying = sum(1 for x in rows if x.get("industry_regime") == "DECAYING")
        stale = sum(1 for x in rows if (x.get("gap_days_open") or 0) > 120)
        rep.kv(estimates_falling=falling, industry_decaying=decaying, gap_stale=stale)
        gate(rep, "DISQ.any_fires", (falling + decaying + stale) > 0,
             "%d disqualifier hits total" % (falling + decaying + stale))

        rep.section("MISPRICED book")
        for x in (cap.get("mispriced_book") or [])[:12]:
            rep.log("  %-6s %-24s gap=%+5.1f SI=%4.1f evid=%d :: %s" % (
                x.get("ticker"), (x.get("industry") or "")[:24],
                x.get("capture_gap") or 0, x.get("structural_importance") or 0,
                x.get("verdict_evidence_n") or 0,
                "; ".join(x.get("verdict_confirms") or [])[:52]))

        rep.section("VALUE_TRAP book — cheap for a reason")
        for x in (cap.get("value_trap_book") or [])[:10]:
            rep.log("  %-6s gap=%+5.1f :: %s" % (
                x.get("ticker"), x.get("capture_gap") or 0,
                "; ".join(x.get("verdict_disqualifiers") or [])[:66]))

        rep.section("Additive")
        for k in ("capture_gap", "structural_importance", "catchup_pct",
                  "revenue_share_pct", "growth_tier"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — mispricings separated from value traps, with evidence")


if __name__ == "__main__":
    main()
