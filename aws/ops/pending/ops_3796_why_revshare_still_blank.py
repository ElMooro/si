#!/usr/bin/env python3
"""ops 3796 — why is revenue_share_pct still 0/50 after the wholesale refresh?

3795: criticality_pctile went 0/50 -> 50/50, proving the wholesale refresh WORKS.
But revenue_share_pct stayed 0/50 and dependency_pct 0/3012. So for those two the
values do not exist ON THE SOURCE ROWS for these particular names — a coverage
fact, not a copy bug. Two candidates, and I will not guess between them:

 H1 COVERAGE. The leaderboard is dominated by micro/nano caps (HERE, DOMO, LDI,
    QTTB, RCMT, CGEN...). revenue_share_pct is suppressed for non-USD filers AND
    for industries where USD filers cover <60% of scored names. With the ledger
    now at 3,411 rows, many industries gained thinly-covered names and may have
    dropped BELOW that 60% floor — which would suppress the share for the whole
    industry including its USD filers. That would be the floor working as
    designed but biting far harder than intended at this ledger size.

 H2 ORDERING (again). revenue_share_pct is assigned inside the v4.1 block. If
    _refresh() runs BEFORE that assignment for these rows, source rows would
    hold None at copy time even though the final all_rows shows values.

Test: compare the SOURCE row for each leaderboard ticker against its copy. If
the source also has None, it is H1 (coverage). If the source has a value and the
copy does not, it is H2 (ordering). Writes no engine code.
"""
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3796_why_revshare_still_blank") as rep:
        rep.heading("ops 3796 — H1 coverage vs H2 ordering")

        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                     Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        lead = cap.get("top_undervalued_all_industries") or []
        src = {r.get("ticker"): r for r in rows}
        rep.kv(version=d.get("version"), rows=len(rows), leaderboard=len(lead))

        rep.section("Source row vs copy, for every leaderboard name")
        h2 = 0
        for x in lead[:20]:
            t = x.get("ticker")
            s = src.get(t) or {}
            sv, cv = s.get("revenue_share_pct"), x.get("revenue_share_pct")
            if sv is not None and cv is None:
                h2 += 1
            rep.log("  %-6s source=%-9s copy=%-9s suppressed=%s" % (
                t, sv, cv, str(s.get("revenue_share_suppressed"))[:52]))
        gate(rep, "DIAG.not_ordering", h2 == 0,
             "%d names have a source value but a null copy (0 => NOT an ordering bug)" % h2)

        rep.section("H1 — coverage floor analysis across the whole ledger")
        pub = [r for r in rows if r.get("revenue_share_pct") is not None]
        sup = [r for r in rows if r.get("revenue_share_suppressed")]
        rep.kv(publishing=len(pub), suppressed=len(sup), total=len(rows))
        reasons = {}
        for r in sup:
            k = str(r.get("revenue_share_suppressed"))
            k = "coverage floor" if "coverage" in k else ("non-USD" if "reports in" in k else k[:40])
            reasons[k] = reasons.get(k, 0) + 1
        for k, v in sorted(reasons.items(), key=lambda z: -z[1]):
            rep.log("  %-30s %d" % (k, v))

        rep.section("USD coverage per industry — is the 60% floor the binding constraint?")
        byi = {}
        for r in rows:
            i = r.get("industry") or "?"
            byi.setdefault(i, {"n": 0, "usd": 0, "cov": r.get("revenue_usd_coverage_pct")})
            byi[i]["n"] += 1
            if (r.get("revenue_currency") or "").upper() == "USD":
                byi[i]["usd"] += 1
        below = [(k, v) for k, v in byi.items()
                 if v["cov"] is not None and v["cov"] < 60 and v["n"] >= 5]
        rep.kv(industries=len(byi), industries_below_60pct_floor=len(below))
        for k, v in sorted(below, key=lambda z: -z[1]["n"])[:12]:
            rep.log("  %-34s n=%-4d usd=%-4d coverage=%.0f%%" % (
                k[:34], v["n"], v["usd"], v["cov"] or 0))

        rep.section("What are the leaderboard names' industries?")
        for x in lead[:12]:
            t = x.get("ticker"); s = src.get(t) or {}
            i = s.get("industry")
            v = byi.get(i, {})
            rep.log("  %-6s %-32s usd_coverage=%s%%  ccy=%s" % (
                t, str(i)[:32], v.get("cov"), s.get("revenue_currency")))

        rep.section("VERDICT")
        if h2 == 0:
            rep.warn("H1 CONFIRMED — coverage, not ordering. The copies are faithful; "
                     "the source rows genuinely have no share for these names. With the "
                     "ledger at %d rows, many industries now fall under the 60%% USD "
                     "coverage floor, which suppresses the share for the ENTIRE industry "
                     "including its USD filers." % len(rows))
            rep.log("OPTIONS (not applied here):")
            rep.log("  a) compute the share against USD filers only and REPORT the")
            rep.log("     coverage alongside it, instead of suppressing wholesale;")
            rep.log("  b) lower the floor and label low-coverage shares explicitly;")
            rep.log("  c) leave as-is and accept the column is mostly blank.")
            rep.log("  (a) preserves honesty AND usefulness: the number is then 'share of")
            rep.log("  the USD-filing subset, which is X% of the industry' — true, and")
            rep.log("  the reader can judge it.")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — cause isolated")


if __name__ == "__main__":
    main()
