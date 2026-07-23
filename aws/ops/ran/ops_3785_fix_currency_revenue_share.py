#!/usr/bin/env python3
"""ops 3785 — fix revenue_share_pct: reject non-USD filers (currency, not coverage).

3784 found the real cause, and it was NOT my hypothesis. Coverage was fine (79
of 81 semis carried revenue). The defect is CURRENCY:

    SKHY  revenue_ttm = 97,146,675,000,000  -> 95.28% of the industry
    TSM   revenue_ttm =  3,848,510,949,000  ->  3.77%
    NVDA  revenue_ttm =    215,938,000,000  ->  0.21%

SKHY files in Korean won and TSM in New Taiwan dollars. The engine summed ₩, NT$
and US$ into one denominator, so a single KRW filer absorbed 95% of the industry
and crushed every US name to ~0. NVDA's own revenue was CORRECT (215.9B); the
denominator was poisoned.

⚠️ WHY EVERY GATE PASSED: shares summed to exactly 100% and stayed within
[0,100] — a mixed-currency total is internally consistent and therefore
invisible to bounds and sum checks. Only a MAGNITUDE PLAUSIBILITY check on a
known name caught it (TSM 3.85tn vs an expected 40-200B). This is the same
lesson as the CAISO 1,009MW-vs-120,495MW parser: shape checks pass, only a
magnitude gate catches a unit error. Now doctrine for any cross-company sum.

FIX (conservative by design):
 1. read reportedCurrency from the income statement; persist as revenue_currency.
 2. revenue_ttm_usd is populated ONLY for USD filers. No FX conversion — this
    engine has no rates feed, and inventing one to rescue a metric would be
    worse than reporting less.
 3. revenue_share_pct is computed from USD filers only, and every row states
    revenue_share_scope (how many of the industry's scored names were usable).
 4. If USD filers cover <60% of an industry's scored names, the share is
    SUPPRESSED to null for that whole industry rather than published against a
    denominator that omits its largest members.
 5. Non-USD names get revenue_share_pct = None and a reason string — an honest
    blank instead of a wrong number that looks right.
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
    with report("3785_fix_currency_revenue_share") as rep:
        rep.heading("ops 3785 — revenue share: USD-only denominator")

        src = LF.read_text()

        rep.section("G0")
        gate(rep, "G0.v41", 'VERSION = "4.1"' in src, "engine at v4.1")
        gate(rep, "G0.rev_field", '"revenue_ttm": _rev_ttm,' in src, "revenue persisted")
        gate(rep, "G0.share_block", '_c["revenue_share_pct"] = (round(100.0 * _rv / _tot, 2)' in src,
             "share computation present to patch")
        gate(rep, "G0.no_currency_yet", "reportedCurrency" not in src,
             "currency never read — the confirmed defect")
        if FAILED:
            sys.exit(1)

        # 1. capture reportedCurrency alongside the revenue pick
        rep.section("[1] Read reportedCurrency in evaluate()")
        a1 = '''        _rev_ttm = None
        for _r in inc:
            if (_r.get("revenue") or 0) > 0:
                _rev_ttm = _r["revenue"]
                break'''
        gate(rep, "P1.anchor", src.count(a1) == 1, "revenue pick anchor unique")
        if FAILED:
            sys.exit(1)
        n1 = '''        _rev_ttm = None
        _rev_ccy = None
        for _r in inc:
            if (_r.get("revenue") or 0) > 0:
                _rev_ttm = _r["revenue"]
                # ops 3785: filers report in local currency. SKHY files in KRW
                # (97tn) and TSM in TWD (3.85tn); summing those with USD gave one
                # KRW filer 95% of the semiconductor denominator.
                _rev_ccy = (_r.get("reportedCurrency") or "").upper() or None
                break'''
        src = src.replace(a1, n1, 1)
        src = src.replace('        "revenue_ttm": _rev_ttm,',
                          '        "revenue_ttm": _rev_ttm, "revenue_currency": _rev_ccy,', 1)

        # 2. carry currency through cap_rows
        rep.section("[2] Carry currency through cap_rows")
        a2 = '                    "revenue_ttm": _r.get("revenue_ttm"),'
        gate(rep, "P2.anchor", src.count(a2) == 1, "cap_rows revenue carry unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(a2, a2 + '\n                    "revenue_currency": _r.get("revenue_currency"),', 1)

        # 3. USD-only denominator + coverage floor
        rep.section("[3] USD-only denominator with a coverage floor")
        a3 = '''            _rev_by_ind, _ctr_by_ind = {}, {}
            for _c in cap_rows:
                _i = _c.get("industry")
                if not _i:
                    continue
                _rv = _c.get("revenue_ttm")
                if isinstance(_rv, (int, float)) and _rv > 0:
                    _rev_by_ind[_i] = _rev_by_ind.get(_i, 0.0) + _rv'''
        gate(rep, "P3.anchor", src.count(a3) == 1, "denominator build anchor unique")
        if FAILED:
            sys.exit(1)
        n3 = '''            _rev_by_ind, _ctr_by_ind = {}, {}
            _usd_n, _rev_n = {}, {}
            MIN_USD_COVERAGE = 0.60   # below this the denominator omits majors
            for _c in cap_rows:
                _i = _c.get("industry")
                if not _i:
                    continue
                _rv = _c.get("revenue_ttm")
                _cc = (_c.get("revenue_currency") or "").upper()
                if isinstance(_rv, (int, float)) and _rv > 0:
                    _rev_n[_i] = _rev_n.get(_i, 0) + 1
                    # ONLY USD filers enter the denominator. No FX conversion:
                    # this engine has no rates feed and inventing one to rescue
                    # a metric is worse than publishing less.
                    if _cc == "USD":
                        _usd_n[_i] = _usd_n.get(_i, 0) + 1
                        _rev_by_ind[_i] = _rev_by_ind.get(_i, 0.0) + _rv'''
        src = src.replace(a3, n3, 1)

        a4 = '''                _c["revenue_share_pct"] = (round(100.0 * _rv / _tot, 2)
                                           if (isinstance(_rv, (int, float)) and _rv > 0 and _tot > 0)
                                           else None)'''
        gate(rep, "P4.anchor", src.count(a4) == 1, "share assignment anchor unique")
        if FAILED:
            sys.exit(1)
        n4 = '''                _cc = (_c.get("revenue_currency") or "").upper()
                _un = _usd_n.get(_i, 0); _rn = _rev_n.get(_i, 0)
                _cov = (_un / _rn) if _rn else 0.0
                _c["revenue_usd_coverage_pct"] = round(100.0 * _cov, 1) if _rn else None
                if _cc != "USD":
                    _c["revenue_share_pct"] = None
                    _c["revenue_share_suppressed"] = "filer reports in %s — not summable with USD peers" % (_cc or "unknown")
                elif _cov < MIN_USD_COVERAGE:
                    _c["revenue_share_pct"] = None
                    _c["revenue_share_suppressed"] = ("industry USD coverage %.0f%% < %.0f%% — "
                                                      "denominator would omit major filers" % (
                                                          100 * _cov, 100 * MIN_USD_COVERAGE))
                else:
                    _c["revenue_share_pct"] = (round(100.0 * _rv / _tot, 2)
                                               if (isinstance(_rv, (int, float)) and _rv > 0 and _tot > 0)
                                               else None)
                    _c["revenue_share_suppressed"] = None'''
        src = src.replace(a4, n4, 1)

        # basis must not claim revenue scale when the share is suppressed
        src = src.replace(
            'elif _c.get("revenue_share_pct") is not None and _c["revenue_share_pct"] >= 20:',
            'elif (_c.get("revenue_share_pct") is not None\n'
            '                      and not _c.get("revenue_share_suppressed")\n'
            '                      and _c["revenue_share_pct"] >= 20):', 1)

        src = src.replace('"scored peers (n=%d of %s listed)" % (',
                          '"USD filers among scored peers (n=%d of %s listed)" % (', 1)
        src = src.replace('VERSION = "4.1"', 'VERSION = "4.1.1"', 1)

        LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("USD-only denominator + coverage floor spliced (v4.1.1)")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.1.1 (revenue share USD-only with coverage floor; non-USD filers suppressed).",
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
                if "revenue_share_suppressed" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True; rep.ok("settled attempt %d" % (i + 1)); break
        gate(rep, "DEPLOY.settled", settled, "v4.1.1 live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke + MAGNITUDE gate (the check that caught this)")
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
        gate(rep, "LIVE.v411", d.get("version") == "4.1.1", "version=%s" % d.get("version"))

        semis = [x for x in rows if (x.get("industry") or "").startswith("Semiconductor")]
        pub = [x for x in semis if x.get("revenue_share_pct") is not None]
        rep.kv(semis=len(semis), semis_published=len(pub),
               semis_suppressed=len(semis) - len(pub))

        rep.section("Semis after the fix")
        for x in sorted(pub, key=lambda z: -(z.get("revenue_share_pct") or 0))[:10]:
            rep.log("  %-6s share=%-7s rev=%-16s ccy=%s" % (
                x.get("ticker"), "%.2f%%" % x["revenue_share_pct"],
                "{:,.0f}".format(x.get("revenue_ttm") or 0), x.get("revenue_currency")))
        for x in semis:
            if x.get("revenue_share_suppressed") and x.get("ticker") in ("SKHY", "TSM", "UMC", "ASX"):
                rep.log("  %-6s SUPPRESSED :: %s" % (x.get("ticker"), x["revenue_share_suppressed"]))

        # THE magnitude gate: NVDA must now hold a credible share of US semis
        nv = next((x for x in rows if x.get("ticker") == "NVDA"), None)
        if nv and nv.get("revenue_share_pct") is not None:
            gate(rep, "MAGNITUDE.nvda", nv["revenue_share_pct"] >= 10,
                 "NVDA now %.1f%% of USD-filing semis (was 0.21%% with KRW in the denominator)"
                 % nv["revenue_share_pct"])
        else:
            rep.warn("NVDA share suppressed — check USD coverage for Semiconductors")

        bad = [x for x in rows if x.get("revenue_share_pct") is not None
               and (x.get("revenue_currency") or "").upper() != "USD"]
        gate(rep, "PURITY.usd_only", len(bad) == 0,
             "%d non-USD filers still publishing a share (must be 0)" % len(bad))

        rep.section("Additive")
        for k in ("capture_gap", "catchup_pct", "global_capture_gap", "criticality_pctile"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — mixed-currency denominator fixed; shares are USD-only or blank")


if __name__ == "__main__":
    main()
