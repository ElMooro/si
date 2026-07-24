#!/usr/bin/env python3
"""ops 3788 — v4.2: revenue growth + S&P500 membership (the two fields filters need).

3787 probe found:
  cap_bucket    ALREADY exists, 6 tiers (mega 68 / large 259 / mid 652 /
                small 1094 / micro 729 / nano 9) -> cap filter needs NO engine work.
  growth        DOES NOT EXIST. rpo_yoy covers 68 of 2,811 names (backlog only).
                A "high/medium/low growth" filter built on that would be a filter
                over 2.4% of the board. So growth must be COMPUTED.
  sp500 flag    DOES NOT EXIST anywhere in the row. Source of truth is the
                fundamental-census matrix (`tickers`), the S&P-wide sweep.

WHAT THIS ADDS
 [1] revenue_growth_yoy / _3y_cagr — from the income statement ALREADY fetched
     inside evaluate() (limit=10 annual). Zero extra API calls. Same currency on
     both sides of a ratio, so the SKHY/KRW class of bug cannot recur here — a
     growth RATE is currency-invariant even when the level is not. That is why
     growth is publishable for non-USD filers where revenue_share_pct is not.
 [2] growth_tier — HIGH >=20% / MEDIUM 5-20% / LOW <5% on YoY, with a 3y CAGR
     cross-check; disagreement is surfaced (growth_basis) rather than hidden.
     Tiers are ABSOLUTE, not percentile: "high growth" should mean the same
     thing on every board, not "top third of whatever happens to be scored".
 [3] in_sp500 — joined from the census matrix ticker set; False when the census
     is unreachable is WRONG, so it is None in that case and the filter says so.
 [4] gm_level / roic already exist -> exposed as filterable, no new compute.
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
    with report("3788_growth_and_sp500") as rep:
        rep.heading("ops 3788 — revenue growth + S&P500 membership (v4.2)")

        src = LF.read_text()
        rep.section("G0")
        gate(rep, "G0.v411", 'VERSION = "4.1.1"' in src, "engine at v4.1.1")
        gate(rep, "G0.inc", 'inc = _gj(f"https://financialmodelingprep.com/stable/income-statement' in src,
             "annual income statement (limit=10) already fetched per name")
        gate(rep, "G0.rev_pick", "_rev_ccy = (_r.get(\"reportedCurrency\") or \"\").upper() or None" in src,
             "revenue/currency block present to extend")
        gate(rep, "G0.cap_carry", '                    "revenue_currency": _r.get("revenue_currency"),' in src,
             "cap_rows carry block present")
        gate(rep, "G0.no_growth", "revenue_growth_yoy" not in src, "growth not yet computed")
        if FAILED:
            sys.exit(1)

        # [1] compute growth in evaluate()
        rep.section("[1] Revenue growth from the statement already fetched")
        a1 = '''                _rev_ccy = (_r.get("reportedCurrency") or "").upper() or None
                break'''
        gate(rep, "P1.anchor", src.count(a1) == 1, "anchor unique")
        if FAILED:
            sys.exit(1)
        n1 = '''                _rev_ccy = (_r.get("reportedCurrency") or "").upper() or None
                break
        # ops 3788: revenue growth. A growth RATE is currency-invariant (same
        # units top and bottom), so unlike revenue_share_pct this is publishable
        # for non-USD filers too — the SKHY/KRW failure cannot recur here.
        _rev_series = [x.get("revenue") for x in inc
                       if isinstance(x.get("revenue"), (int, float)) and x.get("revenue") > 0]
        _g_yoy = _g_cagr = None
        if len(_rev_series) >= 2 and _rev_series[1] > 0:
            _g_yoy = round(100.0 * (_rev_series[0] / _rev_series[1] - 1.0), 1)
        if len(_rev_series) >= 4 and _rev_series[3] > 0:
            try:
                _g_cagr = round(100.0 * ((_rev_series[0] / _rev_series[3]) ** (1.0 / 3.0) - 1.0), 1)
            except Exception:
                _g_cagr = None'''
        src = src.replace(a1, n1, 1)
        src = src.replace('        "revenue_ttm": _rev_ttm, "revenue_currency": _rev_ccy,',
                          '        "revenue_ttm": _rev_ttm, "revenue_currency": _rev_ccy,\n'
                          '        "revenue_growth_yoy": _g_yoy, "revenue_growth_3y_cagr": _g_cagr,', 1)

        # [2] carry through cap_rows
        rep.section("[2] Carry growth + gm_level through cap_rows")
        a2 = '                    "revenue_currency": _r.get("revenue_currency"),'
        src = src.replace(a2, a2 + '\n                    "revenue_growth_yoy": _r.get("revenue_growth_yoy"),'
                                   '\n                    "revenue_growth_3y_cagr": _r.get("revenue_growth_3y_cagr"),', 1)

        # [3] growth tier + sp500 join, spliced with the other percentage work
        rep.section("[3] Growth tiers (absolute) + S&P500 join")
        a3 = '        # ── v4.1 "% critical to industry" — three DISTINCT percentages ──'
        gate(rep, "P3.anchor", src.count(a3) == 1, "percent block anchor unique")
        if FAILED:
            sys.exit(1)
        n3 = '''        # ── v4.2 growth tiers + S&P 500 membership ──
        # Tiers are ABSOLUTE, not percentile: "high growth" must mean the same
        # thing on every board rather than "top third of today's sample".
        try:
            _sp = set()
            _cens = _read("data/fundamental-census-matrix.json") or {}
            _ct = _cens.get("tickers")
            if isinstance(_ct, dict):
                _sp = set(_ct.keys())
            elif isinstance(_ct, list):
                _sp = set(x.get("ticker") if isinstance(x, dict) else x for x in _ct)
            for _c in cap_rows:
                _y = _c.get("revenue_growth_yoy")
                _cg = _c.get("revenue_growth_3y_cagr")
                _c["growth_tier"] = (None if _y is None else
                                     "HIGH" if _y >= 20 else
                                     "MEDIUM" if _y >= 5 else "LOW")
                # cross-check: a single YoY can be a base effect; say so when the
                # 3y trend disagrees rather than silently trusting one year.
                if _y is not None and _cg is not None:
                    _c["growth_basis"] = ("YoY+3y agree" if
                                          ((_y >= 20) == (_cg >= 20) and (_y >= 5) == (_cg >= 5))
                                          else "YoY %.0f%% vs 3y CAGR %.0f%% — disagree" % (_y, _cg))
                elif _y is not None:
                    _c["growth_basis"] = "YoY only (needs 4y for CAGR)"
                else:
                    _c["growth_basis"] = None
                # None (not False) when the census is unreachable — absence of
                # evidence is not evidence of absence.
                _c["in_sp500"] = (_c["ticker"] in _sp) if _sp else None
            capture["stats"]["with_growth"] = sum(
                1 for c in cap_rows if c.get("revenue_growth_yoy") is not None)
            capture["stats"]["sp500_members"] = sum(
                1 for c in cap_rows if c.get("in_sp500") is True)
            capture["growth_note"] = (
                "revenue_growth_yoy is latest annual revenue vs the prior year, from the "
                "same income statement used elsewhere — no extra data call. Growth is a "
                "RATIO, so it is currency-invariant and IS published for non-USD filers "
                "even where revenue_share_pct is suppressed. Tiers are absolute "
                "(HIGH>=20%, MEDIUM 5-20%, LOW<5%), not percentile, so they mean the same "
                "thing on every board. growth_basis flags when a single YoY disagrees with "
                "the 3-year CAGR — often a base effect or a one-off, and worth seeing.")
            diag.append("v4.2: growth=%d sp500=%d" % (
                capture["stats"]["with_growth"], capture["stats"]["sp500_members"]))
        except Exception as _ge:
            capture["growth_error"] = str(_ge)[:250]
            diag.append("v4.2 FAILED: %s" % str(_ge)[:150])

''' + a3
        src = src.replace(a3, n3, 1)

        # expose on leaderboard + member rows
        src = src.replace('"dependency_pct", "criticality_basis",\n                  "criticality_pctile")}',
                          '"dependency_pct", "criticality_basis",\n'
                          '                  "criticality_pctile", "revenue_growth_yoy",\n'
                          '                  "revenue_growth_3y_cagr", "growth_tier", "growth_basis",\n'
                          '                  "in_sp500", "gm_level")}', 1)
        src = src.replace('"criticality_pctile", "criticality_basis")}',
                          '"criticality_pctile", "criticality_basis",\n'
                          '                                  "revenue_growth_yoy", "growth_tier",\n'
                          '                                  "in_sp500", "gm_level")}', 1)
        src = src.replace('VERSION = "4.1.1"', 'VERSION = "4.2"', 1)

        LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("v4.2 spliced + compile clean")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.2 (revenue growth tiers, S&P500 membership, cap tiers).",
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
                if "revenue_growth_yoy" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True; rep.ok("settled attempt %d" % (i + 1)); break
        gate(rep, "DEPLOY.settled", settled, "v4.2 live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke + verify")
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
        gate(rep, "LIVE.v42", d.get("version") == "4.2", "version=%s" % d.get("version"))
        gate(rep, "LIVE.no_err", "growth_error" not in cap, "err=%s" % cap.get("growth_error"))
        rep.kv(scored=st.get("scored"), with_growth=st.get("with_growth"),
               sp500_members=st.get("sp500_members"))
        gate(rep, "LIVE.growth", (st.get("with_growth") or 0) > 500,
             "growth on %s names (rpo_yoy covered only 68)" % st.get("with_growth"))
        gate(rep, "LIVE.sp500", (st.get("sp500_members") or 0) > 200,
             "%s S&P500 members flagged" % st.get("sp500_members"))

        tiers = {}
        for x in rows:
            tiers[x.get("growth_tier")] = tiers.get(x.get("growth_tier"), 0) + 1
        rep.section("Growth tier distribution")
        for k, v in sorted(tiers.items(), key=lambda z: -z[1]):
            rep.log("  %-8s %d" % (k, v))
        gate(rep, "SANITY.tiers_spread", len([k for k in tiers if k]) >= 3,
             "all three tiers populated — not a degenerate split")

        # growth must survive for non-USD filers (the whole point of a ratio)
        nonusd = [x for x in rows if (x.get("revenue_currency") or "").upper() not in ("USD", "")
                  and x.get("revenue_growth_yoy") is not None]
        gate(rep, "SANITY.nonusd_growth", len(nonusd) > 0,
             "%d non-USD filers publish growth (share is suppressed for them)" % len(nonusd))

        rep.section("Sample")
        for t in ("NVDA", "TSM", "AMD", "MSFT", "AVGO"):
            x = next((z for z in rows if z.get("ticker") == t), None)
            if x:
                rep.log("  %-5s yoy=%-8s cagr3y=%-8s tier=%-7s sp500=%-5s basis=%s" % (
                    t, x.get("revenue_growth_yoy"), x.get("revenue_growth_3y_cagr"),
                    x.get("growth_tier"), x.get("in_sp500"), (x.get("growth_basis") or "")[:40]))

        rep.section("Additive")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct", "criticality_pctile"):
            gate(rep, f"ADDITIVE.{k}", any(z.get(k) is not None for z in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — growth + S&P500 live; filters can now be built on real fields")


if __name__ == "__main__":
    main()
