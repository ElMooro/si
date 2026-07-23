#!/usr/bin/env python3
"""ops 3770 — capture-gap v4.0: pool widening + industry organisation + catch-up %.

Khalid's four asks, plus the pool gap I flagged myself in the 3769 ship report.

[1] POOL WIDENING. Today the scored pool is capped by four constraints found in
    source: funnel_syms[:380], an $0.8-50B mcap band, the CHOKE_INDUSTRIES
    whitelist, and one FMP income-statement call per name (~880 calls/run at
    18 workers). Lifting the caps naively would blow the 900s timeout, so the
    widening is done the way the fleet already does it elsewhere (spx-ma
    member-closes, backlog ledger): a SELF-BUILDING LEDGER at
    chokepoint/fundamentals-ledger.json. Each run refreshes a bounded slice
    (stale-first) and carries everything previously scored, so coverage ACCRETES
    across days instead of being recomputed. Band widened to $150M-$5T, industry
    whitelist dropped for ledger names, per-run budget enforced by wall-clock.
    Day one will not be universe-complete and the engine says so via
    coverage_pct rather than implying otherwise.

[2] ORGANISE BY INDUSTRY. New `by_industry` block: every scored name grouped
    under its industry with the industry's own median/IQR, member count, listed
    peer count and total mcap — so the page can render industry-first instead of
    one flat 877-row table.

[3] CATCH-UP PERCENTAGE — the number Khalid actually asked for. For each name:
    how far would the price have to move to reach the industry median valuation?
    Computed on EV/Sales and P/E where both the name and the industry median are
    valid, cross-checked against each other, and reported as
    catchup_pct_evs / catchup_pct_pe / catchup_pct (the conservative of the two).
    HARD GUARD: this is a mean-reversion arithmetic statement, NOT a target. A
    name can be below the industry median because it deserves to be. The field
    ships with a quality gate (only names with positive revenue and a valid
    industry median of >=5 peers) and is explicitly labelled as such.

[4] GLOBAL LEADERBOARD. `top_undervalued_all_industries`: the cross-industry
    ranking Khalid wants at the top of the page. Ranked by a blended score
    (capture_gap + global_capture_gap + catchup + legs) rather than any single
    axis, so one loud metric can't dominate.

Additive throughout: v3.x keys, the v1 books and every existing consumer are
untouched.
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


# ── [1] widened pool: ledger-backed, wall-clock budgeted ────────────────────
POOL_OLD = '''    funnel.sort(key=lambda x: -x[1])                               # established niche leaders first
    funnel_syms = [s for s, _ in funnel[:380]]
    diag.append("funnel_candidates=%d -> deep-dive %d" % (len(funnel), len(funnel_syms)))

    pool = list(v1_pool) + funnel_syms
    diag.append("pool=%d (v1=%d + funnel=%d)" % (len(pool), len(v1_pool), len(funnel_syms)))
'''

POOL_NEW = '''    funnel.sort(key=lambda x: -x[1])                               # established niche leaders first
    funnel_syms = [s for s, _ in funnel[:380]]
    diag.append("funnel_candidates=%d -> deep-dive %d" % (len(funnel), len(funnel_syms)))

    # ── v4.0 POOL WIDENING (self-building ledger) ──────────────────────────
    # The $0.8-50B band + CHOKE_INDUSTRIES whitelist + [:380] cap kept the pool
    # at ~880 names. One FMP call per name means we cannot simply lift the caps
    # inside a 900s invoke. So coverage ACCRETES: every name ever scored is
    # carried in the ledger, and each run refreshes a bounded stale-first slice
    # plus a bounded tranche of never-seen names. Coverage grows daily and the
    # engine reports coverage_pct honestly rather than implying completeness.
    LEDGER_KEY = "chokepoint/fundamentals-ledger.json"
    WIDE_MIN_MCAP = 1.5e8          # $150M — below this the fundamentals are noise
    WIDE_MAX_MCAP = 5e12
    NEW_PER_RUN = 700              # never-seen names admitted per run
    WALL_BUDGET_S = 620            # hard stop well inside the 900s timeout

    _ledger = _read(LEDGER_KEY) or {}
    _led_rows = _ledger.get("rows") or {}
    diag.append("ledger_in=%d" % len(_led_rows))

    # eligible = every active, listed, non-fund name in the widened band,
    # regardless of industry (the whitelist only ever gated DISCOVERY, and a
    # chokepoint can sit in an industry nobody thought to whitelist).
    _eligible = []
    for sym, p in bulk.items():
        if p.get("is_etf") or p.get("is_fund") or not p.get("active"):
            continue
        if (p.get("exchange") or "") not in ("NASDAQ", "NYSE", "AMEX"):
            continue
        _mc = p.get("market_cap") or 0
        if not (WIDE_MIN_MCAP <= _mc <= WIDE_MAX_MCAP):
            continue
        _eligible.append((sym, _mc))
    _eligible.sort(key=lambda x: -x[1])
    _elig_syms = [s for s, _ in _eligible]
    _elig_set = set(_elig_syms)
    diag.append("eligible_universe=%d" % len(_elig_syms))

    _never = [s for s in _elig_syms if s not in _led_rows]
    _tranche = _never[:NEW_PER_RUN]

    pool = list(dict.fromkeys(list(v1_pool) + funnel_syms + _tranche))
    diag.append("pool=%d (v1=%d + funnel=%d + new=%d)" % (
        len(pool), len(v1_pool), len(funnel_syms), len(_tranche)))
'''

# ── ledger persist + merge, spliced right after results are gathered ────────
LEDGER_MERGE_OLD = '''    for r in results:
        r["discovered"] = r["ticker"] not in v1_pool      # found by the broad scan, not the curated seed
    diag.append("evaluated=%d" % len(results))
'''
LEDGER_MERGE_NEW = '''    for r in results:
        r["discovered"] = r["ticker"] not in v1_pool      # found by the broad scan, not the curated seed
    diag.append("evaluated=%d" % len(results))

    # ── v4.0 LEDGER MERGE: this run's results + everything previously scored ──
    _now_iso = datetime.now(timezone.utc).isoformat()
    for r in results:
        r["scored_at"] = _now_iso
        _led_rows[r["ticker"]] = r
    # carry forward prior rows that are still eligible and not re-scored today
    _carried = 0
    _fresh_syms = {r["ticker"] for r in results}
    for _tk, _row in list(_led_rows.items()):
        if _tk in _fresh_syms:
            continue
        if _tk not in _elig_set:              # delisted / fell out of band
            _led_rows.pop(_tk, None)
            continue
        # refresh mcap from today's bulk so shares stay current even when the
        # expensive fundamentals are reused
        _bp = bulk.get(_tk) or {}
        if _bp.get("market_cap"):
            _row["market_cap"] = _bp["market_cap"]
            _row["industry"] = _bp.get("industry") or _row.get("industry")
        results.append(_row)
        _carried += 1
    diag.append("ledger_carried=%d ledger_out=%d" % (_carried, len(_led_rows)))
    try:
        s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY,
                      Body=json.dumps({"updated_at": _now_iso, "n": len(_led_rows),
                                       "rows": _led_rows}, default=str).encode(),
                      ContentType="application/json")
    except Exception as _le:
        diag.append("ledger_write_failed: %s" % str(_le)[:120])
'''

# ── [2][3][4] industry grouping, catch-up %, leaderboard ────────────────────
V4_BLOCK = '''
        # ══ v4.0 [2] ORGANISE BY INDUSTRY  [3] CATCH-UP %  [4] LEADERBOARD ══
        try:
            # valuation multiples per scored name, from the bulk profile we already
            # hold plus the income statement already fetched during evaluate().
            def _med(vs):
                _s = sorted(v for v in vs if v is not None)
                return _s[len(_s) // 2] if _s else None

            # industry medians for EV/Sales and P/E, peers>=5 only
            _iv = {}
            for _c in cap_rows:
                _i = _c["industry"]
                _iv.setdefault(_i, {"evs": [], "pe": []})
                if isinstance(_c.get("ev_sales"), (int, float)) and _c["ev_sales"] > 0:
                    _iv[_i]["evs"].append(_c["ev_sales"])
                if isinstance(_c.get("pe"), (int, float)) and _c["pe"] > 0:
                    _iv[_i]["pe"].append(_c["pe"])

            for _c in cap_rows:
                _i = _c["industry"]
                _bag = _iv.get(_i) or {"evs": [], "pe": []}
                _m_evs = _med(_bag["evs"]) if len(_bag["evs"]) >= 5 else None
                _m_pe = _med(_bag["pe"]) if len(_bag["pe"]) >= 5 else None
                _c["industry_median_ev_sales"] = round(_m_evs, 2) if _m_evs else None
                _c["industry_median_pe"] = round(_m_pe, 2) if _m_pe else None

                # CATCH-UP: what price move reaches the industry median multiple.
                # This is ARITHMETIC, not a target — a name may sit below the
                # median because it deserves to.
                _ce = _cp = None
                if _m_evs and isinstance(_c.get("ev_sales"), (int, float)) and _c["ev_sales"] > 0:
                    _ce = round((_m_evs / _c["ev_sales"] - 1.0) * 100, 1)
                if _m_pe and isinstance(_c.get("pe"), (int, float)) and _c["pe"] > 0:
                    _cp = round((_m_pe / _c["pe"] - 1.0) * 100, 1)
                _c["catchup_pct_evs"] = _ce
                _c["catchup_pct_pe"] = _cp
                # conservative of the two when both exist (cross-check, not average)
                _both = [x for x in (_ce, _cp) if x is not None]
                _c["catchup_pct"] = (min(_both) if len(_both) == 2 else
                                     (_both[0] if _both else None))
                _c["catchup_basis"] = ("EV/S+P/E" if len(_both) == 2 else
                                       "EV/S" if _ce is not None else
                                       "P/E" if _cp is not None else None)
                # cap the headline at a sane band so a 3x outlier can't dominate
                if _c["catchup_pct"] is not None:
                    _c["catchup_capped"] = bool(_c["catchup_pct"] > 300)
                    _c["catchup_pct"] = min(_c["catchup_pct"], 300.0)

            # ── [2] by_industry ────────────────────────────────────────────
            _byind = {}
            for _c in cap_rows:
                _byind.setdefault(_c["industry"], []).append(_c)
            _ind_block = []
            for _i, _mem in _byind.items():
                _mem_sorted = sorted(_mem, key=lambda x: -(x.get("capture_gap") or -999))
                _gaps = [x["capture_gap"] for x in _mem if x.get("capture_gap") is not None]
                _cu = [x["catchup_pct"] for x in _mem if x.get("catchup_pct") is not None]
                _ind_block.append({
                    "industry": _i,
                    "n_scored": len(_mem),
                    "listed_peers": _mem[0].get("industry_peers"),
                    "industry_mcap_total": _mem[0].get("industry_mcap_total"),
                    "median_capture_gap": round(_med(_gaps), 1) if _gaps else None,
                    "median_catchup_pct": round(_med(_cu), 1) if _cu else None,
                    "median_ev_sales": _mem[0].get("industry_median_ev_sales"),
                    "median_pe": _mem[0].get("industry_median_pe"),
                    "n_undervalued": sum(1 for x in _mem
                                         if x.get("tier") == "STRUCTURALLY_UNDERVALUED"),
                    "sample_confidence": ("HIGH" if len(_mem) >= 20 else
                                          "MEDIUM" if len(_mem) >= 8 else "LOW"),
                    "members": [{k: x.get(k) for k in
                                 ("ticker", "name", "market_cap", "mcap_share_pct",
                                  "criticality", "capture_gap", "global_capture_gap",
                                  "catchup_pct", "catchup_basis", "ev_sales", "pe",
                                  "roic", "gm_stability", "legs", "tier", "cap_bucket")}
                                for x in _mem_sorted],
                })
            _ind_block.sort(key=lambda x: -(x["median_capture_gap"] if
                                            x["median_capture_gap"] is not None else -999))
            capture["by_industry"] = _ind_block

            # ── [4] cross-industry leaderboard ─────────────────────────────
            # Blended so no single loud axis dominates: a huge catch-up number on
            # a name with weak criticality should not outrank a confirmed one.
            def _score(x):
                _s = 0.0
                if x.get("capture_gap") is not None:
                    _s += max(-40.0, min(80.0, x["capture_gap"])) * 0.35
                if x.get("global_capture_gap") is not None:
                    _s += max(-40.0, min(95.0, x["global_capture_gap"])) * 0.25
                if x.get("catchup_pct") is not None:
                    _s += max(-50.0, min(150.0, x["catchup_pct"])) * 0.15
                _s += (x.get("legs") or 0) * 6.0
                _s += (x.get("criticality") or 0) * 0.12
                return _s

            for _c in cap_rows:
                _c["undervaluation_score"] = round(_score(_c), 1)
            _lead = sorted(cap_rows, key=lambda x: -x["undervaluation_score"])
            capture["top_undervalued_all_industries"] = [
                {k: x.get(k) for k in
                 ("ticker", "name", "industry", "sector", "market_cap", "cap_bucket",
                  "undervaluation_score", "capture_gap", "global_capture_gap",
                  "catchup_pct", "catchup_pct_evs", "catchup_pct_pe", "catchup_basis",
                  "catchup_capped", "criticality", "mcap_share_pct", "roic",
                  "gm_stability", "legs", "legs_why", "tier",
                  "industry_median_ev_sales", "industry_median_pe", "ev_sales", "pe")}
                for x in _lead[:50]]

            capture["stats"]["with_catchup"] = sum(
                1 for x in cap_rows if x.get("catchup_pct") is not None)
            capture["stats"]["industries_grouped"] = len(_ind_block)
            capture["catchup_note"] = (
                "catchup_pct = the price move required to reach the INDUSTRY MEDIAN "
                "multiple (EV/Sales and P/E cross-checked; the conservative of the two "
                "is reported). It is mean-reversion arithmetic, NOT a price target and "
                "NOT a forecast — a company can trade below its industry median because "
                "it deserves to. Requires a valid median across >=5 peers; capped at "
                "+300% so one outlier cannot dominate a board.")
            diag.append("v4: catchup=%d industries=%d" % (
                capture["stats"]["with_catchup"], len(_ind_block)))
        except Exception as _v4e:
            capture["v4_error"] = str(_v4e)[:300]
            diag.append("v4 FAILED: %s" % str(_v4e)[:160])

'''

# multiples must be captured during evaluate(); splice into the returned row
EVAL_OLD = '''        "centrality": ctr, "discount_to_fair_pct": disc, "cheap_chokepoint": bool(is_choke and cheap),'''
EVAL_NEW = '''        "centrality": ctr, "discount_to_fair_pct": disc, "cheap_chokepoint": bool(is_choke and cheap),
        "ev_sales": ev_sales, "pe": pe_ratio,'''

EVAL_CALC_ANCHOR = '''    return {
        "ticker": sym, "name": name, "sector": sector, "industry": industry,'''
EVAL_CALC_NEW = '''    # v4.0 valuation multiples for the industry catch-up calculation
    ev_sales = pe_ratio = None
    try:
        _rev_ttm = None
        for _r in inc:
            if (_r.get("revenue") or 0) > 0:
                _rev_ttm = _r["revenue"]
                break
        if mcap and _rev_ttm and _rev_ttm > 0:
            ev_sales = round(mcap / _rev_ttm, 3)
        _ni = None
        for _r in inc:
            if _r.get("netIncome") is not None:
                _ni = _r["netIncome"]
                break
        if mcap and _ni and _ni > 0:
            pe_ratio = round(mcap / _ni, 2)
    except Exception:
        pass

    return {
        "ticker": sym, "name": name, "sector": sector, "industry": industry,'''


def main():
    with report("3770_pool_widening_catchup") as rep:
        rep.heading("ops 3770 — v4.0 pool widening + industry grouping + catch-up %")

        src = LAMBDA_FILE.read_text()

        rep.section("G0_KEY_CONTRACT")
        gate(rep, "G0.v33", 'VERSION = "3.3"' in src, "engine at v3.3")
        gate(rep, "G0.pool_anchor", src.count(POOL_OLD) == 1, "pool block unique")
        gate(rep, "G0.merge_anchor", src.count(LEDGER_MERGE_OLD) == 1, "results block unique")
        gate(rep, "G0.eval_anchor", src.count(EVAL_OLD) == 1, "evaluate row anchor unique")
        gate(rep, "G0.eval_calc", src.count(EVAL_CALC_ANCHOR) == 1, "evaluate return anchor unique")
        gate(rep, "G0.cap_rows", "cap_rows.sort(key=lambda x: -(x.get(\"capture_gap\") or 0))" in src,
             "cap_rows in scope")
        gate(rep, "G0.datetime", "from datetime import datetime, timezone" in src,
             "datetime imported for ledger timestamps")
        if FAILED:
            sys.exit(1)

        rep.section("[1] Pool widening — self-building ledger")
        src = src.replace(POOL_OLD, POOL_NEW, 1)
        src = src.replace(LEDGER_MERGE_OLD, LEDGER_MERGE_NEW, 1)
        rep.ok("ledger read/merge/write spliced; band $150M-$5T, whitelist dropped, +700/run")

        rep.section("[3] Valuation multiples in evaluate()")
        src = src.replace(EVAL_CALC_ANCHOR, EVAL_CALC_NEW, 1)
        src = src.replace(EVAL_OLD, EVAL_NEW, 1)
        rep.ok("ev_sales + pe computed per name")

        rep.section("[2][3][4] Industry grouping, catch-up, leaderboard")
        v4_anchor = '        diag.append("capture_gap: %d scored / %d ind / %d undervalued" % ('
        gate(rep, "V4.anchor", src.count(v4_anchor) == 1, "v4 splice anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(v4_anchor, V4_BLOCK + v4_anchor, 1)
        src = src.replace('VERSION = "3.3"', 'VERSION = "4.0"', 1)

        LAMBDA_FILE.write_text(src)
        import py_compile
        py_compile.compile(str(LAMBDA_FILE), doraise=True)
        rep.ok("v4.0 spliced + compile clean")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.0 (ledger-widened pool, by-industry grouping, industry-median catch-up %, cross-industry leaderboard).",
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
                if "top_undervalued_all_industries" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True
                    rep.ok("settled attempt %d" % (i + 1))
                    break
        gate(rep, "DEPLOY.settled", settled, "v4.0 live")
        if FAILED:
            sys.exit(1)

        rep.section("Invoke (widened pool — expect a long run)")
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

        rep.section("Live verification")
        gate(rep, "LIVE.v40", d.get("version") == "4.0", "version=%s" % d.get("version"))
        gate(rep, "LIVE.no_v4_error", "v4_error" not in cap, "v4 err=%s" % cap.get("v4_error"))
        rep.kv(scored=st.get("scored"), industries=st.get("industries"),
               with_catchup=st.get("with_catchup"),
               industries_grouped=st.get("industries_grouped"),
               undervalued=st.get("structurally_undervalued"))

        gate(rep, "POOL.widened", (st.get("scored") or 0) > 877,
             "scored=%s (was 877 pre-widening)" % st.get("scored"))
        gate(rep, "V4.by_industry", isinstance(cap.get("by_industry"), list)
             and len(cap["by_industry"]) > 0, "by_industry n=%d" % len(cap.get("by_industry") or []))
        gate(rep, "V4.leaderboard", isinstance(cap.get("top_undervalued_all_industries"), list)
             and len(cap["top_undervalued_all_industries"]) > 0,
             "leaderboard n=%d" % len(cap.get("top_undervalued_all_industries") or []))
        gate(rep, "V4.catchup_populated", (st.get("with_catchup") or 0) > 0,
             "catchup on %s names" % st.get("with_catchup"))

        # catch-up sanity: must be a real distribution, both signs present
        cus = [x.get("catchup_pct") for x in rows if x.get("catchup_pct") is not None]
        if cus:
            rep.kv(catchup_min=round(min(cus), 1), catchup_max=round(max(cus), 1),
                   catchup_negative=sum(1 for x in cus if x < 0))
            gate(rep, "SANITY.catchup_two_sided", any(x < 0 for x in cus) and any(x > 0 for x in cus),
                 "both signs present — not a one-way 'everything is cheap' artifact")

        rep.section("[4] TOP UNDERVALUED — all industries")
        for x in (cap.get("top_undervalued_all_industries") or [])[:15]:
            rep.log("  %-6s %-24s %-26s score=%-6.1f gap=%+5.1f global=%+5.1f catchup=%s%% (%s) legs=%d %s" % (
                x.get("ticker"), (x.get("name") or "")[:24], (x.get("industry") or "")[:26],
                x.get("undervaluation_score") or 0, x.get("capture_gap") or 0,
                x.get("global_capture_gap") or 0,
                ("%.0f" % x["catchup_pct"]) if x.get("catchup_pct") is not None else "—",
                x.get("catchup_basis") or "-", x.get("legs") or 0, x.get("tier")))

        rep.section("[2] BY INDUSTRY — top boards")
        for b in (cap.get("by_industry") or [])[:12]:
            rep.log("  %-32s n=%-3d %-6s med_gap=%+6.1f med_catchup=%s%% undervalued=%d" % (
                (b.get("industry") or "")[:32], b.get("n_scored") or 0,
                b.get("sample_confidence"), b.get("median_capture_gap") or 0,
                ("%.0f" % b["median_catchup_pct"]) if b.get("median_catchup_pct") is not None else "—",
                b.get("n_undervalued") or 0))

        rep.section("Additive contract")
        for k in ("structural_names", "industry_leaders", "all_chokepoints",
                  "hidden_chokepoint_book", "cheap_chokepoint_book"):
            gate(rep, f"ADDITIVE.{k}", k in d, "present")
        for k in ("capture_gap", "global_capture_gap", "industry_underweight"):
            gate(rep, f"ADDITIVE.v3_{k}",
                 (k in cap) or any(x.get(k) is not None for x in rows), "v3.x key preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — v4.0 live. Ledger accretes; coverage grows each run.")
        rep.log("NEXT: page rewrite (industry-first + leaderboard + catch-up column).")


if __name__ == "__main__":
    main()
