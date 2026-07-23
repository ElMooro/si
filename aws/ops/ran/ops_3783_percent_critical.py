#!/usr/bin/env python3
"""ops 3783 — "% critical to its industry", measured honestly (v4.1).

KHALID: "add percentage wise how that company is critical to that industry."

PRESSURE-TEST FIRST. `criticality` is already 0-100, but it is a QUALITY
COMPOSITE (0.30 margin_level + 0.22 margin_STABILITY + 0.20 ROIC + 0.13 R&D +
0.15 centrality). It answers "how good is this business", NOT "what share of
this industry depends on this company". Rendering it with a % sign would be
the single most misleading number on the platform: a 71.4 would read as
"71% of the industry depends on TSMC", which is not what it measures.

So this ships THREE distinct percentages, each labelled for what it actually is:

 [1] revenue_share_pct — the closest thing to a true dependency share we can
     measure: this company's TTM revenue as a % of all SCORED peers' revenue in
     its industry. Revenue is already fetched inside evaluate() (line ~191) and
     then THROWN AWAY; this persists it. Zero extra API calls.
     ⚠️ DENOMINATOR HONESTY: bulk profiles carry no revenue, so the denominator
     is scored peers, NOT the full listed industry. Every row therefore also
     carries revenue_coverage_pct (scored names / listed peers) and the field is
     labelled "of scored peers" everywhere it renders. Overstating this as
     whole-industry share would be a fabrication.

 [2] criticality_pctile — already computed; it IS a legitimate percentage:
     "ranks above X% of scored peers on business quality". Relabelled, not new.

 [3] dependency_pct — supply-chain edges pointing at this company as supplier,
     as a % of all such edges in its industry. ONLY populated where the curated
     graph actually covers the sector; null elsewhere rather than guessed. The
     graph is sparse by design, so this will be null for most names and that is
     the correct output.

INTERPRETATION FIELD: `criticality_basis` names which of the three is
strongest, so a reader is never left to assume the composite is a share.
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
    with report("3783_percent_critical") as rep:
        rep.heading("ops 3783 — % critical to industry (revenue share + pctile + dependency)")

        src = LF.read_text()

        rep.section("G0 — verify what exists before consuming it")
        gate(rep, "G0.rev_computed", "_rev_ttm = None" in src,
             "evaluate() already derives TTM revenue (currently discarded)")
        gate(rep, "G0.rev_not_persisted", '"revenue_ttm"' not in src,
             "revenue NOT yet a row field — this is the gap")
        gate(rep, "G0.centrality", "ctr = centrality.get(sym, 0)" in src,
             "supply-chain edge count per symbol in scope")
        gate(rep, "G0.cap_rows", '"ev_sales": _r.get("ev_sales"),' in src,
             "cap_rows field-carry block present (3771 lesson: must extend it)")
        gate(rep, "G0.v401", 'VERSION = "4.0.1"' in src, "engine at v4.0.1")
        if FAILED:
            sys.exit(1)

        # ── 1. persist revenue + centrality out of evaluate() ─────────────
        rep.section("[1] Persist revenue_ttm from evaluate()")
        a1 = '        "ev_sales": ev_sales, "pe": pe_ratio,'
        gate(rep, "P1.anchor", src.count(a1) == 1, "evaluate return anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(a1, a1 + '\n        "revenue_ttm": _rev_ttm,', 1)

        # ── 2. carry it through cap_rows (the 3771 failure mode) ──────────
        rep.section("[2] Carry through cap_rows")
        a2 = '                    "ev_sales": _r.get("ev_sales"),\n                    "pe": _r.get("pe"),'
        gate(rep, "P2.anchor", src.count(a2) == 1, "cap_rows carry anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(a2, a2 + '\n                    "revenue_ttm": _r.get("revenue_ttm"),'
                                   '\n                    "centrality": _r.get("centrality"),', 1)

        # ── 3. compute the three percentages ──────────────────────────────
        rep.section("[3] Compute revenue_share_pct / dependency_pct / basis")
        a3 = '        cap_rows.sort(key=lambda x: -(x.get("capture_gap") or 0))'
        gate(rep, "P3.anchor", src.count(a3) == 1, "sort anchor unique")
        if FAILED:
            sys.exit(1)

        block = '''        # ── v4.1 "% critical to industry" — three DISTINCT percentages ──
        # criticality is a 0-100 QUALITY composite, not a share. Rendering it
        # with a % sign would imply "71% of the industry depends on TSM", which
        # is not what it measures. So we publish measured shares separately and
        # label each for exactly what it is.
        try:
            _rev_by_ind, _ctr_by_ind = {}, {}
            for _c in cap_rows:
                _i = _c.get("industry")
                if not _i:
                    continue
                _rv = _c.get("revenue_ttm")
                if isinstance(_rv, (int, float)) and _rv > 0:
                    _rev_by_ind[_i] = _rev_by_ind.get(_i, 0.0) + _rv
                _ct = _c.get("centrality")
                if isinstance(_ct, (int, float)) and _ct > 0:
                    _ctr_by_ind[_i] = _ctr_by_ind.get(_i, 0.0) + _ct
            _scored_by_ind = {}
            for _c in cap_rows:
                _scored_by_ind[_c.get("industry")] = _scored_by_ind.get(_c.get("industry"), 0) + 1

            for _c in cap_rows:
                _i = _c.get("industry")
                _rv = _c.get("revenue_ttm")
                _tot = _rev_by_ind.get(_i) or 0.0
                # [1] revenue share — of SCORED peers only (bulk carries no revenue)
                _c["revenue_share_pct"] = (round(100.0 * _rv / _tot, 2)
                                           if (isinstance(_rv, (int, float)) and _rv > 0 and _tot > 0)
                                           else None)
                _lp = _c.get("industry_peers") or 0
                _sc = _scored_by_ind.get(_i) or 0
                _c["revenue_coverage_pct"] = (round(100.0 * _sc / _lp, 1)
                                              if _lp else None)
                _c["revenue_share_basis"] = "scored peers (n=%d of %s listed)" % (
                    _sc, _lp or "?")
                # [3] dependency share from the curated supply-chain graph;
                # null where the graph does not cover the sector, never guessed
                _ct = _c.get("centrality")
                _ctot = _ctr_by_ind.get(_i) or 0.0
                _c["dependency_pct"] = (round(100.0 * _ct / _ctot, 1)
                                        if (isinstance(_ct, (int, float)) and _ct > 0 and _ctot > 0)
                                        else None)
                # which percentage actually carries the claim for this name
                if _c.get("dependency_pct") is not None and _c["dependency_pct"] >= 25:
                    _c["criticality_basis"] = "supply-chain dependency"
                elif _c.get("revenue_share_pct") is not None and _c["revenue_share_pct"] >= 20:
                    _c["criticality_basis"] = "revenue scale"
                elif (_c.get("criticality_pctile") or 0) >= 80:
                    _c["criticality_basis"] = "business quality (margins/ROIC/R&D)"
                else:
                    _c["criticality_basis"] = "mixed"

            capture["percent_critical_note"] = (
                "Three DIFFERENT percentages, deliberately not merged. "
                "criticality (0-100) is a QUALITY composite — margins, margin "
                "stability, ROIC, R&D, supply-chain centrality — NOT a share of "
                "the industry; criticality_pctile is its honest percentage form "
                "('ranks above X% of scored peers'). revenue_share_pct is the "
                "closest measured dependency proxy: this company's TTM revenue "
                "as a share of SCORED peers in its industry (bulk profiles carry "
                "no revenue, so the denominator is scored names, not the full "
                "listed industry — revenue_coverage_pct states how much of the "
                "industry that sample covers). dependency_pct is the share of "
                "curated supply-chain edges in the industry that point at this "
                "company as supplier, and is null wherever the graph does not "
                "cover the sector rather than guessed.")
            capture["stats"]["with_revenue_share"] = sum(
                1 for c in cap_rows if c.get("revenue_share_pct") is not None)
            capture["stats"]["with_dependency"] = sum(
                1 for c in cap_rows if c.get("dependency_pct") is not None)
            diag.append("pct_critical: rev_share=%d dependency=%d" % (
                capture["stats"]["with_revenue_share"],
                capture["stats"]["with_dependency"]))
        except Exception as _pe:
            capture["percent_critical_error"] = str(_pe)[:250]
            diag.append("pct_critical FAILED: %s" % str(_pe)[:150])

'''
        # must run AFTER cap_rows is built but BEFORE the capture dict is used
        post = '        diag.append("capture_gap: %d scored / %d ind / %d undervalued" % ('
        gate(rep, "P3.post_anchor", src.count(post) == 1, "post anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(post, block + post, 1)

        # surface the new fields on the leaderboard rows too
        src = src.replace('"industry_median_ev_sales", "industry_median_pe", "ev_sales", "pe")}',
                          '"industry_median_ev_sales", "industry_median_pe", "ev_sales", "pe",\n'
                          '                  "revenue_share_pct", "revenue_coverage_pct",\n'
                          '                  "dependency_pct", "criticality_basis",\n'
                          '                  "criticality_pctile")}', 1)
        # and on the by_industry member rows
        src = src.replace('"roic", "gm_stability", "legs", "tier", "cap_bucket")}',
                          '"roic", "gm_stability", "legs", "tier", "cap_bucket",\n'
                          '                                  "revenue_share_pct", "dependency_pct",\n'
                          '                                  "criticality_pctile", "criticality_basis")}', 1)

        src = src.replace('VERSION = "4.0.1"', 'VERSION = "4.1"', 1)
        LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("v4.1 spliced + compile clean")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.1 (revenue share, supply-chain dependency %, criticality percentile).",
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
                if "revenue_share_pct" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True
                    rep.ok("settled attempt %d" % (i + 1))
                    break
        gate(rep, "DEPLOY.settled", settled, "v4.1 live")
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
        st = cap.get("stats") or {}
        rows = cap.get("all_rows") or []

        gate(rep, "LIVE.v41", d.get("version") == "4.1", "version=%s" % d.get("version"))
        gate(rep, "LIVE.no_error", "percent_critical_error" not in cap,
             "err=%s" % cap.get("percent_critical_error"))
        rep.kv(scored=st.get("scored"), with_revenue_share=st.get("with_revenue_share"),
               with_dependency=st.get("with_dependency"))
        gate(rep, "LIVE.rev_share", (st.get("with_revenue_share") or 0) > 100,
             "revenue_share_pct on %s names" % st.get("with_revenue_share"))
        gate(rep, "LIVE.note", bool(cap.get("percent_critical_note")),
             "interpretation note shipped in feed")

        # a share must never exceed 100 and must sum sanely within an industry
        shares = [r0.get("revenue_share_pct") for r0 in rows
                  if r0.get("revenue_share_pct") is not None]
        if shares:
            rep.kv(share_min=min(shares), share_max=max(shares))
            gate(rep, "SANITY.bounds", 0 < max(shares) <= 100.01,
                 "max share %.2f%% within bounds" % max(shares))
        byi = {}
        for r0 in rows:
            if r0.get("revenue_share_pct") is not None:
                byi.setdefault(r0["industry"], 0.0)
                byi[r0["industry"]] += r0["revenue_share_pct"]
        if byi:
            worst = max(byi.values())
            gate(rep, "SANITY.sums_to_100", worst <= 100.5,
                 "max industry share-sum %.1f%% (must be ~100 by construction)" % worst)

        rep.section("Sample — the three percentages side by side")
        for t in ("TSM", "ASML", "NVDA", "AMD", "AVGO", "MSFT"):
            r0 = next((x for x in rows if x.get("ticker") == t), None)
            if r0:
                rep.log("  %-5s crit=%-5s pctile=%-5s rev_share=%-7s dep=%-6s basis=%s" % (
                    t, r0.get("criticality"), r0.get("criticality_pctile"),
                    ("%.2f%%" % r0["revenue_share_pct"]) if r0.get("revenue_share_pct") is not None else "—",
                    ("%.1f%%" % r0["dependency_pct"]) if r0.get("dependency_pct") is not None else "—",
                    r0.get("criticality_basis")))
            else:
                rep.log("  %-5s not scored" % t)

        rep.section("Additive contract")
        for k in ("structural_names", "industry_leaders", "all_chokepoints"):
            gate(rep, f"ADDITIVE.{k}", k in d, "present")
        for k in ("capture_gap", "catchup_pct", "global_capture_gap"):
            gate(rep, f"ADDITIVE.row_{k}", any(x.get(k) is not None for x in rows),
                 "prior field preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — three labelled percentages live; composite never shown as a share")


if __name__ == "__main__":
    main()
