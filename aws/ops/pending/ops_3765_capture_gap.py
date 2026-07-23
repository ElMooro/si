#!/usr/bin/env python3
"""ops 3765 — chokepoint v3.0: CAPTURE GAP layer (value-creation vs value-capture).

KHALID'S THESIS: "TSMC at $1T and ASML at $600B are grossly undervalued given they
are the center of the AI trade when every other company is about a trillion."

The insight is NOT that they screen cheap on P/E. It is that they capture a small
share of their industry's market cap while enabling a large share of its output.
Nothing in the fleet measured that: fleet-wide grep for mcap_share / share_of_industry
returned ZERO hits.

AUDIT (why extend, not build new):
  justhodl-chokepoint ALREADY scores criticality (margin level + margin STABILITY +
  ROIC + R&D + supply-chain centrality) and ALREADY carries market_cap + industry on
  every row. It just never emits the full ledger (books truncated [:80]/[:40]/[:25])
  and never divides one by the other. So v3.0 is ADDITIVE:
    - emit full per-ticker ledger (all_rows) so denominators exist
    - compute industry mcap share from fetch_bulk_universe() = whole market, ZERO
      extra API cost (already fetched for the funnel)
    - capture_gap = criticality_pctile - mcap_share_pctile, within industry
    - join backlog.json (by_ticker) + peer-comparison.json for the other legs
    - 5-leg confirmation ladder; single leg = WATCH only

ADDITIVE CONTRACT: every existing key (criticality, is_chokepoint, hidden/cheap books,
structural_names, industry_leaders, highest_conviction_book) is UNTOUCHED. Existing
consumers keep working.

G0_KEY_CONTRACT gates (house standard — never type a key from memory):
  universe.json      -> "stocks" / symbol / industry / marketCap   [VERIFIED in repo]
  backlog.json       -> "by_ticker"                                 [VERIFIED line 279]
  chokepoint row     -> market_cap / industry / criticality         [VERIFIED line 186-195]
"""
import sys, json, time, zipfile, io, re
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
OUT_KEY = "data/chokepoint.json"
MARKER = "capture_gap_v3"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

FAILED = []


def gate(rep, name, ok, detail=""):
    if ok:
        rep.ok(f"{name} :: {detail}")
    else:
        rep.fail(f"{name} :: {detail}")
        FAILED.append(name)
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# The v3.0 capture-gap block, spliced into the engine before `out = {`
# ─────────────────────────────────────────────────────────────────────────────
CAPTURE_BLOCK = '''
    # ══════════════════════════════════════════════════════════════════════
    # v3.0 CAPTURE GAP — value-creation (criticality) vs value-capture (mcap)
    # ══════════════════════════════════════════════════════════════════════
    # Khalid's TSMC/ASML thesis, made computable. A company can be the single
    # point of failure for its entire industry and still carry a small slice of
    # that industry's market cap. That gap is the mispricing.
    #
    # Denominator = fetch_bulk_universe() (the WHOLE market, already in memory
    # for the funnel — zero extra API calls). Industry totals therefore include
    # names we never deep-dived, which is correct: the denominator must be the
    # full industry, not just our scored sample.
    capture = {}
    try:
        MIN_PEERS = 5          # industries thinner than this have meaningless shares
        ind_total, ind_members = {}, {}
        for _s, _p in bulk.items():
            if _p.get("is_etf") or _p.get("is_fund") or not _p.get("active"):
                continue
            _ind = (_p.get("industry") or "").strip()
            _mc = _p.get("market_cap") or 0
            if not _ind or _mc <= 0:
                continue
            ind_total[_ind] = ind_total.get(_ind, 0.0) + _mc
            ind_members.setdefault(_ind, []).append((_s, _mc))

        # backlog join (G0-verified key: "by_ticker")
        _bk = _read("data/backlog.json") or {}
        _bk_by = _bk.get("by_ticker") or {}

        def _pctile(vals, v):
            if not vals:
                return None
            n = sum(1 for x in vals if x < v)
            return round(100.0 * n / len(vals), 1)

        # group SCORED rows by industry so percentiles are cross-sectional
        scored_by_ind = {}
        for _r in results:
            _i = (_r.get("industry") or "").strip()
            if _i:
                scored_by_ind.setdefault(_i, []).append(_r)

        cap_rows = []
        for _ind, _rows in scored_by_ind.items():
            _tot = ind_total.get(_ind, 0.0)
            _npeers = len(ind_members.get(_ind, []))
            if _tot <= 0 or _npeers < MIN_PEERS:
                continue
            _crits = [x.get("criticality") or 0 for x in _rows]
            _shares_all = [mc for _, mc in ind_members[_ind]]
            for _r in _rows:
                _mc = _r.get("market_cap") or 0
                if _mc <= 0:
                    continue
                _share = 100.0 * _mc / _tot
                _crit_p = _pctile(_crits, _r.get("criticality") or 0)
                _share_p = _pctile(_shares_all, _mc)
                if _crit_p is None or _share_p is None:
                    continue
                _gap = round(_crit_p - _share_p, 1)

                # backlog leg
                _b = _bk_by.get(_r["ticker"]) or {}
                _rpo_g = _b.get("rpo_growth_yoy")
                _bk_accel = bool(_b.get("accelerating")) or (
                    isinstance(_rpo_g, (int, float)) and _rpo_g > 0)

                cap_rows.append({
                    "ticker": _r["ticker"], "name": _r.get("name"),
                    "industry": _ind, "sector": _r.get("sector"),
                    "market_cap": _mc,
                    "industry_mcap_total": round(_tot, 0),
                    "industry_peers": _npeers,
                    "mcap_share_pct": round(_share, 3),
                    "criticality": _r.get("criticality"),
                    "criticality_pctile": _crit_p,
                    "mcap_share_pctile": _share_p,
                    "capture_gap": _gap,
                    "gm_stability": _r.get("gm_stability"),
                    "gm_level": _r.get("gm_level"),
                    "roic": _r.get("roic"),
                    "discount_to_fair_pct": _r.get("discount_to_fair_pct"),
                    "rpo_growth_yoy": _rpo_g,
                    "backlog_accelerating": _bk_accel,
                    "cap_bucket": _r.get("cap_bucket"),
                    "is_chokepoint": _r.get("is_chokepoint"),
                })

        # ── 5-leg confirmation ladder ──────────────────────────────────────
        # One leg is noise. Institutional practice: require independent
        # confirmation across creation, price, quality, growth and visibility.
        for _c in cap_rows:
            legs, why = 0, []
            if (_c["capture_gap"] or 0) >= 20:
                legs += 1; why.append("capture gap %.0fpp" % _c["capture_gap"])
            if isinstance(_c.get("discount_to_fair_pct"), (int, float)) and _c["discount_to_fair_pct"] >= 15:
                legs += 1; why.append("%.0f%% below fair" % _c["discount_to_fair_pct"])
            if isinstance(_c.get("gm_stability"), (int, float)) and _c["gm_stability"] <= 5:
                legs += 1; why.append("margin stability ±%.1fpp" % _c["gm_stability"])
            if isinstance(_c.get("roic"), (int, float)) and _c["roic"] >= 15:
                legs += 1; why.append("%.0f%% ROIC" % _c["roic"])
            if _c.get("backlog_accelerating"):
                legs += 1; why.append("backlog accelerating")
            _c["legs"] = legs
            _c["legs_why"] = why
            _c["tier"] = ("STRUCTURALLY_UNDERVALUED" if legs >= 3 and _c["capture_gap"] >= 20
                          else "WATCH" if legs >= 1 else "NONE")

        cap_rows.sort(key=lambda x: -(x.get("capture_gap") or 0))
        _under = [c for c in cap_rows if c["tier"] == "STRUCTURALLY_UNDERVALUED"]
        _hidden_cap = [c for c in _under if c.get("cap_bucket") in ("nano", "micro", "small", "mid")]

        capture = {
            "marker": "capture_gap_v3",
            "thesis": ("Value CREATION vs value CAPTURE. A company can be the single "
                       "point of failure for its industry and still hold a small slice "
                       "of that industry's market cap. capture_gap = criticality "
                       "percentile minus market-cap-share percentile, within industry. "
                       "Positive = the market underpays for indispensability."),
            "method": {
                "denominator": "full market from profile-bulk (all active non-ETF names)",
                "min_peers": MIN_PEERS,
                "ladder": "3 of 5 legs AND capture_gap>=20pp => STRUCTURALLY_UNDERVALUED",
                "legs": ["capture_gap>=20pp", "discount_to_fair>=15%",
                         "gm_stability<=5pp", "roic>=15%", "backlog_accelerating"],
                "honest_limit": ("mcap share is a proxy for value capture, not a "
                                 "measured revenue share; industries with <5 listed "
                                 "peers are excluded rather than guessed."),
            },
            "stats": {
                "scored": len(cap_rows),
                "industries": len(set(c["industry"] for c in cap_rows)),
                "structurally_undervalued": len(_under),
                "hidden": len(_hidden_cap),
                "backlog_joined": sum(1 for c in cap_rows if c.get("rpo_growth_yoy") is not None),
            },
            "structurally_undervalued": _under[:40],
            "hidden_capture_gaps": _hidden_cap[:25],
            "widest_gaps": cap_rows[:60],
            "all_rows": cap_rows,
        }
        diag.append("capture_gap: %d scored / %d ind / %d undervalued" % (
            len(cap_rows), len(set(c["industry"] for c in cap_rows)), len(_under)))
    except Exception as _e:
        capture = {"marker": "capture_gap_v3", "error": str(_e)[:300]}
        diag.append("capture_gap FAILED: %s" % str(_e)[:160])

'''


def main():
    with report("3765_capture_gap") as rep:
        rep.heading("ops 3765 — chokepoint v3.0 CAPTURE GAP")
        rep.log("Khalid's thesis: TSMC/ASML undervalued vs their structural role in AI.")
        rep.log("Audit verdict: EXTEND justhodl-chokepoint (already has criticality + "
                "market_cap + industry). Do NOT build a new engine.")

        src = LAMBDA_FILE.read_text()

        # ── G0 KEY CONTRACT ───────────────────────────────────────────────
        rep.section("G0_KEY_CONTRACT — grep producers before consuming")
        gate(rep, "G0.chokepoint_row_market_cap", '"market_cap": mcap' in src,
             "chokepoint row emits market_cap")
        gate(rep, "G0.chokepoint_row_industry", '"industry": industry' in src,
             "chokepoint row emits industry")
        gate(rep, "G0.bulk_universe", "def fetch_bulk_universe" in src and "bulk = fetch_bulk_universe()" in src,
             "whole-market denominator available in memory (zero extra API cost)")
        gate(rep, "G0.results_var", "results.sort(key=lambda r: r[\"criticality\"]" in src,
             "`results` holds full scored ledger")
        gate(rep, "G0.reader", "def _read(" in src, "_read() S3 helper present")
        gate(rep, "G0.diag", "diag.append(" in src, "diag list present")

        bk = (ROOT / "lambdas" / "justhodl-backlog" / "source" / "lambda_function.py").read_text()
        gate(rep, "G0.backlog_by_ticker", '"by_ticker": ledger' in bk,
             "backlog.json exposes by_ticker (verified in producer source)")

        if FAILED:
            rep.fail("G0 contract failed — refusing to splice against unverified keys")
            sys.exit(1)

        # ── SPLICE ────────────────────────────────────────────────────────
        rep.section("Splice v3.0 capture block (additive, before `out = {`)")
        if MARKER in src:
            rep.warn("marker already present — re-splicing idempotently")
            src = re.sub(r"\n    # ═+\n    # v3\.0 CAPTURE GAP.*?\n(?=    out = \{)",
                         "\n", src, flags=re.S)

        anchor = "    out = {\n"
        gate(rep, "SPLICE.anchor_unique", src.count(anchor) == 1,
             "anchor `    out = {` occurs %d time(s)" % src.count(anchor))
        if FAILED:
            sys.exit(1)

        src = src.replace(anchor, CAPTURE_BLOCK + anchor, 1)

        # register the new top-level key inside the payload
        key_anchor = '        "all_chokepoints": chokepoints[:80],\n'
        gate(rep, "SPLICE.key_anchor", src.count(key_anchor) == 1, "payload key anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(key_anchor, key_anchor + '        "capture_gap": capture,\n', 1)

        # version bump
        src = src.replace('VERSION = "1.0"', 'VERSION = "3.0"', 1)

        LAMBDA_FILE.write_text(src)
        rep.ok("spliced capture block + payload key + VERSION 3.0")

        # compile proof
        import py_compile
        py_compile.compile(str(LAMBDA_FILE), doraise=True)
        rep.ok("py_compile clean")

        gate(rep, "SPLICE.marker_in_source", MARKER in LAMBDA_FILE.read_text(),
             "marker present in written source")
        gate(rep, "SPLICE.additive", 'if hidden: print("  HIDDEN chokepoints:"' in LAMBDA_FILE.read_text(),
             "pre-existing books untouched (additive contract held)")

        # ── DEPLOY (inherit env; scheduled engine) ────────────────────────
        rep.section("Deploy")
        cfg = lam.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        rep.kv("inherited_env_keys", len(env))

        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC, env_vars=env,
            timeout=900, memory=1024,
            description="Industry-criticality + v3.0 CAPTURE GAP (value creation vs value capture, full-universe mcap share).",
            create_function_url=False, smoke=False,
        )

        # ── ZIP SETTLE (never invoke the old artifact) ────────────────────
        rep.section("Zip settle — prove the NEW artifact is live before invoking")
        settled = False
        for attempt in range(12):
            time.sleep(15)
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") != "Active" or c.get("LastUpdateStatus") != "Successful":
                continue
            import urllib.request
            url = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                body = z.read("lambda_function.py").decode("utf-8", "replace")
            if MARKER in body:
                settled = True
                rep.ok("artifact settled with marker on attempt %d" % (attempt + 1))
                break
        gate(rep, "DEPLOY.zip_settled", settled, "new code confirmed inside deployed zip")
        if FAILED:
            sys.exit(1)

        # ── INVOKE + verify live artifact ─────────────────────────────────
        rep.section("Invoke + field-coverage audit on the LIVE S3 artifact")
        from botocore.config import Config
        lam_long = boto3.client("lambda", region_name="us-east-1",
                                config=Config(read_timeout=890, retries={"max_attempts": 0}))
        t0 = time.time()
        resp = lam_long.invoke(FunctionName=FN, InvocationType="RequestResponse",
                               Payload=json.dumps({"mode": "full"}).encode())
        rep.kv("invoke_status", resp.get("StatusCode"))
        rep.kv("invoke_seconds", round(time.time() - t0, 1))

        obj = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
        data = json.loads(obj["Body"].read())

        gate(rep, "LIVE.version", data.get("version") == "3.0", "version=%s" % data.get("version"))
        cap = data.get("capture_gap") or {}
        gate(rep, "LIVE.capture_key", bool(cap), "capture_gap key present")
        gate(rep, "LIVE.no_error", "error" not in cap, "capture block error=%s" % cap.get("error"))

        st = cap.get("stats") or {}
        rep.kv("scored", st.get("scored"))
        rep.kv("industries", st.get("industries"))
        rep.kv("structurally_undervalued", st.get("structurally_undervalued"))
        rep.kv("hidden", st.get("hidden"))
        rep.kv("backlog_joined", st.get("backlog_joined"))

        gate(rep, "LIVE.scored_nonzero", (st.get("scored") or 0) >= 50,
             "scored=%s (need >=50)" % st.get("scored"))
        gate(rep, "LIVE.industries_nonzero", (st.get("industries") or 0) >= 5,
             "industries=%s" % st.get("industries"))
        gate(rep, "LIVE.all_rows", isinstance(cap.get("all_rows"), list) and len(cap["all_rows"]) > 0,
             "all_rows n=%d" % len(cap.get("all_rows") or []))

        # ADDITIVE proof — old consumers must still find their keys
        rep.section("Additive contract — pre-existing keys must survive")
        for k in ("structural_names", "industry_leaders", "hidden_chokepoint_book",
                  "cheap_chokepoint_book", "all_chokepoints", "highest_conviction_book"):
            gate(rep, f"ADDITIVE.{k}", k in data, "present")

        # sanity: shares must be a real distribution, not all-zero / all-100
        rows = cap.get("all_rows") or []
        if rows:
            shares = [r.get("mcap_share_pct") or 0 for r in rows]
            gaps = [r.get("capture_gap") or 0 for r in rows]
            rep.kv("mcap_share_max_pct", round(max(shares), 2))
            rep.kv("capture_gap_range", "%.1f .. %.1f" % (min(gaps), max(gaps)))
            gate(rep, "SANITY.share_distribution", 0 < max(shares) <= 100,
                 "max share %.2f%% within bounds" % max(shares))
            gate(rep, "SANITY.gap_spread", (max(gaps) - min(gaps)) > 20,
                 "gap spread %.1fpp (a real cross-section, not a constant)" % (max(gaps) - min(gaps)))

            rep.section("LIVE top capture gaps")
            for r in rows[:12]:
                rep.log("  %-6s %-30s gap=%+.1fpp  share=%.2f%%  crit=%s  legs=%d  %s" % (
                    r.get("ticker"), (r.get("industry") or "")[:30], r.get("capture_gap") or 0,
                    r.get("mcap_share_pct") or 0, r.get("criticality"), r.get("legs") or 0,
                    r.get("tier")))

            und = cap.get("structurally_undervalued") or []
            if und:
                rep.section("STRUCTURALLY_UNDERVALUED (>=3 legs)")
                for r in und[:15]:
                    rep.log("  %-6s %-24s gap=%+.1fpp legs=%d :: %s" % (
                        r.get("ticker"), (r.get("name") or "")[:24], r.get("capture_gap") or 0,
                        r.get("legs") or 0, "; ".join(r.get("legs_why") or [])))
            else:
                rep.warn("no STRUCTURALLY_UNDERVALUED names today — engine reports absence honestly")

        # TSMC/ASML observability — Khalid's reference names
        rep.section("Khalid's reference names (TSM / ASML)")
        by_t = {r.get("ticker"): r for r in rows}
        for t in ("TSM", "ASML", "NVDA", "AVGO"):
            r = by_t.get(t)
            if r:
                rep.log("  %-5s gap=%+.1fpp share=%.2f%% crit=%s tier=%s" % (
                    t, r.get("capture_gap") or 0, r.get("mcap_share_pct") or 0,
                    r.get("criticality"), r.get("tier")))
            else:
                rep.log("  %-5s not in scored set (ADR/foreign filter or industry <5 peers)" % t)

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED gates: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — chokepoint v3.0 capture-gap layer live")
        rep.log("NEXT: /capture-gap.html page (page contract) + best-setups join.")


if __name__ == "__main__":
    main()
