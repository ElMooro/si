#!/usr/bin/env python3
"""ops 3767 — capture-gap: page + backlog key fix + CROSS-INDUSTRY denominator.

Three items, auto-continued from ops 3766.

[1] PAGE CONTRACT — /capture-gap.html. An engine whose data no human can see is
    half-built. Surfaces EVERY field capture_gap publishes: all_rows ledger
    (sortable, 18 cols), structurally_undervalued, hidden_capture_gaps,
    creation-vs-capture scatter, industry concentration, method + honest_limit.
    Pinned to nav under "Research & Tools".

[2] BACKLOG JOIN FIX — 3766 reported backlog_joined=0. ROOT CAUSE (my bug, and
    exactly the gate/producer key-mismatch class the house standard warns about):
    I consumed r["rpo_growth_yoy"] and r["accelerating"]; the producer actually
    writes  rpo_yoy · demand_accelerating · deferred_accelerating  (verified by
    grep of justhodl-backlog source lines 149/168). The G0 gate in 3766 checked
    the CONTAINER key ("by_ticker") but never the ROW keys — so a real miss
    silently defaulted to a dead leg. G0 now greps row keys too.

[3] CROSS-INDUSTRY DENOMINATOR (v3.1) — the direct answer to Khalid's premise.
    3766 showed TSM gap=-10.8pp, ASML -0.3pp WITHIN semis: they already rank top
    of their own industry on both axes. But his comparison was never intra-semi;
    it was "TSMC at $1T vs every other company at about a trillion" — i.e. semis
    vs MEGA-CAP SOFTWARE. That needs a second, cross-sectional denominator:
    criticality percentile vs mcap percentile across the WHOLE market, not within
    industry. Ships as global_capture_gap alongside (never replacing) the
    within-industry gap, because the two answer different questions and the
    within-industry one is the more conservative.
"""
import sys, json, time, zipfile, io, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

REPO = ROOT.parent
FN = "justhodl-chokepoint"
SRC = ROOT / "lambdas" / FN / "source"
LAMBDA_FILE = SRC / "lambda_function.py"
BACKLOG_SRC = ROOT / "lambdas" / "justhodl-backlog" / "source" / "lambda_function.py"
PAGE = REPO / "capture-gap.html"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/chokepoint.json"
MARKER31 = "global_capture_gap_v31"

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


GLOBAL_BLOCK = '''
        # ── v3.1 CROSS-INDUSTRY (GLOBAL) CAPTURE GAP ──────────────────────
        # Khalid's original framing was cross-industry, not intra-industry:
        # "TSMC at $1T when every other company is about a trillion". The
        # within-industry gap answers "is it cheap vs its own peers"; this
        # answers "is the whole industry underweighted in the market". Both
        # ship — they are different questions and disagreeing is informative.
        try:
            _g_crit = [c["criticality"] for c in cap_rows if c.get("criticality") is not None]
            _g_mc = [c["market_cap"] for c in cap_rows if (c.get("market_cap") or 0) > 0]
            for _c in cap_rows:
                _gc = _pctile(_g_crit, _c.get("criticality") or 0)
                _gm = _pctile(_g_mc, _c.get("market_cap") or 0)
                _c["global_criticality_pctile"] = _gc
                _c["global_mcap_pctile"] = _gm
                _c["global_capture_gap"] = (round(_gc - _gm, 1)
                                            if (_gc is not None and _gm is not None) else None)
                # divergence: cheap globally but fair within industry (or vice
                # versa) = the whole industry is being repriced, not the name
                if _c.get("capture_gap") is not None and _c["global_capture_gap"] is not None:
                    _c["gap_divergence"] = round(_c["global_capture_gap"] - _c["capture_gap"], 1)
                else:
                    _c["gap_divergence"] = None
            _gsorted = sorted([c for c in cap_rows if c.get("global_capture_gap") is not None],
                              key=lambda x: -x["global_capture_gap"])
            capture["global_marker"] = "global_capture_gap_v31"
            capture["global_method"] = (
                "criticality percentile vs market-cap percentile across the WHOLE scored "
                "cross-section (not within industry). Answers 'is this business "
                "under-capitalised relative to everything listed', which is the "
                "cross-industry version of the question. gap_divergence = global minus "
                "within-industry: large positive means the entire industry is "
                "under-capitalised, not just the name.")
            capture["widest_global_gaps"] = _gsorted[:40]
            capture["industry_underweight"] = sorted(
                [{"industry": _i,
                  "n": len(_v),
                  "median_global_gap": round(sorted(_v)[len(_v) // 2], 1),
                  "industry_mcap_total": _tots.get(_i)}
                 for _i, _v in _bygi.items() if len(_v) >= 3],
                key=lambda x: -x["median_global_gap"])[:25]
            capture["stats"]["global_scored"] = len(_gsorted)
            diag.append("global_capture_gap: %d scored" % len(_gsorted))
        except Exception as _ge:
            capture["global_error"] = str(_ge)[:200]
            diag.append("global_capture_gap FAILED: %s" % str(_ge)[:160])

'''


def main():
    with report("3767_capture_gap_page_and_v31") as rep:
        rep.heading("ops 3767 — page + backlog key fix + cross-industry denominator")

        src = LAMBDA_FILE.read_text()
        bsrc = BACKLOG_SRC.read_text()

        # ── G0: this time grep ROW keys, not just the container ───────────
        rep.section("G0_KEY_CONTRACT — ROW-level keys (the 3766 miss)")
        gate(rep, "G0.backlog_container", '"by_ticker": ledger' in bsrc, "container key by_ticker")
        gate(rep, "G0.row_rpo_yoy", 'rec["rpo_yoy"]' in bsrc or '"rpo_yoy"' in bsrc,
             "producer writes rpo_yoy (NOT rpo_growth_yoy)")
        gate(rep, "G0.row_demand_accel", 'rec["demand_accelerating"]' in bsrc,
             "producer writes demand_accelerating")
        gate(rep, "G0.row_deferred_accel", 'rec["deferred_accelerating"]' in bsrc,
             "producer writes deferred_accelerating")
        gate(rep, "G0.wrong_keys_absent", "rpo_growth_yoy" not in bsrc,
             "confirms rpo_growth_yoy does NOT exist in producer — 3766's leg was dead")
        gate(rep, "G0.capture_present", "capture_gap_v3" in src, "v3.0 block present to patch")
        gate(rep, "G0.pctile_helper", "def _pctile(" in src, "_pctile helper in scope for v3.1")
        if FAILED:
            rep.fail("G0 failed — refusing to patch against unverified keys")
            sys.exit(1)

        # ── [2] BACKLOG KEY FIX ───────────────────────────────────────────
        rep.section("[2] Fix backlog join key mismatch")
        old_join = '''                _b = _bk_by.get(_r["ticker"]) or {}
                _rpo_g = _b.get("rpo_growth_yoy")
                _bk_accel = bool(_b.get("accelerating")) or (
                    isinstance(_rpo_g, (int, float)) and _rpo_g > 0)'''
        new_join = '''                _b = _bk_by.get(_r["ticker"]) or {}
                # producer-verified keys (ops 3767): rpo_yoy / demand_accelerating /
                # deferred_accelerating. 3766 consumed rpo_growth_yoy + accelerating,
                # which the producer never writes -> leg silently dead (0 joins).
                _rpo_g = _b.get("rpo_yoy")
                _bk_accel = bool(_b.get("demand_accelerating") or _b.get("deferred_accelerating"))'''
        gate(rep, "FIX.join_anchor", src.count(old_join) == 1, "join block anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(old_join, new_join, 1)
        src = src.replace('"rpo_growth_yoy": _rpo_g,', '"rpo_yoy": _rpo_g,\n                    "backlog_deferred_accel": bool(_b.get("deferred_accelerating")),', 1)
        rep.ok("join rewritten onto producer-verified keys")

        # industry totals + global grouping need to be reachable by the v3.1 block
        old_tail = '''        cap_rows.sort(key=lambda x: -(x.get("capture_gap") or 0))'''
        new_tail = '''        _tots = ind_total
        _bygi = {}
        cap_rows.sort(key=lambda x: -(x.get("capture_gap") or 0))'''
        gate(rep, "FIX.tail_anchor", src.count(old_tail) == 1, "sort anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(old_tail, new_tail, 1)

        # ── [3] CROSS-INDUSTRY DENOMINATOR ────────────────────────────────
        rep.section("[3] Splice v3.1 global capture gap")
        if MARKER31 in src:
            rep.warn("v3.1 already present — stripping for idempotent re-splice")
            src = re.sub(r"\n        # ── v3\.1 CROSS-INDUSTRY.*?\n(?=        capture = \{)",
                         "\n", src, flags=re.S)

        cap_anchor = '        capture = {\n            "marker": "capture_gap_v3",'
        gate(rep, "V31.anchor", src.count(cap_anchor) == 1, "capture dict anchor unique")
        if FAILED:
            sys.exit(1)
        # v3.1 must run AFTER capture{} exists -> splice after the dict closes
        post_anchor = '''        diag.append("capture_gap: %d scored / %d ind / %d undervalued" % ('''
        gate(rep, "V31.post_anchor", src.count(post_anchor) == 1, "post-dict anchor unique")
        if FAILED:
            sys.exit(1)

        # build _bygi (global gap per industry) inside the v3.1 block itself
        gblock = GLOBAL_BLOCK.replace(
            '            _gsorted = sorted(',
            '            for _c in cap_rows:\n'
            '                if _c.get("global_capture_gap") is not None:\n'
            '                    _bygi.setdefault(_c["industry"], []).append(_c["global_capture_gap"])\n'
            '            _gsorted = sorted(', 1)
        src = src.replace(post_anchor, gblock + post_anchor, 1)
        src = src.replace('VERSION = "3.0"', 'VERSION = "3.1"', 1)

        LAMBDA_FILE.write_text(src)
        import py_compile
        py_compile.compile(str(LAMBDA_FILE), doraise=True)
        rep.ok("v3.1 spliced + py_compile clean")
        gate(rep, "V31.marker", MARKER31 in LAMBDA_FILE.read_text(), "global marker in source")

        # ── [1] PAGE ──────────────────────────────────────────────────────
        rep.section("[1] Page contract — /capture-gap.html")
        gate(rep, "PAGE.exists", PAGE.exists(), "capture-gap.html staged at repo root")
        if PAGE.exists():
            ptxt = PAGE.read_text()
            rep.kv(page_bytes=len(ptxt))
            for k in ("all_rows", "structurally_undervalued", "hidden_capture_gaps",
                      "capture_gap", "criticality_pctile", "mcap_share_pctile",
                      "industry_mcap_total", "industry_peers", "legs_why",
                      "honest_limit", "gm_stability", "roic", "cap_bucket", "tier"):
                gate(rep, f"PAGE.field_{k}", k in ptxt, "rendered")
            gate(rep, "PAGE.nav", "jh-nav-drawer.js" in ptxt, "nav drawer included")

        # nav pin
        navgen = REPO / "scripts" / "gen_nav_manifest.py"
        if navgen.exists():
            nt = navgen.read_text()
            if '"/capture-gap.html"' not in nt:
                nt = nt.replace('    "/import-canary.html": "Macro & Liquidity",',
                                '    "/import-canary.html": "Macro & Liquidity",\n'
                                '    "/capture-gap.html": "Research & Tools",      # ops 3767', 1)
                navgen.write_text(nt)
                rep.ok("nav FORCE pin added -> Research & Tools")
            else:
                rep.warn("nav pin already present")
            gate(rep, "PAGE.nav_pin", '"/capture-gap.html"' in navgen.read_text(), "pinned")

        # ── DEPLOY + SETTLE + INVOKE ──────────────────────────────────────
        rep.section("Deploy")
        cfg = lam.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC, env_vars=env,
            timeout=900, memory=1024,
            description="Industry-criticality + capture gap v3.1 (within-industry AND cross-industry value creation vs capture).",
            create_function_url=False, smoke=False,
        )

        rep.section("Zip settle")
        settled = False
        for i in range(12):
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
            if MARKER31 in body:
                settled = True
                rep.ok("artifact settled with v3.1 marker (attempt %d)" % (i + 1))
                break
        gate(rep, "DEPLOY.settled", settled, "new code live")
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

        data = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        cap = data.get("capture_gap") or {}
        st = cap.get("stats") or {}
        rows = cap.get("all_rows") or []

        gate(rep, "LIVE.version", data.get("version") == "3.1", "version=%s" % data.get("version"))
        gate(rep, "LIVE.no_global_error", "global_error" not in cap,
             "global err=%s" % cap.get("global_error"))
        gate(rep, "LIVE.global_marker", cap.get("global_marker") == MARKER31, "v3.1 marker in feed")

        bj = st.get("backlog_joined") or 0
        rep.kv(backlog_joined=bj, scored=st.get("scored"), global_scored=st.get("global_scored"))
        gate(rep, "FIX.backlog_leg_alive", bj > 0,
             "backlog_joined=%d (was 0 in 3766 due to key mismatch)" % bj)

        gate(rep, "LIVE.global_rows", any(r0.get("global_capture_gap") is not None for r0 in rows),
             "global_capture_gap populated")

        # additive contract
        rep.section("Additive contract")
        for k in ("structural_names", "industry_leaders", "hidden_chokepoint_book",
                  "cheap_chokepoint_book", "all_chokepoints", "highest_conviction_book"):
            gate(rep, f"ADDITIVE.{k}", k in data, "present")
        gate(rep, "ADDITIVE.within_industry_kept",
             any(r0.get("capture_gap") is not None for r0 in rows),
             "within-industry gap preserved alongside global")

        rep.section("KHALID'S PREMISE — within-industry vs cross-industry")
        by_t = {r0.get("ticker"): r0 for r0 in rows}
        for t in ("TSM", "ASML", "NVDA", "AVGO", "MSFT", "AAPL", "GOOGL"):
            r0 = by_t.get(t)
            if r0:
                rep.log("  %-6s within=%+6.1fpp  GLOBAL=%+6.1fpp  div=%+6.1fpp  share=%5.2f%%  crit=%s" % (
                    t, r0.get("capture_gap") or 0, r0.get("global_capture_gap") or 0,
                    r0.get("gap_divergence") or 0, r0.get("mcap_share_pct") or 0, r0.get("criticality")))
            else:
                rep.log("  %-6s not scored" % t)

        iu = cap.get("industry_underweight") or []
        if iu:
            rep.section("Most under-capitalised INDUSTRIES (median global gap)")
            for x in iu[:12]:
                rep.log("  %-34s n=%-3d median global gap %+.1fpp" % (
                    (x.get("industry") or "")[:34], x.get("n") or 0, x.get("median_global_gap") or 0))

        wg = cap.get("widest_global_gaps") or []
        if wg:
            rep.section("Widest CROSS-INDUSTRY gaps")
            for x in wg[:12]:
                rep.log("  %-6s %-26s global=%+.1fpp  within=%+.1fpp  %s" % (
                    x.get("ticker"), (x.get("industry") or "")[:26],
                    x.get("global_capture_gap") or 0, x.get("capture_gap") or 0, x.get("tier")))

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — page live, backlog leg repaired, v3.1 cross-industry shipped")


if __name__ == "__main__":
    main()
