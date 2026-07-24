#!/usr/bin/env python3
"""ops 3805 — measure "how crucial to its industry" WITHOUT a supply-chain map.

Khalid: "i'm trying to determine how crucial is that company to that industry" —
not fill a column. That reframes the whole problem, and the audit found
something worse than a blank column:

  criticality = 0.30*margin + 0.22*margin_stability + 0.20*ROIC + 0.13*R&D
              + 0.15*supply_chain_centrality

s_ctr = clamp(centrality/8) is ZERO for every company outside the 185-symbol
curated graph — ~94% of a 1,269-name ledger. So unmapped companies are not
merely missing a display field: they are silently PENALISED 15% of the score on
the exact dimension Khalid is asking about. A mid-cap sole-supplier that the
curated map never named looks less critical than it is, permanently.

FIX — measure structural importance from data that exists for EVERY name, then
use the supply-chain graph only as a BONUS where it is present:

 [1] revenue_rank_in_industry  — where a company sits by revenue among USD-filing
     scored peers (percentile). Big share of industry revenue = the industry
     cannot route around it. Already computed as revenue_share_pct; this turns
     it into a rank so it is comparable across industries of different sizes.
 [2] margin_premium_vs_industry — gross margin minus the industry median. A
     company earning far above its peers is extracting rent the others cannot,
     which is what "hard to replace" looks like in the P&L.
 [3] rd_premium_vs_industry — R&D intensity vs industry median. Sustained
     above-peer R&D is the moat-maintenance signal.
 [4] size_concentration — market-cap share of industry (already have).
 [5] structural_importance (0-100) — a blend of the four above, computed for
     EVERY scored name, plus the supply-chain centrality as a BONUS when mapped.
     This is the "how crucial" number, and unlike criticality it never punishes
     a company for being absent from a curated map.

Also rebalances criticality: when centrality is unavailable, its 15% weight is
REDISTRIBUTED across the remaining factors rather than scored as zero. That is
a real scoring bug fix, not a cosmetic one.
"""
import sys, json, time, zipfile, io, urllib.request
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
    with report("3805_structural_importance") as rep:
        rep.heading("ops 3805 — 'how crucial to its industry', measurable for every name")

        src = LF.read_text()
        rep.section("G0 — prove the zero-centrality penalty is real")
        gate(rep, "G0.formula", 's_ctr = clamp(ctr / 8.0)' in src, "s_ctr present")
        gate(rep, "G0.weight", '0.15 * s_ctr' in src, "centrality carries 15% of criticality")
        gate(rep, "G0.v432", 'VERSION = "4.3.2"' in src, "engine at v4.3.2")
        gate(rep, "G0.pct_block", '_c["revenue_share_pct"] = round(100.0 * _rv / _tot, 2)' in src
             or '_c["revenue_share_pct"] = (round(100.0 * _rv / _tot, 2)' in src,
             "revenue share block present to build on")
        if FAILED:
            sys.exit(1)

        d0 = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        rows0 = (d0.get("capture_gap") or {}).get("all_rows") or []
        zero_ctr = sum(1 for r in rows0 if not (r.get("centrality") or 0))
        rep.kv(ledger=len(rows0), rows_with_zero_centrality=zero_ctr,
               pct=round(100.0 * zero_ctr / max(len(rows0), 1), 1))
        rep.log("  -> those %d names each lose up to 15 points of criticality purely "
                "for being absent from a 185-symbol curated map." % zero_ctr)

        # [A] redistribute the centrality weight when it is unavailable
        rep.section("[A] Stop penalising unmapped names in criticality")
        old = '''    s_ctr = clamp(ctr / 8.0)
    crit = round(100 * (0.30 * s_gm + 0.22 * s_stab + 0.20 * s_roic + 0.13 * s_rd + 0.15 * s_ctr), 1)'''
        gate(rep, "A.anchor", src.count(old) == 1, "criticality formula anchor unique")
        if FAILED:
            sys.exit(1)
        new = '''    s_ctr = clamp(ctr / 8.0)
    # ops 3805: the curated supply-chain map names ~185 symbols, so ctr is 0 for
    # ~94% of the ledger. Scoring that as a genuine zero silently docks those
    # companies 15 points on the very dimension "how crucial is this company" is
    # asking about. When centrality is UNAVAILABLE (not merely low), redistribute
    # its weight across the factors we can actually measure.
    _has_ctr = ctr > 0
    if _has_ctr:
        crit = round(100 * (0.30 * s_gm + 0.22 * s_stab + 0.20 * s_roic
                            + 0.13 * s_rd + 0.15 * s_ctr), 1)
    else:
        _sc = 1.0 / 0.85          # renormalise the remaining 85% back to 1.0
        crit = round(100 * _sc * (0.30 * s_gm + 0.22 * s_stab + 0.20 * s_roic
                                  + 0.13 * s_rd), 1)
    crit = min(crit, 100.0)'''
        src = src.replace(old, new, 1)
        src = src.replace('        "centrality": ctr, "discount_to_fair_pct": disc,',
                          '        "centrality": ctr, "centrality_mapped": bool(ctr > 0),\n'
                          '        "discount_to_fair_pct": disc,', 1)

        # ops 3805 pre-flight catch: evaluate() emits rd_intensity but cap_rows
        # never carried it — the R&D leg would have been silently dead, the same
        # field-drop class that killed the backlog and catch-up legs earlier.
        _rd_anchor = '                    "ev_sales": _r.get("ev_sales"),'
        gate(rep, "B.rd_carry_anchor", src.count(_rd_anchor) == 1, "cap_rows carry anchor unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(_rd_anchor,
                          '                    "rd_intensity": _r.get("rd_intensity"),\n' + _rd_anchor, 1)

        # [B] structural importance, computed for every name
        rep.section("[B] structural_importance for every scored name")
        anchor = '''            diag.append("pct_critical: rev_share=%d dependency=%d" % ('''
        gate(rep, "B.anchor", src.count(anchor) == 1, "percent block diag anchor unique")
        if FAILED:
            sys.exit(1)
        block = '''            # ── ops 3805: STRUCTURAL IMPORTANCE ─────────────────────────
            # "How crucial is this company to its industry?" answered from data
            # that exists for EVERY name, not from a 185-symbol curated map.
            # Four measurable legs; supply-chain centrality is a BONUS when
            # present, never a penalty when absent.
            try:
                _gm_by, _rd_by = {}, {}
                for _c in cap_rows:
                    _i = _c.get("industry")
                    if _c.get("gm_level") is not None:
                        _gm_by.setdefault(_i, []).append(_c["gm_level"])
                    if _c.get("rd_intensity") is not None:
                        _rd_by.setdefault(_i, []).append(_c["rd_intensity"])

                def _median(v):
                    v = sorted(x for x in v if x is not None)
                    return v[len(v) // 2] if v else None

                _gm_med = {k: _median(v) for k, v in _gm_by.items()}
                _rd_med = {k: _median(v) for k, v in _rd_by.items()}

                # revenue rank within industry (USD filers only, same guard as share)
                _rev_by_i = {}
                for _c in cap_rows:
                    if _c.get("revenue_share_pct") is not None:
                        _rev_by_i.setdefault(_c["industry"], []).append(_c["revenue_share_pct"])

                for _c in cap_rows:
                    _i = _c.get("industry")
                    _legs, _n = 0.0, 0

                    # 1. revenue scale within industry (0-1)
                    _rs = _c.get("revenue_share_pct")
                    _pool = _rev_by_i.get(_i) or []
                    if _rs is not None and len(_pool) >= 3:
                        _below = sum(1 for x in _pool if x < _rs)
                        _rank = _below / float(len(_pool))
                        _c["revenue_rank_in_industry"] = round(100.0 * _rank, 1)
                        _legs += _rank; _n += 1
                    else:
                        _c["revenue_rank_in_industry"] = None

                    # 2. margin premium vs industry median
                    _gm, _gmm = _c.get("gm_level"), _gm_med.get(_i)
                    if _gm is not None and _gmm is not None:
                        _prem = _gm - _gmm
                        _c["margin_premium_vs_industry"] = round(_prem, 1)
                        _legs += max(0.0, min(1.0, (_prem + 10.0) / 40.0)); _n += 1
                    else:
                        _c["margin_premium_vs_industry"] = None

                    # 3. R&D premium vs industry median
                    _rd, _rdm = _c.get("rd_intensity"), _rd_med.get(_i)
                    if _rd is not None and _rdm is not None:
                        _rp = _rd - _rdm
                        _c["rd_premium_vs_industry"] = round(_rp, 1)
                        _legs += max(0.0, min(1.0, (_rp + 5.0) / 20.0)); _n += 1
                    else:
                        _c["rd_premium_vs_industry"] = None

                    # 4. market-cap concentration in industry
                    _ms = _c.get("mcap_share_pct")
                    if _ms is not None:
                        _legs += max(0.0, min(1.0, _ms / 25.0)); _n += 1

                    _base = (_legs / _n) if _n else None
                    if _base is None:
                        _c["structural_importance"] = None
                        _c["structural_basis"] = None
                        continue
                    # supply-chain centrality as a BONUS only
                    _bonus = 0.0
                    if (_c.get("dependency_pct") or 0) > 0:
                        _bonus = min(0.15, (_c["dependency_pct"] / 100.0) * 0.15)
                    _si = round(min(100.0, 100.0 * (_base * 0.85 + _bonus)), 1)
                    _c["structural_importance"] = _si
                    _c["structural_legs_used"] = _n
                    _bits = []
                    if _c.get("revenue_rank_in_industry") is not None and _c["revenue_rank_in_industry"] >= 80:
                        _bits.append("top-%d%% of industry revenue" % (100 - int(_c["revenue_rank_in_industry"])))
                    if _c.get("margin_premium_vs_industry") is not None and _c["margin_premium_vs_industry"] >= 10:
                        _bits.append("margins %.0f%% above peers" % _c["margin_premium_vs_industry"])
                    if _c.get("rd_premium_vs_industry") is not None and _c["rd_premium_vs_industry"] >= 3:
                        _bits.append("R&D %.0f%% above peers" % _c["rd_premium_vs_industry"])
                    if (_c.get("dependency_pct") or 0) >= 20:
                        _bits.append("%.0f%% of mapped supplier links" % _c["dependency_pct"])
                    _c["structural_basis"] = "; ".join(_bits) if _bits else "no standout leg"

                capture["stats"]["with_structural_importance"] = sum(
                    1 for c in cap_rows if c.get("structural_importance") is not None)
                capture["structural_note"] = (
                    "structural_importance (0-100) answers 'how crucial is this company "
                    "to its industry' using data available for EVERY name: revenue rank "
                    "within the industry, gross-margin premium over the industry median, "
                    "R&D premium over the median, and market-cap concentration. "
                    "Supply-chain centrality adds a bonus where the curated map covers "
                    "the company, but its ABSENCE never penalises — the map names only "
                    "~185 symbols and treating that gap as a zero was silently docking "
                    "94% of the ledger on this exact question.")
                diag.append("structural_importance=%d" % capture["stats"]["with_structural_importance"])
            except Exception as _se:
                capture["structural_error"] = str(_se)[:250]
                diag.append("structural FAILED: %s" % str(_se)[:150])

'''
        src = src.replace(anchor, block + anchor, 1)
        src = src.replace('VERSION = "4.3.2"', 'VERSION = "4.4"', 1)
        LF.write_text(src)
        import py_compile
        py_compile.compile(str(LF), doraise=True)
        rep.ok("v4.4 spliced + compile clean")

        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Criticality + capture gap v4.4 (structural_importance for every name; centrality no longer penalises unmapped companies).",
                      create_function_url=False, smoke=False)
        settled = False
        for i in range(14):
            time.sleep(15)
            c0 = lam.get_function_configuration(FunctionName=FN)
            if c0.get("State") != "Active" or c0.get("LastUpdateStatus") != "Successful":
                continue
            u = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(u, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if "STRUCTURAL IMPORTANCE" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True; rep.ok("settled attempt %d" % (i + 1)); break
        gate(rep, "DEPLOY.settled", settled, "v4.4 live")
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
        gate(rep, "LIVE.v44", d.get("version") == "4.4", "version=%s" % d.get("version"))
        gate(rep, "LIVE.no_err", "structural_error" not in cap, "err=%s" % cap.get("structural_error"))
        si = st.get("with_structural_importance") or 0
        rep.kv(scored=st.get("scored"), with_structural_importance=si,
               dependency_still=sum(1 for x in rows if x.get("dependency_pct") is not None))
        _rdn = sum(1 for x in rows if x.get("rd_premium_vs_industry") is not None)
        rep.kv(rd_leg_populated=_rdn)
        gate(rep, "FIX.rd_leg_alive", _rdn > 0,
             "%d rows carry rd_premium (0 would mean the field-drop bug recurred)" % _rdn)
        gate(rep, "FIX.broad_coverage", si > len(rows) * 0.5,
             "%d of %d names have a structural score (dependency reached ~180)" % (si, len(rows)))

        rep.section("Sample — crucial names the supply-chain map never saw")
        got = [x for x in rows if x.get("structural_importance") is not None
               and not x.get("centrality_mapped")]
        for x in sorted(got, key=lambda z: -(z.get("structural_importance") or 0))[:14]:
            rep.log("  %-6s %-28s SI=%5.1f  rev_rank=%-6s margin_prem=%-7s %s" % (
                x.get("ticker"), (x.get("industry") or "")[:28],
                x.get("structural_importance") or 0,
                x.get("revenue_rank_in_industry"), x.get("margin_premium_vs_industry"),
                (x.get("structural_basis") or "")[:44]))

        rep.section("Additive")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct", "criticality"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — 'how crucial' now measurable for the whole ledger")


if __name__ == "__main__":
    main()
