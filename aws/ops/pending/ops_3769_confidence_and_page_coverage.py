#!/usr/bin/env python3
"""ops 3769 — close the two gaps I flagged in my own 3768 ship report.

[A] THIN-SAMPLE MEDIANS (engine). industry_underweight admits any industry with
    n>=3, so Pharmaceuticals (n=4, +51.3pp) and Medical Equipment (n=3, +47.9pp)
    outrank Software-Application (n=81, +43.7pp) — and the page renders that
    ordering as if all three readings were equally trustworthy. A median over 3
    observations is not a base rate. FIX: raise the floor to n>=5, attach an
    explicit sample_confidence tier (HIGH n>=20 / MEDIUM n>=8 / LOW n>=5), carry
    IQR so dispersion is visible, and sort HIGH-confidence industries first so a
    3-name curiosity can never top the board. Nothing is dropped silently — thin
    industries still ship under `industry_underweight_thin` with a reason.

[B] PAGE FIELD COVERAGE. capture-gap.html was authored against v3.0 and renders
    NONE of the fields v3.1/v3.2 added: global_capture_gap, global_criticality_pctile,
    global_mcap_pctile, gap_divergence, widest_global_gaps, industry_underweight,
    legs_available, backlog_covered, backlog_overlap, backlog_ledger_size,
    rpo_yoy, backlog_deferred_accel. Grep of the page returns ZERO hits for all
    of them. That is the exact "engine ships fields the page ignores" defect the
    page contract exists to prevent, and I introduced it. FIX: add a cross-industry
    section (dual-gap table + divergence), an industry-underweight board with
    confidence chips, and wire the backlog coverage fields into the hero + ledger.

Both halves are additive: the within-industry gap, the v3.0 books and every
pre-existing consumer key stay exactly as they are.
"""
import sys, json, time, zipfile, io
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
PAGE = REPO / "capture-gap.html"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


NEW_INDUSTRY_BLOCK = '''            def _iqr(vs):
                if len(vs) < 4:
                    return None
                _s = sorted(vs)
                _q1 = _s[len(_s) // 4]
                _q3 = _s[(3 * len(_s)) // 4]
                return round(_q3 - _q1, 1)

            def _conf(n):
                # a median over 3 observations is not a base rate. Tiering makes
                # the reader's trust proportional to the evidence.
                return "HIGH" if n >= 20 else "MEDIUM" if n >= 8 else "LOW"

            _ind_all = []
            for _i, _v in _bygi.items():
                _n = len(_v)
                _med = round(sorted(_v)[_n // 2], 1)
                _ind_all.append({
                    "industry": _i, "n": _n,
                    "median_global_gap": _med,
                    "iqr_global_gap": _iqr(_v),
                    "max_global_gap": round(max(_v), 1),
                    "min_global_gap": round(min(_v), 1),
                    "sample_confidence": _conf(_n),
                    "industry_mcap_total": _tots.get(_i),
                })
            _rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
            # sort by CONFIDENCE first, then magnitude: a 3-name curiosity can
            # never outrank an 81-name reading on the board again.
            capture["industry_underweight"] = sorted(
                [x for x in _ind_all if x["n"] >= 5],
                key=lambda x: (_rank[x["sample_confidence"]], -x["median_global_gap"]))[:25]
            capture["industry_underweight_thin"] = sorted(
                [dict(x, reason="n<5 — median not a base rate, shown for completeness only")
                 for x in _ind_all if x["n"] < 5],
                key=lambda x: -x["median_global_gap"])[:15]
            capture["stats"]["industries_high_conf"] = sum(
                1 for x in _ind_all if x["sample_confidence"] == "HIGH")
'''


def main():
    with report("3769_confidence_and_page_coverage") as rep:
        rep.heading("ops 3769 — sample-confidence tiers + v3.1/v3.2 page coverage")

        src = LAMBDA_FILE.read_text()
        ptxt = PAGE.read_text()

        # ── G0 ────────────────────────────────────────────────────────────
        rep.section("G0_KEY_CONTRACT")
        gate(rep, "G0.bygi", "_bygi" in src, "_bygi industry->gaps map in scope")
        gate(rep, "G0.tots", "_tots = ind_total" in src, "_tots industry totals in scope")
        gate(rep, "G0.v32", 'VERSION = "3.2"' in src, "engine at v3.2")
        gate(rep, "G0.page_missing_v31",
             "global_capture_gap" not in ptxt and "industry_underweight" not in ptxt,
             "confirms page renders NO v3.1/v3.2 fields (the gap being closed)")
        if FAILED:
            sys.exit(1)

        # ── [A] ENGINE: confidence tiers ──────────────────────────────────
        rep.section("[A] Engine — sample-confidence tiers on industry medians")
        old = '''            capture["industry_underweight"] = sorted(
                [{"industry": _i,
                  "n": len(_v),
                  "median_global_gap": round(sorted(_v)[len(_v) // 2], 1),
                  "industry_mcap_total": _tots.get(_i)}
                 for _i, _v in _bygi.items() if len(_v) >= 3],
                key=lambda x: -x["median_global_gap"])[:25]
'''
        gate(rep, "A.anchor", src.count(old) == 1, "industry_underweight block unique")
        if FAILED:
            sys.exit(1)
        src = src.replace(old, NEW_INDUSTRY_BLOCK, 1)
        src = src.replace('VERSION = "3.2"', 'VERSION = "3.3"', 1)
        LAMBDA_FILE.write_text(src)
        import py_compile
        py_compile.compile(str(LAMBDA_FILE), doraise=True)
        rep.ok("confidence tiers spliced (n>=5 floor, HIGH/MEDIUM/LOW, IQR) + v3.3")

        # ── [B] PAGE: render the v3.1/v3.2 fields ─────────────────────────
        rep.section("[B] Page — add cross-industry + coverage sections")

        sec_anchor = '<div class="sec">📊 Full Ledger — every scored name</div>'
        gate(rep, "B.sec_anchor", ptxt.count(sec_anchor) == 1, "ledger section anchor unique")
        if FAILED:
            sys.exit(1)
        new_secs = '''<div class="sec">🌍 Cross-Industry Gap — is the whole industry under-capitalised?</div>
<div class="card" id="globalbox"></div>

<div class="sec">🏭 Under-Capitalised Industries — confidence-tiered</div>
<div class="card" id="indunder"></div>

''' + sec_anchor
        ptxt = ptxt.replace(sec_anchor, new_secs, 1)

        # hero gains real backlog coverage accounting
        old_hero = ("+'<div class=\"tile\"><div class=\"lbl\">Backlog joined</div><div class=\"big\" style=\"color:'"
                    "+((st.backlog_joined||0)>0?NEU:NEG)+'\">'+(st.backlog_joined||0)+'</div>"
                    "<div class=\"note\">'+((st.backlog_joined||0)>0?'RPO leg active':'⚠ leg inactive — no overlap')+'</div></div>'")
        new_hero = ("+'<div class=\"tile\"><div class=\"lbl\">Backlog leg</div><div class=\"big\" style=\"color:'"
                    "+((st.backlog_joined||0)>0?POS:NEG)+'\">'+(st.backlog_joined||0)+'</div>"
                    "<div class=\"note\">'+(st.backlog_joined||0)+' joined of '+(st.backlog_overlap||0)"
                    "+' overlapping · ledger '+(st.backlog_ledger_size||0)+'</div></div>'")
        gate(rep, "B.hero_anchor", ptxt.count(old_hero) == 1, "hero backlog tile anchor unique")
        if FAILED:
            sys.exit(1)
        ptxt = ptxt.replace(old_hero, new_hero, 1)

        # ledger: swap the dead rpo_growth_yoy column for the live fields
        ptxt = ptxt.replace("['rpo_growth_yoy','RPO YoY']", "['rpo_yoy','RPO YoY']", 1)
        ptxt = ptxt.replace(
            "['capture_gap','Gap'],",
            "['capture_gap','Gap'],['global_capture_gap','Global gap'],['gap_divergence','Diverg'],", 1)
        ptxt = ptxt.replace(
            "+'<td style=\"color:'+(r.rpo_growth_yoy==null?MUT:(r.rpo_growth_yoy>0?POS:NEG))+'\">'+num(r.rpo_growth_yoy)+'</td>'",
            "+'<td style=\"color:'+(r.rpo_yoy==null?MUT:(r.rpo_yoy>0?POS:NEG))+'\">'+num(r.rpo_yoy)+'</td>'", 1)
        ptxt = ptxt.replace(
            "+'<td style=\"color:'+gapCol(r.capture_gap)+';font-weight:600\">'+(r.capture_gap>0?'+':'')+num(r.capture_gap)+'pp</td>'",
            "+'<td style=\"color:'+gapCol(r.capture_gap)+';font-weight:600\">'+(r.capture_gap>0?'+':'')+num(r.capture_gap)+'pp</td>'"
            "+'<td style=\"color:'+gapCol(r.global_capture_gap)+'\">'+(r.global_capture_gap>0?'+':'')+num(r.global_capture_gap)+'pp</td>'"
            "+'<td style=\"color:'+(r.gap_divergence>0?POS:MUT)+'\">'+(r.gap_divergence>0?'+':'')+num(r.gap_divergence)+'</td>'", 1)

        # legs column shows availability (4 vs 5) so a missing leg isn't read as a failed one
        ptxt = ptxt.replace(
            "+'<td>'+legDots(r.legs||0)+'</td><td>'+tierChip(r.tier)+'</td></tr>';",
            "+'<td>'+legDots(r.legs||0)+(r.legs_available===4?'<span style=\"color:'+MUT+';font-size:9px\"> 4av</span>':'')+'</td>'"
            "+'<td>'+tierChip(r.tier)+'</td></tr>';", 1)

        # the two new renderers, injected before the METHOD block
        method_anchor = "// METHOD"
        gate(rep, "B.method_anchor", ptxt.count(method_anchor) == 1, "method anchor unique")
        if FAILED:
            sys.exit(1)
        renderer = '''// CROSS-INDUSTRY
var wg=c.widest_global_gaps||[];
document.getElementById('globalbox').innerHTML=(wg.length?('<table><tr><th>Ticker</th><th>Industry</th><th>Global gap</th><th>Within-industry</th><th>Divergence</th><th>Crit %ile (global)</th><th>Mcap %ile (global)</th><th>Tier</th></tr>'
+wg.map(function(r){return '<tr><td style="color:'+ACC+';font-weight:600">'+r.ticker+'</td>'
+'<td style="color:'+DIM+'">'+(r.industry||'').slice(0,26)+'</td>'
+'<td style="color:'+gapCol(r.global_capture_gap)+';font-weight:600">'+(r.global_capture_gap>0?'+':'')+num(r.global_capture_gap)+'pp</td>'
+'<td style="color:'+gapCol(r.capture_gap)+'">'+(r.capture_gap>0?'+':'')+num(r.capture_gap)+'pp</td>'
+'<td style="color:'+(r.gap_divergence>0?POS:MUT)+'">'+(r.gap_divergence>0?'+':'')+num(r.gap_divergence)+'pp</td>'
+'<td style="color:'+DIM+'">'+num(r.global_criticality_pctile)+'</td>'
+'<td style="color:'+DIM+'">'+num(r.global_mcap_pctile)+'</td>'
+'<td>'+tierChip(r.tier)+'</td></tr>';}).join('')+'</table>'
+'<div class="note">'+(c.global_method||'')+'</div>'
+'<div class="note">A large positive <strong>divergence</strong> (global ≫ within-industry) means the name looks fairly valued against its own peers but cheap against the whole market — i.e. the entire industry is under-capitalised, not the individual company. Both numbers ship because they answer different questions and disagreeing is informative.</div>')
:'<span class="mono" style="color:'+MUT+'">Cross-industry layer pending.</span>');

// INDUSTRY UNDERWEIGHT (confidence-tiered)
var iu=c.industry_underweight||[],iuT=c.industry_underweight_thin||[];
function confChip(k){var col=k==='HIGH'?POS:k==='MEDIUM'?ACC:MUT;
 return '<span class="tier" style="background:'+col+'22;color:'+col+';border:1px solid '+col+'55">'+k+'</span>';}
document.getElementById('indunder').innerHTML=(iu.length?('<table><tr><th>Industry</th><th>Names</th><th>Confidence</th><th>Median global gap</th><th>IQR</th><th>Range</th><th>Industry mcap</th></tr>'
+iu.map(function(x){return '<tr><td>'+(x.industry||'').slice(0,32)+'</td>'
+'<td style="color:'+DIM+'">'+x.n+'</td><td>'+confChip(x.sample_confidence)+'</td>'
+'<td style="color:'+gapCol(x.median_global_gap)+';font-weight:600">'+(x.median_global_gap>0?'+':'')+num(x.median_global_gap)+'pp</td>'
+'<td style="color:'+MUT+'">'+(x.iqr_global_gap==null?'—':num(x.iqr_global_gap)+'pp')+'</td>'
+'<td style="color:'+MUT+'">'+num(x.min_global_gap)+' … '+num(x.max_global_gap)+'</td>'
+'<td>'+money(x.industry_mcap_total)+'</td></tr>';}).join('')+'</table>'
+'<div class="note">Sorted by <strong>confidence first</strong>, then magnitude. A median over 3 names is not a base rate, so an 81-name reading outranks a 4-name one even if the 4-name median is higher. HIGH ≥20 names · MEDIUM ≥8 · LOW ≥5. IQR shows how much the names inside an industry actually disagree.</div>'
+(iuT.length?('<div class="note" style="margin-top:10px;color:'+MUT+'">Excluded as too thin (n&lt;5, shown for completeness, not for acting on): '
+iuT.slice(0,8).map(function(x){return x.industry+' (n='+x.n+', '+num(x.median_global_gap)+'pp)';}).join(' · ')+'</div>'):''))
:'<span class="mono" style="color:'+MUT+'">Industry board pending.</span>');

// METHOD'''
        ptxt = ptxt.replace(method_anchor, renderer, 1)
        PAGE.write_text(ptxt)
        rep.ok("page rewritten with cross-industry + confidence-tiered industry board")

        for k in ("global_capture_gap", "gap_divergence", "widest_global_gaps",
                  "industry_underweight", "sample_confidence", "iqr_global_gap",
                  "legs_available", "backlog_overlap", "backlog_ledger_size",
                  "global_criticality_pctile", "global_mcap_pctile", "rpo_yoy"):
            gate(rep, f"PAGE.renders_{k}", k in ptxt, "now rendered")
        gate(rep, "PAGE.no_dead_field", "rpo_growth_yoy" not in ptxt,
             "dead 3766 field removed from page")

        # ── DEPLOY ────────────────────────────────────────────────────────
        rep.section("Deploy")
        env = (lam.get_function_configuration(FunctionName=FN).get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1024,
                      description="Industry-criticality + capture gap v3.3 (within+cross industry, confidence-tiered industry medians).",
                      create_function_url=False, smoke=False)

        settled = False
        for i in range(12):
            time.sleep(15)
            c0 = lam.get_function_configuration(FunctionName=FN)
            if c0.get("State") != "Active" or c0.get("LastUpdateStatus") != "Successful":
                continue
            import urllib.request
            u = lam.get_function(FunctionName=FN)["Code"]["Location"]
            with urllib.request.urlopen(u, timeout=90) as r:
                blob = r.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if "sample_confidence" in z.read("lambda_function.py").decode("utf-8", "replace"):
                    settled = True
                    rep.ok("settled attempt %d" % (i + 1))
                    break
        gate(rep, "DEPLOY.settled", settled, "v3.3 live")
        if FAILED:
            sys.exit(1)

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
        iu = cap.get("industry_underweight") or []

        rep.section("Live verification")
        gate(rep, "LIVE.v33", d.get("version") == "3.3", "version=%s" % d.get("version"))
        gate(rep, "LIVE.iu_present", len(iu) > 0, "industry_underweight n=%d" % len(iu))
        gate(rep, "LIVE.conf_field", all("sample_confidence" in x for x in iu),
             "every row carries sample_confidence")
        gate(rep, "LIVE.floor", all(x.get("n", 0) >= 5 for x in iu),
             "n>=5 floor enforced (min n=%s)" % (min([x.get("n", 0) for x in iu]) if iu else "-"))
        if iu:
            top = iu[0]
            gate(rep, "LIVE.conf_sorted", top.get("sample_confidence") == "HIGH",
                 "top row is HIGH confidence (%s, n=%d) — thin curiosities demoted"
                 % (top.get("industry"), top.get("n", 0)))
        rep.kv(industries_high_conf=st.get("industries_high_conf"),
               backlog_joined=st.get("backlog_joined"),
               backlog_overlap=st.get("backlog_overlap"),
               scored=st.get("scored"))

        rep.section("Confidence-tiered industry board (was: 3-name medians on top)")
        for x in iu[:12]:
            rep.log("  %-34s n=%-3d %-6s median %+6.1fpp  IQR %s" % (
                (x.get("industry") or "")[:34], x.get("n") or 0,
                x.get("sample_confidence"), x.get("median_global_gap") or 0,
                x.get("iqr_global_gap")))
        thin = cap.get("industry_underweight_thin") or []
        if thin:
            rep.log("  --- demoted as too thin (n<5) ---")
            for x in thin[:6]:
                rep.log("  %-34s n=%-3d median %+.1fpp" % (
                    (x.get("industry") or "")[:34], x.get("n") or 0, x.get("median_global_gap") or 0))

        rep.section("Additive contract")
        for k in ("structural_names", "industry_leaders", "all_chokepoints",
                  "hidden_chokepoint_book", "cheap_chokepoint_book"):
            gate(rep, f"ADDITIVE.{k}", k in d, "present")
        rows = cap.get("all_rows") or []
        gate(rep, "ADDITIVE.within_gap", any(x.get("capture_gap") is not None for x in rows),
             "within-industry gap preserved")
        gate(rep, "ADDITIVE.global_gap", any(x.get("global_capture_gap") is not None for x in rows),
             "cross-industry gap preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — thin-sample flaw closed, page now renders every published field")


if __name__ == "__main__":
    main()
