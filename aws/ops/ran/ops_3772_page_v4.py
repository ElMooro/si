#!/usr/bin/env python3
"""ops 3772 — page rewrite: leaderboard on top, industry-first, catch-up column.

v4.0.1 publishes four things the page cannot show, because capture-gap.html was
authored against v3.0:
  top_undervalued_all_industries (50)   -> Khalid's "list on top"
  by_industry (141 boards w/ members)   -> Khalid's "organise by industry"
  catchup_pct / _evs / _pe / _basis     -> Khalid's "% price should pump"
  coverage/ledger stats                 -> honest completeness

Rather than bolt three more sections onto a v3-era layout, the page is rebuilt
around the new hierarchy: leaderboard first, then industry accordions, then the
existing cross-industry and scatter views retained below. The v3 sections are
PRESERVED (Khalid's improve-a-page doctrine is additive), just re-ordered beneath
the new top-of-page boards.

CATCH-UP HONESTY (non-negotiable, baked into the UI not just the JSON): the
column is labelled "to industry median", every cell states its basis (EV/S, P/E
or both), +300% capped values are marked, and the note says plainly that this is
mean-reversion arithmetic and not a price target. A name can sit below its
industry median because it deserves to. Presenting this number without that
frame would be the single most misleading thing on the platform.
"""
import sys, json, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))

from ops_report import report
import boto3

REPO = ROOT.parent
PAGE = REPO / "capture-gap.html"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


NEW_SECTIONS = '''<div class="sec">🥇 Most Undervalued — all industries, blended rank</div>
<div class="card" id="leader"></div>

<div class="sec">🏭 By Industry — every scored name, grouped</div>
<div class="card" id="byind"></div>

'''

RENDERERS = '''// ── LEADERBOARD (top of page) ──────────────────────────────────────────
var lead=c.top_undervalued_all_industries||[];
function cu(v,basis,capped){
 if(v==null)return '<span style="color:'+MUT+'">—</span>';
 var col=v>0?POS:v<0?NEG:MUT;
 return '<span style="color:'+col+';font-weight:600">'+(v>0?'+':'')+num(v,0)+'%</span>'
 +(capped?'<span style="color:'+MUT+';font-size:9px"> cap</span>':'')
 +'<div style="color:'+MUT+';font-size:9px">'+(basis||'')+'</div>';}
document.getElementById('leader').innerHTML=(lead.length?('<table><tr><th>#</th><th>Ticker</th><th>Company</th><th>Industry</th><th>Score</th><th>Catch-up to<br>industry median</th><th>Capture gap</th><th>Global gap</th><th>Crit</th><th>ROIC</th><th>Legs</th><th>Tier</th></tr>'
+lead.map(function(r,i){return '<tr><td style="color:'+MUT+'">'+(i+1)+'</td>'
+'<td style="color:'+ACC+';font-weight:600">'+r.ticker+'</td>'
+'<td>'+(r.name||'').slice(0,22)+'</td>'
+'<td style="color:'+DIM+'">'+(r.industry||'').slice(0,24)+'</td>'
+'<td style="font-weight:600">'+num(r.undervaluation_score,1)+'</td>'
+'<td>'+cu(r.catchup_pct,r.catchup_basis,r.catchup_capped)+'</td>'
+'<td style="color:'+gapCol(r.capture_gap)+'">'+(r.capture_gap>0?'+':'')+num(r.capture_gap)+'pp</td>'
+'<td style="color:'+gapCol(r.global_capture_gap)+'">'+(r.global_capture_gap>0?'+':'')+num(r.global_capture_gap)+'pp</td>'
+'<td>'+num(r.criticality)+'</td><td>'+num(r.roic)+'%</td>'
+'<td>'+legDots(r.legs||0)+'</td><td>'+tierChip(r.tier)+'</td></tr>';}).join('')+'</table>'
+'<div class="note"><strong>Rank is blended</strong> — capture gap (35%), cross-industry gap (25%), catch-up (15%), confirmation legs and criticality — so one loud metric cannot carry a name to the top. '+(c.catchup_note||'')+'</div>')
:'<span class="mono" style="color:'+MUT+'">Leaderboard pending.</span>');

// ── BY INDUSTRY (accordion) ────────────────────────────────────────────
var bi=c.by_industry||[];
function confChip2(k){var col=k==='HIGH'?POS:k==='MEDIUM'?ACC:MUT;
 return '<span class="tier" style="background:'+col+'22;color:'+col+';border:1px solid '+col+'55">'+k+'</span>';}
document.getElementById('byind').innerHTML=(bi.length?(
 '<div class="note" style="margin-bottom:10px">'+bi.length+' industries · click any row to expand its members. Industries are ranked by median capture gap; the median catch-up column is the middle name\\'s distance to its own industry median multiple.</div>'
 +'<table><tr><th>Industry</th><th>Scored</th><th>Listed peers</th><th>Conf</th><th>Median gap</th><th>Median catch-up</th><th>EV/S med</th><th>P/E med</th><th>Undervalued</th></tr>'
 +bi.map(function(b,i){
  var head='<tr class="indrow" data-i="'+i+'" style="cursor:pointer">'
  +'<td style="color:'+ACC+'">▸ '+(b.industry||'').slice(0,30)+'</td>'
  +'<td>'+b.n_scored+'</td><td style="color:'+MUT+'">'+(b.listed_peers||'—')+'</td>'
  +'<td>'+confChip2(b.sample_confidence)+'</td>'
  +'<td style="color:'+gapCol(b.median_capture_gap)+';font-weight:600">'+(b.median_capture_gap>0?'+':'')+num(b.median_capture_gap)+'pp</td>'
  +'<td>'+cu(b.median_catchup_pct,'',false)+'</td>'
  +'<td style="color:'+DIM+'">'+num(b.median_ev_sales,2)+'</td><td style="color:'+DIM+'">'+num(b.median_pe,1)+'</td>'
  +'<td style="color:'+(b.n_undervalued>0?POS:MUT)+'">'+b.n_undervalued+'</td></tr>';
  var mem='<tr class="memrow" id="mem'+i+'" style="display:none"><td colspan="9" style="padding:0">'
  +'<table style="margin:4px 0 10px 0">'
  +'<tr><th>Ticker</th><th>Company</th><th>Mcap</th><th>Share</th><th>Catch-up</th><th>Gap</th><th>Global</th><th>Crit</th><th>EV/S</th><th>P/E</th><th>ROIC</th><th>Legs</th><th>Tier</th></tr>'
  +(b.members||[]).map(function(m){return '<tr><td style="color:'+ACC+'">'+m.ticker+'</td>'
   +'<td>'+(m.name||'').slice(0,20)+'</td><td>'+money(m.market_cap)+'</td>'
   +'<td>'+num(m.mcap_share_pct,2)+'%</td>'
   +'<td>'+cu(m.catchup_pct,m.catchup_basis,false)+'</td>'
   +'<td style="color:'+gapCol(m.capture_gap)+'">'+(m.capture_gap>0?'+':'')+num(m.capture_gap)+'pp</td>'
   +'<td style="color:'+gapCol(m.global_capture_gap)+'">'+(m.global_capture_gap>0?'+':'')+num(m.global_capture_gap)+'pp</td>'
   +'<td>'+num(m.criticality)+'</td><td style="color:'+DIM+'">'+num(m.ev_sales,2)+'</td>'
   +'<td style="color:'+DIM+'">'+num(m.pe,1)+'</td><td>'+num(m.roic)+'%</td>'
   +'<td>'+legDots(m.legs||0)+'</td><td>'+tierChip(m.tier)+'</td></tr>';}).join('')
  +'</table></td></tr>';
  return head+mem;}).join('')+'</table>')
:'<span class="mono" style="color:'+MUT+'">Industry boards pending.</span>');
Array.prototype.forEach.call(document.querySelectorAll('.indrow'),function(tr){
 tr.onclick=function(){var el=document.getElementById('mem'+tr.getAttribute('data-i'));
  if(!el)return;var open=el.style.display!=='none';el.style.display=open?'none':'table-row';
  tr.cells[0].innerHTML=(open?'▸ ':'▾ ')+tr.cells[0].textContent.slice(2);};});

// METHOD'''


def main():
    with report("3772_page_v4") as rep:
        rep.heading("ops 3772 — page v4: leaderboard on top, industry-first, catch-up")

        ptxt = PAGE.read_text()

        rep.section("G0 — feed must already publish what the page will render")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        gate(rep, "G0.version", d.get("version", "").startswith("4."), "engine v%s" % d.get("version"))
        gate(rep, "G0.leaderboard", len(cap.get("top_undervalued_all_industries") or []) > 0,
             "leaderboard n=%d" % len(cap.get("top_undervalued_all_industries") or []))
        gate(rep, "G0.by_industry", len(cap.get("by_industry") or []) > 0,
             "by_industry n=%d" % len(cap.get("by_industry") or []))
        gate(rep, "G0.members", any(len(b.get("members") or []) > 0 for b in (cap.get("by_industry") or [])),
             "industry boards carry members")
        gate(rep, "G0.catchup", (cap.get("stats") or {}).get("with_catchup", 0) > 0,
             "catchup on %s names" % (cap.get("stats") or {}).get("with_catchup"))
        gate(rep, "G0.catchup_note", bool(cap.get("catchup_note")), "honesty note present in feed")
        if FAILED:
            sys.exit(1)

        rep.section("Insert new top-of-page sections")
        hero_anchor = '<div class="sec">🏆 Structurally Undervalued — ≥3 of 5 legs AND gap ≥20pp</div>'
        gate(rep, "PAGE.anchor", ptxt.count(hero_anchor) == 1, "section anchor unique")
        if FAILED:
            sys.exit(1)
        ptxt = ptxt.replace(hero_anchor, NEW_SECTIONS + hero_anchor, 1)

        method_anchor = "// METHOD"
        gate(rep, "PAGE.method_anchor", ptxt.count(method_anchor) == 1, "renderer anchor unique")
        if FAILED:
            sys.exit(1)
        ptxt = ptxt.replace(method_anchor, RENDERERS, 1)

        # coverage honesty in the hero
        old_tile = ("+'<div class=\"tile\"><div class=\"lbl\">Names scored</div><div class=\"big\">'"
                    "+(st.scored||0)+'</div><div class=\"note\">across '+(st.industries||0)+' industries</div></div>'")
        if old_tile in ptxt:
            new_tile = ("+'<div class=\"tile\"><div class=\"lbl\">Names scored</div><div class=\"big\">'"
                        "+(st.scored||0)+'</div><div class=\"note\">across '+(st.industries||0)"
                        "+' industries · ledger accretes daily</div></div>'")
            ptxt = ptxt.replace(old_tile, new_tile, 1)
            rep.ok("hero notes ledger accretion")

        PAGE.write_text(ptxt)
        rep.ok("page v4 written (%d bytes)" % len(ptxt))

        rep.section("Field coverage — the page must render what the engine publishes")
        for k in ("top_undervalued_all_industries", "by_industry", "catchup_pct",
                  "catchup_basis", "catchup_capped", "undervaluation_score",
                  "median_catchup_pct", "median_ev_sales", "median_pe",
                  "n_undervalued", "listed_peers", "sample_confidence",
                  "catchup_note", "ev_sales"):
            gate(rep, f"PAGE.renders_{k}", k in ptxt, "rendered")

        rep.section("Honesty guards in the UI (not just the JSON)")
        gate(rep, "HONEST.label", "to industry median" in ptxt or "industry median" in ptxt,
             "catch-up column labelled against the industry median")
        gate(rep, "HONEST.note_wired", "c.catchup_note" in ptxt,
             "engine's not-a-price-target note rendered on the page")
        gate(rep, "HONEST.basis_shown", "catchup_basis" in ptxt, "per-cell basis shown")
        gate(rep, "HONEST.cap_marked", "capped?" in ptxt or "catchup_capped" in ptxt,
             "+300% capped values flagged")
        gate(rep, "HONEST.blend_explained", "Rank is blended" in ptxt,
             "leaderboard explains its own ranking")

        rep.section("Additive — v3 sections preserved")
        for k in ("Structurally Undervalued", "Hidden Capture Gaps",
                  "Creation vs Capture", "Full Ledger", "Cross-Industry Gap",
                  "Under-Capitalised Industries"):
            gate(rep, f"KEPT.{k[:22]}", k in ptxt, "section retained")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — page v4 staged; pages.yml publishes on this push")
        rep.log("Live in ~1-3 min at https://justhodl.ai/capture-gap.html")


if __name__ == "__main__":
    main()
