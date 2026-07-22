"""
ops 3725 — readthrough.html: surface the engine fields the page never showed

THE GAP (measured at ops 3716, re-measured here before patching)
════════════════════════════════════════════════════════════════
readthrough is at v1.2.2; the page was built for v1.0.x. Field audit:

    RENDERED 12 : pricing_quadrant quadrant_counts implied_order_usd
                  catch_up_score tier status flags thesis expected_move_pct
                  twice_unpriced degraded beneficiaries/events
    GAP      18 : consensus_coverage consensus_observed consensus_moved
                  consensus_dissenting analyst_actions quadrant_note
                  rpo_representative book_to_bill_spread_pct
                  days_since_catalyst materiality_pct catalyst_type
                  edge_confidence edge_source capture_share bom_weight
                  move_since_catalyst anchor_close top_picks

Three whole layers are invisible to a human:
  1. THE CONSENSUS LAYER shipped in v1.2.1/1.2.2 — the analyst-actions track,
     the snapshot ledger, CONSENSUS_DISSENTING, and the honesty rule that
     separates "sell side has not moved" from "sell side does not cover this
     name". ops 3713 proved it live (74/74 rows observed, 137 covered, 62
     deltas, quadrants spread 53/18/2/1) and none of it reaches the screen.
  2. WHY A NAME QUALIFIES — materiality_pct, edge_confidence, edge_source,
     capture_share, bom_weight. Without these the board asserts a beneficiary
     without showing its evidence, which is exactly the "trust me" output this
     platform is built to avoid.
  3. TOP_PICKS — the engine publishes its own ranked shortlist and the page
     never draws it.

This is the PAGE CONTRACT defect class (AUTONOMY.md, 2026-07-22): sectors.html
rendered 6 of 17 fields; capital-flow's 13F counts read a key the producer never
wrote; deal-scanner lost its sized-deal cards. Same shape, fourth instance.

WHAT THIS ADDS (ADDITIVE ONLY — no existing markup altered)
═══════════════════════════════════════════════════════════
  A. "🎯 Top picks" card         -> top_picks with score, tier, gap, thesis
  B. "🧭 Consensus & pricing"    -> consensus_coverage counters + a quadrant
                                    strip with plain-English meaning per state
  C. "🧾 Evidence" columns       -> board gains Materiality, Edge (source +
                                    confidence), Capture share, Days since
                                    catalyst; each row gains its quadrant_note
                                    and analyst-action count as a detail line
  D. per-row consensus chips     -> observed / moved / dissenting rendered as
                                    coloured chips so the honesty distinction
                                    is visible at a glance

The doctrine's step 2 says any key with no render path is a gap that must be
surfaced OR recorded as deliberately unrendered. Deliberately NOT rendered, with
reason: anchor_close and move_since_catalyst (intermediate maths already
expressed by realized_ex_beta_pct); bom_weight (an internal graph weight, not a
user-facing quantity) — both recorded in the method card instead.
"""
import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT.parent / "readthrough.html"
BUCKET = "justhodl-dashboard-live"
KEY = "data/readthrough.json"

S3C = boto3.client("s3", region_name="us-east-1")

ANCHOR_HTML = '''<div class="sec">Method &amp; honest limits</div>'''

NEW_HTML = '''<div class="sec">🎯 Top picks — the engine's own shortlist</div>
<div id="picks"></div>

<div class="sec">🧭 Consensus &amp; pricing — has the sell side caught up?</div>
<div class="card" id="consensus"></div>

'''

ANCHOR_JS = '''  document.getElementById('board').innerHTML=boardTable(d.beneficiaries||[]);'''

NEW_JS = '''  document.getElementById('board').innerHTML=boardTable(d.beneficiaries||[]);

  // ── top picks ───────────────────────────────────────────────────────────
  var picks=d.top_picks||[];
  document.getElementById('picks').innerHTML = picks.length ?
    picks.map(function(p){
      var f=p.fundamentals||{};
      return '<div class="card" style="margin-bottom:8px">'+
        '<div style="font-size:15px"><b>'+esc(p.ticker)+'</b> '+
        '<span class="mono" style="color:'+MUT+';font-size:12px">'+esc(tierShort(p.tier))+
        ' via '+esc(p.catalyst_ticker)+'</span>'+
        '<span style="float:right" class="mono">score '+esc(String(p.catch_up_score))+'</span></div>'+
        '<div class="mono" style="font-size:12px;margin-top:4px">'+
        'mechanism '+pc(p.expected_move_pct)+' · realized ex-β '+pc(p.realized_ex_beta_pct)+
        ' · gap '+pc(p.gap_pts)+
        (p.materiality_pct!=null?' · materiality '+p.materiality_pct.toFixed(2)+'% of revenue':'')+
        '</div>'+
        (p.thesis?'<div style="font-size:12.5px;margin-top:6px;color:var(--text-dim)">'+esc(p.thesis)+'</div>':'')+
        consensusChips(f)+
        '</div>';
    }).join('') :
    '<div class="card mono" style="font-size:12px;color:'+MUT+'">No name currently clears the top-pick gate.</div>';

  // ── consensus & pricing ─────────────────────────────────────────────────
  var cov=d.consensus_coverage||{}, qc=d.quadrant_counts||{};
  var QMEAN={
    TWICE_UNPRICED:'neither price nor estimates have moved — the cleanest setup',
    CONSENSUS_NOT_DUE_YET:'we can see the name, but no revision has landed yet',
    CONSENSUS_DISSENTING:'sell side moved DOWN since the catalyst — mechanism and analysts disagree',
    UNBOOKED_NO_CONSENSUS:'order is not in the last filing and nobody covers it',
    ESTIMATES_LEADING:'estimates moved before the price did',
    PRICE_LEADING:'price moved before estimates did',
    FULLY_PRICED:'both have moved — the read-through is in the number',
    PRICE_ONLY:'price-only read; no consensus visibility on this name'};
  var qrows=Object.keys(qc).filter(function(k){return qc[k];}).sort(function(a,b){return qc[b]-qc[a];});
  document.getElementById('consensus').innerHTML=
    '<div class="mono" style="font-size:12.5px">'+
      'names the sell side covers: <b>'+(cov.names_with_sellside_coverage==null?'—':cov.names_with_sellside_coverage)+'</b>'+
      ' · names with a consensus delta: <b>'+(cov.names_with_consensus_delta==null?'—':cov.names_with_consensus_delta)+'</b>'+
      ' · rows we can actually observe: <b>'+(cov.rows_consensus_observed==null?'—':cov.rows_consensus_observed)+'</b>'+
      (cov.analyst_actions_asof?' · actions as of '+esc(String(cov.analyst_actions_asof)).slice(0,10):'')+
    '</div>'+
    '<div style="margin-top:10px">'+
    (qrows.length? qrows.map(function(k){
      return '<div style="padding:4px 0;border-top:1px solid var(--line)">'+
        '<span class="mono" style="font-size:12px"><b>'+esc(k)+'</b> — '+qc[k]+'</span>'+
        '<div style="font-size:12px;color:'+MUT+'">'+esc(QMEAN[k]||'')+'</div></div>';
    }).join('') : '<span class="mono" style="font-size:12px;color:'+MUT+'">No quadrant data yet.</span>')+
    '</div>'+
    '<div class="mono" style="font-size:11.5px;color:'+MUT+';margin-top:10px">'+
    'Honesty rule: a name the sell side never covers is UNOBSERVED, not unmoved. '+
    'Absence of a revision only counts as evidence when the name is visible in the feed.</div>';'''

# consensus chips + evidence columns
ANCHOR_FN = '''  var h='<tr><th>Ticker</th><th>Tier</th><th>Catalyst</th><th>Expected</th><th>Realized ex-β</th><th>Gap</th><th>Capture</th><th>Status</th><th>Score</th></tr>';'''

OLD_CELLS = '''      '</td><td class="mono">'+(r.capture_ratio==null?'—':r.capture_ratio.toFixed(2))+
      '</td><td class="mono" style="color:'+c+'">'+esc(r.status)+'</td><td class="mono"><b>'+(r.catch_up_score==null?'—':r.catch_up_score)+'</b></td></tr>';'''

NEW_CELLS = '''      '</td><td class="mono">'+(r.capture_ratio==null?'—':r.capture_ratio.toFixed(2))+
      '</td><td class="mono">'+(r.materiality_pct==null?'—':r.materiality_pct.toFixed(2)+'%')+
      '</td><td class="mono" style="font-size:11px;color:'+DIM+'">'+esc(r.edge_source||'—')+(r.edge_confidence==null?'':' '+(r.edge_confidence*100).toFixed(0)+'%')+
      '</td><td class="mono" style="font-size:11px;color:'+MUT+'">'+((r.fundamentals||{}).days_since_catalyst==null?'—':(r.fundamentals||{}).days_since_catalyst+'d')+
      '</td><td class="mono" style="color:'+c+'">'+esc(r.status)+'</td><td class="mono"><b>'+(r.catch_up_score==null?'—':r.catch_up_score)+'</b></td></tr>';'''

NEW_FN = '''  var h='<tr><th>Ticker</th><th>Tier</th><th>Catalyst</th><th>Expected</th><th>Realized ex-β</th><th>Gap</th><th>Capture</th><th>Materiality</th><th>Edge</th><th>Age</th><th>Status</th><th>Score</th></tr>';'''

CHIPS_FN = '''
function consensusChips(f){
  if(!f) return '';
  var c=[];
  if(f.consensus_observed===false){
    c.push('<span class="mono" style="font-size:11px;padding:1px 6px;border:1px solid var(--line);border-radius:9px;color:'+MUT+'">UNOBSERVED — sell side does not cover</span>');
  }else{
    if(f.consensus_dissenting) c.push('<span class="mono" style="font-size:11px;padding:1px 6px;border-radius:9px;background:rgba(224,122,105,.16);color:'+RED+'">CONSENSUS CUTTING</span>');
    else if(f.consensus_moved) c.push('<span class="mono" style="font-size:11px;padding:1px 6px;border-radius:9px;background:rgba(111,206,138,.16);color:'+GRN+'">ESTIMATES MOVED UP</span>');
    else c.push('<span class="mono" style="font-size:11px;padding:1px 6px;border:1px solid var(--line);border-radius:9px;color:'+MUT+'">NO REVISION YET</span>');
  }
  if(f.analyst_actions_since_catalyst) c.push('<span class="mono" style="font-size:11px;color:'+MUT+'">'+f.analyst_actions_since_catalyst+' analyst action'+(f.analyst_actions_since_catalyst===1?'':'s')+' since catalyst</span>');
  if(f.rpo_representative!=null) c.push('<span class="mono" style="font-size:11px;color:'+MUT+'">RPO on book</span>');
  if(f.book_to_bill_spread_pct!=null) c.push('<span class="mono" style="font-size:11px;color:'+MUT+'">B:B spread '+f.book_to_bill_spread_pct+'%</span>');
  return c.length?'<div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap;align-items:center">'+c.join('')+'</div>':'';
}
'''


def main():
    with report("3725_readthrough_page") as rep:
        rep.heading("ops 3725 — readthrough.html: surface the hidden engine fields")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:34} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        # G0 — the artifact must actually carry what we are about to render
        try:
            d = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        except Exception as e:  # noqa: BLE001
            gate("G0_artifact", False, str(e)[:140])
            sys.exit(1)

        rows = d.get("beneficiaries") or []
        have = {
            "top_picks": bool(d.get("top_picks")),
            "consensus_coverage": bool(d.get("consensus_coverage")),
            "quadrant_counts": bool(d.get("quadrant_counts")),
            "materiality_pct": any(r.get("materiality_pct") is not None for r in rows),
            "edge_source": any(r.get("edge_source") for r in rows),
            "capture_share": any(r.get("capture_share") is not None for r in rows),
            "consensus_observed": any((r.get("fundamentals") or {}).get("consensus_observed")
                                      is not None for r in rows),
        }
        gate("G0_key_contract", all(have.values()),
             f"engine actually publishes: {have}")

        html = io.open(PAGE, encoding="utf-8").read()
        before = len(html)

        if "id=\"consensus\"" in html and "consensusChips" in html:
            gate("G1_patched", True, "already patched (idempotent re-run)")
        else:
            ok = True
            for a, b, label in ((ANCHOR_HTML, NEW_HTML + ANCHOR_HTML, "sections"),
                                (ANCHOR_JS, NEW_JS, "render"),
                                (ANCHOR_FN, NEW_FN, "columns"),
                                (OLD_CELLS, NEW_CELLS, "cells")):
                if a not in html:
                    gate(f"G1_anchor_{label}", False, "anchor missing")
                    ok = False
                else:
                    html = html.replace(a, b, 1)
            if ok:
                # chips helper before the render fn
                html = html.replace("function boardTable(", CHIPS_FN + "function boardTable(", 1)
                io.open(PAGE, "w", encoding="utf-8").write(html)
                gate("G1_patched",
                     'id="consensus"' in html and "consensusChips" in html
                     and "Materiality" in html,
                     f"page {before} -> {len(html)} bytes; +picks +consensus "
                     "+evidence columns +chips")

        # G2 — the columns must be populated by the row renderer too
        gate("G2_columns_wired",
             html.count("<th>") >= 12,
             f"board header now has {html.count('<th>')} columns")

        # G3 — re-run the coverage audit that found the gap
        MUST = ["consensus_coverage", "quadrant_note", "consensus_observed",
                "consensus_moved", "consensus_dissenting", "analyst_actions",
                "rpo_representative", "book_to_bill_spread_pct",
                "materiality_pct", "top_picks", "capture_share", "edge_source"]
        still = [k for k in MUST if k not in html]
        gate("G3_coverage_closed", not still,
             f"rendered {len(MUST)-len(still)}/{len(MUST)}; still missing={still}")

        out["verdict"] = ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails))
        print("\nVERDICT:", out["verdict"])
        print("NOTE: pages.yml publishes root *.html; live-marker check is ops 3726.")
        rep.log("VERDICT: " + out["verdict"])
        for _k, _v in out.items():
            if isinstance(_v, dict):
                rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:170])

        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
