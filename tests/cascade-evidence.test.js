const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const ui=require('../jh-theme-cascade-evidence.js');
const flags={calls_eligible:false,ranking_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function packet(){const ref={bytes:1234,sha256:'a'.repeat(64)};return {measurement_contract:'theme-cascade-evidence.v1',status:'research_only',call:'WAIT',call_semantics:'abstain',...flags,generated_at:'2026-09-28T07:00:00Z',sources:[{source:'theme_rotation',source_key:'data/theme-momentum.json',status:'received_object_unqualified',original_ref:ref,...flags}],reported_theme_roster:{status:'reported_roster_unqualified',...flags,source_ref:{...ref},theme_occurrences:[{etf:'ETF1',source_index:0,source_pointer:'/all_themes/0',status:'reported_etf'},{etf:'ETF1',source_index:1,source_pointer:'/all_themes/1',status:'duplicate_reported_etf'}],membership_occurrences:[{etf:'ETF1',ticker:'SYNA',source_pointer:'/breadth_details/ETF1/constituents_perf/0',status:'reported_membership'}]}};}
test('missing legacy and forged eligible packets expose no legacy sizes',()=>{
 for(const p of [null,{}, {alert_tier:[{ticker:'FAKE',position_sizing:{final_pct:18}}]},{...packet(),sizing_eligible:true}]){
  const html=ui.render(p);assert.match(html,/WAIT \/ abstain/);assert.match(html,/Unavailable, conflicting or legacy/);assert.doesNotMatch(html,/FAKE|18/);
 }
});
test('seven source rows and exact whole-payload identity are displayed',()=>{
 const html=ui.render(packet());assert.equal((html.match(/<th scope="row">/g)||[]).length,9);assert.match(html,/1234 bytes/);assert.match(html,/a{64}/);
 assert.match(html,/1 of 7 declared inputs/);assert.match(html,/tabindex="0" aria-label="Cascade source evidence table"/);
 assert.match(html,/2 reported theme occurrences/);assert.match(html,/duplicate_reported_etf/);assert.match(html,/\/all_themes\/1/);
 assert.match(html,/1 extracted membership occurrences/);assert.match(html,/\/breadth_details\/ETF1\/constituents_perf\/0/);
});
test('missing source binding duplicate source rows and boolean counts cannot qualify a roster',()=>{
 const a=packet();a.reported_theme_roster.source_ref.bytes=false;assert.match(ui.render(a),/roster coordinates are unavailable/);
 const b=packet();b.sources.push({...b.sources[0]});assert.match(ui.render(b),/Missing or conflicting source/);assert.doesNotMatch(ui.render(b),/2 reported theme occurrences/);
 const c=packet();c.reported_theme_roster.source_ref.sha256='b'.repeat(64);assert.doesNotMatch(ui.render(c),/2 reported theme occurrences/);
 const d=packet();d.reported_theme_roster.theme_occurrences[0].source_index=true;assert.doesNotMatch(ui.render(d),/2 reported theme occurrences/);
});
test('unknown memberships are not reported as zero holdings',()=>{
 const p=packet();p.reported_theme_roster.membership_occurrences=null;const html=ui.render(p);
 assert.match(html,/Membership occurrence inventory unavailable/);assert.doesNotMatch(html,/0 extracted membership/);
});
test('source text and private annotations cannot inject markup or decisions',()=>{
 const p=packet();p.generated_at='<img src=x onerror=bad()>';p.sources[0].source_generated_at='<script>bad</script>';
 p.reported_theme_roster.theme_occurrences[0].private_note='private-pending';p.reported_theme_roster.membership_occurrences[0].ticker='<img>';
 const html=ui.render(p);assert.doesNotMatch(html,/<img|<script|private-pending/);assert.match(html,/&lt;img/);
});
test('current page clears stale claims and keeps read-only existing packet request',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../pre-pump-radar.html'),'utf8'),nodes={'theme-cascade-hero':{style:{}},'theme-cascade-content':{innerHTML:'18% trade'},'theme-cascade-age':{textContent:'fresh'}};
 const start=html.indexOf('function renderThemeCascade(doc) {'),end=html.indexOf('function _legacy_renderThemeCascade(doc) {');
 vm.runInNewContext(html.slice(start,end)+'renderThemeCascade(null);',{JHCascadeEvidence:ui,document:{getElementById:id=>nodes[id]}});
 assert.equal(nodes['theme-cascade-hero'].style.display,'block');assert.doesNotMatch(nodes['theme-cascade-content'].innerHTML,/18% trade/);
 assert.equal(nodes['theme-cascade-age'].textContent,'Research only');assert.match(html,/async function fetchThemeCascade\(\) \{\s+renderThemeCascade\(null\);/);
 assert.match(html,/THEME_CASCADE_URL \+ "\?exact=1&nogen=1&t="/);assert.match(html,/src="\/jh-theme-cascade-evidence.js/);
});
