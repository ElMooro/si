const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const ui=require('../jh-issuer-classifications.js');
const at='2026-09-28T07:00:00Z',now=Date.parse(at),flags={calls_eligible:false,ranking_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
function packet(){
 const ref={bytes:1234,sha256:'a'.repeat(64),key:'audit-private/20260909-originals/momentum-leaders-research/classification-context/sources/'+'a'.repeat(64)+'.bin'};
 const source={ticker:'SYNA',endpoint:'https://financialmodelingprep.com/stable/profile?symbol=SYNA',status:'received',http_status:200,original_ref:ref,requested_at:at,received_at:at,acquisition:'provider_request',network_attempted:true};
 return {measurement_contract:'issuer-classification-observations.v1',status:'research_only',call:'WAIT',call_semantics:'abstain',...flags,generated_at:at,
  selection:{selected_tickers:['SYNA']},profile_sources:[source],classification_observations:[{ticker:'SYNA',status:'reported_classification',industry:'Software—Application',sector:'Technology',issuer_symbol_matched:true,source_pointer:'/0',original_ref:{...ref},received_at:at,requested_at:at,acquisition:'provider_request'}]};
}
test('legacy missing future and eligible packets withhold active-theme claims',()=>{
 for(const p of [null,{}, {themes:{Fake:{is_active:true}}},{...packet(),sizing_eligible:true},{...packet(),generated_at:'2099-01-01T00:00:00Z'}]){
  const html=ui.render(p,now);assert.match(html,/evidence unavailable, conflicting or legacy/);assert.doesNotMatch(html,/Fake|Software/);
 }
});
test('classification uses literal provider text and exposes exact source identity and coordinate',()=>{
 const html=ui.render(packet(),now);assert.match(html,/Software—Application/);assert.doesNotMatch(html,/AI software/);
 assert.match(html,/1234 bytes/);assert.match(html,/a{64}/);assert.match(html,/\/0\/industry/);
 assert.match(html,/tabindex="0" aria-label="Issuer classification evidence table"/);assert.match(html,/New provider acquisition/);
});
test('ambiguous lists forged identities and row clocks never display a classification',()=>{
 for(const edit of [p=>p.selection.selected_tickers.push('SYNA'),p=>p.classification_observations[0].original_ref.bytes=false,
  p=>p.classification_observations[0].ticker='OTHER',p=>p.classification_observations[0].industry=true,p=>p.profile_sources[0].http_status=true,
  p=>p.classification_observations[0].received_at='2026-09-28T06:00:00Z',p=>p.profile_sources[0].original_ref.key='private/accounts.json']){
  const p=packet();edit(p);assert.doesNotMatch(ui.render(p,now),/Software—Application/);
 }
});
test('reused profile displays original clock and becomes visibly expired',()=>{
 const p=packet();p.profile_sources[0].acquisition='retained_profile';p.profile_sources[0].network_attempted=false;p.classification_observations[0].acquisition='retained_profile';
 assert.match(ui.render(p,now+86400000),/Reused · original clock preserved/);
 assert.match(ui.render(p,now+604800001),/Older than seven days/);assert.match(ui.render(p,now+604800001),/2026-09-28T07:00:00Z/);
});
test('empty scope and missing secondary classification never become a hot zero',()=>{
 const p=packet();p.selection.selected_tickers=[];p.profile_sources=[];p.classification_observations=[];
 assert.match(ui.render(p,now),/explicitly reports an empty/);assert.doesNotMatch(ui.render(p,now),/0 themes/);
 const q=packet();q.classification_observations[0].industry=null;q.classification_observations[0].status='industry_unavailable';assert.match(ui.render(q,now),/Unavailable \/ conflicting/);
});
test('provider labels and unselected private fields cannot inject markup',()=>{
 const p=packet();p.classification_observations[0].industry='<img src=x onerror=bad()>';p.classification_observations[0].private_note='secret-note';
 const html=ui.render(p,now);assert.doesNotMatch(html,/<img|secret-note/);assert.match(html,/&lt;img/);
});
test('actual page clears classification state before refresh and uses existing exact non-generating request',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../pre-pump-radar.html'),'utf8');
 assert.match(html,/async function load\(\)\{\s+THEMES = null;\s+EARLY = null;/);
 assert.match(html,/fetch\(THEMES_URL \+ "\?exact=1&nogen=1&t="/);assert.match(html,/src="\/jh-issuer-classifications.js/);
 const start=html.indexOf('function renderEarlyDetection(){'),end=html.indexOf('function _legacy_renderEarlyDetection(){',start);
 const nodes={'early-panel':{style:{}},'early-content':{innerHTML:'old trade'}};
 vm.runInNewContext(html.slice(start,end)+'renderEarlyDetection();',{document:{getElementById:id=>nodes[id]},EARLY:null,THEMES:null,JHVolumeObservations:{render:()=>''},JHIssuerClassifications:ui});
 assert.match(nodes['early-content'].innerHTML,/Classification evidence unavailable/);assert.doesNotMatch(nodes['early-content'].innerHTML,/old trade/);
});
