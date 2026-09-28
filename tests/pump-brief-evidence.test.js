const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../pre-pump-radar.html'),'utf8');
const start=html.indexOf('function renderBriefHero(){'),end=html.indexOf('function _legacy_renderBriefHero(){',start);
assert.ok(start>=0&&end>start);const code=html.slice(start,end);
function packet(){return {measurement_contract:'pump-brief-evidence.v1',status:'research_only',call:'WAIT',call_semantics:'abstain',calls_eligible:false,ranking_eligible:false,sizing_eligible:false,execution_eligible:false,generated_at:'2026-09-28T01:00:00Z',sources:[]};}
function render(p){
 const nodes={'brief-hero':{style:{}},'brief-content':{innerHTML:''},'brief-age':{textContent:''}};
 const scope={BRIEF:p,document:{getElementById:id=>nodes[id]},esc:v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))};
 vm.runInNewContext(code+'\nrenderBriefHero();',scope);return {html:nodes['brief-content'].innerHTML,age:nodes['brief-age'].textContent,display:nodes['brief-hero'].style.display};
}
test('missing, failed, legacy or forged eligible brief remains visible and abstains',()=>{
 for(const p of [null,{}, {status:'error'}, {conviction_grade:'A',top_3_long_ideas:[{sized_position:'99%'}]}, {...packet(),sizing_eligible:true}]){
  const r=render(p);assert.equal(r.display,'block');assert.match(r.html,/WAIT \/ abstain/);assert.match(r.html,/unavailable or legacy/);assert.doesNotMatch(r.html,/99%|grade-a/);
 }
});
test('all thirteen declared sources have a visible row even when omitted or duplicated',()=>{
 const p=packet();p.sources=[{source:'pairs',source_key:'data/pair-trades.json',status:'received_object_unqualified'},{source:'pairs',source_key:'data/pair-trades.json',status:'source_error'}];
 const r=render(p);assert.equal((r.html.match(/<th scope="row">/g)||[]).length,13);assert.match(r.html,/Missing or conflicting source/);assert.match(r.html,/role="region" aria-label="Brief source availability"/);assert.match(r.html,/tabindex="0"/);
});
test('whole-source identity and future publication clocks are explicit without raw private context',()=>{
 const p=packet();p.sources=[{source:'momentum',source_key:'data/momentum-leaders.json',status:'received_object_unqualified',source_generated_at:'2099-01-01T00:00:00Z',source_clock_status:'future',original_ref:{bytes:2621725,sha256:'a'.repeat(64)},private_note:'do not render'}];
 const r=render(p);assert.match(r.html,/2621725 bytes/);assert.match(r.html,/future clock; unqualified/);assert.match(r.html,/JSON received · unqualified/);assert.doesNotMatch(r.html,/do not render/);assert.match(r.html,/does not establish observation freshness/);
});
test('untrusted source labels and model narratives cannot inject markup or trades',()=>{
 const p=packet();p.executive_summary='<img src=x onerror=alert(1)>';p.risk_warnings=['buy now'];p.sources=[{source:'momentum',source_key:'data/momentum-leaders.json',status:'<script>alert(1)</script>',source_generated_at:'<b>bad date</b>',original_ref:{bytes:false,sha256:'x'}}];
 const r=render(p);assert.doesNotMatch(r.html,/<script|<img|<b>|buy now/);assert.match(r.html,/Unrecognized status/);assert.match(r.html,/Unavailable/);
});
test('whole predecessor is retained and only the brief renderer is replaced',()=>{
 const old=fs.readFileSync(path.join(__dirname,'fixtures/pre-pump-brief-evidence-pre-pump-radar.html.txt'),'utf8');assert.ok(old.length>200000);
 assert.ok(old.includes('function renderBriefHero(){'));assert.ok(html.includes('function _legacy_renderBriefHero(){'));
 assert.equal(old.replace('function renderBriefHero(){',code+'function _legacy_renderBriefHero(){').replace(/\r\n/g,'\n'),html.replace(/\r\n/g,'\n'));
});
