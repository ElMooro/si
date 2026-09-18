const test=require('node:test'),assert=require('node:assert/strict');
const {eligible,view,panelHTML,install}=require('../tenor-signals.js');
const now=Date.parse('2026-09-18T12:00:00Z');
const packet=()=>({schema_version:'treasury-tenor-research.v2',role:'research_measurements',generated_at:new Date(now).toISOString(),measurements:{nominal_2y:{state:'AVAILABLE',latest_auction:{auction_date:'2026-09-17',quote_basis:'nominal_yield_pct',reopening:false},prior_auction:{auction_date:'2026-08-17'},metrics:{yield_change_bp:0}}}});
test('old or stale feeds cannot show CALM or a synthetic zero score',()=>{
 assert.equal(view({any_firing:false,composite_score:0},now).status,'UNAVAILABLE');
 const d=packet();d.generated_at='2025-01-01';assert.equal(eligible(d,now),false);
 assert.doesNotMatch(panelHTML(d,now),/CALM|0\/100/);
});
test('measured zero is distinct from unavailable and carries a quote/date label',()=>{
 const html=panelHTML(packet(),now);assert.match(html,/0\.00 bp/);assert.match(html,/nominal_yield_pct/);assert.match(html,/2026-08-17/);assert.match(html,/Unavailable/);
 assert.match(html,/do not authorize a position size/);
});
test('provider strings and retained-manifest links cannot inject HTML or script URLs',()=>{
 const d=packet();d.measurements.nominal_2y.latest_auction.quote_basis='<img onerror=x>';d.reproducibility={key:'javascript:alert(1)'};
 const html=panelHTML(d,now);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|href="javascript/);
});
test('a failed refresh clears previously displayed measurements',async()=>{
 const panel={innerHTML:''},pill={};let cycle,fail=false;
 const doc={readyState:'complete',querySelector:()=>pill,getElementById:id=>id==='jhTenorStyles'?{}:panel};
 const win={document:doc,setInterval:fn=>{cycle=fn},fetch:async()=>{if(fail)throw Error('offline');const d=packet();d.generated_at=new Date().toISOString();return {ok:true,json:async()=>d}}};
 install(win);await new Promise(resolve=>setImmediate(resolve));assert.match(panel.innerHTML,/RESEARCH ONLY/);
 fail=true;await cycle();assert.doesNotMatch(panel.innerHTML,/0\.00 bp/);assert.match(pill.textContent,/unavailable/);
});
