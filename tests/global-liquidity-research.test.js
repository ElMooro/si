const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const api=require('../jh-global-liquidity-research.js');
const now=Date.parse('2026-09-19T06:00:00Z'),stamp='2026-09-19T05:00:00Z';
function packet(){
 const ids=['WALCL','ECBASSETSW','JPNASSETS','DEXUSEU','DEXJPUS','WTREGEN','RRPONTSYD','M2SL'];
 const series=Object.fromEntries(ids.map(sid=>[sid,{available:true,current_decimal:'0',last_observed_value:'0',name:sid,date:'2026-09-18',frequency:'D',unit:'Native unit',acquired_at:stamp,quality:{status:'fresh'},changes:{year:{pct_change:0}}}]));
 const selection={status:'descriptive',max_carry_age_days:10,carry_age_days:1,selected:{effective_observation_date:'2026-09-18',observation_label:'2026-09-18',native_decimal:'0'}};
 const snapshot={status:'descriptive',valuation_date:'2026-09-19',total_usd_millions_decimal:'0',missing_components:[],unit:'USD_millions',components:Object.fromEntries(['WALCL','ECBASSETSW','JPNASSETS'].map(sid=>[sid,{balance:selection,fx:null,fx_series_id:null,usd_millions_decimal:'0'}]))};
 return {contract:api.CONTRACT,generated_at:stamp,source_generated_at:stamp,series,quality:{fresh_series:8,expected_series:8},three_bank_subtotal:snapshot,us_net_liquidity_proxy:{net:0},us_m2:{year_comparison:{pct_change:0}},calendar_research:{history:{'2026-09-18':snapshot},latest_changes:{'13w':{status:'missing_endpoint',start:'2026-06-19',end:'2026-09-18',calendar_days:91}},complete_weekly_slots:1,expected_weekly_slots:1}};
}
test('legacy packet cannot render as current research and zero is data',()=>{
 assert.throws(()=>api.render({regime:'EXPANDING'}));assert.equal(api.num(0),'0');assert.equal(api.num(null),'—');assert.equal(api.num(false),'—');
 const view=api.render(packet(),now);assert.match(view.summary,/<strong>0<\/strong>/);assert.match(view.status,/abstain/);
});
test('expiry prevents fresh headlines; a pinned snapshot is explicitly dated',()=>{
 const p=packet(),later=now+2*86400000;assert.equal(api.currentFresh(p,later),false);
 assert.ok(!api.render(p,later).summary.includes('<strong>0</strong>'));
 assert.match(api.render(p,later,true).status,/Pinned snapshot/);assert.match(api.render(p,later,true).summary,/Snapshot three-bank/);
 const c=p.three_bank_subtotal.components.WALCL;c.balance={...c.balance,selected:{effective_observation_date:'2026-09-08'}};
 assert.equal(api.currentFresh(p,now),false);p.generated_at='2099-01-01T00:00:00Z';assert.equal(api.currentFresh(p,now),false);
});
test('provider strings and evidence links are safe, missing periods remain explicit',()=>{
 const p=packet();p.series.WALCL.name='<img onerror=alert(1)>';p.series.WALCL.evidence={observations:{key:'javascript:alert(1)'}};
 const view=api.render(p,now);assert.ok(!view.series.includes('<img'));assert.ok(!view.series.includes('href="javascript:'));
 assert.equal(api.path('data/evidence/../secret'),null);assert.match(api.change(p,'13w'),/Comparison unavailable/);
 assert.match(api.change(p,'13w'),/91 calendar days/);
});
test('chart retains missing observations as gaps and no page AI route exists',()=>{
 const html=api.chart({'2026-09-04':{total_usd_millions_decimal:'1'},'2026-09-11':{total_usd_millions_decimal:null},'2026-09-18':{total_usd_millions_decimal:'2'}});
 assert.match(html,/d=" M[^L"]+ M/);
 const page=fs.readFileSync('global-liquidity.html','utf8');assert.ok(!page.includes('jh-page-ai'));assert.match(page,/13 weeks · 91 days/);
});
test('permanent snapshot validates manifest and full output bytes and refuses corruption',async()=>{
 const output=new TextEncoder().encode(JSON.stringify(packet())),hash=await api.sha(output);
 const m={contract:'global-liquidity-replay.v1',generated_at:stamp,output_sha256:hash,output:{key:api.PREFIX+'outputs/'+hash+'.json',sha256:hash,bytes:output.length},compilers:{}};
 const raw=new TextEncoder().encode(JSON.stringify(m)),id=await api.sha(raw);
 const files={['/'+api.PREFIX+'runs/'+id+'.json']:raw,['/'+m.output.key]:output};
 const fetcher=async key=>new Response(files[key],{status:files[key]?200:404});
 const p=await api.loadSnapshot(id,fetcher);assert.equal(p.contract,api.CONTRACT);assert.equal(p.replay.output_sha256,hash);
 files['/'+m.output.key]=new TextEncoder().encode('{}');await assert.rejects(api.loadSnapshot(id,fetcher),/output differs/);
 await assert.rejects(api.loadSnapshot('../latest',fetcher),/Invalid snapshot/);
 files['/'+api.PREFIX+'runs/'+id+'.json']=new TextEncoder().encode('{}');await assert.rejects(api.loadSnapshot(id,fetcher),/manifest hash differs/);
});
test('artifact size guard cancels oversized stream rather than accepting a partial snapshot',async()=>{
 const fetcher=async()=>new Response(new Uint8Array(256*1024+1));
 await assert.rejects(api.loadSnapshot('a'.repeat(64),fetcher),/size bound/);
});
