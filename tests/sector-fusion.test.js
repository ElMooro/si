const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-sector-fusion.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/sector-fusion-native.json'),'utf8'));
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Sector matrix browser verifies actual Python run/output bytes and rejects tampering',async()=>{
 assert.deepEqual(await api.verifyPacket(f.packet,fetcher),f.packet);
 const bad=structuredClone(f.packet);bad.sectors[0].windows['5d'].issuer.value_usd+=1;await assert.rejects(api.verifyPacket(bad,fetcher),/Current sector body differs/);
 await assert.rejects(api.verifyPacket(f.packet,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained sector run differs/);
 for(const key of ['private/positions.json','data/sector-fusion-research/runs/../../private.json','https://example.invalid/'])await assert.rejects(api.load(key,fetcher),/Unapproved/);
});
test('Sector matrix preserves explicit authority and distinct source periods',()=>{
 for(const k of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'])assert.equal(api.typed({...f.packet,[k]:true}),false);
 assert.equal(api.typed({...f.packet,generated_at:'1900-01-01'}),false);assert.equal(api.typed({...f.packet,sectors:f.packet.sectors.slice(1)}),false);
 assert.equal(api.current(f.packet,Date.parse(f.packet.generated_at)-1),false);assert.equal(api.current(f.packet,Date.parse(f.packet.source_valid_until)),false);
 assert.match(api.render(f.packet,Date.parse(f.packet.source_valid_until)),/source check overdue/);
 const detail=api.detail(f.packet,'XLK','5d');assert.match(detail,/dates differ/);assert.match(detail,/not the holdings of this ETF/);assert.match(detail,/not a confidence interval/);
 const p=structuredClone(f.packet);p.sectors[0].windows['5d'].comparison.status='issuance_not_larger_than_display_sensitivity';assert.match(api.matrix(p,'5d'),/Estimate ≤ display sensitivity/);
});
test('Historical basket uses signed exposure and a disclosed net-dollar benchmark',()=>{
 const a=api.scenario(f.packet,{XLK:100000,XLF:-50000},'5d');assert.equal(a.pnl_usd,5000);assert.equal(a.gross_exposure_usd,150000);assert.equal(a.net_exposure_usd,50000);assert.equal(a.net_exposure_SPY_pnl_usd,2500);assert.equal(a.excess_vs_net_SPY_usd,2500);
 const b=api.scenario(f.packet,{XLK:-100000,XLF:50000},'5d');assert.equal(b.pnl_usd,-5000);assert.equal(b.gross_exposure_usd,a.gross_exposure_usd);
 assert.equal(api.scenario(f.packet,{XLK:100000,XLF:-100000},'5d').net_exposure_SPY_pnl_usd,0);
 for(const values of [{},{XLK:NaN},{XLK:true},{XLK:'10000000001'},{BAD:1}])assert.throws(()=>api.scenario(f.packet,values,'5d'));
 assert.throws(()=>api.scenario(f.packet,{XLK:10},'forecast'));
});
test('Portfolio refuses mismatched periods and missing endpoints',()=>{
 const p=structuredClone(f.packet);p.sectors[1].windows['5d'].price.start_date='2026-09-09';assert.throws(()=>api.scenario(p,{XLK:100,XLF:100},'5d'),/identical dates/);
 p.sectors[0].windows['5d'].price.status='missing_exact_endpoint';assert.throws(()=>api.scenario(p,{XLK:100},'5d'),/unavailable/);
});
test('Sector scenario invalidates on edits and source refresh',()=>{
 const form={elements:{horizon:{value:'5d'},...Object.fromEntries(f.packet.sectors.map(r=>[r.symbol,{value:r.symbol==='XLK'?'100000':'0'}]))}},out={textContent:'',innerHTML:''},host={querySelector:s=>s==='[data-fusion-scenario]'?form:out};let packet=f.packet;
 const invalidate=api.bindScenario(host,()=>packet);form.onsubmit({preventDefault(){}});assert.match(out.innerHTML,/\$10,000.00/);assert.match(out.innerHTML,/not risk-equivalent/);form.oninput();assert.match(out.textContent,/recalculate/);packet=null;invalidate();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/Wait for verified/);
});
test('Sector evidence escapes source labels and preserves the complete old page',()=>{
 const p=structuredClone(f.packet);p.sectors[0].sector='<img src=x onerror=evil()>';assert.match(api.matrix(p,'5d'),/&lt;img/);assert.doesNotMatch(api.matrix(p,'5d'),/<img/);
 const page=fs.readFileSync(path.join(__dirname,'../sector-flow.html'),'utf8');assert.match(page,/jh-sector-fusion.js\?v=20260921-native1/);assert.doesNotMatch(page,/jh-wire.js|jh-page-ai.js|legacyRenderMoneyFlow/);
 for(const match of page.matchAll(/href="(\/[^"?#]*)"/g)){const route=match[1],target=route.endsWith('/')?route+'index.html':route;assert.ok(fs.existsSync(path.join(__dirname,'..',target.slice(1))),route);}
 const archive=fs.readFileSync(path.join(__dirname,'../docs/legacy/sector-flow-pre-native-20260921.html.txt'),'utf8');assert.match(archive,/function legacyRenderMoneyFlow/);
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'sector-flow-state.json'/);assert.match(worker,/\|sector-fusion-research\|sector-capital-research/);
});
