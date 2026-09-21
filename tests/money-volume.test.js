const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-money-volume.js'),f=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/money-volume-native.json'),'utf8'));
const fetcher=async key=>{const raw=f.artifacts[key.slice(1)];return {ok:typeof raw==='string',status:typeof raw==='string'?200:404,arrayBuffer:async()=>new TextEncoder().encode(raw).buffer};};
test('Price-volume browser matches real Python compiler output and rejects tampered evidence',async()=>{
 assert.deepEqual(await api.verifyPacket(f.packet,fetcher),f.packet);
 const bad=structuredClone(f.packet);bad.stocks[0].price_volume_pressure_usd_proxy+=1;await assert.rejects(api.verifyPacket(bad,fetcher),/Current body differs/);
 await assert.rejects(api.verifyPacket(f.packet,async()=>({ok:true,arrayBuffer:async()=>new TextEncoder().encode('{}').buffer})),/Retained run differs/);
 for(const key of ['private/positions.json','data/money-volume-research/runs/../../private.json','https://example.invalid/'])await assert.rejects(api.load(key,fetcher),/Unapproved/);
});
test('Price-volume contracts preserve false authority, exact period and age limits',()=>{
 for(const key of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'])assert.equal(api.typed({...f.packet,[key]:true}),false);
 assert.equal(api.typed({...f.packet,generated_at:'1900-01-01'}),false);assert.equal(api.typed({...f.packet,period:{...f.packet.period,price_intervals:6}}),false);
 assert.equal(api.current(f.packet,Date.parse(f.packet.generated_at)-1),false);assert.equal(api.current(f.packet,Date.parse(f.packet.source_valid_until)),false);
 const html=api.render(f.packet,Date.parse(f.packet.source_valid_until));assert.match(html,/source check overdue/);assert.match(html,/Five price intervals; six turnover sessions/);assert.match(html,/does not measure investor net inflows/);assert.match(html,/Missing constituents are named/);
});
test('Signed exposure arithmetic and observed turnover scale have separate meanings',()=>{
 const a=api.scenario(f.packet,' aaa ','100000'),b=api.scenario(f.packet,'AAA','-100000');assert.equal(a.pnl_usd,20000);assert.equal(b.pnl_usd,-20000);assert.equal(a.observed_turnover_fraction_pct,10000);assert.equal(a.observed_turnover_fraction_pct,b.observed_turnover_fraction_pct);
 assert.equal(a.start_date,f.packet.period.start_date);assert.equal(a.end_date,f.packet.period.end_date);
 const noTurnover=structuredClone(f.packet);noTurnover.stocks[0].exact.mean_session_turnover_usd_proxy=null;assert.equal(api.scenario(noTurnover,'AAA',10).observed_turnover_fraction_pct,null);
 for(const value of ['',null,'Infinity','NaN','10000000001'])assert.throws(()=>api.scenario(f.packet,'AAA',value));
 assert.throws(()=>api.scenario(f.packet,'UNKNOWN',100),/retained configured/);
 const missing=structuredClone(f.packet);missing.stocks[0].missing_sessions=[f.packet.period.start_date];assert.throws(()=>api.scenario(missing,'AAA',100),/unavailable/);
});
test('Stock search, sector filter and bounded order retain uncovered rows honestly',()=>{
 assert.equal(api.selectRows(f.packet,{query:'Alpha'}).rows[0].ticker,'AAA');assert.equal(api.selectRows(f.packet,{sector:'Technology'}).count,2);
 assert.deepEqual(api.selectRows(f.packet,{sort:'ticker'}).rows.map(r=>r.ticker),['AAA','BBB','CCC']);assert.throws(()=>api.selectRows(f.packet,{sector:'Invented'}));assert.throws(()=>api.selectRows(f.packet,{sort:'forecast'}));
 assert.match(api.stockTable(f.packet,{query:'absent'}),/Showing 0 of 0/);
 const many=structuredClone(f.packet);many.stocks=Array.from({length:70},(_,i)=>({...many.stocks[0],ticker:'X'+i}));assert.equal(api.selectRows(many).rows.length,50);assert.equal(api.selectRows(many).count,70);
});
test('Stock scenario invalidates on edits and verified source refresh',()=>{
 const form={elements:{ticker:{value:'AAA'},exposure:{value:'100000'}}},out={textContent:''},host={querySelector:s=>s==='[data-volume-scenario]'?form:out};let packet=f.packet;
 const invalidate=api.bindScenario(host,()=>packet);form.onsubmit({preventDefault(){}});assert.match(out.textContent,/\$20,000.00/);assert.match(out.textContent,/not an executable participation rate/);form.oninput();assert.match(out.textContent,/recalculate/);packet=null;invalidate();form.onsubmit({preventDefault(){}});assert.match(out.textContent,/Wait for verified/);
});
test('Research renders safe labels and local page routes exist',()=>{
 const p=structuredClone(f.packet);p.sector_measurements[0].label='<img src=x onerror=evil()>';const html=api.render(p);assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
 const page=fs.readFileSync(path.join(__dirname,'../money-flow.html'),'utf8');assert.match(page,/jh-money-volume.js\?v=20260921-native1/);assert.doesNotMatch(page,/jh-wire.js|jh-page-ai.js/);
 for(const match of page.matchAll(/href="(\/[^"?#]*)"/g)){const route=match[1],target=route.endsWith('/')?route+'index.html':route;assert.ok(fs.existsSync(path.join(__dirname,'..',target.slice(1))),route);}
 const old=fs.readFileSync(path.join(__dirname,'../docs/legacy/sector-flow-pre-native-20260921.html.txt'),'utf8');assert.match(old,/function legacyRenderMoneyFlow/);assert.match(old,/Inspect original-source evidence/);
 const sector=fs.readFileSync(path.join(__dirname,'../sector-flow.html'),'utf8');assert.match(sector,/\/money-flow.html/);
 const worker=fs.readFileSync(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'),'utf8');assert.match(worker,/'money-flow-state.json'/);assert.match(worker,/\|money-volume-research/);
 assert.equal(fs.statSync(path.join(__dirname,'../aws/lambdas/justhodl-money-flow-state/source/legacy_money_flow_state.py')).size,6199);
});
