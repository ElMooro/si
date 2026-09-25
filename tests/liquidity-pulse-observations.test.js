const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto');
const ui=require('../liquidity-pulse.js');
const now=Date.parse('2026-09-25T21:00:00Z');
const packet=()=>({generated_at:'2026-09-25T16:00:00Z',series:{
 WALCL:{latest_value:6747704,latest_date:'2026-09-23'},WTREGEN:{latest_value:977084,latest_date:'2026-09-23'},
 OTHL1690:{latest_value:2855,latest_date:'2026-09-23'},SWP1690:{latest_value:0,latest_date:'2026-09-23'},
 BAMLH0A3HYC:{latest_value:11.12,latest_date:'2026-09-24'},HQMCB10YR:{latest_value:5.58,latest_date:'2026-08-01',frequency:'daily'}}});

test('million-dollar series convert by 1000 to billions; real zero and percentages stay distinct',()=>{
 const rows=Object.fromEntries(ui.view(packet(),now).rows.map(r=>[r.sid,r]));
 assert.equal(rows.WALCL.value,'$6,747.704 bn');assert.equal(rows.WTREGEN.value,'$977.084 bn');
 assert.equal(rows.OTHL1690.value,'$2.855 bn');assert.equal(rows.SWP1690.value,'$0.000 bn');
 assert.equal(rows.BAMLH0A3HYC.value,'11.12%');assert.match(rows.OTHL1690.basis,/all reported loans/);
 assert.match(rows.WTREGEN.basis,/Weekly average/);assert.equal(rows.HQMCB10YR.observation,'2026-08 (monthly period)');
 assert.match(rows.HQMCB10YR.basis,/Monthly estimated yield/);
});

test('unknown series, conflicting units, malformed dates and non-numbers cannot be current measurements',()=>{
 const p=packet();p.series.UNKNOWN={latest_value:123,latest_date:'2026-09-23'};
 p.series.WALCL.units='Billions of Dollars';p.series.WTREGEN.latest_value='977084';
 p.series.OTHL1690.latest_date='2026-02-30';p.series.SWP1690.latest_date='2026-09-26';p.series.HQMCB10YR.latest_date='2026-08-15';
 for(const r of ui.view(p,now).rows.filter(r=>['UNKNOWN','WALCL','WTREGEN','OTHL1690','SWP1690','HQMCB10YR'].includes(r.sid)))assert.equal(r.value,'Unavailable');
 for(const value of [null,undefined,NaN,Infinity,true,'0'])assert.equal(ui.format(value,'usd_mn'),'Unavailable');
});

test('stale wrapper or observations are labeled stale while dated reported evidence remains visible',()=>{
 const p=packet();p.generated_at='2026-09-20T00:00:00Z';
 let v=ui.view(p,now);assert.equal(v.status,'Unavailable or stale packet');assert.equal(v.rows[0].status,'Stale reported observation');
 assert.equal(v.rows[0].value,'$6,747.704 bn');
 p.generated_at='2026-09-25T16:00:00Z';p.series.WALCL.latest_date='2026-08-01';
 assert.equal(ui.view(p,now).rows[0].status,'Stale reported observation');
 p.generated_at='2026-09-26T00:00:00Z';assert.equal(ui.view(p,now).status,'Unavailable or stale packet');
});

test('legacy score/narrative fields never become direction, crisis status, or unescaped HTML',()=>{
 const p=packet();p.summary='<img src=x onerror=alert(1)>';p.composites={liquidity_score:99,liquidity_regime:'CRISIS'};
 p.series.WALCL.interpretation='Buy everything now';p.series['<svg onload=alert(1)>']={latest_value:1};
 const html=ui.panelHTML(p,now);assert(!html.includes('<img'));assert(!html.includes('<svg'));
 assert(!html.includes('Buy everything now'));assert(!html.includes('CRISIS'));assert.match(html,/&lt;svg/);
 assert.match(html,/Complete legacy packet/);assert.match(html,/have not been replayed against retained originals/);
});

test('failed refresh clears old data; browser requests omit credentials and recheck age',async()=>{
 const host={innerHTML:''},head={appendChild(){}},timers=[],requests=[];let good=true;
 const doc={readyState:'complete',head,getElementById:id=>id==='liquidity-pulse-panel'?host:null,createElement:()=>({})};
 const win={document:doc,JUSTHODL_LIQ_NO_PILL:true,setInterval:(f,t)=>timers.push({f,t}),fetch:async(url,options)=>{requests.push({url,options});return {ok:good,json:async()=>packet()};}};
 ui.install(win);await new Promise(resolve=>setImmediate(resolve));assert.match(host.innerHTML,/6,747\.704/);
 assert.equal(requests[0].options.credentials,'omit');assert.equal(requests[0].options.cache,'no-store');
 assert(timers.some(t=>t.t===60000));good=false;await timers.find(t=>t.t===300000).f();
 assert(!host.innerHTML.includes('6,747.704'));assert.match(host.innerHTML,/Unavailable or stale packet/);
});

test('complete predecessors are byte-preserved and liquidity no longer fetches unqualified AI commentary',()=>{
 const migration=JSON.parse(fs.readFileSync('tests/fixtures/liquidity-observation-display-migration.json','utf8'));
 for(const p of migration.complete_predecessors){const raw=fs.readFileSync(p.predecessor);assert.equal(raw.length,p.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),p.sha256);}
 const page=fs.readFileSync('liquidity.html','utf8');assert(!page.includes('ai-commentary/'));assert(!page.includes('jh-page-ai.js'));assert(!page.includes('JHAIBrief.mount'));
 assert.match(page,/How to read this research/);assert.match(page,/id="liquidity-pulse-panel"/);
});

test('Macro Rooms shares the complete unit-aware viewer and cannot synthesize a missing Pulse score',()=>{
 const page=fs.readFileSync('macro-rooms.html','utf8');
 assert.equal((page.match(/id="liquidity-pulse-panel"/g)||[]).length,1);
 assert.equal((page.match(/src="\/liquidity-pulse.js\?v=20260925research"/g)||[]).length,1);
 assert.match(page,/window.JUSTHODL_LIQ_NO_PILL=true/);
 assert(!page.includes('get("/data/liquidity-pulse.json")'));
 assert(!page.includes('sum.score||sum.composite||50'));assert(!page.includes('panels.slice(0,3)'));
 const all=ui.view(packet(),now).rows;assert.equal(all.length,11);
 assert.equal(all.find(row=>row.sid==='WALCL').nativeUnit,'USD millions');
 assert.equal(all.find(row=>row.sid==='HQMCB10YR').nativeUnit,'Percent');
 const ref=JSON.parse(fs.readFileSync('tests/fixtures/macro-rooms-pulse-migration.json','utf8')).complete_predecessor;
 const raw=fs.readFileSync(ref.path);assert.equal(raw.length,ref.bytes);
 assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),ref.sha256);
});
