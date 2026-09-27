const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const M=require('../jh-materials-orders.js'),V=require('../jh-table-values.js'),root=path.join(__dirname,'..');
function macro(){return {NEWORDER:{source:{series_id:'NEWORDER'},series:'NEWORDER',unit:'Millions of U.S. dollars',frequency:'M',seasonal_adjustment:'SA',data_unavailable:false,value:110,prev:100,as_of:'2026-08-01',observation_period:'2026-08-01',prev_date:'2026-07-01'}};}
const neworder=p=>M.official(p,'2026-09-27').find(r=>r.id==='NEWORDER');
test('orders MoM requires adjacent monthly periods and explicit official identity',()=>{
 assert.equal(neworder(macro()).mom,10.000000000000009);
 for(const [key,value] of [['prev_date','2026-06-01'],['prev_date','2026-07-31'],['prev',0],['prev',false],['value',false]]){
  const p=macro();p.NEWORDER[key]=value;assert.equal(neworder(p).mom,null);
 }
 for(const [key,value] of [['frequency','Q'],['unit','billions'],['seasonal_adjustment','NSA'],['data_unavailable',true],['as_of','2026-12-01'],['observation_period','2026-07-01']]){
  const p=macro();p.NEWORDER[key]=value;assert.equal(neworder(p).level,null);assert.equal(neworder(p).mom,null);
 }
 const p=macro();p.NEWORDER.source.series_id='OTHER';assert.equal(neworder(p).level,null);
});
test('explicit zero is preserved and year rollover uses calendar months',()=>{
 const p=macro();p.NEWORDER.value=0;assert.equal(neworder(p).level,0);assert.equal(neworder(p).mom,-100);
 p.NEWORDER.as_of=p.NEWORDER.observation_period='2026-01-01';p.NEWORDER.prev_date='2025-12-01';assert.equal(neworder(p).mom,-100);
 assert.equal(M.nextMonth('2026-02-30'),'');assert.equal(M.day('2026-02-29'),'');
 const missing=M.official(null,'2026-09-27');assert.equal(missing.length,6);assert.ok(missing.every(r=>r.level===null&&r.mom===null));
 assert.equal(missing.find(r=>r.id==='ISM_NO_INV').unit,'index points');
});
test('every company occurrence survives without merging conflicting ticker periods',()=>{
 const p={inventory:{generated_at:'older',sector_drawdown:[{sector:'Wholesale'},false],stock_drawdown_board:[{ticker:'A',dio_latest:0},{ticker:'A',dio_latest:30},null]},backlog:{generated_at:'newer',by_ticker:{A:{ticker:'A',rpo:null},B:{ticker:'B',deferred_rev:500},BAD:false}},capex:{rows:[{ticker:'A',capex_ttm_b:null,current_window:{start_date:'2025-07-01',end_date:'2026-06-30',status:'unavailable'}},{ticker:'',capex_ttm_b:0}]}};
 const data=M.records(p);assert.equal(data.sectors.length,1);assert.equal(data.firms.length,6);assert.equal(data.issues.length,3);
 assert.equal(data.firms.filter(r=>r.ticker==='A').length,4);assert.equal(data.firms[0].dio,0);assert.equal(data.firms[1].dio,30);assert.equal(data.firms[2].rpo,null);
 assert.equal(data.firms[3].original.deferred_rev,500);assert.equal(data.firms[4].window_start,'2025-07-01');assert.equal(data.firms[5].ticker,'');
 assert.ok(data.firms.every(r=>r.po===undefined));
});
test('display typing does not turn false or empty strings into zero',()=>{
 for(const v of [false,true,'', ' ',Infinity,{},[]])assert.equal(M.formatted(v,'number'),'');
 assert.equal(M.formatted(0,'number'),'0');assert.equal(M.formatted(0,'percent'),'0.00%');assert.equal(M.formatted(false,'text'),'');
 assert.equal(M.esc('<img>'),'&lt;img&gt;');
});
async function render(packets,failed){
 const nodes=new Map(),calls=[];function get(id){if(!nodes.has(id))nodes.set(id,{value:'',textContent:'',innerHTML:''});return nodes.get(id);}
 const map={'/data/inventory-drawdown.json':'inventory','/data/backlog.json':'backlog','/data/capex-pulse.json':'capex','/data/canary-macro.json':'macro'};
 const context=vm.createContext({document:{getElementById:get,querySelectorAll:()=>[]},JHTableValues:{...V,load:async p=>{calls.push(p);if(map[p]===failed){const e=Error('Malformed source');e.original_text='{"x":0,"x":1}';throw e;}const packet=packets[map[p]];return {packet,raw:JSON.stringify(packet)};}}});
 vm.runInContext(fs.readFileSync(path.join(root,'jh-materials-orders.js'),'utf8'),context);await vm.runInContext('JHMaterialsOrders.start()',context);return{get,calls};
}
test('all four full sources remain inspectable and one failed feed does not hide others',async()=>{
 const p={inventory:{sector_drawdown:[{sector:'<img src=x>',latest_ratio:0}],stock_drawdown_board:[{ticker:'A',dio_latest:0}]},backlog:{by_ticker:{A:{ticker:'A',rpo:10}}},capex:{rows:[{ticker:'B',capex_ttm_b:null}]},macro:macro()};
 const s=await render(p);assert.equal(s.calls.length,4);assert.equal(new Set(s.calls).size,4);for(const name of Object.keys(p))assert.equal(s.get('original-'+name).textContent,JSON.stringify(p[name]));
 assert.ok(!s.get('secBoard').innerHTML.includes('<img'));assert.match(s.get('secBoard').innerHTML,/&lt;img/);assert.match(s.get('firmStatus').textContent,/3 of 3/);
 assert.match(s.get('firmBoard').innerHTML,/<a href="\/ticker.html\?symbol=A">A<\/a>/);
 s.get('q').value='capex';s.get('q').oninput();assert.match(s.get('firmStatus').textContent,/1 of 3/);
 const failed=await render(p,'backlog');assert.equal(failed.get('original-backlog').textContent,'{"x":0,"x":1}');assert.match(failed.get('firmStatus').textContent,/2 of 2/);assert.match(failed.get('kpis').textContent,/backlog.json unavailable/);
});
test('whole predecessor, explicit source scripts and keyboard-accessible regions remain',()=>{
 const raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-materials-orders-source-separation.html.txt'));assert.equal(raw.length,12773);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'9dc1edbf40d47cd7ca2c12b60f5f48153c89e235434b1aa94f75206ad9172f57');
 const page=fs.readFileSync(path.join(root,'materials-orders.html'),'utf8');assert.match(page,/jh-materials-orders.js/);assert.match(page,/<label for="q">/);assert.equal((page.match(/class="table-region" role="region"[^>]*tabindex="0"/g)||[]).length,3);
 assert.match(page,/jh-table-values\.js\?v=materials-20260927/); // Existing browsers must load the expanded source-path contract.
 assert.match(page,/fred.stlouisfed.org\/series\/NEWORDER/);assert.match(page,/does not establish declining inventories/);assert.ok(!page.includes('Date.now()'));
});
test('materials source reads retain exact non-generating paths',async()=>{
 const calls=[];const source='{"generated_at":"2026-09-27","rows":[]}';
 for(const p of ['/data/inventory-drawdown.json','/data/canary-macro.json']){
  const r=await V.load(p,{fetcher:async(url,opts)=>{calls.push({url,opts});return{ok:true,body:{getReader:()=>{let read=false;return{read:async()=>read?{done:true}:(read=true,{done:false,value:new TextEncoder().encode(source)}),cancel:async()=>{}};}}};}});assert.equal(r.raw,source);
 }
 assert.deepEqual(calls.map(x=>x.url),['/data/inventory-drawdown.json?exact=1&nogen=1','/data/canary-macro.json?exact=1&nogen=1']);assert.ok(calls.every(x=>x.opts.redirect==='error'));
 await assert.rejects(V.load('/data/private-account.json'),/Unreviewed/);
});
