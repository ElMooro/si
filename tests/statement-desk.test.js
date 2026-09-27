const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const D=require('../jh-statement-desk.js'),V=require('../jh-table-values.js'),root=path.join(__dirname,'..');

test('statement values distinguish absent, false, zero, typed flags and impossible dates',()=>{
 for(const v of [null,undefined,false,true,'', ' ',{},[],Infinity,'1e-999'])assert.equal(D.amount(v),'');
 assert.equal(D.amount(0),'0');assert.equal(D.amount('1000000000'),'1.00B');assert.equal(D.flag('false'),'');assert.equal(D.flag(false),'No');assert.equal(D.flag(true),'Yes');
 for(const v of ['2026-02-30','yesterday',false,'2026-09-01T12:00:00Z'])assert.equal(D.date(v),'');
 assert.equal(D.date('2026-02-28'),'2026-02-28');
});
test('dilution retains every overlapping, conflicting and unidentified source occurrence',()=>{
 const packet={dilution_offset_warnings:[{symbol:'A',issuance_ttm:0},{symbol:'',issuance_ttm:1},false],net_shrinkers:[{symbol:'A',issuance_ttm:200}],fresh_authorizations:false};
 const p=D.population(packet,'dilution');assert.equal(p.rows.length,3);assert.equal(p.malformed,1);assert.equal(p.rows[2].record.issuance_ttm,200);assert.equal(p.rows[2].source,'net_shrinkers[0]');assert.ok(p.issues.includes('fresh_authorizations is a malformed population'));assert.equal(packet.net_shrinkers[0].issuance_ttm,200);
 assert.equal(D.population({dilution_offset_warnings:[]},'dilution').rows.length,0);
});
test('deferred records retain source keys, unknown fields and malformed values',()=>{
 const packet={by_ticker:{A:{ticker:'A',deferred_rev:false,unknown:42},B:null,C:{ticker:'C',deferred_rev:0}}};
 const p=D.population(packet,'deferred');assert.equal(p.rows.length,2);assert.equal(p.malformed,1);assert.equal(p.rows[0].source,'by_ticker["A"]');assert.equal(p.rows[0].record.unknown,42);
 assert.throws(()=>D.population({by_ticker:[]},'deferred'),/missing or malformed/);assert.throws(()=>D.population({},'unexpected'),/Unreviewed/);
});
test('statement numeric sort is numeric, stable and unavailable-last in both directions',()=>{
 const rows=['',false,'10','2',0].map(v=>({record:{v},source:'record'}));
 assert.deepEqual(rows.slice().sort((a,b)=>D.compare(a,b,'v',1,'number')).map(x=>x.record.v),[0,'2','10','',false]);
 assert.deepEqual(rows.slice().sort((a,b)=>D.compare(a,b,'v',-1,'number')).map(x=>x.record.v),['10','2',0,'',false]);
 assert.equal(D.cell({record:{net_issuer:'false'}},'net_issuer','boolean'),'');
});

async function render(packet,mode,error){
 const nodes=new Map(),calls=[];const get=id=>{if(!nodes.has(id))nodes.set(id,{value:'',innerHTML:'',textContent:''});return nodes.get(id);};
 const raw=JSON.stringify(packet),context=vm.createContext({console,document:{getElementById:get,querySelectorAll:()=>[]},JHTableValues:{...V,load:async source=>{calls.push(source);if(error)throw error;return{packet,raw};}}});
 vm.runInContext(fs.readFileSync(path.join(root,'jh-statement-desk.js'),'utf8'),context);
 context.options={mode,source:'/data/'+(mode==='deferred'?'backlog':'buyback-engine')+'.json'};
 await vm.runInContext('JHStatementDesk.start(options)',context);return{get,calls,raw};
}
test('statement browser rendering keeps full raw source, escapes metadata and filters occurrences',async()=>{
 const s=await render({by_ticker:{TEN:{ticker:'TEN',deferred_rev:'10',deferred_accelerating:'false'},TWO:{ticker:'TWO',deferred_rev:'2',sector:'<img src=x>'},BAD:false},generated_at:'<img src=x>'},'deferred');
 const h=s.get('board').innerHTML;assert.ok(h.indexOf('TEN')<h.indexOf('TWO'));assert.ok(!h.includes('<img'));assert.ok(h.includes('&lt;img'));assert.match(h,/2 of 2 received source records/);assert.match(h,/1 malformed/);assert.equal(s.get('original').textContent,s.raw);assert.deepEqual(s.calls,['/data/backlog.json']);
 s.get('q').value='two';s.get('q').oninput();assert.match(s.get('board').innerHTML,/1 of 2/);
 const d=await render({dilution_offset_warnings:[{symbol:'A',net_issuer:'false'},{symbol:'A',net_issuer:false}],net_shrinkers:[{symbol:'A',net_issuer:true}]},'dilution');assert.match(d.get('board').innerHTML,/3 of 3/);assert.match(d.get('board').innerHTML,/>No</);assert.match(d.get('board').innerHTML,/>Yes</);
});
test('statement failed reads and malformed populations retain available originals',async()=>{
 const error=Error('Duplicate JSON key');error.original_text='{"a":0,"a":1}';const failed=await render(null,'deferred',error);assert.match(failed.get('board').textContent,/Publication unavailable/);assert.equal(failed.get('original').textContent,error.original_text);
 const malformed=await render({by_ticker:false},'deferred');assert.match(malformed.get('board').textContent,/missing or malformed/);assert.equal(malformed.get('original').textContent,'{"by_ticker":false}');
});
test('whole page predecessors survive and new pages explain unverified units and periods',()=>{
 for(const [name,hash]of [['deferred-revenue','f4103f246722e86c172084d9b3103b0dd680ef0e4d2dd0a4c0555a583b28ec56'],['dilution','0764769e90cd0a60cc607fb9cc399e7ed76b69d54f6f558fad76a295f0b02c9f']]){
  const old=fs.readFileSync(path.join(__dirname,'fixtures/pre-'+name+'-desk.html.txt'));assert.equal(crypto.createHash('sha256').update(old).digest('hex'),hash);
  const source=fs.readFileSync(path.join(root,name+'.html'),'utf8');assert.match(source,/<label for="q">/);assert.match(source,/role="region"[^>]*tabindex="0"/);assert.match(source,/Complete original publication/);assert.match(source,/not a verified|not verified/);assert.ok(!/[\u00c2\u00c3\ufffd]/.test(source));
 }
});

test('buyback ledger stays visible with empty forecast boards and full period metadata',async()=>{
 const packet={tickers:{A:{symbol:'A',gross_repurchases_ttm:0,net_buyback_ttm:null,measurements:{cashflow_window:{unit:'USD',start_date:'2025-07-01',end_date:'2026-06-30',status:'four_reported_calendar_quarters'}}},B:{symbol:'B',company_name:'<img src=x>',gross_repurchases_ttm:false}},high_conviction_pumps:[]};
 const s=await render(packet,'buyback');assert.match(s.get('board').innerHTML,/2 of 2/);assert.ok(s.get('board').innerHTML.includes('2025-07-01'));assert.ok(s.get('board').innerHTML.includes('2026-06-30'));assert.ok(s.get('board').innerHTML.includes('USD'));assert.ok(!s.get('board').innerHTML.includes('<img'));
 assert.equal(s.get('original').textContent,JSON.stringify(packet));assert.deepEqual(s.calls,['/data/buyback-engine.json']);
 s.get('q').value='B';s.get('q').oninput();assert.match(s.get('board').innerHTML,/1 of 2/);
 const d=D.population({...packet,net_shrinkers:[{symbol:'A',net_buyback_ttm:900}]},'dilution');assert.equal(d.rows.length,3);assert.equal(d.rows[2].record.net_buyback_ttm,900);
 assert.throws(()=>D.population({tickers:[]},'buyback'),/missing or malformed/);
});

test('buyback page retains its complete original and binds the descriptive ticker ledger',()=>{
 const raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-buyback-accounting-desk.html.txt'));
 assert.equal(raw.length,14188);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'1e99c155b08c2949243c35a65ee4bf9edabfed3e99b6bc829b1a74a90db6797e');
 const html=fs.readFileSync(path.join(root,'buybacks.html'),'utf8');assert.match(html,/mode:"buyback"/);assert.match(html,/<label for="q">/);assert.match(html,/role="region"[^>]*tabindex="0"/);assert.match(html,/Complete original publication/);assert.ok(!html.includes('sc=r.buyback_score||0'));
});
