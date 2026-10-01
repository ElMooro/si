const assert=require('node:assert/strict'),test=require('node:test'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const external=fs.existsSync(path.join(__dirname,'jh-chart-catalog.js'));
const root=external?path.join(__dirname,'..','si-batch-improvements'):path.join(__dirname,'..');
const candidate=external?path.join(__dirname,'jh-chart-catalog.js'):path.join(root,'jh-chart-catalog.js');
async function load(master,file=candidate){
 const inputs={'/data/symbology/master.json':master,'/data/indicator-bus.json':{indicators:{}},'/data/provider-catalog.json':{providers:[]},
  'https://justhodl-data-proxy.raafouis.workers.dev/data/symdir/instruments.json.gz':{rows:[],cols:[]},'/data/cryptoquant-series.json':{series:{}}};
 const requests=[],context={setTimeout,clearTimeout};context.window=context;
 context.fetch=async url=>{assert.ok(Object.hasOwn(inputs,url));requests.push(url);return {ok:true,status:200,json:async()=>JSON.parse(JSON.stringify(inputs[url]))};};
 vm.runInNewContext(fs.readFileSync(file,'utf8'),context,{filename:'/invented/catalog.js'});
 const api=context.JHChartCatalog,counts=await api.ensureIndex();assert.equal(requests.length,5);
 return {api,counts,inputs,requests};
}
const row={name:'Invented issuer',cik:'0000000111',isin:'US0000000000',isin_match_qualified:false};
test('unqualified identifiers remain searchable candidates without automatic conversion',async()=>{
 const {api}=await load({by_ticker:{ZZTEST:row}});
 assert.equal(api.lookupSym(row.isin),'');assert.equal(api.chartId(row.isin),'');
 const hits=api.suggest(row.isin);assert.equal(hits.length,1);assert.equal(hits[0].s,'ZZTEST');assert.match(hits[0].extra,/Unverified identifier candidates/);
});
test('duplicate identifiers never silently choose the first issuer',async()=>{
 const {api}=await load({by_ticker:{ZZFIRST:row,ZZSECOND:{...row,cik:'0000000222'}}});
 assert.equal(api.lookupSym(row.isin),'');assert.deepEqual(Array.from(api.suggest(row.isin),r=>r.s).sort(),['ZZFIRST','ZZSECOND']);
});
test('every malformed identifier type is isolated from valid ticker lookup',async()=>{
 for(const value of [true,99,{},[],['x']]){
  const {api}=await load({by_ticker:{ZZBAD:{name:{bad:true},cusip:value,isin:value,figi:value},ZZGOOD:row}});
  assert.equal(api.lookupSym('unrelated'),'');assert.equal(api.lookupSym('ZZGOOD'),'ZZGOOD');assert.equal(api.lookupSym('ZZBAD'),'ZZBAD');
 }
});
test('hyphenated and dotted tickers retain their exact identity',async()=>{
 const {api}=await load({by_ticker:{'TEST-A':{name:'Invented A'},'TEST.B':{name:'Invented B'}}});
 assert.equal(api.lookupSym(' test-a '),'TEST-A');assert.equal(api.lookupSym('test.b'),'TEST.B');assert.equal(api.lookupSym('TESTA'),'');
});
test('case-colliding source tickers are ambiguous instead of first wins',async()=>{
 const {api}=await load({by_ticker:{TEST:row,test:{...row,cik:'0000000222'}}});assert.equal(api.lookupSym('TEST'),'');
});
test('an upstream boolean claim alone cannot grant identifier routing',async()=>{
 const {api}=await load({by_ticker:{ZZTEST:{...row,isin_match_qualified:true}}});assert.equal(api.lookupSym(row.isin),'');
});
test('invalid populations and optional rows do not crash the whole catalog',async()=>{
 for(const master of [null,{by_ticker:[]},{by_ticker:true},{by_ticker:{ZZBAD:null,ZZGOOD:row}}]){
  const {api}=await load(master);assert.equal(api.lookupSym('unrelated'),'');assert.equal(api.chartId('fred:DFF'),'fred:DFF');
 }
});
test('malformed string identifiers are not advertised as valid identifier values',async()=>{
 const {api}=await load({by_ticker:{ZZTEST:{...row,isin:'<img src=x>',figi:'invalid',cusip:'bad'}}});
 const hits=api.suggest('ZZTEST');assert.equal(hits[0].s,'ZZTEST');assert.doesNotMatch(hits[0].extra,/<img|FIGI invalid|CUSIP bad/);
});
test('exact qualified namespaces keep their existing routing',async()=>{
 const {api}=await load({by_ticker:{ZZTEST:row}});
 for(const id of ['fred:DFF','CQ:btc_mvrv','provider:fred','CISS:ea'])assert.equal(api.chartId(id),id);
});
test('complete retained predecessor reproduces all four recorded failures',async()=>{
 const evidence=JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/symbol-directory/catalog-reproductions.json'),'utf8'));
 const predecessor=path.join(root,'tests/fixtures/symbol-directory/pre-integrity-chart-catalog.js.txt');
 for(const c of Object.values(evidence.cases)){
  const {api,counts}=await load(c.inputs['/data/symbology/master.json'],predecessor);let result=null,error=null;
  try{result=api.lookupSym(c.query);}catch(e){error={name:e.name,message:e.message};}
  assert.equal(result,c.lookup_result);assert.deepEqual(error,c.lookup_error);assert.deepEqual(JSON.parse(JSON.stringify(counts)),c.counts);
  assert.deepEqual(JSON.parse(JSON.stringify(api.suggest(c.query))),c.suggestions);
 }
});
