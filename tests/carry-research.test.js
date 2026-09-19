const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),api=require('../jh-carry-research.js');
const stamp='2026-09-19T14:00:00Z',at=Date.parse(stamp);
function row(){return {id:'DFF',name:'Overnight rate',unit:'Percent',frequency:'D',value_decimal:'0',as_of:'2026-09-18',history:null,quality:{status:'fresh',acquired_at:stamp,max_observation_age_days:7}};}
function packet(){return {contract:api.CONTRACT,generated_at:stamp,call:null,calls_eligible:false,sizing_eligible:false,execution_eligible:false,portfolio_action:'WAIT',unwind_overlay:{cohort_fragility:null},measurements:{DFF:row()},equities:{},comparisons:{},by_class:{fx:[{symbol:'USD',research_id:'DFF',rate_pct_decimal:'0'}]},errors:{},legacy_preservation:{},decision_qualification:{reason:'research only'},cross_asset_top:[],cross_asset_bottom:[],risk_adjusted_leaders:[],dislocation_leaders:[],replay:{manifest_key:api.PREFIX+'runs/'+'a'.repeat(64)+'.json'}};}
test('current board withholds expired observations, while selected archived values stay inspectable',()=>{
 const p=packet();assert.match(api.summary(p,'fx','',0,at).board,/0<br/);assert.match(api.summary(p,'fx','',0,at+27*3600000).board,/—<br/);
 assert.equal(api.status(row(),at+27*3600000,true),'fresh_at_snapshot');assert.match(api.detail(p,'DFF',at+27*3600000),/0 Percent/);
 p.calls_eligible=true;assert.throws(()=>api.boundary(p),/contract/);
});
test('legacy numeric fragility and rankings cannot enter the research boundary',()=>{const p=packet();p.unwind_overlay.cohort_fragility=0;assert.throws(()=>api.boundary(p));p.unwind_overlay.cohort_fragility=null;p.cross_asset_top=[{symbol:'X'}];assert.throws(()=>api.boundary(p),/rankings/);});
test('equity scenario reconciles realized price, cash, tax, partial funding and fees',()=>{
 const x={shares:100,start:100,end:90,distributions:2,funded:.5,rate:6,days:360,basis:360,tax:15,fees:20},r=api.scenario('equity',x);
 assert.equal(r.net_pnl,-1150);assert.equal(r.initial_value,10000);assert.equal(r.return_on_initial_value_pct,-11.5);assert.equal(r.break_even_end_price_or_fx,101.5);
 assert.equal(api.scenario('equity',{...x,end:r.break_even_end_price_or_fx}).net_pnl,0);assert.equal(r.forecast,false);assert.equal(r.position_size,null);
});
test('FX scenario uses explicit quote direction and both day bases',()=>{
 const x={principal:1000000,start:150,end:140,assetRate:5,fundingRate:1,days:360,assetBasis:360,fundingBasis:360,fees:100000},r=api.scenario('fx',x);
 assert.equal(r.net_pnl,-4600000);assert.ok(Math.abs(r.break_even_end_price_or_fx-151600000/1050000)<1e-10);
 assert.ok(Math.abs(api.scenario('fx',{...x,end:r.break_even_end_price_or_fx}).net_pnl)<1e-7);
 assert.throws(()=>api.scenario('fx',{...x,assetRate:-20,days:3660}),/negative principal/);
});
test('bond scenario separates duration, convexity, income, funding and entered credit loss',()=>{
 const x={value:1000000,duration:5,convexity:30,yieldMove:100,income:4,funded:1,rate:3,days:360,incomeBasis:360,basis:360,creditLoss:1,fees:500},r=api.scenario('bond',x);
 assert.equal(r.net_pnl,-49000);assert.equal(r.components[0][1],-50000);assert.equal(r.components[1][1],1500);
 assert.throws(()=>api.scenario('bond',{...x,duration:100,convexity:0,yieldMove:2000}),/negative terminal price/);
});
test('no scenario silently converts missing assumptions to zero or uses an invented day basis',()=>{
 const x={shares:0,start:100,end:90,distributions:0,funded:0,rate:0,days:1,basis:360,tax:0,fees:0};assert.equal(api.scenario('equity',x).net_pnl,0);
 for(const bad of [{shares:null},{fees:''},{rate:NaN},{funded:1.1},{basis:364},{days:1.5},{fees:-1}])assert.throws(()=>api.scenario('equity',{...x,...bad}));
 const missing={...x};delete missing.rate;assert.throws(()=>api.scenario('equity',missing));
});
test('equity board searches and pages without changing metric meanings',()=>{
 const p=packet();p.by_class.equity=Array.from({length:110},(_,i)=>({symbol:'X'+i,research_id:'FMP:X'+i}));
 assert.equal(api.summary(p,'equity','',0,at).pages,5);assert.equal(api.summary(p,'equity','X109',0,at).count,1);assert.equal(api.summary(p,'equity','',99,at).page,4);
});
test('missing observations break lines; corporate-action event plots show points',()=>{
 const h={id:'x',unit:'USD',frequency:'D',rows:[{date:'2026-09-01',value_decimal:'2'},{date:'2026-09-02',value_decimal:null},{date:'2026-09-03',value_decimal:'3'}]},d=api.chart(h).match(/<path d="([^"]*)"/)[1];
 assert.equal((d.match(/M/g)||[]).length,2);assert.ok(!d.includes('L'));assert.match(api.chart({...h,frequency:'event'}),/<circle/);
});
test('labels cannot inject markup; evidence paths remain public and bounded',()=>{const p=packet();p.measurements.DFF.name='<img onerror="x">';assert.match(api.detail(p,'DFF',at),/&lt;img/);assert.equal(api.path('data/../../secret'),null);assert.equal(api.path('https://example.com'),null);});
test('snapshot verifies immutable manifest, output hash and clock before rendering',async()=>{
 const p=packet();delete p.replay;const output=new TextEncoder().encode(JSON.stringify(p)),sha=await api.sha(output),m={contract:'carry-original-replay.v1',generated_at:stamp,output_sha256:sha,output:{key:api.PREFIX+'outputs/'+sha+'.json',sha256:sha,bytes:output.length}},raw=new TextEncoder().encode(JSON.stringify(m)),id=await api.sha(raw),files={['/'+api.PREFIX+'runs/'+id+'.json']:raw,['/'+m.output.key]:output};
 const fetcher=async key=>new Response(files[key],{status:files[key]?200:404});assert.equal((await api.loadSnapshot(id,fetcher)).contract,api.CONTRACT);files['/'+m.output.key]=new TextEncoder().encode('{}');await assert.rejects(api.loadSnapshot(id,fetcher),/differs/);
});
test('history verifies hash and identity while preserving ambiguous duplicate corporate-action dates',async()=>{
 const r=row(),h={contract:'carry-history.v1',id:r.id,unit:'USD',frequency:'event',kind:'distributions',rows:[{date:r.as_of,value_decimal:'1',row_index:0},{date:r.as_of,value_decimal:'1',row_index:1}]},raw=new TextEncoder().encode(JSON.stringify(h)),sha=await api.sha(raw);
 r.history={key:api.PREFIX+'histories/'+sha+'.json',sha256:sha,bytes:raw.length,observations:2,from:r.as_of,to:r.as_of,kind:h.kind,unit:h.unit,frequency:h.frequency};assert.deepEqual(await api.loadHistory(r,async()=>new Response(raw)),h);r.id='OTHER';await assert.rejects(api.loadHistory(r,async()=>new Response(raw)),/contract/);
});
test('page uses one immutable snapshot and invalidates scenario exports after input changes',()=>{
 const html=fs.readFileSync(require.resolve('../carry.html'),'utf8'),js=fs.readFileSync(require.resolve('../jh-carry-page.js'),'utf8');
 assert.match(html,/jh-carry-research.js/);for(const old of ['jh-page-ai','Sharpe-of-carry','s3.amazonaws.com'])assert.ok(!html.includes(old));
 assert.match(js,/ticket!==request/);assert.match(js,/Current pointer differs/);assert.match(js,/Assumptions changed/);assert.match(js,/research_snapshot:packet.replay/);assert.match(js,/setInterval\(\(\)=>render\(\),60000\)/);
});
