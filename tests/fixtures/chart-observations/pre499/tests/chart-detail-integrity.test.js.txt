const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(require.resolve('../jh-chart-engine.js'),'utf8');
const slice=source.slice(source.indexOf('  function numish('),source.indexOf('  function currentSyms('));
const rows=()=>Array.from({length:400},(_,i)=>({time:Date.UTC(2024,0,1)/1000+i*86400,open:100,high:101,low:99,close:100,volume:10}));
const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function setup(){
 const elements={detail:{innerHTML:'',classList:{add(){}},querySelectorAll:()=>[]},fin:{innerHTML:''}};
 const ctx={Date,Number,Math,console,active:'SPY',tf:'1d',lastBars:rows(),barEvidence:new WeakMap(),quotes:{},news:[],
  finCache:{_pSPY:1,_fSPY:1},document:{getElementById:id=>elements[id]},bare:s=>s.replace(/^US:/,''),classifySym:()=> 'ETF',
  escHtml:esc,fmt:x=>x==null?'Unavailable':String(x),fmtVol:x=>String(x),UP:'green',DN:'red',showInfo(){},fetch(){throw Error('unexpected network');}};
 ctx.barEvidence.set(ctx.lastBars,{symbol:'SPY',interval:'1d',source:'synthetic'});vm.createContext(ctx);vm.runInContext(slice,ctx);
 return {ctx,elements};
}
test('empty, malformed, future or wrong-identity frames cannot display finite sentinels or another ticker prices',()=>{
 for(const mutate of [c=>c.lastBars=[],c=>c.lastBars[2].high=null,c=>c.lastBars[2].time=c.lastBars[1].time,
  c=>c.lastBars.at(-1).time=Date.now()/1000+86400,c=>c.tf='1h',c=>c.active='QQQ']){
  const {ctx:c,elements:e}=setup();c.finCache._pQQQ=1;mutate(c);c.renderDetail();
  assert.match(e.detail.innerHTML,/No matching valid dated chart frame/);assert.match(e.detail.innerHTML,/Unavailable/);
  assert.doesNotMatch(e.detail.innerHTML,/1e\+99|1e99|NaN|Infinity|class=rg>/);
 }
});
test('ranges describe observed bars and exact dates instead of assuming a day, 252 sessions or 52-week completeness',()=>{
 const {ctx:c,elements:e}=setup();c.tf='1w';c.barEvidence.set(c.lastBars,{symbol:'SPY',interval:'1w'});
 const range=c.detailBarRanges(c.lastBars);assert.equal(range.rows,366);assert.equal(range.low,99);assert.equal(range.high,101);
 assert.equal(range.start,new Date(c.lastBars[34].time*1000).toISOString());
 c.renderDetail();assert.match(e.detail.innerHTML,/LATEST BAR RANGE · 1w/);assert.match(e.detail.innerHTML,/366 retained bars/);
 assert.doesNotMatch(e.detail.innerHTML,/DAY'S RANGE|52-WEEK RANGE/);
 c.lastBars=c.lastBars.slice(-3);c.barEvidence.set(c.lastBars,{symbol:'SPY',interval:'1w'});c.renderDetail();
 assert.match(e.detail.innerHTML,/3 retained bars/);assert.match(e.detail.innerHTML,/not a session or completeness claim/);
});
test('constant and zero-valued ranges remain valid while overflow or unordered dates are unavailable',()=>{
 const {ctx:c}=setup(),small=rows().slice(-3).map(b=>({...b,open:0,high:0,low:0,close:0}));
 assert.equal(c.detailBarRanges(small).low,0);assert.equal(c.detailBarRanges(small).high,0);
 const overflow=[{...small[0],high:1e308,low:-1e308}];assert.equal(c.detailBarRanges(overflow),null);
 assert.equal(c.detailBarRanges([...small].reverse()),null);assert.equal(c.detailBarRanges(small,NaN),null);
});
test('fundamentals fallback uses identified dated bars and preserves missing versus zero volumes',()=>{
 const {ctx:c,elements:e}=setup();c.fillFinFromBars(e.fin);assert.match(e.fin.innerHTML,/last 252 bars/);assert.match(e.fin.innerHTML,/trailing 365 days/);
 c.lastBars.at(-1).volume=null;c.fillFinFromBars(e.fin);assert.match(e.fin.innerHTML,/bars<\/span><span>Unavailable/);
 c.lastBars.forEach(b=>b.volume=0);c.fillFinFromBars(e.fin);assert.match(e.fin.innerHTML,/bars<\/span><span>0/);
 e.fin.innerHTML='waiting';c.active='QQQ';c.fillFinFromBars(e.fin);assert.equal(e.fin.innerHTML,'waiting');
});
test('unrelated and substring-matched news cannot become a ticker-specific headline; metadata stays text',()=>{
 const {ctx:c,elements:e}=setup();c.news=[null,{ticker:'SPYWARE',title:'not this ticker'},{ticker:'DNUT',title:'unrelated'}];c.renderDetail();
 assert.doesNotMatch(e.detail.innerHTML,/unrelated|not this ticker/);
 c.news.push({tickers:['QQQ','US:SPY'],title:'<img src=x onerror=x>',source:'<svg>',date:'<script>x'});
 c.finCache.SPY={price:{shortName:'<script>',exchangeName:'<img>'}};c.renderDetail();
 assert.match(e.detail.innerHTML,/&lt;img/);assert.match(e.detail.innerHTML,/&lt;script&gt;/);assert.doesNotMatch(e.detail.innerHTML,/<img|<script|<svg/);
});
const deferred=()=>{let resolve;return {promise:new Promise(r=>{resolve=r;}),resolve:v=>resolve(v)};};
const flush=()=>new Promise(r=>setImmediate(r));
for(const method of ['renderFin','renderDetail'])test(method+' caches a delayed response for its requested symbol and never paints the next ticker',async()=>{
 const {ctx:c,elements:e}=setup(),d=deferred();delete c.finCache._pSPY;delete c.finCache._fSPY;
 c.fetch=url=>{assert.match(url,/ticker=SPY$/);return d.promise;};c[method]();
 c.active='QQQ';e.fin.innerHTML='QQQ fundamentals';e.detail.innerHTML='QQQ details';
 d.resolve({json:async()=>({ok:true,price:{shortName:'SPY response'}})});await flush();
 assert.equal(c.finCache.SPY.price.shortName,'SPY response');assert.equal(c.finCache.QQQ,undefined);
 assert.equal(e.fin.innerHTML,'QQQ fundamentals');assert.equal(e.detail.innerHTML,'QQQ details');
});
test('fundamental zeros survive fallback and empty, boolean or nonfinite values are not numbers',()=>{
 const {ctx:c,elements:e}=setup();for(const x of ['', ' ',false,true,NaN,Infinity,{raw:Infinity}])assert.equal(c.numish(x),null);
 assert.equal(c.firstNumber(0,12),0);assert.equal(c.firstNumber({raw:0},12),0);assert.equal(c.firstNumber(null,12),12);
 c.finCache.SPY={ok:true,price:{shortName:'<img>',marketCap:0},summaryDetail:{marketCap:999},defaultKeyStatistics:{trailingEps:0},financialData:{trailingEps:99},
  incomeStatementHistory:{incomeStatementHistory:[{endDate:{fmt:'<img>x'},totalRevenue:0,netIncome:0}]}};
 c.renderFin();assert.match(e.fin.innerHTML,/Mkt cap<\/span><span>0/);assert.match(e.fin.innerHTML,/EPS<\/span><span>0/);
 assert.doesNotMatch(e.fin.innerHTML,/<img/);assert.match(e.fin.innerHTML,/&lt;img&gt;/);
});
