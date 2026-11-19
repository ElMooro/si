// Khalid 2026-11-19: "Requested: VANTAGE:SP500 - Resolved route: unavailable ... im trying to pull sp500 and i cant".
// TradingView's own search answers "sp500" / "spx" / "us500" with broker and index-vendor ids of the S&P 500 cash index; the
// watchlist router knew none of those venues. Actual source functions (watchlist router, native resolver, catalog); no network.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const read=f=>fs.readFileSync(f,'utf8'),watch=read('jh-chart-tvwatch.js'),engine=read('jh-chart-engine.js'),catalog=read('jh-chart-catalog.js');
const fixture=JSON.parse(read('tests/fixtures/chart-spx-cash/tradingview-search-2026-11-19.json'));
const acorn={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(acorn.exports,acorn);
const trees=new Map();function nodes(src){if(trees.has(src))return trees.get(src);const out=[],stack=[acorn.exports.parse(src,{ecmaVersion:'latest'})];while(stack.length){const n=stack.pop();if(!n||typeof n!=='object')continue;if(n.type)out.push(n);for(const v of Object.values(n)){if(Array.isArray(v))stack.push(...v);else if(v&&typeof v==='object')stack.push(v);}}trees.set(src,out);return out;}
function fn(src,name){const a=nodes(src).filter(n=>n.type==='FunctionDeclaration'&&n.id.name===name);assert.equal(a.length,1,name);return src.slice(a[0].start,a[0].end);}
function variable(src,name){const n=nodes(src).find(n=>n.type==='VariableDeclarator'&&n.id.name===name);assert.ok(n,name);return 'var '+src.slice(n.start,n.end)+';';}
const assignments=nodes(engine).filter(n=>n.type==='AssignmentExpression'&&n.left.type==='MemberExpression'&&n.left.object.name==='q'&&n.left.property.name==='onkeydown'&&n.right.type==='FunctionExpression'&&engine.slice(n.right.start,n.right.end).includes('var s=q.value.trim().toUpperCase(); if(TABS.indexOf(s)<0)'));
assert.equal(assignments.length,1);const nativeQ=engine.slice(assignments[0].right.start,assignments[0].right.end);
function harness(map=null){
 const c={window:{},Set,Number,Date,AbortController,setTimeout,clearTimeout,painted:[],toasts:[],loads:[],symMap:map,symMapReady:1,chartSelection:null,chartRouteObserver:null,TABS:[],active:'INITIAL',filter:''};
 c.q={value:'',dispatchEvent(e){this.onkeydown(e);}};c.document={getElementById(id){return id==='q'?c.q:null;}};c.KeyboardEvent=function(_,o){return o;};c.paintCard=s=>c.painted.push(s);c.toast=s=>c.toasts.push(s);c.loadSymMap=()=>Promise.resolve();
 c.loadDraw=c.renderTabs=c.renderList=()=>{};c.load=()=>c.loads.push(c.active);c.activeSym=()=>c.active;c.lookupSym=()=>'';
 vm.createContext(c);vm.runInContext(fn(catalog,'H')+['EXCH','SERIES_PROV','CURATED','DEFILLAMA_IDS','REGIONAL_FED_IDS'].map(n=>variable(catalog,n)).join('\n')+fn(catalog,'isWarehouse')+fn(catalog,'chartId'),c);
 c.window.JHChartCatalog={isWarehouse:c.isWarehouse,chartId:c.chartId,curated:c.CURATED};
 vm.runInContext(['resolveSym','yahooSym','bare','observationId'].map(n=>fn(engine,n)).join('\n'),c);vm.runInContext(fn(engine,'normQ')+fn(engine,'classifySym')+variable(engine,'SEARCH_ALIASES'),c);
 const exported=nodes(engine).find(n=>n.type==='AssignmentExpression'&&n.left.type==='MemberExpression'&&n.left.property.name==='jhWatchlistResolve');assert.ok(exported);vm.runInContext(engine.slice(exported.start,exported.end),c);c.q.onkeydown=vm.runInContext('('+nativeQ+')',c);
 c.paintChartRoute=()=>{};c.paintEconomicSources=()=>{};
 vm.runInContext(['extraChart','fredId','chartSymbol','providerRest','routeHints','chartRoute','economicQualification','economicQualificationText','chartRouteText','openSym'].map(n=>fn(watch,n)).join('\n'),c);return c;
}
// the table as written in the source
const table=(()=>{const src=fn(watch,'chartSymbol'),m=/var spxCash = (\{[^}]*\});/.exec(src);assert.ok(m,'spxCash table');return Object.keys(JSON.parse(m[1]));})();
const NOT_CASH=/futur|forward|perp|rolling/i,rows=new Map(fixture.search_rows.map(r=>[r.id,r]));

test('the routed ids are exactly the reviewed list, each read from TradingView search as a cash index or found in his lists',()=>{
 assert.deepEqual(table.slice().sort(),fixture.routed.slice().sort());assert.equal(table.length,24);assert.equal(new Set(table).size,24);
 for(const id of table){
  assert.equal(id,id.toUpperCase());assert.match(id,/^[A-Z0-9]+:[A-Z0-9]+$/);
  const row=rows.get(id);
  if(!row){assert.ok(fixture.from_lists[id]&&fixture.from_lists[id].lists>0,id+' has no recorded evidence');continue;}
  assert.equal(row.type,'index',id);assert.doesNotMatch(row.description,NOT_CASH,id);
 }
 assert.ok(table.includes('VANTAGE:SP500'));assert.equal(rows.get('VANTAGE:SP500').description,'S&P Index Cash CFD (USD)');
});
for(const id of fixture.routed)test('charts the S&P 500 index for '+id,()=>{
 const c=harness(),r=c.chartRoute(id);
 assert.equal(r.requested,id);assert.equal(r.handoff,'^GSPC');assert.equal(r.resolved,'^GSPC');assert.equal(r.primary,'^GSPC');assert.equal(r.relation,'mapped-route');
 assert.equal(r.reason,'Possible proxy or source substitution; equivalence unverified');
 c.openSym(id);assert.deepEqual(Array.from(c.loads),['^GSPC']);assert.equal(c.chartSelection.requested,id);assert.deepEqual(Array.from(c.toasts),[]);
 const text=c.chartRouteText(r,null,[]);assert.ok(text.startsWith('Requested: '+id+' \u00b7 Resolved route: ^GSPC \u00b7 Handoff: ^GSPC'));assert.doesNotMatch(text,/No supported chart route|Chart unchanged/);
});
test('his exact request, in any letter case, is the same route',()=>{
 for(const id of ['VANTAGE:SP500','vantage:sp500','Vantage:Sp500',' VANTAGE:SP500 ']){const c=harness(),r=c.chartRoute(id);assert.equal(r.handoff,'^GSPC',id);assert.equal(r.requested,id.trim());}
});
test('every search row that is not the cash index stays off the index route',()=>{
 let checked=0;
 for(const row of fixture.search_rows){
  if(/\s/.test(row.id))continue;                       // an id with a space is refused before any routing
  if(table.includes(row.id))continue;
  if(row.id==='TVC:SPX')continue;                      // already routed to the index before this change
  const c=harness(),r=c.chartRoute(row.id);checked++;
  assert.notEqual(r.handoff,'^GSPC',row.id+' ('+row.description+')');
 }
 assert.ok(checked>70,String(checked));
 for(const id of ['VANTAGE:SP500FT','BLACKBULL:US500.F','PEPPERSTONE:US500F','PEPPERSTONE:US500.P','TRADENATION:US500FU','CBOE:XSP','PRIMEXBT:US500','LSE:SPX','GETTEX:SPX','XETR:SPX','CME:ES','MEXC:SP500USDT.P']){
  const c=harness(),r=c.chartRoute(id);assert.notEqual(r.handoff,'^GSPC',id);c.openSym(id);assert.ok(!Array.from(c.loads).includes('^GSPC'),id);
 }
});
test('the ids that already charted are untouched',()=>{
 for(const [id,handoff] of [['SPX','^GSPC'],['TVC:SPX','^GSPC'],['INDEX:SPX','^GSPC'],['INDEX:SP500','^GSPC'],['^GSPC','^GSPC'],['SPY','SPY'],['AMEX:SPY','AMEX:SPY'],['FRED:SP500','FRED:SP500'],['SP500','FRED:SP500'],['TVC:GOLD','GC=F'],['NASDAQ:AAPL','NASDAQ:AAPL']]){
  const c=harness(),r=c.chartRoute(id);assert.equal(r.handoff,handoff,id);
 }
});
test('a symbol-map entry for the same id still decides first',()=>{
 const c=harness({'VANTAGE:SP500':{source:'FRED',id:'SP500'}});assert.equal(c.chartRoute('VANTAGE:SP500').handoff,'FRED:SP500');
 const held=harness({'VANTAGE:SP500':{source:'UNPRICED',id:''}});assert.equal(held.chartRoute('VANTAGE:SP500').handoff,null);
});
test('the details card writes the history line of a non-ticker symbol from the last bar it holds',()=>{
 const src=read('jh-tv-details.js');
 assert.ok(src.includes('fdate(day(b[0].time)) + " \u2192 " + fdate(day(L0.time)) + "</span><span>Observations</span>'));
 assert.ok(!src.includes('fdate(day(b[0].time)) + " \u2192 " + fdate(day(L.time))'));
});
