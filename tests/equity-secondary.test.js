const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'..'),html=fs.readFileSync(path.join(ROOT,'why.html'),'utf8');
const start=html.indexOf('const LENSES_CDN_BASE ='),end=html.indexOf('window.renderRiskTrade=',start),code=html.slice(start,end);
const now=Date.parse('2026-09-25T15:00:00Z');
const packet=ticker=>({ticker,generated:'2026-09-25T14:00:00Z',price:125,lenses:{buffett:{ok:true,fair_value:999,upside_pct:500}},
 summary:{lens_median_fair_value:999,n_pass:4},indicators:{price:125,ma200:100,pct_b:0,signals:[]},confluence:{dir:'bull',lean:99}});
const escape=s=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
function harness(fetch){
 const elements={},inspected=[];
 function select(ticker){for(const name of ['lenses','tasig']){
  elements[name+'-section']={dataset:{ticker}};
  elements[name+'-body']={innerHTML:'',textContent:'',children:[],append(v){this.children.push(v);}};
  elements[name+'-pill']={textContent:''};
 }}
 const context={window:{JHResearchInspection:{show:(body,rows)=>inspected.push(...rows)}},
  document:{getElementById:id=>elements[id],createElement:()=>({})},Date:{parse:Date.parse,now:()=>now},
  esc:escape,fetchWithTimeout:fetch,encodeURIComponent};
 vm.runInNewContext(code,context);select('ABC');return{context,elements,inspected,select};
}
const response=doc=>({ok:true,json:async()=>doc});

test('old ticker or replaced same-ticker sections cannot receive a late supplemental response',async()=>{
 for(const loader of ['fetchInvestorLenses','fetchTechSignals'])for(const next of ['ABC','XYZ']){
  const queue=[],h=harness(()=>new Promise(resolve=>queue.push(resolve)));
  const first=h.context.window[loader]('ABC');h.select(next);
  const second=h.context.window[loader](next);
  queue[0](response(packet('ABC')));await first;assert.equal(h.inspected.length,0);
  queue[1](response(packet(next)));await second;assert.equal(h.inspected.length,1);assert.equal(h.inspected[0].document.ticker,next);
 }
});

test('wrong identity, missing dates and future secondary packets do not acquire current authority',async()=>{
 for(const loader of ['fetchInvestorLenses','fetchTechSignals']){
  for(const doc of [{...packet('OTHER')},{...packet('ABC'),generated:null},{...packet('ABC'),generated:'2026-10-01T00:00:00Z'},
   {...packet('ABC'),generated:'2026-09-25T14:00:00'}]){
   const h=harness(async()=>response(doc));await h.context.window[loader]('ABC');assert.equal(h.inspected.length,0);
   const prefix=loader==='fetchInvestorLenses'?'lenses':'tasig';assert.equal(h.elements[prefix+'-pill'].textContent,'unavailable');
   assert.match(h.elements[prefix+'-body'].textContent,/identity\/date validation/);
  }
 }
});

test('unqualified lens targets remain whole in the inspector without becoming a cheap/rich call',async()=>{
 const doc=packet('ABC'),original=structuredClone(doc),h=harness(async()=>response(doc));await h.context.window.fetchInvestorLenses('ABC');
 assert.equal(h.elements['lenses-pill'].textContent,'unqualified');
 assert.match(h.elements['lenses-body'].innerHTML,/have not passed source, assumption/);
 assert.doesNotMatch(h.elements['lenses-body'].innerHTML,/999|500|CHEAP|RICH|lenses aligned/);
 assert.deepEqual(doc,original);assert.equal(h.inspected[0].document,doc);
 assert.match(h.inspected[0].source,/investor-lenses\/ABC\.json$/);
});

test('missing/invalid moving averages are unavailable, equality is At, and reported zero survives',async()=>{
 for(const [ma,relation] of [[null,'unavailable'],[undefined,'unavailable'],[0,'unavailable'],['100','unavailable'],[Infinity,'unavailable'],[125,'At 200'],[130,'Below 200'],[100,'Above 200']]){
  const doc=packet('ABC');doc.indicators.ma200=ma;
  const h=harness(async()=>response(doc));await h.context.window.fetchTechSignals('ABC');const body=h.elements['tasig-body'].innerHTML;
  assert.match(body,new RegExp(relation));assert.match(body,/%b <b>0\.00/);assert.doesNotMatch(body,/neutral tape|Net lean/);
  assert.match(body,/does not establish a neutral market/);assert.equal(h.inspected[0].document,doc);
 }
});

test('reported signal strings are escaped and old packet clocks remain explicit',async()=>{
 const doc=packet('ABC');doc.generated='2026-09-20T00:00:00Z';
 doc.indicators.signals=[{name:'<img src=x onerror=alert(1)>',dir:'<script>bad</script>',detail:'<b>unsafe</b>'},null];
 doc.confluence.trust_status='<svg onload=alert(1)>';
 const h=harness(async()=>response(doc));await h.context.window.fetchTechSignals('ABC');const body=h.elements['tasig-body'].innerHTML;
 assert.match(body,/older than 48 hours/);assert.match(body,/underlying observation freshness.*unverified/);
 assert.doesNotMatch(body,/<img|<script|<svg/);assert.match(body,/&lt;img/);assert.match(body,/&lt;b&gt;unsafe/);
 assert.equal(h.inspected[0].document,doc);
});

test('whole predecessor survives and revised supplemental loading copy makes no completion promise',()=>{
 const manifest=JSON.parse(fs.readFileSync(path.join(ROOT,'tests/fixtures/equity-secondary-migration.json')));
 const original=fs.readFileSync(path.join(ROOT,manifest.predecessor));assert.equal(original.length,manifest.bytes);
 assert.equal(crypto.createHash('sha256').update(original).digest('hex'),manifest.sha256);
 assert.doesNotMatch(code,/what would the greats|refreshes daily|refresh after each US close|lenses aligned/);
 assert.doesNotMatch(html,/Run will populate on next 17:00 ET schedule|\$99\/mo institutional feed/);
});
