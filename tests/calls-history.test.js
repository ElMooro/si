const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm');
const NOW=Date.parse('2026-09-26T16:00:00Z');
const base={schema_version:'calls.v2',timestamp:'2026-09-26T15:00:00Z',expires_at:'2026-09-26T17:00:00Z',
  decision_status:'ABSTAIN',call_verb:'WAIT',decision_reason:'decision_model_not_validated'};
const qualified={...base,decision_status:'VALID',decision_eligible:true,sizing_eligible:true,validation_status:'validated',
  model_version:'synthetic-test-only',evidence_ids:['synthetic'],call_verb:'LONG'};
function setup(){
  delete require.cache[require.resolve('../calls-page.js')];
  const nodes=new Map();
  const document={getElementById:id=>{if(!nodes.has(id))nodes.set(id,{style:{},textContent:'',innerHTML:'',disabled:false});return nodes.get(id);}};
  global.document=document;return {view:require('../calls-page.js'),nodes,document};
}
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
const response=packet=>({ok:true,json:async()=>packet});

test('all rows and duplicate timestamps survive every page including invalid records',()=>{
  const {view}=setup(),rows=Array.from({length:123},(_,i)=>({...base,highest_weight_signal:String(i)}));
  rows.push(null,42,{timestamp:'not-a-date'}, {...base,timestamp:'2026-09-27T00:00:00Z'});
  const visited=[];
  for(let page=0;page<6;page++){
    const result=view.historyPage(rows,{page,size:25,now:NOW});
    assert.equal(result.total,127);assert.equal(result.invalid,4);
    assert.ok(result.rows.length<=25);visited.push(...result.rows.map(r=>r.index));
  }
  assert.equal(visited.length,127);assert.equal(new Set(visited).size,127);
  assert.deepEqual([...visited].sort((a,b)=>a-b),Array.from({length:127},(_,i)=>i));
  assert.equal(view.historyPage(rows,{page:1000,size:25,now:NOW}).page,5);
});

test('filters preserve full population counts and separate invalid rows from qualified history',()=>{
  const {view}=setup(),rows=[base,qualified,{...base,decision_status:'ERROR'},null,{call_verb:'LONG',timestamp:base.timestamp}];
  for(const [kind,index] of [['ABSTAIN',0],['QUALIFIED',1],['ERROR',2],['INVALID',3],['LEGACY',4]]){
    const page=view.historyPage(rows,{kind,now:NOW});
    assert.equal(page.total,5);assert.equal(page.matched,1);assert.equal(page.rows[0].index,index);
  }
});

test('real renderer pages and filters without deleting or refetching ledger records',()=>{
  const {view,nodes}=setup(),clock=Date.now;Date.now=()=>NOW;
  try{
    const rows=Array.from({length:123},()=>({...base}));rows.push(null);
    view.render({snapshots:rows});
    assert.equal((nodes.get('history-body').innerHTML.match(/<tr>/g)||[]).length,50);
    assert.match(nodes.get('history-range').textContent,/1–50 of 124.*124 retained/);
    nodes.get('history-next').onclick();assert.match(nodes.get('history-range').textContent,/51–100/);
    nodes.get('history-size').value='25';nodes.get('history-size').onchange();
    assert.match(nodes.get('history-range').textContent,/1–25/);
    nodes.get('history-filter').value='INVALID';nodes.get('history-filter').onchange();
    assert.match(nodes.get('history-body').innerHTML,/Original entry 124/);
    assert.equal(nodes.get('history-next').disabled,true);
    assert.equal(rows.length,124);
  }finally{Date.now=clock;}
});

test('future and invalid entries cannot replace the current decision or inflate recent counts',()=>{
  const {view,nodes}=setup(),clock=Date.now;Date.now=()=>NOW;
  try{
    view.render({snapshots:[base,{...qualified,timestamp:'2026-09-27T15:00:00Z',expires_at:'2026-09-27T17:00:00Z'},null]});
    assert.equal(nodes.get('n-total').textContent,3);
    assert.equal(nodes.get('now-verb').textContent,'ABSTAIN');
    assert.match(nodes.get('kpi-row').innerHTML,/Qualified decisions · 7d<\/h3><div class="v">0<\/div>/);
    assert.match(nodes.get('notice-area').innerHTML,/2 entries.*invalid or future clocks/);
    Date.now=()=>NOW+86400000;view.expire();
    assert.equal(nodes.get('now-verb').textContent,'ABSTAIN','A previously invalid future row cannot become authority without a new fetch');
  }finally{Date.now=clock;}
});

test('open-tab expiry clears the actionable banner and decision KPI together',()=>{
  const {view,nodes}=setup(),clock=Date.now;Date.now=()=>NOW;
  try{
    view.render({snapshots:[qualified]});assert.equal(nodes.get('now-verb').textContent,'LONG');
    Date.now=()=>Date.parse(qualified.expires_at);view.expire();
    assert.equal(nodes.get('now-verb').textContent,'EXPIRED');
    assert.equal(nodes.get('decision-kpi-value').textContent,'EXPIRED');
    assert.match(nodes.get('now-ctx').textContent,/expired/);
  }finally{Date.now=clock;}
});

test('request timeout covers the response body even if a fetcher ignores abort',async()=>{
  const {view}=setup();let signal;
  await assert.rejects(view.get(async(url,options)=>{signal=options.signal;assert.equal(url,'/data/decisive-call-history.json?exact=1&nogen=1');
    return {ok:true,json:()=>new Promise(()=>{})};},10),/timed out/);
  assert.equal(signal.aborted,true);
});

test('a late success cannot restore a decision after a newer failed refresh',async()=>{
  const {view,nodes}=setup(),older=deferred();
  const first=view.load(()=>older.promise);
  await view.load(async()=>({ok:false,status:503}));
  older.resolve(response({snapshots:[qualified]}));await first;
  assert.equal(nodes.get('now-banner').style.display,'none');
  assert.equal(nodes.get('status').textContent,'ledger unavailable');
  assert.match(nodes.get('kpi-row').innerHTML,/unavailable/);
  assert.equal(nodes.get('history-next').disabled,true);
});

test('recovery reenables controls and retained malformed-row content is escaped',async()=>{
  const {view,nodes}=setup();
  await view.load(async()=>({ok:false,status:503}));
  await view.load(async()=>response({snapshots:[{timestamp:'invalid',phase:'<img src=x onerror=bad()>',call_verb:'<script>bad()</script>'}]}));
  assert.equal(nodes.get('history-filter').disabled,false);
  assert.equal(nodes.get('history-size').disabled,false);
  assert.match(nodes.get('history-body').innerHTML,/&lt;img/);
  assert.ok(!nodes.get('history-body').innerHTML.includes('<script>'));
  assert.equal(nodes.get('n-total').textContent,1);
});

test('browser back-forward restoration restarts freshness timers and fetches current data',async()=>{
  const {document}=setup(),listeners={},timers=new Set();let reads=0,id=0;
  const window={document,fetch:async()=>{reads++;return response({snapshots:[]});},
    setInterval:()=>{timers.add(++id);return id;},clearInterval:key=>timers.delete(key),addEventListener:(event,handler)=>{listeners[event]=handler;}};
  vm.runInNewContext(fs.readFileSync(require.resolve('../calls-page.js'),'utf8'),{window,AbortController,setTimeout,clearTimeout});
  await new Promise(setImmediate);assert.equal(timers.size,2);assert.equal(reads,1);
  listeners.pagehide();assert.equal(timers.size,0);
  listeners.pageshow({persisted:true});await new Promise(setImmediate);
  assert.equal(timers.size,2);assert.equal(reads,2);listeners.pagehide();
});
