const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
const core=require('../jh-stock-desk-research.js'),view=require('../jh-chart-stock-desk.js');
const NOW=Date.parse('2026-09-26T17:00:00Z');
function bars(n=60){return Array.from({length:n},(_,i)=>({time:Date.UTC(2020,0,1)/1000+i*86400,open:100,high:101,low:99,close:100,volume:20}));}
const frame=rows=>({symbol:'SPY',interval:'1d',source:'synthetic fixture only',bars:rows,published_at:'2026-09-26T16:00:00Z'});

test('relative volume excludes the latest bar and retains the old inclusive definition',()=>{
 const rows=bars(21);rows.at(-1).volume=40;const model=core.calculate(frame(rows),NOW);
 assert.equal(model.relative_volume.ratio,2);assert.equal(model.relative_volume.mean,20);
 assert.equal(model.relative_volume.end,core.label(rows[19].time));
 assert.equal(model.legacy_inclusive_relative_volume.ratio,40/21);
 assert.equal(model.legacy_inclusive_relative_volume.includes_latest,true);
 assert.equal(core.calculate(frame(bars(20)),NOW).relative_volume.ratio,null);
});
test('missing and invalid volumes never become zeros; explicit zero remains a zero',()=>{
 for(const value of [null,undefined,-1,NaN,Infinity,'20']){
  const rows=bars(21);rows[5].volume=value;
  const model=core.calculate(frame(rows),NOW);assert.equal(model.valid,true);
  assert.equal(model.relative_volume.ratio,null);assert.equal(model.missing_volume_rows,1);
  rows[5].volume=20;rows.at(-1).volume=value;
  assert.equal(core.calculate(frame(rows),NOW).relative_volume.latest,null);
 }
 const rows=bars(21);rows.at(-1).volume=0;
 assert.equal(core.calculate(frame(rows),NOW).relative_volume.ratio,0);
 rows.forEach(b=>b.volume=0);assert.equal(core.calculate(frame(rows),NOW).relative_volume.ratio,null);
 rows.forEach(b=>b.volume=Number.MAX_VALUE);assert.equal(core.calculate(frame(rows),NOW).relative_volume.ratio,null);
});
test('Bollinger history includes the first complete window and uses an empirical midrank for ties',()=>{
 const rows=bars(20),first=core.calculate(frame(rows),NOW).bollinger;
 assert.equal(first.ratio,0);assert.equal(first.history_windows,1);assert.equal(first.percentile,0.5);
 rows.push({...bars(21).at(-1),high:120,close:120});
 const second=core.calculate(frame(rows),NOW).bollinger;
 assert.equal(second.history_windows,2);assert.equal(second.percentile,0.75);
 assert.ok(Math.abs(second.ratio-(4*Math.sqrt(19)/101))<1e-14);
 assert.equal(core.calculate(frame(bars(19)),NOW).bollinger,null);
});
test('all price and clock defects withhold calculations while retaining the entire input',()=>{
 for(const mutate of [r=>r[2].close=null,r=>r[2].open='100',r=>r[2].high=99,r=>r[2].low=102,
  r=>r[2].close=0,r=>r[2].time=r[1].time,r=>r[2].time=r[0].time,r=>r[2].time='2020-02-30',
  r=>r.at(-1).time=NOW/1000+1,r=>r[2].time={year:2020,month:13,day:1}]){
  const rows=bars();mutate(rows);const model=core.calculate(frame(rows),NOW);
  assert.equal(model.valid,false);assert.equal(model.input_rows,rows);assert.ok(model.errors.length);
  assert.equal(model.bollinger,undefined);assert.equal(model.patterns.length,0);
 }
 assert.equal(core.time(-86400),-86400000);assert.equal(core.time({year:1960,month:2,day:29}),Date.parse('1960-02-29T00:00:00Z'));
 assert.equal(core.calculate(frame(bars()),NaN).valid,false);
 const overflow=bars(20);overflow.forEach(r=>{r.open=r.high=r.low=r.close=1e308;});
 assert.equal(core.calculate(frame(overflow),NOW).valid,false);
 for(const key of ['symbol','interval','source']){const f=frame(bars());f[key]='';assert.equal(core.calculate(f,NOW).valid,false);}
 const sparse=bars();delete sparse[1];assert.equal(core.calculate(frame(sparse),NOW).valid,false);
});
function geometric(n=60){const rows=bars(n);for(let i=10;i<n-6;i+=15){rows[i].low=98;rows[i].volume=i===10?10:5;}rows.at(-1).high=106;rows.at(-1).close=105;return rows;}
test('dated geometry states its actual latest-close comparison and five-bar hindsight',()=>{
 const rows=geometric(45),model=core.calculate(frame(rows),NOW),pair=model.patterns.find(p=>p.kind==='paired_lows');
 assert.equal(pair.left.index,10);assert.equal(pair.right.index,25);assert.equal(pair.neckline,101);
 assert.equal(pair.left.at,core.label(rows[10].time));assert.equal(pair.right.identifiable_after,core.label(rows[30].time));
 assert.equal(pair.latest_close_relation,'above');assert.equal(pair.comparison_at,core.label(rows.at(-1).time));
 assert.equal(pair.second_extremum_quieter,true);assert.equal(pair.forecast_qualified,false);
 assert.doesNotMatch(JSON.stringify(model),/confirmed|accumulation|distribution tell/);
 rows[25].volume=null;assert.equal(core.calculate(frame(rows),NOW).patterns[0].second_extremum_quieter,null);
 assert.equal(core.extrema(bars(),'high').length,0,'flat plateaus cannot supply dozens of artificial extrema');
});
test('the complete pair population survives pagination, export model, and all false authority flags',()=>{
 const rows=geometric(400),before=JSON.stringify(rows),model=core.calculate(frame(rows),NOW);
 assert.ok(model.patterns.length>25);const all=[];
 for(let i=0;i<view.page(model).pages;i++)all.push(...view.page(model,i).rows);
 assert.deepEqual(all,model.patterns);assert.equal(new Set(all.map(p=>p.id)).size,all.length);
 assert.deepEqual(model.input_rows,rows);assert.equal(JSON.stringify(rows),before);
 for(const k of ['calls_eligible','sizing_eligible','forecast_qualified','source_replayed','point_in_time_qualified','volume_unit_verified','corporate_action_basis_verified'])assert.equal(model[k],false,k);
 assert.equal(view.page(model,9999).index,view.page(model).pages-1);
});
test('rendered metadata is escaped, missing quantities labeled, and zero preserved',()=>{
 const f=frame(bars(21));f.symbol='<script>';f.source='<img onerror="x">';f.bars.at(-1).volume=0;
 const html=view.markup(core.calculate(f,NOW));
 assert.match(html,/&lt;script&gt;/);assert.doesNotMatch(html,/<img|<script/);assert.match(html,/>0×</);
 f.bars.at(-1).volume=null;assert.match(view.markup(core.calculate(f,NOW)),/Latest reported volume<\/span><span>Unavailable/);
 assert.match(html,/upstream volume defaults/);assert.match(html,/not a probability/);
});
function setup(){
 let selected='SPY',writes=0,html='',attributes={},parts=new Map();const events={},timers=new Map();let next=0;
 const host={get innerHTML(){return html;},set innerHTML(v){html=v;writes++;parts=new Map();if(v.includes('data-stock-pairs'))for(const k of ['pairs','rows','range','prev','next','export'])parts.set(k,{innerHTML:'',textContent:'',disabled:false,open:false,addEventListener(type,fn){this[type]=fn;}});},
  getAttribute:k=>attributes[k],setAttribute:(k,v)=>{attributes[k]=v;},querySelector:s=>parts.get(s.match(/data-stock-(\w+)/)[1])||null};
 const win={lastBars:geometric(400),document:{getElementById:()=>host,querySelector:s=>{assert.equal(s,'#tabs .tab.on[data-id]');return {getAttribute:()=>selected};}},
  setInterval:fn=>{timers.set(++next,fn);return next;},clearInterval:id=>timers.delete(id),addEventListener:(e,fn)=>{events[e]=fn;}};
 win.jhChartEvidence=frame(win.lastBars);const controller=view.install(win);
 return {win,host,events,timers,controller,get writes(){return writes;},get parts(){return parts;},select:s=>{selected=s;}};
}
test('unchanged refresh preserves an open disclosure; interior revisions and overwritten hosts recover',()=>{
 const s=setup();assert.equal(s.controller.getModel().valid,true);const count=s.writes;
 const details=s.parts.get('pairs');details.open=true;details.toggle();s.parts.get('next').onclick();
 assert.match(s.parts.get('range').textContent,/26–50/);s.controller.refresh();assert.equal(s.writes,count);assert.equal(details.open,true);
 s.win.lastBars[15].volume=999;s.controller.refresh();assert.equal(s.writes,count+1);assert.equal(s.controller.getModel().input_rows[15].volume,999);
 s.host.innerHTML='old oscillator panel';s.controller.refresh();assert.match(s.host.innerHTML,/CHART PRICE/);assert.ok(s.parts.get('pairs'));
});
test('ticker mismatch, replaced arrays and malformed frame clear previous calculations',()=>{
 const s=setup();s.select('AAPL');s.controller.refresh();assert.equal(s.controller.getModel(),null);assert.match(s.host.innerHTML,/No matching chart frame/);
 s.select('SPY');s.win.lastBars=[...s.win.lastBars];s.controller.refresh();assert.equal(s.controller.getModel(),null);
 s.win.jhChartEvidence=frame(s.win.lastBars);s.controller.refresh();assert.equal(s.controller.getModel().valid,true);
 s.win.lastBars[0].cycle=s.win.lastBars;s.controller.refresh();assert.equal(s.controller.getModel(),null);assert.match(s.host.innerHTML,/could not be validated/);
});
test('BFCache resumes one timer and recomputes changed data after the page was hidden',()=>{
 const s=setup();assert.equal(s.timers.size,1);s.events.pagehide();assert.equal(s.timers.size,0);
 const before=s.writes;s.win.lastBars[1].volume=91;s.controller.refresh();assert.equal(s.writes,before);
 s.events.pageshow();assert.equal(s.timers.size,1);assert.equal(s.controller.getModel().input_rows[1].volume,91);
 s.events.pageshow();assert.equal(s.timers.size,1);
});
test('the actual download includes every bar, all pairs and limitations without a network write',()=>{
 const s=setup();let output,clicked=false,revoked=false,cleanup;
 s.win.Blob=class{constructor(parts,options){output=JSON.parse(parts.join(''));assert.equal(options.type,'application/json');}};
 s.win.URL={createObjectURL:()=> 'blob:fixture',revokeObjectURL:url=>{assert.equal(url,'blob:fixture');revoked=true;}};
 s.win.document.createElement=tag=>{assert.equal(tag,'a');return {click(){assert.equal(this.href,'blob:fixture');assert.equal(this.download,'chart-frame-research.json');clicked=true;}};};
 s.win.setTimeout=fn=>{cleanup=fn;};s.parts.get('export').onclick();
 assert.equal(clicked,true);assert.deepEqual(output.input_rows,s.win.lastBars);
 assert.deepEqual(output.patterns,s.controller.getModel().patterns);assert.equal(output.calls_eligible,false);
 assert.equal(output.legacy_inclusive_relative_volume.includes_latest,true);assert.ok(output.methodology.hindsight);
 cleanup();assert.equal(revoked,true);
});
const source=fs.readFileSync(require.resolve('../jh-chart-engine.js'),'utf8');
test('actual chart publication binds to acquired array identity, never repaint-time labels',()=>{
 const start=source.indexOf('  var barEvidence=new WeakMap();'),end=source.indexOf('  var calCache=',start);
 const publication=source.split('\n').find(l=>l.includes('window.jhChartEvidence='));
 const context={window:{},wipe(){},active:'SPY',tf:'1d',INDS:[],OSC:[],paint(){},lastSource:'wrong racing source',volOn:true,d:bars()};vm.createContext(context);
 vm.runInContext(source.slice(start,end)+'\nidentifyBars(d,"SPY","1d","real returned frame");\n'+publication,context);
 assert.equal(context.window.jhChartEvidence.source,'real returned frame');assert.equal(context.window.jhChartEvidence.bars,context.d);
 context.active='AAPL';vm.runInContext(publication,context);assert.equal(context.window.jhChartEvidence,null);
 context.active='SPY';context.tf='1h';vm.runInContext(publication,context);assert.equal(context.window.jhChartEvidence,null);
 context.tf='1d';context.d=[...context.d];vm.runInContext(publication,context);assert.equal(context.window.jhChartEvidence,null,'Unidentified replay slice does not inherit current-frame authority');
 const html=fs.readFileSync(require.resolve('../chart.html'),'utf8');assert.ok(html.indexOf('/jh-stock-desk-research.js')<html.indexOf('/jh-chart-stock-desk.js'));
});
test('actual live-refresh function rejects delayed ticker responses and detects interior-volume revisions',async()=>{
 const start=source.indexOf('  async function tickLive(){'),end=source.indexOf('  function clock(){',start);
 let resolve,paints=0;const context={liveOn:true,replay:{on:false},active:'SPY',tf:'1d',loadGen:1,barCache:{},resolveSym:s=>({ticker:s}),
  klines:()=>new Promise(r=>{resolve=r;}),lastBars:bars(),paint:async()=>{paints++;},loadTape(){}};vm.createContext(context);vm.runInContext(source.slice(start,end),context);
 const old=context.tickLive();context.active='AAPL';resolve(bars());await old;assert.equal(paints,0);
 context.active='SPY';const changed=bars();changed[2].volume=999;
 const update=context.tickLive();resolve(changed);await update;assert.equal(paints,1);
 const late=context.tickLive();context.loadGen++;resolve(changed);await late;assert.equal(paints,1);
 const replay=context.tickLive();context.replay.on=true;resolve(changed);await replay;assert.equal(paints,1);
});
test('full predecessor is preserved byte-for-byte for review without executing it',()=>{
 const raw=fs.readFileSync(require.resolve('./fixtures/legacy-stock-desk-overlay.js.txt'));
 assert.equal(raw.length,6496);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'2830168bc65574615b8a92dce44f778bfd081e6fa912f7c576c7538fc347ff97');
});
