const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs');
const ui=require('../jh-global-stress-research.js');
const packet=()=>({contract:'global-stress-research.v1',generated_at:'2026-09-20T08:00:00Z',instruments:{SPY:{symbol:'SPY',group:'Equity',label:'US equity',close:0,adjusted_close:null,quality:{status:'unavailable'},history:[]}},measurements:{},correlations:{pairs:[]},context:{},source_failures:{}});
test('legacy scores and absent values cannot become portfolio advice',()=>{
 assert.match(ui.render({global_stress_index:99}),/Verified Global Stress research unavailable/);
 const html=ui.render(packet());assert.match(html,/WAIT/);assert.match(html,/Unavailable/);assert.doesNotMatch(html,/NaN|undefined|buy SPY/);
 assert.equal(ui.fmt(false),'Unavailable');assert.equal(ui.fmt(0),'0');
});
test('adjustment basis and observed intervals are explicit',()=>{
 assert.match(ui.render(packet()),/Provider dividend-adjusted close/);
 assert.match(ui.render(packet(),{basis:'price'}),/Split-adjusted price; dividends excluded/);
 assert.match(ui.render(packet()),/21 observed-interval return/);assert.match(ui.render(packet()),/not local-currency/);
});
test('source keys and untrusted text cannot inject HTML',()=>{
 const p=packet();p.instruments.SPY.label='<img src=x onerror=alert(1)>';
 assert.match(ui.render(p),/&lt;img/);assert.doesNotMatch(ui.render(p),/<img/);
 assert.equal(ui.link('data/../private','source'),'source unavailable');assert.equal(ui.link('javascript:alert(1)','x'),'x unavailable');
});
test('stale generation clock is separate from instrument freshness',()=>{
 assert.match(ui.render(packet(),{now:Date.parse('2026-09-22T00:00:00Z')}),/Packet stale/);
 assert.doesNotMatch(ui.render(packet(),{now:Date.parse('2026-09-20T09:00:00Z')}),/Packet stale/);
});
test('calendar chart breaks missing values rather than drawing a zero',()=>{
 const rows=[{date:'2026-09-01',adjusted_close:'1'},{date:'2026-09-02',adjusted_close:null},{date:'2026-09-11',adjusted_close:'2'}];
 const html=ui.chart(rows,'adjusted','all');assert.equal((html.match(/ M /g)||[]).length,2);assert.doesNotMatch(html,/ L /);
 rows[1].adjusted_close='1.5';assert.match(ui.chart(rows,'adjusted','all'),/L 66\.40 /);
});
test('correlation displays unavailable and sample size without a crisis label',()=>{
 const html=ui.correlationTable({pairs:[{left:'SPY',right:'FEZ',value:null,n_matched_intervals:39,n_available_union_intervals:60,matched_intervals:[]}]});
 assert.match(html,/N\/A/);assert.match(html,/n=39/);assert.doesNotMatch(html,/ACUTE|CRISIS|99%/);
});
test('three pages use native data and dedicated qualification views',()=>{
 for(const name of ['global-stress.html','gsi-calibration.html','horizons-gsi.html']){
  const html=fs.readFileSync(name,'utf8');assert.match(html,/jh-global-stress-research\.js\?v=52/);assert.match(html,/aria-live="polite"/);
 }
 const html=ui.render(packet(),{view:'qualification',status:{contract:'gsi-qualification-status.v1',generated_at:'2026-09-20',validated_forecast_observations:0,requirements:['Held-out results']}});
 assert.match(html,/Validated forecast observations: 0/);assert.match(html,/Held-out results/);assert.doesNotMatch(html,/global-basis/);
});
test('control choices are bounded and default to dividend-adjusted research',()=>{
 const html=ui.render(packet(),{basis:'unknown',horizon:'unknown'});assert.match(html,/value="adjusted" selected/);assert.match(html,/value="63" selected/);
 assert.match(ui.render(packet(),{horizon:'all'}),/value="all" selected/);
});
test('actual Crisis feed card cannot turn any research number into a global score',()=>{
 const source=fs.readFileSync('crisis.html','utf8');
 const start=source.indexOf("fetch('/data/'+f+'?t='+Date.now())");
 const bodyStart=source.indexOf('.then(function(d){',start)+'.then(function(d){'.length;
 const end=source.indexOf('}).catch(function()',bodyStart);
 assert.ok(start>0&&bodyStart>start&&end>bodyStart);
 const renderCard=new Function('f','d','c','pick','pickS','NK','SK',source.slice(bodyStart,end));
 const nodes={'.v':{},'.s':{}},fail=()=>{throw new Error('recursive score picker must not run');};
 renderCard('global-stress.json',{global_stress_index:99,instruments:{SPY:{close:761}}},{querySelector:k=>nodes[k]},fail,fail,[],[]);
 assert.equal(nodes['.v'].textContent,'Research only');assert.match(nodes['.s'].textContent,/No qualified composite/);
});
