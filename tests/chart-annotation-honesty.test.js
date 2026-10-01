const test=require('node:test');
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const {source,chrome,engine,rows,context,scFixture,distributionFixture,clippedDistribution}=require('./helpers/chart-annotation-harness.cjs');
const indices=(w,d,kind)=>w.jhVolEventTable(d).filter(e=>e.kind===kind).map(e=>e.i);
const click=button=>button.onclick({stopPropagation(){}});
const freeze=value=>JSON.stringify(value);

test('retained predecessor sources are pinned independently of editable production',()=>{
  const manifest=JSON.parse(source('tests/fixtures/chart-annotation/source-manifest.json'));
  assert.equal(crypto.createHash('sha256').update(source('tests/fixtures/chart-annotation/indux-predecessor.js.txt')).digest('hex'),manifest.indux_predecessor_sha256);
  assert.equal(crypto.createHash('sha256').update(source('tests/fixtures/chart-annotation/distribution-predecessor.js.txt')).digest('hex'),manifest.unchanged_sources['jh-chart-distribution.js']);
});

test('warning is visible for each volume/cycle study, campaign and DIST; hidden studies do not trigger it',()=>{
  const {w,nodes}=chrome();
  for(const id of ['voltape','volev','wyckoff','livermore','accum','distrib','vsa','tape','pats']) {
    w.jhInduxLegend(context([], [{id,n:id,on:true}]));
    assert.match(nodes.get('legend').innerHTML,/Marker dates show event locations, not first availability/);
    assert.match(nodes.get('legend').innerHTML,/Later bars or corrections may revise/);
    assert.match(nodes.get('legend').innerHTML,/not predictive validation or measured signed flow/);
    w.jhInduxLegend(context([], [{id,n:id,on:true,hide:true}]));
    assert.doesNotMatch(nodes.get('legend').innerHTML,/class=annotation-timing/);
  }
  for(const flag of ['bottom','accum','dist']) {
    w.__jhDistOn=flag==='dist';w.__jhCampLayer={[flag]:1};
    w.jhInduxLegend(context([],[]));
    assert.match(nodes.get('legend').innerHTML,/class=annotation-timing/);
  }
  w.__jhDistOn=0;w.__jhCampLayer={};
  w.jhInduxLegend(context([],[{id:'sma20',n:'SMA',on:true}]));
  assert.doesNotMatch(nodes.get('legend').innerHTML,/class=annotation-timing/);
});

test('Timing button opens honest stages and DIST detail; escape still closes help',()=>{
  const {w,nodes,listeners}=chrome();w.jhInduxLegend(context());
  click(nodes.get('legend').querySelector('[data-annotation-timing]'));
  const help=nodes.get('indhelp');assert.equal(help.className,'on');
  for(const text of ['Candidate:', 'Coded confirmation:', 'Retrospective selection:', 'First availability is unknown', 'no per-event confirmation status']) assert.ok(help.innerHTML.includes(text),text);
  click(help.querySelector("[data-kind='dist']"));
  assert.match(help.innerHTML,/incomplete window/);assert.match(help.innerHTML,/280 loaded bars/);
  assert.match(help.innerHTML,/No per-event window status/);
  listeners.keydown({key:'Escape'});assert.equal(help.className,'');
});

test('missing/nonfinite dates cannot become a fabricated availability or evaluation timestamp',()=>{
  const {w,nodes}=chrome();
  for(const bars of [[],[{}],[{time:null}],[{time:NaN}],[{time:Infinity}],[{time:0}],[{time:1700000000}]]) {
    w.jhInduxLegend(context(bars));w.jhInduxHelp('volume-timing');
    const warning=nodes.get('legend').innerHTML.split('<div class=annotation-timing>')[1];
    assert.doesNotMatch(warning,/1970|2023|NaN|Infinity|undefined|evaluated through/i);
    assert.match(nodes.get('indhelp').innerHTML,/Missing dates remain unknown/);
  }
});

test('help corrects separate Volume Tape/VSA baselines and threshold semantics',()=>{
  const {w,nodes}=chrome();
  const cases={capit:['−2.8%','bottom 42%','no universal minimum-volume'],bc:['+2.8%','45%','1.25×','5.40'],abs:['2.10×','30%','1.20×','1.55×','34%','1.05×'],sc:['38%','eight later','90 loaded bars','clustered'],breakout:['1.55×','55%','No additional previous-close-inside'],voltape:['preceding 60-bar window','40 later bars']};
  for(const [id,texts]of Object.entries(cases)) {
    w.jhInduxHelp(id);const html=nodes.get('indhelp').innerHTML;
    for(const t of texts)assert.ok(html.includes(t),id+': '+t);
    assert.match(html,/not predictive validation or measured signed flow/);
  }
});

test('CAPIT, BC and ABS can first appear at startup gate, not event index',()=>{
  const w=engine();
  for(const[k,bar]of Object.entries({capit:{open:100,high:101,low:90,close:90,volume:300},bc:{open:100,high:106,low:100,close:106,volume:400},abs:{open:100,high:101,low:99,close:100,volume:300}})) {
    const d=rows(100);Object.assign(d[60],bar);
    assert.deepEqual(indices(w,d.slice(0,69),k),[]);
    assert.deepEqual(indices(w,d.slice(0,70),k),[60]);
    assert.deepEqual(indices(w,d,k),[60]);
  }
});

test('SC and public VSA also respect their distinct startup gates',()=>{
  const w=engine(),sc=scFixture('rally').slice(10).map((b,i)=>({...b,time:1700000000+i*86400}));
  assert.deepEqual(indices(w,sc.slice(0,89),'sc'),[]);
  assert.deepEqual(indices(w,sc.slice(0,90),'sc'),[80]);
  const d=rows(45);d[20].volume=300;
  assert.deepEqual(w.jhTapeRead(d.slice(0,39)).vsa.markers,[]);
  assert.ok(w.jhTapeRead(d.slice(0,40)).vsa.markers.some(m=>m.text==='ABS'&&m.time===d[20].time));
});

test('SC delayed rally, tail expiration and chained replacement remain explicit counterexamples',()=>{
  const w=engine(),rally=scFixture('rally'),expiry=scFixture('expiration'),chain=scFixture('chain');
  assert.deepEqual(indices(w,rally.slice(0,93),'sc'),[]);
  assert.deepEqual(indices(w,rally.slice(0,94),'sc'),[90]);
  assert.deepEqual(indices(w,expiry.slice(0,98),'sc'),[90]);
  assert.deepEqual(indices(w,expiry.slice(0,99),'sc'),[]);
  for(const [n,expected]of[[91,90],[99,98],[107,106],[125,106]]) assert.deepEqual(indices(w,chain.slice(0,n),'sc'),[expected]);
});

test('DIST gate delays earlier date; incomplete window moves, appending reclaim removes it',()=>{
  const w=engine(),d=distributionFixture(),z=clippedDistribution();
  assert.deepEqual(w.jhDistributionScan(d.slice(0,279)),[]);
  assert.deepEqual(w.jhDistributionScan(d.slice(0,280)).map(e=>e.i),[259]);
  assert.deepEqual(w.jhDistributionScan(z.slice(0,318)),[]);
  for(const[n,i]of[[319,318],[320,319],[321,320],[322,321],[323,322],[324,322]])assert.deepEqual(w.jhDistributionScan(z.slice(0,n)).map(e=>e.i),[i]);
  const live=z.slice(0,320);assert.equal(w.jhDistributionScan(live).length,1);
  live.push({...z[320],open:170,high:172,low:169,close:171.4});assert.deepEqual(w.jhDistributionScan(live),[]);
  const corrected=z.slice(0,324).map(b=>({...b}));assert.equal(w.jhDistributionScan(corrected).length,1);
  Object.assign(corrected[320],{high:172,close:171.4});assert.deepEqual(w.jhDistributionScan(corrected),[]);
});

test('all prefix and mutation outputs retain exact legacy events and marker projections',()=>{
  const w=engine(),old={};
  new Function('window',source('tests/fixtures/chart-volume-cache-predecessor.js.txt'))(old);
  new Function('window',source('tests/fixtures/chart-annotation/distribution-predecessor.js.txt'))(old);
  chrome(undefined,w);w.__jhDistOn=old.__jhDistOn=1;
  const compare=d=>{
    // A fresh identity for the legacy cache, whose mutation defect is intentionally retained.
    const fresh=d.map(b=>({...b}));
    assert.equal(freeze(w.jhVolumeTape(d)),freeze(old.jhVolumeTape(fresh)));
    assert.equal(freeze(w.jhDistributionScan(d)),freeze(old.jhDistributionScan(fresh)));
    assert.equal(freeze(w.jhDistributionMarks(d,'1d','candles')),freeze(old.jhDistributionMarks(fresh,'1d','candles')));
  };
  for(const fixture of [scFixture('rally'),scFixture('expiration'),scFixture('chain'),clippedDistribution()]) {
    const live=[];for(const b of fixture){live.push({...b});compare(live);}
    for(const n of [100,91,70,69,0]){live.length=n;compare(live);}
  }
  const live=scFixture('chain');compare(live);
  for(const key of ['time','open','high','low','close','volume']) {const v=live[98][key];live[98][key]+=1;compare(live);live[98][key]=v;compare(live);}
});

test('legend and help add zero classification/scan calls and never touch input bars',()=>{
  const w=engine(),d=clippedDistribution();w.jhVolumeTape(d);w.jhDistributionScan(d);
  const before=[w.__classifyCalls,w.__distributionCalls],bars=freeze(d),{nodes}=chrome(undefined,w);
  for(let i=0;i<30;i++) {w.jhInduxLegend(context(d));click(nodes.get('legend').querySelector('[data-annotation-timing]'));w.jhInduxHelp('dist');w.jhInduxHelp('sc');}
  assert.deepEqual([w.__classifyCalls,w.__distributionCalls],before);assert.equal(freeze(d),bars);
  assert.equal(w.jhVolEventTable(d)[0].timing,undefined,'No unused timing fields added');
});
