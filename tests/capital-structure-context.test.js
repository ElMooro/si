const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const ROOT=path.join(__dirname,'..'),read=p=>fs.readFileSync(path.join(ROOT,p),'utf8');
const gate=require('../jh-capital-structure-context.js');
const legacy={tickers:{AAOI:{read:'EXTREME_DILUTION',sh_yoy_pct:300,flags:['ATM_SHELF_ACTIVE'],buyback_yield_pct:50}},calls_eligible:true,score:100};
const settle=()=>new Promise(resolve=>setImmediate(resolve));

test('legacy, malformed and self-declared qualified packets cannot grant a capital-structure vote',()=>{
 for(const packet of [null,[],7,legacy,{...legacy,contract:'capital-structure-original-research.v1'}]){
  const before=structuredClone(packet),view=gate.decisionView(packet);
  assert.deepEqual(view.tickers,{});assert.deepEqual(view.rows,[]);assert.equal(view.score,null);
  for(const key of ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'])assert.equal(view[key],false);
  assert.equal(view.independent_investment_votes,0);assert.equal(view.research_context.output_digest_verified,false);
  assert.equal(view.research_context.original_source_replay_performed,false);assert.deepEqual(packet,before);
 }
});

class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.style={};this.textContent='';}
 append(...children){this.children.push(...children);}appendChild(child){this.append(child);}setAttribute(k,v){this[k]=v;}
}
function dom(){
 const body=new Element('body');return{body,createElement:t=>new Element(t),getElementById:id=>body.children.find(n=>n.id===id),
  querySelector:()=>({getAttribute:()=> 'AAOI'}),querySelectorAll:()=>[]};
}
test('availability note is accessible, idempotent and states missing evidence is not stability',()=>{
 const document=dom();gate.mount(document);gate.mount(document);
 assert.equal(document.body.children.length,1);const detail=document.body.children[0];assert.equal(detail.tagName,'details');
 assert.equal(detail.children[0].tagName,'summary');assert.match(detail.children[0].textContent,/unqualified/);
 assert.match(detail.children[1].textContent,/does not mean stable shares/);
 // The existing fixed cached-brief launcher occupies the bottom-right 56px.
 const bottom=Number(/bottom:(\d+)px/.exec(detail.style.cssText)[1]);assert(bottom>=72);
 const predecessor=JSON.parse(read('tests/fixtures/capital-structure-notice-placement.json'));
 const bytes=fs.readFileSync(path.join(ROOT,predecessor.predecessor));assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),predecessor.sha256);
});

test('actual chart scripts cannot raise legacy warnings with a missing or loaded guard',async()=>{
 for(const file of ['jh-chart-risk.js','jh-chart-risk-v3.js'])for(const loaded of [false,true]){
  const document=dom(),window=loaded?{JHCapitalStructureContext:gate}:{};let paint;
  vm.runInNewContext(read(file),{window,document,location:{pathname:'/chart.html',search:'?s=AAPL'},URLSearchParams,
   fetch:async()=>({ok:true,json:async()=>legacy}),setInterval:fn=>paint=fn});
  await settle();assert.equal(typeof paint,'function');paint();
  assert.equal(document.getElementById('jh-risk-banner').style.display,'none');
 }
 // Preserve the active tab contract rather than silently returning the GO box symbol.
 const code=read('jh-chart-risk-v3.js'),start=code.indexOf('  function currentSym()'),end=code.indexOf('  function paint(',start);
 const document=dom(),symbol=vm.runInNewContext(code.slice(start,end)+'currentSym()', {document,norm:s=>s});assert.equal(symbol,'AAOI');
});

test('volume-fusion and fundamental chips do not bypass the guard',async()=>{
 for(const loaded of [false,true]){
  const window=loaded?{JHCapitalStructureContext:gate}:{},document=dom();document.documentElement={};document.querySelectorAll=()=>[];
  const fetch=async url=>({ok:true,json:async()=>url.includes('share-flows')?legacy:{}});
  vm.runInNewContext(read('jh-chart-instvol.js'),{window,fetch});
  const packet=await window.JHInstVol.of('AAOI');assert.equal(packet.shares,null);
  vm.runInNewContext(read('jh-fund-chips.js'),{window,document,fetch,MutationObserver:class{observe(){}}});
  await window.JHF.ready;assert.equal(window.JHF.chips('AAOI'),'');assert.equal(window.JHF.readline('AAOI'),'');
 }
});

test('Why does not reconstruct dilution from legacy fallback or call missing shares STABLE',async()=>{
 const code=read('why.html'),start=code.indexOf('function renderDilutionPillar('),end=code.indexOf('function renderPeerComparison(',start);
 const html=vm.runInNewContext(code.slice(start,end)+'renderDilutionPillar({dilution:{verdict:"DEATH_SPIRAL",risk_flag:true}})');
 assert.match(html,/Ownership dilution: unqualified/);assert.doesNotMatch(html,/DEATH_SPIRAL|SHAREHOLDERS ARE BEING/);
 const begin=code.indexOf('window.fillJHVitals='),finish=code.indexOf('/* ── ops 3781',begin);
 for(const loaded of [false,true]){
  const window=loaded?{JHCapitalStructureContext:gate}:{},el={};
  vm.runInNewContext(code.slice(begin,finish),{window,document:{getElementById:()=>el},gj9:async()=>legacy,
    n9:()=> '—',fm$:()=> '—'});
  await window.fillJHVitals({ticker:'AAOI',dilution:{verdict:'DEATH_SPIRAL',risk_flag:true},quant_risk:{dilution_1y_pct:999}});
  assert.match(el.innerHTML,/UNQUALIFIED/);assert.doesNotMatch(el.innerHTML,/STABLE|DILUTING|SHRINKING|999|300/);
 }
});

test('whole predecessors remain exact and every edited page script parses',()=>{
 const manifest=JSON.parse(read('tests/fixtures/capital-structure-browser-migration.json'));
 for(const entry of manifest.files){
  const raw=fs.readFileSync(path.join(ROOT,entry.predecessor));assert.equal(raw.length,entry.bytes);
  assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),entry.sha256);
  const code=read(entry.target);
  if(entry.target.endsWith('.js'))new vm.Script(code,{filename:entry.target});
  if(entry.target.endsWith('.html')){
   assert(code.indexOf('/jh-capital-structure-context.js')<code.indexOf('</head>'));
   let index=0;for(const m of code.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
    if(/\bsrc\s*=|application\/ld\+json|application\/json|type=["']module/i.test(m[1]))continue;
    new vm.Script(m[2],{filename:entry.target+':inline-'+index++});
   }
  }
 }
 assert.match(read('scripts/build_workspaces.py'),/jh-capital-structure-context\.js/);
 assert.match(read('scripts/build_workspaces.py'),/biggest_velocity_surges: \[\], calls_eligible: false/);
});
