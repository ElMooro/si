const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const G=require('../jh-gold-rotation-research.js'),A=require('../jh-option-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/gold-rotation-native.json'),'utf8')),clone=v=>JSON.parse(JSON.stringify(v));
function network(tamper){const calls=[];return {calls,fetch:async key=>{calls.push(key);const value=fixture.objects[key.slice(1)];assert.equal(typeof value,'string','Unexpected artifact read '+key);return new Response(tamper?tamper(key,value):value);}};}
test('public evidence binds eight dated histories and qualified compiler bytes without fetching originals',async()=>{
 const n=network(),p=await G.verified(clone(fixture.packet),n.fetch);assert.equal(Object.keys(p.instruments).length,8);assert(Object.isFrozen(p));
 assert.equal(G.evidence(p).run.contract,'gold-rotation-replay.v1');assert(n.calls.every(k=>k.startsWith('/data/gold-rotation-research/')));
 assert.match(G.recordedUrl(p,'GLD','full'),/symbol=GLD&basis=full&run=[a-f0-9]{64}$/);
});
test('an exact run does not request a mutable current or use latest as a fallback',async()=>{
 const n=network(),id=fixture.packet.replay.manifest_key.split('/').pop().slice(0,-5);const p=await G.recordedRun(id,n.fetch);
 assert.equal(p.generated_at,fixture.packet.generated_at);assert(!n.calls.includes('/'+G.CURRENT));
 const before=n.calls.length;await assert.rejects(()=>G.recordedRun('',n.fetch),/Exact recorded/);assert.equal(n.calls.length,before);
});
test('changed source/compiler bytes and promoted or altered publications are refused',async()=>{
 const n=network((key,value)=>key.includes('/compilers/')?value+' ':value);await assert.rejects(()=>G.verified(clone(fixture.packet),n.fetch),/bytes differ/);
 for(const mutation of [p=>p.calls_eligible=true,p=>p.state='BUY',p=>p.instruments.GLD.latest.full='999']){
  const p=clone(fixture.packet);mutation(p);await assert.rejects(()=>G.verified(p,network().fetch));
 }
});
test('private artifacts, foreign current keys and unverified exports are refused',async()=>{
 const n=network();for(const k of ['audit-private/example.bin','data/trade-tickets.json','data/other/outputs/'+ 'a'.repeat(64)+'.json'])await assert.rejects(()=>G.load(k,n.fetch),/Unapproved/);
 assert.equal(n.calls.length,0);assert.throws(()=>G.evidence(fixture.packet),/Verify/);assert.throws(()=>G.recordedUrl(fixture.packet,'GLD','full'),/Verify/);
});
test('exposure assumptions use exact decimal arithmetic and include costs without implying forecast',async()=>{
 const p=await G.verified(clone(fixture.packet),network().fetch),result=G.scenario(p,'GLD','full','3.125','-10.005','0.003');
 assert.equal(result.reported_close,'100');assert.equal(G.format(result.exposure_usd),'312.5');assert.equal(G.format(result.price_effect_usd),'-31.265625');assert.equal(G.format(result.net_effect_usd),'-31.268625');assert.equal(result.forecast,false);
 assert.equal(G.format(G.scenario(p,'GLD','full','0','0','0').net_effect_usd),'0');
 for(const args of [['GLD','dividend-adjusted','1','1','0'],['GLD','full','-1','1','0'],['GLD','full','1','-101','0'],['GLD','full','1','0','-1'],['GLD','full','NaN','0','0'],['INVALID','full','1','0','0']])assert.throws(()=>G.scenario(p,...args));
});
test('exact decimals keep missing separate from zero and preserve all source precision',()=>{
 assert.equal(G.format(null),'Unavailable');assert.equal(G.format('0.000'),'0');assert.equal(G.format('12345.67890123456789'),'12,345.67890123456789');
});
function dom(Gold,search,customURL=URL){const elements=new Map(),get=s=>{if(!elements.has(s))elements.set(s,{textContent:'old',innerHTML:'old',disabled:false,value:''});return elements.get(s);};
 const document={readyState:'complete',querySelector:()=>({querySelector:get})},root={JHOptionResearch:A,JHGoldRotationResearch:Gold,fetch(){throw Error('No mutable fallback');}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-gold-rotation-page.js'),'utf8'),{window:root,document,location:{search},URLSearchParams,AbortController,URL:customURL,Blob});return {get,root,document};}
test('unavailable pinned record clears all previous evidence and disables actions',async()=>{
 let requested;const d=dom({...G,recordedRun:async id=>{requested=id;throw Error('Missing record');}},'?run=missing');await new Promise(r=>setImmediate(r));
 assert.equal(requested,'missing');assert(d.get('[data-gr-export]').disabled);assert(d.get('[data-gr-calculate]').disabled);assert.equal(d.get('[data-gr-history]').textContent,'');assert.match(d.get('[data-gr-status]').textContent,/No latest or legacy substitute/);
});
test('basis and assumption changes clear scenario; pagination and selection stay on recorded evidence',async()=>{
 const p=await G.verified(clone(fixture.packet),network().fetch),d=dom({...G,recordedRun:async()=>p},'?run=verified&symbol=GLD&basis=full');await new Promise(r=>setImmediate(r));
 assert(!d.get('[data-gr-export]').disabled);assert.match(d.get('[data-gr-history]').innerHTML,/2026-09-24/);
 for(const [name,value] of [['shares','1'],['change','-10'],['cost','1']])d.get('[data-gr-'+name+']').value=value;
 d.get('[data-gr-form]').onsubmit({preventDefault(){}});assert.match(d.get('[data-gr-scenario]').innerHTML,/-11/);
 d.get('[data-gr-shares]').oninput();assert.equal(d.get('[data-gr-scenario]').textContent,'');
 d.get('[data-gr-basis]').value='dividend-adjusted';d.get('[data-gr-basis]').onchange();assert(d.get('[data-gr-calculate]').disabled);
 d.get('[data-gr-older]').onclick();assert.match(d.get('[data-gr-page]').textContent,/Page 2/);
 d.get('[data-gr-symbol]').value='SPY';d.get('[data-gr-symbol]').onchange();assert.match(d.get('[data-gr-page]').textContent,/Page 1/);assert.match(d.get('[data-gr-identity]').textContent,/SPY/);
});
test('export carries verified record and current assumptions, with a live download URL',async()=>{
 const p=await G.verified(clone(fixture.packet),network().fetch);let blob,appended=false,clicked=false,removed=false,revoked=false,cleanup;
 const url={createObjectURL(b){blob=b;return 'blob:gold';},revokeObjectURL(){revoked=true;}};
 const d=dom({...G,recordedRun:async()=>p},'?run=verified',url);await new Promise(r=>setImmediate(r));
 const anchor={click(){assert(appended&&!revoked);clicked=true;},remove(){removed=true;}};
 d.document.createElement=()=>anchor;d.document.body={appendChild(){appended=true;}};d.root.setTimeout=(fn,ms)=>{assert.equal(ms,1000);cleanup=fn;};
 for(const [n,v] of [['shares','1'],['change','-10'],['cost','1']])d.get('[data-gr-'+n+']').value=v;
 d.get('[data-gr-form]').onsubmit({preventDefault(){}});d.get('[data-gr-export]').onclick();
 const out=JSON.parse(await blob.text());assert.deepEqual(out.publication,p);assert.equal(G.format(out.assumed_exposure.net_effect_usd),'-11');assert.equal(out.selection.symbol,'GLD');
 assert(clicked&&removed&&!revoked);assert.equal(anchor.download,'gold-equity-research-evidence.json');cleanup();assert(revoked);
});
