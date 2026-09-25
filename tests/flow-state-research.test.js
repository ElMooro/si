const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const F=require('../jh-flow-state-research.js'),A=require('../jh-option-research.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/flow-state-native.json'),'utf8'));
const clone=v=>JSON.parse(JSON.stringify(v));
function fetcher(tamper){const calls=[];return {calls,fetch:async key=>{calls.push(key);const body=fixture.objects[key.slice(1)];assert.equal(typeof body,'string','Unexpected artifact read '+key);return new Response(tamper?tamper(key,body):body,{headers:{'Content-Type':'application/json'}});}};}

test('all parent artifacts bind and exact displayed arithmetic stays descriptive',async()=>{
 const network=fetcher(),p=await F.verifyPacket(clone(fixture.packet),network.fetch);
 const proof=F.evidence(p);assert.equal(Object.keys(proof.parents).length,5);assert.equal(p.asset_class_rotation.length,3);
 assert(p.asset_class_rotation.every(r=>r.direction==='net_issuance_estimate'));assert.equal(p.category_total,null);
 assert.equal(p.monthly_transactions[0].unit,'usd_bn');assert.equal(p.independent_investment_votes,0);
 assert(network.calls.every(k=>k.startsWith('/data/')));assert(Object.isFrozen(p));
 assert(F.recordedUrl(p).includes(p.replay.manifest_key.split('/').pop().slice(0,-5)));
});
test('recorded run never substitutes a current or legacy head',async()=>{
 const network=fetcher(),id=fixture.packet.replay.manifest_key.split('/').pop().slice(0,-5);
 const p=await F.recordedRun(id,network.fetch);assert.equal(p.generated_at,fixture.packet.generated_at);
 assert(!network.calls.includes('/'+F.CURRENT));
 const before=network.calls.length;await assert.rejects(()=>F.recordedRun('',network.fetch),/Exact recorded/);assert.equal(network.calls.length,before);
});
test('changed compiler bytes and changed head are rejected',async()=>{
 const tampered=fetcher((key,body)=>key.includes('/compilers/')?body+' ':body);
 await assert.rejects(()=>F.verifyPacket(clone(fixture.packet),tampered.fetch),/bytes differ/);
 const network=fetcher(),bad=clone(fixture.packet);bad.asset_class_rotation[0].value_decimal='101.125';
 await assert.rejects(()=>F.verifyPacket(bad,network.fetch),/differs from recorded/);
});
test('private and unreviewed paths cannot be fetched',async()=>{
 const network=fetcher();for(const key of ['data/trade-tickets.json','audit-private/original.bin','data/unknown/runs/'+'a'.repeat(64)+'.json'])await assert.rejects(()=>F.load(key,network.fetch),/Unapproved/);
 assert.equal(network.calls.length,0);assert.throws(()=>F.evidence(fixture.packet),/Verify/);
});
test('source arithmetic rejects false outflows, double counts and unit changes',async()=>{
 const network=fetcher(),p=await F.verifyPacket(clone(fixture.packet),network.fetch),parents=F.evidence(p).parents;
 for(const mutate of [p=>p.asset_class_rotation[1].direction='net_redemption_estimate',p=>p.category_overlap=[],p=>p.monthly_transactions[0].unit='usd',p=>p.asset_class_rotation[0].value_decimal='100.126']){
  const bad=clone(p);mutate(bad);assert.throws(()=>F.verifyRows(bad,parents));
 }
});
test('decimal display keeps exact fractional digits and missing distinct from zero',()=>{
 assert.equal(F.formatAmount('1000000.123456'),'1,000,000.123456');assert.equal(F.formatAmount('-3.560000'),'-3.56');
 assert.equal(F.formatAmount('0.000000'),'0');assert.equal(F.formatAmount(null),'Unavailable');
 assert(F.equalDecimal('1.0','1.000'));assert(!F.equalDecimal('1.0000000000000001','1'));
});
test('self-promoted legacy and unverified publications cannot be exported',async()=>{
 const network=fetcher();await assert.rejects(()=>F.verifyPacket({version:'1.0',headline:'BUY'},network.fetch),/descriptive/);
 await assert.rejects(()=>F.verifyPacket({...fixture.packet,calls_eligible:true},network.fetch),/descriptive/);
 assert.equal(network.calls.length,0);assert.throws(()=>F.recordedUrl(clone(fixture.packet)),/Verify/);
});
test('page clears old regions and export state when a pinned record is unavailable',async()=>{
 const elements=new Map(),get=selector=>{if(!elements.has(selector))elements.set(selector,{textContent:'old',innerHTML:'old',disabled:false});return elements.get(selector);};
 const host={querySelector:get},document={readyState:'complete',querySelector:()=>host};
 let requested=null;const root={JHOptionResearch:A,JHFlowStateResearch:{...F,recordedRun:async id=>{requested=id;throw Error('Unavailable exact record');}},fetch(){throw Error('No latest fallback');}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-flow-state-page.js'),'utf8'),{window:root,document,location:{search:'?run=missing'},URLSearchParams,AbortController,URL,Blob});
 await new Promise(resolve=>setImmediate(resolve));assert.equal(requested,'missing');assert(get('[data-fs-export]').disabled);
 assert.equal(get('[data-fs-categories]').textContent,'');assert.match(get('[data-fs-status]').textContent,/No latest or legacy substitute/);
});
test('export contains verified evidence and keeps the object URL alive until download starts',async()=>{
 const elements=new Map(),get=s=>{if(!elements.has(s))elements.set(s,{textContent:'',innerHTML:'',disabled:false});return elements.get(s);};
 const network=fetcher(),p=await F.verifyPacket(clone(fixture.packet),network.fetch);
 let blob,appended=false,clicked=false,removed=false,revoked=false,cleanup;
 const anchor={click(){assert(appended);assert(!revoked);clicked=true;},remove(){removed=true;}};
 const document={readyState:'complete',querySelector:()=>({querySelector:get}),body:{appendChild(a){assert.equal(a,anchor);appended=true;}},createElement:()=>anchor};
 const root={JHOptionResearch:A,JHFlowStateResearch:{...F,recordedRun:async()=>p},fetch:network.fetch,setTimeout(fn,ms){assert.equal(ms,1000);cleanup=fn;}};
 const objectURL={createObjectURL(b){blob=b;return 'blob:verified';},revokeObjectURL(u){assert.equal(u,'blob:verified');revoked=true;}};
 vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../jh-flow-state-page.js'),'utf8'),{window:root,document,location:{search:'?run='+p.replay.manifest_key.split('/').pop().slice(0,-5)},URLSearchParams,AbortController,URL:objectURL,Blob});
 await new Promise(resolve=>setImmediate(resolve));get('[data-fs-export]').onclick();
 assert(clicked&&removed&&!revoked);assert.equal(anchor.download,'cross-asset-flow-evidence.json');
 const exported=JSON.parse(await blob.text());assert.deepEqual(exported.publication,p);assert.equal(Object.keys(exported.parents).length,5);
 cleanup();assert(revoked);
});
