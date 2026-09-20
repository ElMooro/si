const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path');
const ui=require('../jh-breadth-research.js'),sha=raw=>crypto.createHash('sha256').update(raw).digest('hex');
function packet(){const days=['2026-09-16','2026-09-17','2026-09-18'];return {contract:'breadth-native-research.v1',generated_at:'2026-09-20T12:00:00Z',as_of:days[2],
 calls_eligible:false,forecast_eligible:false,sizing_eligible:false,execution_eligible:false,breadth_score:null,decision:{verb:'WAIT'},
 calendar:{requested_sessions:days},series:{PCT_ABOVE_200DMA:Object.fromEntries(days.map((d,i)=>[d,[10,null,0][i]]))},latest:{PCT_ABOVE_200DMA:[days[2],0]},
 coverage:Object.fromEntries(days.map(d=>[d,{status:'available',current_filter_population:2,sma200_denominator:1,sma200_numerator:0}])),
 current_constituents:[{symbol:'AAPL',change:'unchanged',above_50:null,above_200:false,closing_high:null,closing_low:null,prior_consecutive_sessions:201,source_row_index:0}],
 field_units:{PCT_ABOVE_200DMA:'percent'},source_evidence:{},source_errors:{}};}
function retain(p){const raw=Buffer.from(JSON.stringify(p)),digest=sha(raw),output={key:'data/breadth-research/outputs/'+digest+'.json',sha256:digest,bytes:raw.length};
 const m={contract:'breadth-native-replay.v1',generated_at:p.generated_at,output_sha256:digest,output},run=Buffer.from(JSON.stringify(m)),key='data/breadth-research/runs/'+sha(run)+'.json';
 return {packet:{...p,replay:{manifest_key:key,output_sha256:digest}},objects:{[key]:run,[output.key]:raw}};}
function fetcher(objects){return async url=>{const raw=objects[url.slice(1)];return {ok:!!raw,status:raw?200:404,arrayBuffer:async()=>new Uint8Array(raw).buffer};};}
test('page refuses legacy score and producer self-qualification',()=>{assert.equal(ui.typed({breadth_score:80}),false);const p=packet();p.calls_eligible=true;assert.equal(ui.typed(p),false);});
test('current zero and separate population stay visible',()=>{const html=ui.render(packet(),Date.parse('2026-09-20T13:00:00Z'));assert.match(html,/>0 <small>%/);assert.match(html,/0 \/ 1 complete 200-session windows/);assert.doesNotMatch(html,/undefined|NaN/);});
test('stale publication is dated explicitly',()=>{assert.match(ui.render(packet(),Date.parse('2026-09-25T13:00:00Z')),/not current readings/);assert.equal(ui.current(packet(),Date.parse('2026-09-19T13:00:00Z')),false);});
test('source run and exact output bytes bind the public packet',async()=>{const f=retain(packet());assert.equal(await ui.verifyPacket(f.packet,fetcher(f.objects)),f.packet);const changed=structuredClone(f.packet);changed.latest.PCT_ABOVE_200DMA[1]=99;await assert.rejects(ui.verifyPacket(changed,fetcher(f.objects)),/Current packet differs/);f.objects[Object.keys(f.objects)[1]]=Buffer.from('{}');await assert.rejects(ui.verifyPacket(f.packet,fetcher(f.objects)),/Output bytes differ/);});
test('native history has two isolated points across a missing session',()=>{const html=ui.history(packet(),'PCT_ABOVE_200DMA');assert.equal((html.match(/<circle /g)||[]).length,2);assert.doesNotMatch(html,/<polyline/);assert.match(html,/2 numeric observations \/ 3 requested sessions/);assert.match(html,/2026-09-17/);});
test('constituent flags distinguish false from missing and escape symbol text',()=>{const p=packet();p.current_constituents[0].symbol='<img onerror=x>';const html=ui.constituents(p,'img');assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);assert.match(html,/>No<\/td>/);assert.match(html,/>Unavailable<\/td>/);});
test('native page loads the reviewed renderer and omits legacy score claims',()=>{const page=fs.readFileSync(path.join(__dirname,'../market-internals.html'),'utf8');assert.match(page,/jh-breadth-research.js/);assert.doesNotMatch(page,/JHKit|Breadth divergences lead|jh-page-ai.js/);});
test('current packet and request status bypass cache while originals stay private',async()=>{
 const {pathToFileURL}=require('node:url');const worker=(await import(pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js')).href)).default;
 const calls=[],stored=[];globalThis.caches={default:{match:async()=>undefined,put:async(...args)=>stored.push(args)}};globalThis.fetch=async url=>{calls.push(String(url));return new Response('{}',{status:200});};
 for(const key of ['data/market-internals.json','data/breadth-research/requests/'+'a'.repeat(64)+'.json']){const waits=[];const response=await worker.fetch(new Request('https://justhodl.ai/'+key),{},{waitUntil:p=>waits.push(p)});await Promise.all(waits);assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');}
 assert.equal(stored.length,0);const before=calls.length;const response=await worker.fetch(new Request('https://justhodl.ai/audit-private/20260909-originals/market-internals/'+'a'.repeat(64)+'.bin'),{},{waitUntil(){}});assert([401,403].includes(response.status));assert.equal(calls.length,before);
});
