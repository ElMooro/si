const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto');
global.crypto=crypto.webcrypto;
const D=require('../jh-dollar-research.js'),ui=require('../jh-macro-rooms.js');
const fixture=JSON.parse(fs.readFileSync('tests/fixtures/dollar-native.json','utf8'));
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
const loaded=()=>D.verifyPacket(structuredClone(fixture.publication),fetcher);

test('overview retains all 32 source series with quote direction and pinned drill-downs',async()=>{
 const p=await loaded(),html=ui.render(p,Date.parse(p.generated_at)+1000);
 assert.equal((html.match(/<tr>/g)||[]).length,33);
 assert.match(html,/USD per EUR/);assert.match(html,/JPY per USD/);
 for(const sid of D.SERIES)assert(html.includes(D.recordedUrl(p,sid).replaceAll('&','&amp;')));
 assert.match(html,/Retained observation \/ unit/);assert(!html.includes('GREEN'));assert(!html.includes('/100'));
});
test('unverified packets cannot render and expiry labels the retained observation as overdue',async()=>{
 assert.throws(()=>ui.render(fixture.publication),/Verify/);
 const p=await loaded(),html=ui.render(p,Date.parse(p.generated_at)+8*86400000);
 assert.match(html,/Source review overdue/);assert.match(html,/recorded observations, not executable quotes/);
 assert.throws(()=>ui.render(p,Date.parse(p.generated_at)-1000),/Future/);
});
test('failed reload clears prior data and browser reads omit credentials',async()=>{
 let resolve,good=true,writes=0;const ready=new Promise(r=>resolve=r),timers=[],requests=[];
 const host={_html:'',_text:'',set innerHTML(v){this._html=v;this._text='';writes++;resolve();},get innerHTML(){return this._html;},
  set textContent(v){this._text=v;this._html='';},get textContent(){return this._text;}};
 const win={document:{getElementById:()=>host},setInterval:(f,t)=>timers.push({f,t}),fetch:async(url,options)=>{
  requests.push(options);if(!good)return new Response('',{status:503});
  return url==='/'+D.CURRENT?Response.json(fixture.publication):fetcher(url);}};
 ui.install(win);await ready;assert.match(host.innerHTML,/32|DTWEXBGS/);
 timers.find(t=>t.t===60000).f();assert.equal(writes,1);
 assert(requests.every(r=>r.credentials==='omit'));good=false;await timers.find(t=>t.t===300000).f();
 assert.equal(host.innerHTML,'');assert.match(host.textContent,/Earlier values have been cleared/);
});
test('page shares native viewers and preserves the complete predecessor without synthetic rings or status defaults',()=>{
 const page=fs.readFileSync('macro-rooms.html','utf8');
 for(const id of ['rooms-dollar','liquidity-pulse-panel','rg-research','rg-search','rg-category','rg-horizon','eurodollar-context','jh-liq-fails-panel'])
  assert.equal((page.match(new RegExp('id="'+id+'"','g'))||[]).length,1);
 for(const script of ['jh-macro-rooms.js','jh-risk-gate-research.js','liquidity-pulse.js','jh-eurodollar-research.js'])assert(page.includes('/'+script));
 assert(!page.includes('flagOf'));assert(!page.includes('wr-ring'));assert(!page.includes('d.dollar_pressure||0'));
 const ref=JSON.parse(fs.readFileSync('tests/fixtures/macro-rooms-native-overview-migration.json','utf8')).complete_predecessor;
 const raw=fs.readFileSync(ref.path);assert.equal(raw.length,ref.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),ref.sha256);
});
