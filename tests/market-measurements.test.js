const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),zlib=require('node:zlib');
const market=require('../jh-market-measurements.js');
const now=Date.parse('2026-09-18T22:00:00Z');
function row(){return {instrument_id:'equity:US:AAA',symbol:'AAA',asset_class:'equity',name:'A <script>unsafe</script>',
 price:100,last_observed_price:100,observed_at:'2026-09-18T04:00:00Z',acquired_at:'2026-09-18T21:00:00Z',
 quality:{status:'fresh',original_source_verified:true},price_kind:'completed ET daily aggregate close',source_row:0,period_start:'1970-01-01T00:00:00.001Z',
 changes:{month:{value:2,baseline_date:'2026-08-17',current_date:'2026-09-17',baseline_close:98}}};}
test('render dated measurements without current prices from stale or unverified inputs',()=>{
 const r=row(),html=market.render(r,now);assert.match(html,/100 <small>USD/);assert.match(html,/2026-08-17/);
 assert(!html.includes('<script>'));assert.match(html,/&lt;script&gt;/);assert.match(html,/No call or sizing authority/);
 const stale=market.render(r,now+2*86400000);assert.match(stale,/Unavailable <small>USD/);assert.match(stale,/STALE/);
 r.quality.original_source_verified=false;assert.equal(market.status(r,now),'unverified');
 r.quality.original_source_verified=true;r.acquired_at='2099-01-01T00:00:00Z';assert.equal(market.status(r,now),'unavailable');
});
test('crypto enumeration preserves provider identity even if symbols collide',()=>{
 const rows=market.records({contract:'daily-research-report.v1',market_measurement_quality:{},stocks:{},crypto_by_id:{one:{symbol:'ONE',provider_id:'one'},two:{symbol:'ONE',provider_id:'two'}}});
 assert.equal(rows.length,2);assert.equal(rows[1].provider_id,'two');
 assert.throws(()=>market.records({stocks:{}}));assert.equal(market.safeKey('data/evidence/../../private.json'),false);
});
test('actual source inspector verifies decompressed bytes, identity, hash and displayed close',async()=>{
 const r=row(),raw=Buffer.from(JSON.stringify({ticker:'AAA',adjusted:true,results:[{c:100,t:1}]}));
 const sha=crypto.createHash('sha256').update(raw).digest('hex'),key='data/evidence/polygon/request/'+sha+'.bin.gz';
 r.evidence={key,sha256:sha,bytes:raw.length};
 const fetcher=async()=>new Response(zlib.gzipSync(raw),{headers:{'X-JH-Artifact-Key':key}});
 const out=await market.original(r,fetcher);assert.equal(out.original.c,100);assert.equal(out.verified_sha256,sha);
 await assert.rejects(()=>market.original(r,async()=>new Response(raw,{headers:{'X-JH-Artifact-Key':'wrong'}})),/identity differs/);
 await assert.rejects(()=>market.original({...r,last_observed_price:101},fetcher),/differs from original/);
 await assert.rejects(()=>market.original({...r,evidence:{...r.evidence,sha256:'0'.repeat(64)}},fetcher),/hash differs/);
});
