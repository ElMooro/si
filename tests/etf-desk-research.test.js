const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-etf-desk-research.js'),H=require('../jh-etf-holdings.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/etf-desk-native.json'),'utf8'));
const p=fixture.publication,AT=Date.parse('2026-09-21T11:00:00Z');
global.crypto=require('node:crypto').webcrypto;
const fetcher=async url=>new Response(fixture.artifacts[url.slice(1)]||'',{status:fixture.artifacts[url.slice(1)]?200:404});
test('recorded ETF run stays pinned and never asks for latest',async()=>{
  const seen=[],id=p.replay.manifest_key.split('/').pop().slice(0,-5);
  const recorded=await api.recordedRun(id,async url=>{seen.push(url);return fetcher(url);});
  assert.deepEqual(recorded,p);assert.equal(seen.includes('/'+api.CURRENT),false);
  assert.equal(api.recordedUrl(recorded,'BND'),'/etf.html?fund=BND&run='+id);
  assert.equal(api.scenario(recorded,'BND',10000,-5,25,AT).run.manifest_key,p.replay.manifest_key);
});
test('unavailable or invalid recorded run cannot silently choose a latest publication',async()=>{
  let calls=0;await assert.rejects(()=>api.recordedRun('../latest',async()=>{calls++;}),/Exact recorded/);assert.equal(calls,0);
  const id=p.replay.manifest_key.split('/').pop().slice(0,-5),seen=[];
  await assert.rejects(()=>api.recordedRun(id,async url=>{seen.push(url);return new Response('',{status:404});}),/request failed/);
  assert.deepEqual(seen,['/'+p.replay.manifest_key]);
});
test('desk body and immutable output bind the exact source run',async()=>{
  assert.equal(await api.verifyPacket(p,fetcher),p);
  const changed=structuredClone(p);changed.funds.BND.profiles.current.effective_date='2026-09-21';
  await assert.rejects(()=>api.verifyPacket(changed,fetcher),/body differs/);
  await assert.rejects(()=>api.verifyPacket(p,async url=>new Response((fixture.artifacts[url.slice(1)]||'')+' ')),/run differs/);
});
test('only reviewed public artifacts can be fetched',async()=>{
  let calls=0;for(const key of ['audit-private/source.bin','data/portfolio.json',api.PREFIX+'requests/'+'a'.repeat(64)+'.json','https://example.com/source']){
    await assert.rejects(()=>api.load(key,async()=>{calls++;}),/Unapproved/);
  }assert.equal(calls,0);
});
test('authority, ticker and source clock mismatches fail closed',()=>{
  for(const mutate of [x=>x.sizing_eligible=true,x=>x.funds.BND.ticker='SPY',x=>x.funds.BND.flows.calls_eligible=true,
      x=>x.canonical_sources.flows.generated_at='2026-09-22T00:00:00Z']){
    const changed=structuredClone(p);mutate(changed);assert.equal(api.typed(changed),false);
  }
});
test('profile evidence preserves raw fee scale, zero, source rows and distinct holding dates',async()=>{
  const doc=await api.profile(p,'BND','current',fetcher),html=api.profileView(doc,'current');
  assert.match(html,/provider_reported_net_expenses_scale_unqualified/);assert.match(html,/field net_expenses/);
  assert.match(html,/Profile effective 2026-09-18/);assert.match(api.fundView(p,'BND',AT),/holdings 2026-08-31/);
  assert.equal(doc.profiles[0].exposures.currency_exposure.normalized,false);
});
test('large decimal text retains all digits and distinguishes missing from zero',()=>{
  assert.equal(api.exact('999999999999999999999999.1234567890123456789'),'999,999,999,999,999,999,999,999.1234567890123456789');
  assert.equal(api.exact('0'),'0');assert.equal(api.exact(null),'Unavailable');assert.equal(api.exact(false),'Unavailable');
});
test('source strings cannot inject markup through profile or inventory',async()=>{
  const doc=await api.profile(p,'BND','current',fetcher);doc.profiles[0].text.description.value='<img src=x onerror=alert(1)>';
  const html=api.profileView(doc,'current');assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);
  const q=structuredClone(p);q.funds.BND.profiles.current.summary.text.description.value='<script>bad</script>';
  assert.doesNotMatch(api.inventory(q,'',AT),/<script>/);
});
test('new page compilation cannot turn old source windows current',()=>{
  assert.notEqual(api.windowValue(p.funds.BND,5,AT),null);
  const at=Date.parse('2026-10-01T00:00:00Z');assert.equal(api.windowValue(p.funds.BND,5,at),null);
  assert.match(api.summary(p,at),/profiles 0\/3, flows 0\/3, complete returned holdings 0\/3/);
  assert.match(api.inventory(p,'',at),/3 of 3/);assert.match(api.inventory(p,'',at),/Unavailable \/ overdue/);
});
test('full old tape stays separate from verified history and every original row is available',async()=>{
  const h=await api.history(p,'BND',fetcher);assert.equal(h.history.length,25);
  assert.match(api.fundView(p,'BND',AT),/2017-04-03/);assert.match(api.fundView(p,'BND',AT),/not merged/);
  const wrapped={funds:{BND:p.funds.BND.holdings}},snapshot=await H.snapshot(wrapped,'BND','current',fetcher);
  const rows=await H.rowPart(snapshot,0,fetcher);assert.equal(rows.length,2);assert.equal(rows[1].constituent_ticker,null);
});
test('fund scenario uses explicit signed assumptions and costs without multiplying raw source exposure',()=>{
  const long=api.scenario(p,'BND',10000,-5,25,AT),short=api.scenario(p,'BND',-10000,-5,25,AT);
  assert.equal(long.net_change_usd,-525);assert.equal(short.net_change_usd,475);assert.equal(long.source_profile_weight_or_fee_used,false);
  assert.equal(long.run,p.replay);
  for(const args of [[0,5,0],[100,NaN,0],[100,-101,0],[100,5,-1],[Infinity,5,0]])assert.throws(()=>api.scenario(p,'BND',...args,AT));
  assert.throws(()=>api.scenario(p,'BND',10000,-5,25,Date.parse('2026-10-01T00:00:00Z')),/current source check/);
});
