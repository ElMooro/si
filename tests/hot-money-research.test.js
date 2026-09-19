const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-hot-money-research.js'),stamp='2026-09-19T08:30:00Z',now=Date.parse(stamp);
function board(){return {status:'LIVE',scope:'TWSE fixture',latest_bn:0,latest_day:'20260918',quality:{status:'fresh',observation_date:'2026-09-18',acquired_at:stamp},
 verified_observations:1,legacy_unverified_observations:20,windows:{},latest:{date:'20260918',net_twd:'0',acquired_at:stamp,original:{key:'data/evidence/fixture.bin.gz'},categories:{},definition:{unit_source:'response.hints'},notes:[]},history:[]};}
function packet(){const tw=board();tw.otc=board();tw.combined={status:'LIVE',latest_bn:0,latest_day:'20260918',windows:{}};
 return {contract:api.CONTRACT,generated_at:stamp,source_generated_at:stamp,call:null,calls_eligible:false,sizing_eligible:false,execution_eligible:false,countries:{taiwan:tw},decision:{reason:'Research only'}};}

test('current verified zero is visible, missing and boolean values do not become zero',()=>{
 assert.equal(api.num(0),'0');assert.equal(api.num(null),'Unavailable');assert.equal(api.num(false),'Unavailable');
 assert.match(api.render(packet(),now).combined,/<strong>0 TWD billions/);assert.throws(()=>api.render({methodology_version:'exchange-observations.v2'}));
});
test('source acquisition and Taipei date expiry do not renew from publication time',()=>{
 const p=packet(),b=p.countries.taiwan;b.quality.acquired_at='2026-09-17T08:30:00Z';
 assert.equal(api.fresh(b,p,now),false);assert.match(api.render(p,now).combined,/<strong>Unavailable/);
 assert.match(api.render(p,now,true).hero,/Pinned historical/);assert.match(api.render(p,now,true).combined,/<strong>0 TWD billions/);
 b.quality.acquired_at=stamp;b.quality.observation_date='2026-09-20';assert.equal(api.fresh(b,p,now),false);
});
test('original rows, units, legacy counts and unsupported calendar claims are visible',()=>{
 const out=api.render(packet(),now);assert.match(out.countries,/20 legacy dates awaiting originals/);assert.match(out.countries,/No exchange-calendar completeness claim/);
 assert.match(out.countries,/Original rows and exact TWD/);assert.match(out.countries,/divide by 1,000,000,000/);assert.match(out.combined,/at least 60 verified/);
});
test('strings and evidence paths cannot inject page HTML',()=>{
 const p=packet();p.countries.taiwan.scope='<img onerror=alert(1)>';p.countries.taiwan.latest.original.key='javascript:alert(1)';
 const html=api.render(p,now).countries;assert.ok(!html.includes('<img'));assert.ok(!html.includes('href="javascript:'));
 assert.equal(api.path('data/../private'),null);assert.throws(()=>api.render(p,now,false,'invalid'));
 p.execution_eligible=true;assert.throws(()=>api.render(p,now));
});
test('snapshot hash verification rejects changed outputs and oversized manifests',async()=>{
 const output=new TextEncoder().encode(JSON.stringify(packet())),hash=await api.sha(output);
 const manifest={contract:'hot-money-replay.v1',generated_at:stamp,output_sha256:hash,output:{key:api.PREFIX+'outputs/'+hash+'.json',sha256:hash,bytes:output.length},compilers:{}};
 const raw=new TextEncoder().encode(JSON.stringify(manifest)),id=await api.sha(raw),files={['/'+api.PREFIX+'runs/'+id+'.json']:raw,['/'+manifest.output.key]:output};
 const fetcher=async key=>new Response(files[key],{status:files[key]?200:404});assert.equal((await api.loadSnapshot(id,fetcher)).contract,api.CONTRACT);
 files['/'+manifest.output.key]=new TextEncoder().encode('{}');await assert.rejects(api.loadSnapshot(id,fetcher),/output differs/);
 await assert.rejects(api.loadSnapshot('../latest',fetcher),/Invalid snapshot/);
 await assert.rejects(api.loadSnapshot('a'.repeat(64),async()=>new Response(new Uint8Array(256*1024+1))),/size bound/);
});
test('page loads one immutable snapshot and no separately mutable history charts',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../hot-money.html'),'utf8');assert.match(html,/id="sub"/);assert.match(html,/jh-hot-money-research.js/);
 assert.ok(!html.includes('bfi82u-foreign.json'));assert.ok(!html.includes('jh-page-ai'));
 const js=fs.readFileSync(path.join(__dirname,'../jh-hot-money-page.js'),'utf8');assert.match(js,/setInterval\(refresh,60000\)/);assert.match(js,/Current pointer differs/);
});

test('dated chart preserves real zero and reports gaps without fabricated observations',()=>{
 const p=packet();p.countries.taiwan.history=[{date:'20260916',net_twd:'0',categories:{},notes:[]},{date:'20260918',net_twd:'1000000000',categories:{},notes:[]}];
 const html=api.render(p,now).history;assert.match(html,/horizontal axis uses calendar dates/);assert.match(html,/height="0.00"/);
 assert.ok(!html.includes('20260917'));assert.match(html,/exact TWD/);
});
