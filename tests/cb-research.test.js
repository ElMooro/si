const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const api=require('../jh-cb-research.js');
const stamp='2026-09-19T05:00:00Z',now=Date.parse('2026-09-19T06:00:00Z');
function measurement(value=0){return {latest:value,unit:'percent_per_annum',source_id:'RATE',source_unit:'Percent',scale_decimal:'1',
 quality:{status:'fresh',observation_date:'2026-09-18',acquired_at:stamp,source_generated_at:stamp,max_age_days:10},
 selected:{date:'2026-09-18',period:'2026-09-18',native_decimal:String(value),original_row_index:0,original:{key:'data/evidence/fixture.bin.gz'}},
 changes:{'1':{level_change:0,start_date:'2026-08-18',end_date:'2026-09-18'}}};}
function packet(){const m=measurement();return {contract:api.CONTRACT,call:null,calls_eligible:false,sizing_eligible:false,generated_at:stamp,source_generated_at:stamp,
 quality:{expected_native_series:1},measurements:{RATE:m},central_banks:[{cb:'SNB',balance_sheet:{},rate:m,rate_definition:'Monthly interbank rate, not policy rate',decomposition:{status:'unavailable'}}],
 fx_context:{},pd_settlement_fails:{},decision:{reason:'Research only'}};}
test('dated zero and negative rates survive; missing and boolean do not become zero',()=>{
 assert.equal(api.num(0),'0');assert.equal(api.num(-.045),'-0.045');assert.equal(api.num(null),'Unavailable');assert.equal(api.num(false),'Unavailable');
 assert.match(api.render(packet(),now).cbs,/<strong>0 %<\/strong>/);assert.throws(()=>api.render({methodology_version:'cb-component-measurements.v2'}));
});
test('stale acquisition, future periods and elapsed source age withhold current values',()=>{
 const p=packet(),m=p.measurements.RATE;assert.equal(api.fresh(m,p,now),true);m.quality.acquired_at='2026-09-17T00:00:00Z';
 assert.equal(api.fresh(m,p,now),false);assert.ok(!api.render(p,now).cbs.includes('<strong>0 %</strong>'));
 assert.match(api.render(p,now,true).hero,/Pinned snapshot/);assert.match(api.render(p,now,true).cbs,/<strong>0 %<\/strong>/);
 m.quality.acquired_at=stamp;m.quality.observation_date='2026-09-30';assert.equal(api.fresh(m,p,now),false);
});
test('source strings are escaped and unsafe evidence paths never link',()=>{
 const p=packet();p.central_banks[0].cb='<img onerror=alert(1)>';p.measurements.RATE.selected.original.key='javascript:alert(1)';
 const out=api.render(p,now);assert.ok(!out.cbs.includes('<img'));assert.ok(!out.cbs.includes('href="javascript:'));
 assert.equal(api.path('data/evidence/../private'),null);assert.throws(()=>api.render(p,now,false,'3'));
 p.calls_eligible=true;assert.throws(()=>api.render(p,now));
});
test('settlement scopes require consistency and remain explicitly unverified-provider context',()=>{
 const p=packet();p.pd_settlement_fails={source_generated_at:stamp,label:'Including TIPS',ftd_bn:100,ftr_bn:90,combined_bn:190,as_of:'2026-09-09',quality:{status:'fresh'},reconciliation:{consistent:false},
  ust_ex_tips:{label:'Excluding TIPS',ftd_bn:86,ftr_bn:87,combined_bn:173,as_of:'2026-09-09',quality:{status:'fresh'},reconciliation:{consistent:true}}};
 const html=api.render(p,now).fails;assert.match(html,/not been verified/);assert.ok(!html.includes('<td>190</td>'));assert.match(html,/<td>173<\/td>/);
});
test('immutable snapshot validates both byte hashes and refuses corruption',async()=>{
 const output=new TextEncoder().encode(JSON.stringify(packet())),hash=await api.sha(output);
 const m={contract:'cb-replay.v1',generated_at:stamp,output_sha256:hash,output:{key:api.PREFIX+'outputs/'+hash+'.json',sha256:hash,bytes:output.length},compilers:{}};
 const raw=new TextEncoder().encode(JSON.stringify(m)),id=await api.sha(raw),files={['/'+api.PREFIX+'runs/'+id+'.json']:raw,['/'+m.output.key]:output};
 const fetcher=async key=>new Response(files[key],{status:files[key]?200:404});assert.equal((await api.loadSnapshot(id,fetcher)).contract,api.CONTRACT);
 files['/'+m.output.key]=new TextEncoder().encode('{}');await assert.rejects(api.loadSnapshot(id,fetcher),/output differs/);
 await assert.rejects(api.loadSnapshot('../latest',fetcher),/Invalid snapshot/);
 await assert.rejects(api.loadSnapshot('a'.repeat(64),async()=>new Response(new Uint8Array(256*1024+1))),/size bound/);
});
test('page has source evidence and no unqualified page-AI narrative route',()=>{
 const html=fs.readFileSync(path.join(__dirname,'../cb-injection.html'),'utf8');assert.ok(!html.includes('jh-page-ai'));assert.match(html,/jh-cb-research.js/);assert.match(html,/id="fails"/);
 const js=fs.readFileSync(path.join(__dirname,'../jh-cb-page.js'),'utf8');assert.match(js,/setInterval\(refresh,60000\)/);
});
