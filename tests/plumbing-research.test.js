const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs');
const ui=require('../jh-plumbing-research.js'),sha=raw=>crypto.createHash('sha256').update(raw).digest('hex');
const now=Date.parse('2026-09-20T11:00:00Z');
function row(id='SOFR'){return {id,series_id:id,label:'Native source',value_decimal:'0',last_observed_value:'0',unit:'Percent',frequency:'D',observation_date:'2026-09-17',source_row_index:0,
 acquired_at:'2026-09-20T10:00:00Z',source_generated_at:'2026-09-20T10:30:00Z',quality:{status:'fresh',maximum_acquisition_age_hours:26,maximum_observation_age_days:10}};}
function packet(){return {contract:'plumbing-research.v1',generated_at:'2026-09-20T11:00:00Z',calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_eligible:false,
 composite:{composite_stress_score:null},decision:{verb:'WAIT'},measurements:Object.fromEntries(['SOFR',...Array.from({length:52},(_,i)=>'S'+i)].map(id=>[id,row(id)])),quality:{expected_measurements:62},groups:{funding_rates:['SOFR']}};}
function retain(p){const raw=Buffer.from(JSON.stringify(p)),digest=sha(raw),output={key:'data/plumbing-research/outputs/'+digest+'.json',sha256:digest,bytes:raw.length};
 const manifest={contract:'plumbing-replay.v1',generated_at:p.generated_at,output_sha256:digest,output},run=Buffer.from(JSON.stringify(manifest)),key='data/plumbing-research/runs/'+sha(run)+'.json';
 return {packet:{...p,replay:{manifest_key:key,output_sha256:digest}},objects:{[key]:run,[output.key]:raw}};}
function fetcher(objects){return async url=>{const raw=objects[url.slice(1)];return {ok:!!raw,status:raw?200:404,arrayBuffer:async()=>new Uint8Array(raw).buffer};};}
function history(rows){const doc={contract:'plumbing-history.v1',series_id:'SOFR',unit:'Percent',frequency:'D',columns:['date','value_decimal','source_row_index'],rows},raw=Buffer.from(JSON.stringify(doc)),digest=sha(raw),key='data/plumbing-research/histories/'+digest+'.json';
 return {row:{...row(),history:{key,sha256:digest,bytes:raw.length,observations:rows.length,first:rows[0][0],last:rows.at(-1)[0]}},objects:{[key]:raw}};}
test('legacy scores and producer self-qualification cannot enter the page',()=>{
 assert.match(ui.render({composite:{composite_stress_score:99}},now),/research unavailable/);
 const p=packet();p.calls_eligible=true;assert.match(ui.render(p,now),/research unavailable/);
});
test('current zero is visible and acquisition expiry withholds it before packet age changes',()=>{
 const r=row();assert.equal(ui.rowCurrent(r,now),true);assert.equal(ui.rowCurrent(r,now+26*3600000),false);
 assert.equal(ui.rowCurrent({...r,acquired_at:'2027-01-01T00:00:00Z'},now),false);
 assert.match(ui.render(packet(),now),/>0<small>Percent/);assert.match(ui.render(packet(),now+26*3600000),/>Withheld<small>Percent/);
});
test('comparison lacks a bp value when a native leg is missing',()=>{
 const p=packet();p.comparisons={test:{series_ids:['SOFR','S0'],label:'No basis',value_decimal:null,source_latest_dates:{SOFR:'2026-09-17',S0:'2026-08-01'},source_rows:{SOFR:0,S0:null},reason:'Monthly rate is not a daily basis.'}};
 const html=ui.render(p,now);assert.match(html,/Monthly rate is not a daily basis/);assert.doesNotMatch(html,/Unavailable <small>bp/);
});
test('native output and run hashes bind the current packet',async()=>{
 const f=retain(packet());assert.equal(await ui.verifyPacket(f.packet,fetcher(f.objects)),f.packet);
 const bad=structuredClone(f.packet);bad.measurements.SOFR.value_decimal='99';await assert.rejects(ui.verifyPacket(bad,fetcher(f.objects)),/Current packet differs/);
 f.objects[Object.keys(f.objects)[1]]=Buffer.from('{}');await assert.rejects(ui.verifyPacket(f.packet,fetcher(f.objects)),/Output bytes differ/);
});
test('history is bound to hash, identity, decimals, endpoints and original rows',async()=>{
 const h=history([['2026-09-16','1.23',1],['2026-09-17',null,0]]);const rows=await ui.verifyHistory(h.row,fetcher(h.objects));
 assert.equal(rows[0].value_decimal,'1.23');assert.equal(rows[1].value_decimal,null);
 h.row.unit='Index';await assert.rejects(ui.verifyHistory(h.row,fetcher(h.objects)),/History identity/);
 const duplicate=history([['2026-09-17','1',0],['2026-09-17','2',1]]);await assert.rejects(ui.verifyHistory(duplicate.row,fetcher(duplicate.objects)),/History record/);
});
test('evidence navigation is bounded and source labels cannot inject markup',()=>{
 for(const key of ['data/brain.json','data/portfolio.json','data/plumbing-research/../x.json','javascript:alert(1)'])assert.equal(ui.safe(key),false);
 const p=packet();p.measurements.SOFR.label='<img onerror=x>';const html=ui.render(p,now);assert.doesNotMatch(html,/<img/);assert.match(html,/&lt;img/);
});
test('native history breaks across missing rows and preserves full download',()=>{
 const r=row();const html=ui.historyHTML(r,[{date:'2026-09-15',value_decimal:'1',source_row_index:2},{date:'2026-09-16',value_decimal:null,source_row_index:1},{date:'2026-09-17',value_decimal:'2',source_row_index:0}],63);
 assert.match(html,/Missing/);assert.match(html,/current provider vintage/i);assert.doesNotMatch(html,/NaN|undefined/);
});
test('page uses native modules and current packet without legacy scoring script',()=>{
 const page=fs.readFileSync(require('node:path').join(__dirname,'../crisis.html'),'utf8');
 assert.match(page,/jh-plumbing-research.js/);assert.match(page,/data\/crisis-plumbing.json/);assert.doesNotMatch(page,/computeComposite|score-circle|Synthetic 3M/);
});
