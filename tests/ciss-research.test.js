'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {state,pointsPath,csvRows,verifiedRow,path}=require('../jh-ciss-research.js');
const {gzipSync}=require('node:zlib'),{createHash,webcrypto}=require('node:crypto');
if(!globalThis.crypto)globalThis.crypto=webcrypto;
const now=Date.parse('2026-09-19T00:00:00Z');
const packet={contract:'ciss-research.v1',generated_at:'2026-09-18T22:00:00Z'};
function row(){return {quality:{status:'fresh',maximum_observation_age_days:14,maximum_acquisition_age_seconds:72*3600},observation_period_end:'2026-09-15',acquired_at:'2026-09-18T22:00:00Z',latest:0};}
test('CISS reevaluates both clocks, zero is valid and missing is not replaced',()=>{
  assert.equal(state(row(),packet,now),'fresh');
  assert.equal(state({...row(),latest:null},packet,now),'missing');
  assert.equal(state({...row(),observation_period_end:'2026-09-01'},packet,now),'stale');
  assert.equal(state({...row(),acquired_at:'2026-09-14T00:00:00Z'},packet,now),'stale');
  assert.equal(state(row(),{...packet,generated_at:'2026-09-20T00:00:00Z'},now),'invalid');
  assert.equal(state({...row(),quality:{...row().quality,status:'incomplete'}},packet,now),'incomplete');
});
test('Date chart leaves missing breaks and places points by elapsed time',()=>{
  const got=pointsPath([['2026-01-01',0],['2026-01-02',1],['2026-01-05',null],['2026-01-11',.5]],100,100);
  assert.equal(got.d,'M0.00,100.00 L10.00,0.00  M100.00,50.00');
  assert.doesNotMatch(pointsPath([['2026-01-01',0]]).d,/NaN|Infinity/);
  assert.equal(pointsPath([['2026-01-01',null]]).d,'');
});
test('ECB CSV quoted commas, newline fields and source-row indexing survive',()=>{
  const rows=csvRows('KEY,TITLE,OBS_VALUE\r\nCISS.X,"a, b\nsecond line",0\r\nCISS.Y,"say ""yes""",\r\n');
  assert.equal(rows[0].TITLE,'a, b\nsecond line');assert.equal(rows[1].TITLE,'say "yes"');assert.equal(rows[1].OBS_VALUE,'');
  assert.throws(()=>csvRows('KEY,TITLE\nX,"cut off'),/Incomplete/);
  assert.equal(path('data/../private.json'),null);assert.equal(path('https://example.com'),null);
});
test('Browser verifies original bytes and exact selected ECB observation, rejects corruption',async()=>{
  const key='CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX',source='https://data-api.ecb.europa.eu/service/data/CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX';
  const raw=Buffer.from('KEY,TIME_PERIOD,OBS_VALUE,OBS_STATUS,UNIT,UNIT_MULT\n'+key+',2026-09-15,0.03,A,PURE_NUMB,0\n');
  const hash=b=>createHash('sha256').update(b).digest('hex'),sha=hash(raw),evidence={contract:'source-evidence.v1',provider:'ecb',captured:true,source_url:source,sha256:sha,bytes:raw.length,key:'data/evidence/ecb/'+hash(source)+'/'+sha+'.bin.gz'};
  const r={key,latest_date:'2026-09-15',latest:.03,latest_decimal:'0.03',source_row:0,observation_status:'A',evidence};
  const fetcher=async()=>new Response(gzipSync(raw));
  assert.equal((await verifiedRow(r,fetcher)).original.OBS_VALUE,'0.03');
  await assert.rejects(verifiedRow({...r,latest:.04},fetcher),/value differs/);
  await assert.rejects(verifiedRow(r,async()=>new Response(gzipSync(Buffer.from('wrong')))),/hash differs/);
});
