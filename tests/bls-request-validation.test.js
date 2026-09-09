const test=require('node:test'),assert=require('node:assert/strict'),https=require('https');
const {handler}=require('../aws/lambdas/bls-employment-api-v2/source/index.js');
test('invalid BLS requests return 400 without provider calls or loops',async()=>{
 const original=https.request;let calls=0;https.request=()=>{calls++;throw new Error('must not call')};
 try {
  for(const body of ['{',JSON.stringify({startYear:'1900',endYear:2026}),JSON.stringify({startYear:2026,endYear:2000}),JSON.stringify({startYear:1800}),JSON.stringify({mode:'unexpected'})]) {
   const result=await handler({body});assert.equal(result.statusCode,400);assert.equal(JSON.parse(result.body).error,'invalid_request');
  }
  assert.equal(calls,0);
 } finally {https.request=original;}
});
const vm=require('node:vm'),fs=require('node:fs');
const scope={exports:{},require,console,process,Buffer,setTimeout};
vm.runInNewContext(fs.readFileSync(require.resolve('../aws/lambdas/bls-employment-api-v2/source/index.js'),'utf8'),scope);
test('BLS annual averages remain available without invalid thirteenth-month dates',()=>{
 const rows=scope.processHistoricalData([{seriesID:'LNS14000000',data:[{year:'2026',period:'M13',value:'5'},{year:'2026',period:'M01',value:'0'},{year:'2026',period:'M02',value:'-'}]}]);
 const s=rows.LNS14000000;assert.equal(s.provider_observations.length,3);assert.equal(s.annual_averages.length,1);assert.equal(s.data.length,2);assert.equal(s.data[0].value,0);assert.equal(s.summary.latest,null);assert.equal(s.summary.latest_observation_date,'2026-02-01');assert.equal(scope.parseBlsDate('M13','2026'),null);
 assert.ok(scope.performCrisisAnalysis(rows).status.includes('Insufficient'));
});
test('BLS rates and payroll units match their series types',()=>{
 for(const id of ['LNS13327716','LASST060000000000003','LNS14000006']) assert.equal(scope.getSeriesUnits(id),'Percent');
 assert.equal(scope.getSeriesUnits('CES0500000002'),'Hours');assert.equal(scope.getSeriesUnits('CES0500000003'),'Dollars');assert.equal(scope.getSeriesUnits('CES0000000001'),'Thousands');
});
test('BLS trends require six contiguous monthly observations',()=>{
 const rows=Array.from({length:6},(_,i)=>({year:2026,period:'M'+String(i+1).padStart(2,'0'),value:i}));
 assert.equal(scope.calculateTrend(rows),'increasing');rows[5].value=null;assert.equal(scope.calculateTrend(rows),'insufficient data');rows[5].value=5;rows[5].period='M08';assert.equal(scope.calculateTrend(rows),'insufficient contiguous monthly data');
});
test('BLS missing chunks are unavailable and merged history updates the latest observation',async()=>{
 const fetch=scope.fetchBLSData,sleep=scope.sleep;scope.sleep=async()=>{};
 try {
  scope.fetchBLSData=async()=>({});
  const empty=JSON.parse((await scope.exports.handler({body:JSON.stringify({startYear:2026,endYear:2026})})).body);
  assert.equal(empty.status,'UNAVAILABLE');assert.equal(empty.success,false);assert.equal(empty.chunk_coverage[0].missing_series.length,4);
  scope.fetchBLSData=async(ids,key,start,end)=>scope.processHistoricalData(ids.map(id=>({seriesID:id,data:[{year:String(end),period:'M12',value:end===2019?'4':'5'}]})));
  const combined=JSON.parse((await scope.exports.handler({body:JSON.stringify({startYear:2000,endYear:2026})})).body);
  assert.equal(combined.live_data.LNS14000000.data.length,2);assert.equal(combined.live_data.LNS14000000.summary.latest,5);assert.equal(combined.live_data.LNS14000000.summary.latest_observation_date,'2026-12-01');
 } finally {scope.fetchBLSData=fetch;scope.sleep=sleep;}
});
