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
