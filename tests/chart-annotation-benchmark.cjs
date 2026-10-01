// Offline chrome-only cost comparison. Run: node tests/chart-annotation-benchmark.cjs
const {performance}=require('node:perf_hooks');
const {source,chrome,engine,rows,context}=require('./helpers/chart-annotation-harness.cjs');
function sample(fn){for(let i=0;i<100;i++)fn();const times=[];for(let s=0;s<9;s++){const start=performance.now();for(let i=0;i<300;i++)fn();times.push((performance.now()-start)/300);}times.sort((a,b)=>a-b);return {median_ms:+times[4].toFixed(5),min_ms:+times[0].toFixed(5),max_ms:+times[8].toFixed(5),batches:9,iterations_per_batch:300};}
const report={node:process.version,scope:'Ordinary Node, whole chrome source, DOM contract stub. Excludes browser DOM parsing/layout/paint. Synthetic bars test workload size only; no market data, providers or predictive validation.',runs:[]};
for(const n of [1000,10000,11536]){
  const d=rows(n),ctx=context(d,[{id:'voltape',n:'Volume Tape',on:true},{id:'wyckoff',n:'Wyckoff',on:true},{id:'vsa',n:'VSA',on:true}]),versions={};
  const order=n===10000?['current','predecessor']:['predecessor','current'];
  for(const version of order){const w=engine();chrome(version==='current'?undefined:source('tests/fixtures/chart-annotation/indux-predecessor.js.txt'),w);
    versions[version]={legend:sample(()=>w.jhInduxLegend(ctx)),volume_help:sample(()=>w.jhInduxHelp('voltape')),classification_calls:w.__classifyCalls,distribution_calls:w.__distributionCalls};}
  report.runs.push({bars:n,versions});
}
console.log(JSON.stringify(report,null,2));
