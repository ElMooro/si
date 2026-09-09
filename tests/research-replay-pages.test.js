const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const helper=require('../jh-research-inspection.js');
function row(source,day){return {source_key:source,capture_date:day,key:'data/snapshots/'+source.replaceAll('/','_').replace('.json','')+'-'+day+'.json',immutable:false,point_in_time_certified:false};}
function code(file,start,end){const s=fs.readFileSync(require('node:path').join(__dirname,'..',file),'utf8');return s.slice(s.indexOf(start),s.indexOf(end,s.indexOf(start)));}
test('archive traversal uses complete listed dates and exact reviewed source paths',()=>{
 const a=row('data/event-study.json','2026-09-01'),b=row('data/yield-curve.json','2026-09-09');
 const result=helper.archiveRows({schema_version:'daily-snapshot-index.v1',complete:true,snapshots:[b,a,a]});
 assert.deepEqual(result.map(x=>x.capture_date),['2026-09-01','2026-09-09']);
 for(const bad of [{...a,key:'data/brain.json'},{...a,source_key:'portfolio/snapshot.json'},{...a,immutable:true},{...a,capture_date:'2026-02-30'}])assert.throws(()=>helper.archiveRows({schema_version:'daily-snapshot-index.v1',complete:true,snapshots:[bad]}));
 assert.throws(()=>helper.archiveRows({schema_version:'daily-snapshot-index.v1',complete:false,snapshots:[a]}));
});
test('full returned report reaches typed inspector without dropping rows, zero, null or nested metadata',()=>{
 const calls=[];class Node{constructor(){this.children=[];}append(...x){this.children.push(...x);}replaceChildren(){this.children=[];}addEventListener(name,fn){this.listener=fn;}}
 global.document={createElement:()=>new Node()};global.JHDataInspector={inspect:(...args)=>calls.push(args)};
 const root=new Node(),doc={rows:Array.from({length:401},(_,i)=>({i,zero:0,missing:null,nested:{value:i}}))};
 helper.show(root,[{label:'TEST',source:'fixture',document:doc}]);const details=root.children[2];details.open=true;details.listener();
 assert.equal(calls[0][1].document,doc);assert.equal(calls[0][1].document.rows.length,401);assert.equal(calls[0][1].document.rows[400].missing,null);
 delete global.document;delete global.JHDataInspector;
});
test('actual compare fetch rejects future cache and preserves complete fresh Lambda response',async()=>{
 const calls=[],good={ticker:'SPY',generated_at:new Date().toISOString(),rows:[0,null,{all:'fields'}]},future={ticker:'SPY',generated_at:'2999-01-01T00:00:00Z'};
 const c=vm.createContext({JHResearchInspection:helper,CDN_BASE:'https://fixture.invalid/equity-research',LAMBDA_URL:'https://engine.invalid/',Date,AbortController,setTimeout,clearTimeout,
  fetch:async url=>{calls.push(url);return Response.json(calls.length===1?future:good);}});
 vm.runInContext(code('compare.html','async function fetchOneTicker','async function fetchAndRender'),c);const got=await c.fetchOneTicker('SPY');
 assert.equal(calls.length,2);assert.deepEqual(JSON.parse(JSON.stringify(got.document)),good);
 assert.throws(()=>helper.report('SPY',{ticker:'QQQ'}));
});
test('actual replay fetches every indexed source, retains unsupported summary fields and ignores stale date requests',async()=>{
 const fields={panels:{},'complete-replay-data':{replaceChildren(){}}},captures=[],pending={};
 const c=vm.createContext({ARCHIVE_ROWS:[row('data/event-study.json','2026-09-01'),row('data/ab-test-results.json','2026-09-01'),row('data/event-study.json','2026-09-09')],replayRevision:0,
  document:{getElementById:id=>fields[id]},esc:String,num:String,JHResearchInspection:{show:(root,records)=>captures.push(records)},gj:key=>new Promise(resolve=>{pending[key]=resolve;})});
 vm.runInContext(code('signal-replay.html','async function loadDate','async function init'),c);
 const older=c.loadDate('2026-09-01'),newer=c.loadDate('2026-09-09');
 pending[row('data/event-study.json','2026-09-09').key]({nested:{zero:0,missing:null}});await newer;
 pending[row('data/event-study.json','2026-09-01').key]({old:true});pending[row('data/ab-test-results.json','2026-09-01').key]({old:true});await older;
 assert.equal(captures.length,1);assert.equal(captures[0][0].document.nested.zero,0);assert.match(fields.panels.innerHTML,/2026-09-09/);
 assert.equal(Object.keys(pending).length,3);
});
