const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(require('node:path').join(__dirname,'../jh-right-rail.js'),'utf8');
function render(data){
 let html='';const element={setAttribute(){},addEventListener(){}};
 const wrap={classList:{add(){},remove(){},contains(){return false;}},querySelector(){return element;},set innerHTML(value){html=value;}};
 const document={createElement(){return wrap;},body:{appendChild(){}},addEventListener(){}};
 vm.runInNewContext(source,{window:{__jhRail:data},document,localStorage:{getItem(){return null;}}});return html;
}
test('cached pressure and obsolete DEFCON claims cannot re-enter a repaired desk',()=>{
 const html=render({research:{theme:'STRESS',pressure:99,verdict:'CRISIS',div:'crisis-composite reads 4',href:'javascript:alert(1)'},feeds:[]});
 assert.match(html,/Open panels research/);assert.doesNotMatch(html,/STRESS|CRISIS|reads 4|javascript:|99p/);
});
test('legacy age without a dated snapshot is never called fresh',()=>{
 const html=render({feeds:[{label:'crisis-composite',h:0}]});
 assert.match(html,/capture time unavailable/);assert.match(html,/Live status unverified/);
 assert.doesNotMatch(html,/>fresh<|jhr-ok|jhr-mid|jhr-stale/);
});
test('dated file metadata is distinguished from observation freshness',()=>{
 const html=render({snapshot_at:'2026-09-20T11:00:00Z',feeds:[{label:'Crisis',href:'/data/crisis-composite.json',modified_at:'2026-09-20T10:51:00Z'}]});
 assert.match(html,/File modified 2026-09-20T10:51:00Z/);assert.match(html,/Page snapshot at 2026-09-20T11:00:00Z/);
 assert.match(html,/do not establish observation freshness/);assert.match(html,/href="\/data\/crisis-composite.json"/);
});
test('source references reject script and traversal URLs and escape labels',()=>{
 const html=render({feeds:[{label:'<img onerror=x>',href:'javascript:alert(1)'},{label:'Private',href:'/data/../private.json'}]});
 assert.doesNotMatch(html,/javascript:|href=|<img/);assert.match(html,/&lt;img/);
});
