const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
function page(){
 const html=fs.readFileSync('composite/index.html','utf8');
 const code=Array.from(html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g),m=>m[1]).find(s=>s.includes('const SIDECAR_URL')).replace(/\nload\(\);\s*$/,'');
 const nodes=new Map(),document={getElementById(id){if(!nodes.has(id))nodes.set(id,{});return nodes.get(id)}};
 const ctx={document,window:{}};vm.createContext(ctx);vm.runInContext(code,ctx);return {ctx,nodes};
}
test('missing dimensions and history stay unavailable, not zero or green',()=>{
 const {ctx,nodes}=page();ctx.render({modules:[],dimensions:{},composite_score:null});
 assert.equal(nodes.get('composite-score').textContent,'—');assert.ok(nodes.get('dims-grid').innerHTML.includes('>—</div>'));
 ctx.renderHistory({snapshots:[{composite_score:null,dim_scores:{vol:null}}]});
 const history=nodes.get('history-bar').innerHTML;
 assert.ok(history.includes('background:var(--border)'));assert.ok(history.includes('title="unavailable"'));
 assert.ok(!history.includes('rgba(0,230,118'));
});
test('CISS context renders no directional vote and escapes feed text',()=>{
 const {ctx,nodes}=page();ctx.render({modules:[{missing:false,vote_eligible:false,polarity:null,label:'<img src=x>',signal:'<script>bad</script>',page:'javascript:bad',dimension:'vol'}]});
 const html=nodes.get('mods-grid').innerHTML;
 assert.ok(html.includes('SOURCE CONTEXT'));assert.ok(html.includes('no directional vote'));assert.ok(html.includes('href="#"'));
 assert.ok(html.includes('&lt;script&gt;'));assert.ok(!html.includes('<img'));assert.ok(!html.includes('+null'));
});
