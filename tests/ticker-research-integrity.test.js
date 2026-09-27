const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../ticker.html'),'utf8');
function setup(fetcher=async()=>({ok:true,json:async()=>({tracker:{upcoming:[],recent:[]}})})){
 const nodes=new Map();for(const match of source.matchAll(/id="([^"]+)"/g)){let text='',html='';nodes.set(match[1],{value:'',get textContent(){return text;},set textContent(v){text=v;html='';},get innerHTML(){return html;},set innerHTML(v){html=v;text='';},addEventListener(){}});}
 const calls=[],window={JHShortInterestResearch:require('../jh-short-interest-research.js')},document={getElementById:id=>nodes.get(id)},location={href:'https://justhodl.ai/ticker.html'},history={replaceState(_,__,url){location.href=String(url);}};
 const ctx={document,window,location,history,URL,console,fetch:async(...args)=>{calls.push(args);return fetcher(...args);}};vm.createContext(ctx);
 const scripts=[...source.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).filter(s=>s.trim());assert.equal(scripts.length,1);vm.runInContext(scripts[0],ctx);return{ctx,nodes,calls};
}
test('ticker rejects injected or overlong symbols before public reads and escapes provider text',async()=>{
 const s=setup(async()=>({ok:true,json:async()=>({tracker:{upcoming:[{symbol:'ABC',report_date:'<img src=x onerror=alert(1)>'}]}})}));
 for(const symbol of ['<img src=x onerror=alert(1)>','A'.repeat(21),''])await s.ctx.loadTicker(symbol);assert.equal(s.calls.length,0);
 await s.ctx.loadTicker('abc');assert.match(s.nodes.get('results').innerHTML,/&lt;img/);assert.ok(!s.nodes.get('results').innerHTML.includes('<img'));assert.equal(s.nodes.get('symbolHeader').textContent,'ABC');assert.match(s.nodes.get('results').innerHTML,/descriptive research/);assert.equal(s.calls.length,1);assert.equal(s.calls[0][0],'/data/earnings-tracker.json?exact=1&nogen=1');assert.equal(s.calls[0][1].redirect,'error');
});
test('older ticker response cannot repaint the current symbol',async()=>{
 const pending=[];const s=setup(()=>new Promise(resolve=>pending.push(resolve)));
 const a=s.ctx.loadTicker('AAA'),b=s.ctx.loadTicker('BBB');pending[1]({ok:true,json:async()=>({tracker:{upcoming:[{symbol:'BBB',report_date:'2026-10-02'}]}})});await b;pending[0]({ok:true,json:async()=>({tracker:{upcoming:[{symbol:'AAA',report_date:'2026-10-01'}]}})});await a;
 assert.equal(s.nodes.get('symbolHeader').textContent,'BBB');assert.match(s.nodes.get('results').innerHTML,/2026-10-02/);assert.ok(!s.nodes.get('results').innerHTML.includes('2026-10-01'));
});
test('failed provider and malformed populations do not crash or invent an earnings schedule',async()=>{
 const s=setup(async()=>({ok:false}));await s.ctx.loadTicker('ABC');assert.match(s.nodes.get('results').innerHTML,/source unavailable/);
 const m=setup(async()=>({ok:true,json:async()=>({tracker:{upcoming:{fake:1},recent:[null]}})}));await m.ctx.loadTicker('ABC');assert.match(m.nodes.get('results').innerHTML,/No earnings row.*received packet/);assert.match(m.nodes.get('results').innerHTML,/settlement positions/);
 const n=setup();delete n.ctx.window.JHShortInterestResearch;await n.ctx.loadTicker('ABC');assert.match(n.nodes.get('results').innerHTML,/qualification is unavailable/);assert.equal(await n.ctx.fetchJson('portfolio/private.json'),null);assert.equal(n.calls.length,1);
});
test('both full ticker predecessors and concurrent inventory additions are preserved',()=>{
 for(const name of ['pre-ticker-research-repair.html.txt','pre-ticker-concurrent-replacement.html.txt']){const text=fs.readFileSync(path.join(__dirname,'fixtures',name),'utf8');assert.match(text,/<\/html>/);assert.ok(text.length>6000);}
 for(const script of ['private-artifacts.js?v=20260909','jh-short-interest-research.js','jh-inventory-v2.js','jh-inventory-v2-mount.js','jh-book-fuse.js'])assert.ok(source.includes('src="/'+script+'"'));
 assert.ok(!source.includes('PLACEHOLDER'));assert.ok(!source.includes('s3.amazonaws.com'));assert.match(source,/maxlength="20"/);
});
