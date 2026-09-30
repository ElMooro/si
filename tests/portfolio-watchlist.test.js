const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
const path='tests/fixtures/pre-watchlist-completeness/complete-synthetic.json';
const prior=JSON.parse(fs.readFileSync(path,'utf8'));
function context(snapshot=prior.complete.input){
 const html=fs.readFileSync('portfolio/index.html','utf8'),elements=new Map();
 const document={addEventListener(){},getElementById(id){if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',disabled:false,style:{}});return elements.get(id);},querySelector(){return {style:{}};}};
 const scripts=[...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n');
 const scope=vm.createContext({document,window:{},console,JHPortfolioRisk:require('../jh-portfolio-risk-contract.js'),Date,AbortController,setInterval(){},fetch(){throw Error('No network in watchlist test');}});
 vm.runInContext(scripts,scope);scope.supplied=structuredClone(snapshot);vm.runInContext('snapshot=supplied;renderWatchlist();',scope);
 return {scope,elements,render(value){scope.supplied=structuredClone(value);vm.runInContext('snapshot=supplied;renderWatchlist();',scope);},page(delta){scope.delta=delta;vm.runInContext('changeWatchPage(delta);',scope);},body(){return elements.get('watch-body').innerHTML;},status(){return elements.get('watch-page-status').textContent;}};
}
function symbols(ctx){return [...ctx.body().matchAll(/href="\/stock\/\?symbol=([^"]+)"/g)].map(m=>decodeURIComponent(m[1]));}

test('whole predecessor reproduction stays byte-bound and inert',()=>{
 const evidence=JSON.parse(fs.readFileSync('docs/audit/2026-09-30/watchlist-source-gap.json','utf8'));
 for(const row of [evidence.source,evidence.complete_synthetic]){
  const raw=fs.readFileSync(row.fixture);assert.equal(raw.length,row.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),row.sha256);
 }
 assert.equal(prior.complete.rendered_rows,30);assert.equal(prior.complete.display_count,'105 entries');assert.equal(prior.complete.last_supplied_row_visible,false);
});

test('all 105 original supplied records are reachable once and in source order',()=>{
 const ctx=context(),seen=[];
 for(let page=0;page<5;page++){
  assert.equal(ctx.elements.get('watch-prev').disabled,page===0);
  assert.equal(ctx.elements.get('watch-next').disabled,page===4);
  const rows=symbols(ctx);assert.equal(rows.length,page===4?5:25);seen.push(...rows);
  assert.match(ctx.status(),new RegExp('page '+(page+1)+' of 5'));
  if(page<4)ctx.page(1);
 }
 assert.deepEqual(seen,prior.complete.input.watchlist.map(w=>w.symbol));
 assert.equal(JSON.stringify(ctx.scope.supplied),JSON.stringify(prior.complete.input));
 ctx.page(1);assert.equal(symbols(ctx).at(-1),'S104');
 for(let i=0;i<5;i++)ctx.page(-1);
 assert.equal(symbols(ctx)[0],'S000');assert.equal(ctx.elements.get('watch-prev').disabled,true);
});

test('unknown watchlist shapes stay unavailable rather than measured empty',()=>{
 for(const input of [null,{},...['UNKNOWN',false,true,0,null,{}].map(watchlist=>({watchlist}))]){
  const ctx=context(input);assert.match(ctx.status(),/unavailable/);
  assert.equal(ctx.elements.get('watch-count').textContent,'Watchlist count unavailable');
  assert.match(ctx.body(),/Missing or malformed/);assert.doesNotMatch(ctx.body(),/Watchlist empty/);
  assert.equal(ctx.elements.get('watch-prev').disabled,true);assert.equal(ctx.elements.get('watch-next').disabled,true);
 }
});

test('only explicit empty arrays show a genuine empty watchlist',()=>{
 const ctx=context({watchlist:[]});assert.equal(ctx.elements.get('watch-count').textContent,'0 entries');
 assert.equal(ctx.status(),'No watchlist entries in this snapshot.');assert.match(ctx.body(),/Watchlist empty/);
 assert.equal(ctx.elements.get('watch-prev').disabled,true);assert.equal(ctx.elements.get('watch-next').disabled,true);
});

test('malformed records preserve their complete absolute position across pages',()=>{
 const snapshot=structuredClone(prior.complete.input);snapshot.watchlist[25]=null;snapshot.watchlist[104]={symbol:true};
 const ctx=context(snapshot);assert.equal(ctx.elements.get('watch-count').textContent,'105 supplied records · 2 invalid');
 ctx.page(1);assert.match(ctx.body(),/Watchlist record 26 unavailable/);assert.equal((ctx.body().match(/<tr>/g)||[]).length,25);
 for(let i=0;i<3;i++)ctx.page(1);
 assert.match(ctx.body(),/Watchlist record 105 unavailable/);assert.equal((ctx.body().match(/<tr>/g)||[]).length,5);
 assert.equal(JSON.stringify(ctx.scope.supplied),JSON.stringify(snapshot));
});

test('duplicates and unknown original fields are not deduplicated or rewritten',()=>{
 const row={...prior.complete.input.watchlist[0],unknown:{nested:[false,0,null,'日本']}};
 const snapshot={watchlist:[row,{...row,name:'Second occurrence'}]};const ctx=context(snapshot);
 assert.deepEqual(symbols(ctx),['S000','S000']);assert.equal(JSON.stringify(ctx.scope.supplied),JSON.stringify(snapshot));
});

test('refresh preserves the selected page and clamps it when the complete list shrinks',()=>{
 const ctx=context();for(let i=0;i<4;i++)ctx.page(1);
 ctx.render(prior.complete.input);assert.match(ctx.status(),/page 5 of 5/);
 ctx.render({watchlist:prior.complete.input.watchlist.slice(0,31)});
 assert.equal(ctx.status(),'Showing 26–31 of 31 supplied records · page 2 of 2');
 assert.equal(symbols(ctx).length,6);assert.equal(ctx.elements.get('watch-next').disabled,true);
});

test('load failure clears old rows and navigation recovers with new complete data',()=>{
 const ctx=context();ctx.page(1);ctx.render(null);
 assert.doesNotMatch(ctx.body(),/S025/);assert.equal(ctx.elements.get('watch-next').disabled,true);
 ctx.render(prior.complete.input);assert.match(ctx.status(),/Showing 1–25/);assert.equal(ctx.elements.get('watch-next').disabled,false);
});

test('invalid page deltas cannot create holes or reorder the list',()=>{
 const ctx=context();const before=ctx.body();
 for(const delta of [null,true,'1',0,.5,2,Infinity,NaN]){ctx.page(delta);assert.equal(ctx.body(),before);}
});

test('measured zero is retained while an absent source is not relabeled manual',()=>{
 const ctx=context({watchlist:[prior.complete.input.watchlist[0]]});
 assert.match(ctx.body(),/\$0/);assert.match(ctx.body(),/>0</);assert.doesNotMatch(ctx.body(),/MANUAL/);
});

test('names remain available in full and arbitrary metadata is escaped',()=>{
 const name='Long invented issuer name with "quotes" <img src=x onerror=bad> 日本';
 const ctx=context({watchlist:[{...prior.complete.input.watchlist[0],name,sector:'<script>bad</script>',source:'<svg onload=bad>'}]});
 assert.match(ctx.body(),/title="Long invented issuer name with &quot;quotes&quot; &lt;img/);
 assert.doesNotMatch(ctx.body(),/<img|<script|<svg/);assert.match(ctx.body(),/日本/);
});

test('pagination uses keyboard-accessible buttons and an announced complete range',()=>{
 const html=fs.readFileSync('portfolio/index.html','utf8');
 for(const id of ['watch-prev','watch-next'])assert.match(html,new RegExp('<button type="button" id="'+id+'" aria-controls="watch-table"'));
 assert.match(html,/id="watch-page-status" role="status" aria-live="polite"/);
 assert.match(html,/\.watch-controls button:focus-visible/);
});
