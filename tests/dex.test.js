const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const api = require('../dex.js');
const inspector = require('../jh-data-inspector.js');
class Element {
  constructor(tag) { this.tagName=tag;this.children=[];this.events={};this.attrs={};this.dataset={};this.style={};this._text='';this.value='';this.className='';this.classList={add:c=>{this.className+=' '+c;},remove:c=>{this.className=this.className.split(' ').filter(x=>x!==c).join(' ');}}; }
  set textContent(v) {this._text=String(v);this.children=[];}
  get textContent() {return this._text+this.children.map(x=>typeof x==='string'?x:x.textContent||'').join('');}
  set innerHTML(_) {throw new Error('Unsafe HTML sink');}
  set outerHTML(_) {throw new Error('Unsafe HTML replacement');}
  append(...nodes) {this.children.push(...nodes);}
  prepend(...nodes) {this.children.unshift(...nodes);}
  replaceChildren(...nodes) {this.children=[...nodes];this._text='';}
  setAttribute(k,v) {this.attrs[k]=v;}
  addEventListener(k,fn) {this.events[k]=fn;}
}
function walk(root) {return [root,...root.children.flatMap(x=>typeof x==='object'?walk(x):[])];}
const html=fs.readFileSync(require.resolve('../dex.html'),'utf8');
const ids=[...html.matchAll(/id="([^"]+)"/g)].map(m=>m[1]);
function fixture(fetch, opts={}) {
  const elements=Object.fromEntries(ids.map(id=>{const n=new Element(id.endsWith('table')?'tbody':'div');n.id=id;return [id,n];}));
  const document={createElement:tag=>new Element(tag),getElementById:id=>elements[id],querySelectorAll:selector=>selector==='.tab-content'?Object.values(elements).filter(n=>n.id.startsWith('tab-')):[]};
  const inspections=[],requests=[];global.document=document;
  const app=api.createApp({document,fetch:async(...args)=>{requests.push(args);return fetch(...args);},now:()=>new Date('2026-09-09T12:00:00Z'),storage:opts.storage||{getItem:()=>null,setItem:()=>{}},setInterval:()=>{},setTimeout:()=>1,clearTimeout:()=>{},inspector:{inspect:(...args)=>{inspections.push(args);inspector.inspect(...args);}},...opts});
  return {app,document,elements,inspections,requests,text:()=>Object.values(elements).map(n=>n.textContent).join('\n')};
}
const response = (data,status=200) => ({ok:status>=200&&status<300,status,json:async()=>data});
function pair(i, changes={}) {return {chainId:'solana',pairAddress:'pair-'+i,baseToken:{symbol:'ROW'+i,name:'Token '+i},quoteToken:{symbol:'SOL'},priceUsd:'0',priceChange:{m5:0,h1:2,h6:6,h24:-1},volume:{m5:0,h1:100,h24:2000},txns:{m5:{buys:'0',sells:'1'},h1:{buys:3,sells:0}},liquidity:{usd:5000},marketCap:0,fdv:null,...changes};}

test('finite inputs preserve zero and reject missing metrics; browser score requires real inputs',()=>{
  for(const v of [null,undefined,'',false,true,NaN,Infinity,'100x',{},[]])assert.equal(api.number(v),null);
  assert.equal(api.number('0'),0);assert.equal(api.fmtP(0),'$0');assert.equal(api.fmt(null),'—');
  assert.equal(api.buyRatio({buys:'2',sells:'3'}),40);assert.equal(api.buyRatio({buys:0,sells:0}),null);assert.equal(api.buyRatio({buys:0}),null);
  assert.equal(api.hotScore(pair(1)),6+9-.5+120+120+.5);
  assert.equal(api.hotScore(pair(1,{volume:{h1:0,h24:0}})),null);
  assert.equal(api.hotScore(pair(1,{priceChange:{h1:0,h6:null,h24:0}})),null);
  assert.equal(api.hotScore(pair(1,{txns:{h1:{buys:-1,sells:1}}})),null);
});

test('search summary caps at 50 while full actual inspector retains every returned field with no second fetch',async()=>{
  const pairs=Array.from({length:62},(_,i)=>pair(i));pairs[61].later_only={zero:0,nil:null,warning:'LAST ROW EVIDENCE'};
  const f=fixture(async()=>response({schemaVersion:'1.0.0',pairs,unknown:{complete:'untouched'}}));f.elements['token-search-input'].value='example';
  await f.app.searchToken();assert.equal(f.requests.length,1);
  assert.match(f.elements['search-results-area'].textContent,/50 shown of 62/);assert.ok(!f.elements['search-results-area'].textContent.includes('ROW61'));
  const receipt=f.app.receipts.get('https://api.dexscreener.com/latest/dex/search?q=example');assert.strictEqual(receipt.payload.response.pairs,pairs);assert.equal(receipt.payload.response.pairs[61].later_only.zero,0);assert.equal(receipt.payload.response.pairs[61].later_only.nil,null);
  f.app.inspectReceipt('https://api.dexscreener.com/latest/dex/search?q=example');assert.equal(f.requests.length,1);
  const search=walk(f.elements['dex-data-inspector']).find(n=>n.tagName==='input');search.value='later_only';search.events.input();assert.match(f.elements['dex-data-inspector'].textContent,/LAST ROW EVIDENCE/);assert.match(f.elements['dex-data-inspector'].textContent,/\/response\/pairs\/61\/later_only\/zero/);
});

test('provider text, query, detail fields and model replies remain text; links and callback arguments stay safe',async()=>{
  const attack='<img src=x onerror=alert(1)><script>throw 1</script>', address="x');global.pwned=true;//";
  const p=pair(1,{pairAddress:address,baseToken:{symbol:attack,name:attack},chainId:'solana',dexId:attack,url:'javascript:alert(1)'});
  const f=fixture(async(url)=>response(url.startsWith('https://api.justhodl.ai')?{response:attack}:{pairs:[p]}));
  f.elements['token-search-input'].value=attack;await f.app.searchToken();assert.ok(f.elements['search-results-area'].textContent.includes(attack));
  const clickable=walk(f.elements['search-results-area']).find(n=>n.tagName==='tr'&&n.events.click);await clickable.events.click();
  assert.ok(f.requests.at(-1)[0].includes(encodeURIComponent(address)));assert.ok(f.elements['detail-content'].textContent.includes(attack));assert.equal(walk(f.elements['detail-content']).filter(n=>['script','img','a'].includes(n.tagName)).length,0);
  f.elements['ai-query-input'].value=attack;await f.app.sendAIQuery();assert.equal(walk(f.elements['ai-messages']).filter(n=>n.tagName==='script'||n.tagName==='img').length,0);assert.ok(f.elements['ai-messages'].textContent.includes(attack));
  f.app.renderAlpha(JSON.stringify([{token:attack,sentiment:'bullish" onclick="alert(1)',signal:'BUY" onclick="bad',reasoning:attack,extra:{provider_field:'retained in full reply'}}]));
  assert.ok(f.elements['alpha-feed'].textContent.includes('retained in full reply'));assert.ok(f.elements['alpha-feed'].textContent.includes(attack));assert.ok(!walk(f.elements['alpha-feed']).some(n=>n.className.includes('onclick')));
  assert.equal(global.pwned,undefined);
});

test('boost/profile/takeover fields cannot inject links, markup, classes or event attributes',async()=>{
  const attack='"><svg onload=alert(1)>', data=[{chainId:attack,tokenAddress:attack,description:attack,icon:'data:image/svg+xml,<svg/>',url:'javascript:alert(1)',claimDate:attack,amount:0,totalAmount:0,links:[{type:'website',label:attack,url:'https://example.com/info'},{type:'twitter',url:'data:text/html,bad'},{label:'unsafe',url:'javascript:alert(1)'}]}];
  const f=fixture(async()=>response(data));await f.app.loadBoosted();await f.app.loadNewListings();
  for(const id of ['boosted-top-table','boosted-latest-table','new-listings-table','cto-table']){assert.ok(f.elements[id].textContent.includes(attack));for(const n of walk(f.elements[id])){assert.ok(!['svg','script'].includes(n.tagName));assert.ok(!n.className.includes('onload'));if(n.tagName==='a'){assert.equal(n.href,'https://example.com/info');assert.equal(n.rel,'noopener noreferrer');}}}
  assert.equal(walk(f.elements['new-listings-table']).filter(n=>n.tagName==='img').length,0);
  for(const bad of ['javascript:alert(1)','data:text/html,a','https://user:pass@example.com/','//example.com','not a url'])assert.equal(api.safeUrl(bad),null);
  assert.equal(api.safeUrl('https://example.com/a?b=1'),'https://example.com/a?b=1');
});

test('scan uses all returned query rows, deduplicates by chain and address, discloses full calculation inputs',async()=>{
  const rows=Array.from({length:42},(_,i)=>pair(i));rows.push(pair(0,{chainId:'ethereum'}));rows[41].later='formerly hidden after per-query cap';
  const f=fixture(async()=>response({pairs:rows}));await f.app.setChain('all');
  const calc=f.app.receipts.get('browser:hot-score').payload;assert.equal(calc.eligible_unique_pairs,43);assert.equal(calc.rows.length,43);assert.equal(calc.rows.filter(p=>p.pairAddress==='pair-0').length,2);assert.equal(calc.rows.find(p=>p.pairAddress==='pair-41').provider_pair.later,rows[41].later);
  assert.match(calc.limitations,/Uncalibrated/);assert.match(f.elements['hot-runners-table'].textContent,/30 shown of 43/);assert.match(f.elements['chain-heatmap'].textContent,/not total chain flow/);
  assert.equal(f.requests.length,12);assert.ok(f.requests.every(r=>r[0].startsWith('https://api.dexscreener.com/')));
});

test('late previous-chain scan cannot overwrite selected-chain data',async()=>{
  const deferred=[];let pending=true;
  const f=fixture(url=>pending?new Promise(resolve=>deferred.push(resolve)):Promise.resolve(response({pairs:[pair(2,{chainId:'ethereum'})]})));
  const first=f.app.loadHotRunners();pending=false;await f.app.setChain('ethereum');
  deferred.forEach(resolve=>resolve(response({pairs:[pair(1)]})));await first;
  assert.equal(f.app.receipts.get('browser:hot-score').payload.chain,'ethereum');assert.equal(f.app.receipts.get('browser:hot-score').payload.rows[0].chainId,'ethereum');assert.ok(!f.elements['hot-runners-table'].textContent.includes('ROW1'));
});

test('failed requests are inspectable and never produce a successful updated timestamp',async()=>{
  const f=fixture(async()=>response({error:'quota',complete_failure_context:{retry:null}},429));await f.app.loadHotRunners();
  assert.equal(f.elements['last-update'].textContent,'');assert.match(f.elements['scan-badge'].textContent,/0\/12/);
  const provider=[...f.app.receipts.values()].find(r=>r.payload.http_status===429);assert.equal(provider.payload.available,false);assert.equal(provider.payload.response.complete_failure_context.retry,null);
  f.app.inspectReceipt([...f.app.receipts.keys()][0]);const search=walk(f.elements['dex-data-inspector']).find(n=>n.tagName==='input');search.value='error';search.events.input();assert.match(f.elements['dex-data-inspector'].textContent,/quota/);
});

test('corrupt watchlist storage does not break init and no automatic AI request is sent',async()=>{
  const f=fixture(async()=>response({pairs:[]}),{storage:{getItem:()=>'{broken',setItem:()=>{throw new Error('denied');}}});await f.app.init();
  assert.equal(f.requests.length,12);assert.ok(f.requests.every(r=>!r[0].includes('api.justhodl.ai')));
  f.app.addWatch('solana','same','SOL');f.app.addWatch('ethereum','same','ETH');await f.app.refreshWatchlist();assert.match(f.elements['watchlist-container'].textContent,/2 local watchlist entries/);
});

test('standalone page loads safe renderer and has explicit sampled-data/cap disclosure',()=>{
  assert.match(html,/<script src="\/dex.js"><\/script>/);assert.match(html,/Returned data and browser calculations/);assert.match(html,/unbounded browser heuristic/);assert.match(html,/ticker 20/);
  const source=fs.readFileSync(require.resolve('../dex.js'),'utf8');assert.ok(!/\.(innerHTML|outerHTML)\s*=/.test(source));assert.ok(!source.includes("message:'ping'"));
});

test('unstructured long model reply is never truncated and response failure stays explicit in summaries',async()=>{
 const f=fixture(async()=>response({error:'quota'},429)),text='Long model evidence '.repeat(90)+'LAST';f.app.renderAlpha(text);assert.ok(f.elements['alpha-feed'].textContent.includes(text));await f.app.loadBoosted();assert.match(f.elements['boosted-top-table'].textContent,/Provider data unavailable/);
});
