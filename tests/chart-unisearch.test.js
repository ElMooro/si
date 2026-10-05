const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
test('chart.html loads the universal search and TradingView watchlist after the engine',()=>{const h=read('chart.html'),e=h.indexOf('/jh-chart-engine.js?'),s=h.indexOf('/jh-uni-search.js?v=20261005-us2'),w=h.indexOf('/jh-tv-watchlist.js?v=20261005-wl1');assert.ok(e>0&&s>e&&w>s);});
test('universal search queries the whole warehouse index, not a whitelist',()=>{const s=read('jh-uni-search.js');for(const k of ['/symsearch?','/browse?','/explorer','/tv-search?','instruments.json.gz'])assert.ok(s.includes(k),k);assert.match(s,/JHUniSearch\s*=/);});
test('watchlist keeps TradingView list format and seeds from every existing source',()=>{const s=read('jh-tv-watchlist.js');assert.match(s,/###/);for(const k of ['/data/tv-watchlists.json','jh_custom_watchlists','jh-chart-custom-lists','/quote?ids='])assert.ok(s.includes(k),k);});
test('default chart type is line; legacy saved layouts do not force candles',()=>{const s=read('jh-chart-engine.js');assert.ok(s.includes('kind="line", scaleMode=0;'));assert.ok(s.includes('if(lay.kind && lay.kindV===2) kind=lay.kind;'));assert.ok(s.includes('saveJSON(LAY_KEY,{kindV:2,'));});
test('search assist: aliases, ticker spellings, typo correction and watchlist names',async()=>{
 const vm=require("node:vm"),c={console,setTimeout,clearTimeout,setInterval:()=>0,fetch:()=>Promise.reject(new Error("offline")),Promise,JSON,Math,Object,Array,String,Number,RegExp,Date,Map,Set};c.window=c;
 c.document={readyState:'complete',addEventListener(){},getElementById(){return null},createElement(){return{style:{},setAttribute(){},addEventListener(){},querySelector(){return null}}},head:{appendChild(){}},body:{appendChild(){}}};
 c.localStorage={getItem(){return null},setItem(){}};c.addEventListener=()=>{};c.MutationObserver=function(){this.observe=()=>{}};
 vm.createContext(c);vm.runInContext(read('jh-watchlist-names.js'),c);vm.runInContext(read('jh-uni-search.js'),c);
 const U=c.JHUniSearch;await U.loadWLX();
 assert.ok(U.expandQuery('btc-usd').includes('BTCUSD'));assert.ok(U.expandQuery('^VIX').includes('VIX'));assert.ok(U.expandQuery('cpi').includes('consumer price index'));assert.ok(U.expandQuery('fed balance sheet').includes('WALCL'));
 assert.equal(U.correct('inflaton'),'inflation');assert.equal(U.correct('bitcon'),'bitcoin');
 const names=c.JH_WL_NAMES,ids=Object.keys(names);assert.ok(ids.length>10000);
 let missing=0;for(const id of ids){if(!names[id][0])missing++;}assert.equal(missing,0,'every watchlist symbol has a display name');
 for(const id of ids.filter((_,i)=>i%97===0)){const hit=U.localWatch(id,[],5).some(r=>r.id.toUpperCase()===id.toUpperCase());assert.ok(hit,id+' findable by its own symbol');}
 assert.ok(U.localWatch('XLY/XLP',[],5).some(r=>/XLY\/AMEX:XLP|XLY\/XLP/.test(r.id)));
});
