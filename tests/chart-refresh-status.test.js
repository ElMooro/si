const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/chart-refresh-status'),negative=process.env.JH_REFRESH_BADGE_NEGATIVE_CONTROL==='1',raw=fs.readFileSync(negative?path.join(D,'pre509/jh-chart-engine.js.txt'):path.join(R,'jh-chart-engine.js'),'utf8'),p={exports:{}};
Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(p.exports,p);
const body=p.exports.parse(raw,{ecmaVersion:'latest'}).body.find(n=>n.expression?.callee?.type==='FunctionExpression').expression.callee.body.body;
const named=Object.fromEntries(body.filter(n=>n.type==='FunctionDeclaration').map(n=>[n.id.name,raw.slice(n.start,n.end)])),now=Date.UTC(2026,9,1,11);
function setup(input){const el={attrs:{},setAttribute(k,v){this.attrs[k]=v;}},c={active:'NEW',liveOn:true,replay:{on:false},tape:{sym:'NEW',src:'binance',prints:[]},Date:{now:()=>now},...input,document:{getElementById:()=>el}};vm.createContext(c);vm.runInContext(named.lastPrintAgeSec+'\n'+named.syncLivePill,c);return {c,el};}
test('all whole predecessor states describe refresh settings without LIVE or EOD certification',()=>{
 const previous=JSON.parse(fs.readFileSync(path.join(D,'whole-badge-predecessor.json'),'utf8'));
 for(const row of previous.cases){const h=setup(structuredClone(row.input));h.c.syncLivePill();assert.equal(h.el.innerHTML,row.input.liveOn?'<i></i> AUTO':'<i></i> PAUSED');assert.doesNotMatch(h.el.innerHTML,/LIVE|EOD/);assert.match(h.el.title,/do not establish real-time chart prices/);assert.equal(h.el.attrs.tabindex,'0');}
});
test('other-symbol tape cannot supply age information for the selected symbol',()=>{
 const h=setup({tape:{sym:'OLD',src:'binance',prints:[{t:now-1000}]}});assert.equal(h.c.lastPrintAgeSec(),null);h.c.syncLivePill();assert.match(h.el.title,/No usable timestamped tape entry/);
});
test('future, nonfinite, boolean, string and unavailable timestamps remain unknown',()=>{
 for(const t of [undefined,null,true,false,'',String(now),NaN,Infinity,-Infinity,-1,0,now+1,now/1000+1]){const h=setup({tape:{sym:'NEW',prints:[{t}]}});assert.equal(h.c.lastPrintAgeSec(),null,String(t));}
});
test('supported current second and millisecond representations return the reported age',()=>{
 for(const t of [now-30000,(now-30000)/1000])assert.equal(setup({tape:{sym:'NEW',prints:[{t}]}}).c.lastPrintAgeSec(),30);
 assert.equal(setup({tape:{sym:'NEW',prints:[{t:now}]}}).c.lastPrintAgeSec(),0);
});
test('mixed and out-of-order entries retain all input records and use only the newest usable clock',()=>{
 const tape={sym:'NEW',prints:[{t:now-90000},{t:now+100000},{t:now-10000},{t:null},{t:now-30000}]},before=structuredClone(tape),h=setup({tape});assert.equal(h.c.lastPrintAgeSec(),10);assert.deepEqual(tape,before);
});
test('unusable device clocks cannot certify an entry age',()=>{
 for(const value of [NaN,Infinity,-1,0])assert.equal(setup({Date:{now:()=>value},tape:{sym:'NEW',prints:[{t:now-1000}]}}).c.lastPrintAgeSec(),null);
});
test('replay indicates automatic refresh suspension even if the saved preference is on',()=>{
 const h=setup({replay:{on:true},tape:{sym:'NEW',prints:[{t:now-1000}]}});h.c.syncLivePill();assert.equal(h.el.innerHTML,'<i></i> REPLAY');assert.equal(h.el.className,'');assert.match(h.el.title,/automatic price refresh is suspended/);
});
test('same DOM element transitions from enabled through paused and replay without stale status',()=>{
 const h=setup();h.c.syncLivePill();assert.equal(h.el.className,'on');h.c.liveOn=false;h.c.syncLivePill();assert.equal(h.el.innerHTML,'<i></i> PAUSED');assert.equal(h.el.className,'');h.c.replay.on=true;h.c.syncLivePill();assert.equal(h.el.innerHTML,'<i></i> REPLAY');h.c.replay.on=false;h.c.liveOn=true;h.c.syncLivePill();assert.equal(h.el.innerHTML,'<i></i> AUTO');
});
test('no footer element is harmless and initial HTML does not claim a live source',()=>{
 const h=setup();h.c.document.getElementById=()=>null;h.c.syncLivePill();const html=fs.readFileSync(negative?path.join(D,'pre509/chart.html.txt'):path.join(R,'chart.html'),'utf8');assert.match(html,/<span id="livepill"[^>]*>[\s\S]*?REFRESH —<\/span>/);assert.doesNotMatch(html,/<span id="livepill"[^>]*>[\s\S]*? LIVE<\/span>/);
});
