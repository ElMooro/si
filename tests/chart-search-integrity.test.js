const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const external=fs.existsSync(path.join(__dirname,'candidate-engine.js'));
const root=external?path.join(__dirname,'..','si-batch-improvements'):path.join(__dirname,'..');
const source=fs.readFileSync(external?path.join(__dirname,'candidate-engine.js'):path.join(root,'jh-chart-engine.js'),'utf8');
const bundled=process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'],moduleParser={exports:{}};
Function('exports','module',bundled)(moduleParser.exports,moduleParser);
const tree=moduleParser.exports.parse(source,{ecmaVersion:'latest'}),named={};
function visit(n){if(!n||typeof n!=='object')return;if(n.type==='FunctionDeclaration'&&n.id)named[n.id.name]=source.slice(n.start,n.end);for(const v of Object.values(n))if(Array.isArray(v))v.forEach(visit);else if(v&&typeof v==='object')visit(v);}
visit(tree);
const functions=['escHtml','identifierQuery','syncSsSelection','moveSsSelection','submitSsSearch','paintSs','ssKind','flagFor','ssRowHtml','paintFacets','paintSsList'];
function setup(){
 const actions=[],messages=[],elements=[],c={ssRows:[],ssSel:-1,ssChoice:'',ssFacets:[],ssProv:'',compare:[],recents:[],window:{JHChartCatalog:{lookupSym:()=>''}},document:{getElementById:()=>({dataset:{dest:'chart'}}),querySelectorAll:()=>elements},displayTicker:s=>s,bare:s=>s,logoColor:()=> '#2962ff',goSymbol:(s,d)=>actions.push([s,d]),toast:m=>messages.push(m)};
 vm.createContext(c);vm.runInContext(functions.map(n=>{assert.ok(named[n],n);return named[n];}).join('\n'),c);
 return {c,actions,messages,elements};
}
test('identifier suggestions do not receive a default Enter action',()=>{
 const {c,actions,messages}=setup();c.ssRows=[{s:'ZZFIRST'},{s:'ZZSECOND'}];c.syncSsSelection('US0000000000');c.submitSsSearch('chart');
 assert.equal(c.ssSel,-1);assert.equal(actions.length,0);assert.match(messages[0],/Choose a specific result/);
});
test('identifier-like strings cannot be advertised as unqualified direct ticker entries',()=>{
 const {c}=setup();for(const q of ['US0000000000','BBG000BLNNH6','037833100'])assert.equal(c.identifierQuery(q),true);
 for(const q of ['TEST-A','BRK.B','fred:DFF'])assert.equal(c.identifierQuery(q),false);
});
test('an exact unique catalog ticker keeps direct keyboard navigation',()=>{
 const {c,actions}=setup();c.ssRows=[{s:'ZZFIRST'},{s:'TEST-A'}];c.window.JHChartCatalog.lookupSym=q=>q==='test-a'?'TEST-A':'';
 c.syncSsSelection('test-a');c.submitSsSearch('chart');assert.deepEqual(actions,[['TEST-A','chart']]);
});
test('ambiguous lookup and duplicate result IDs cannot silently choose a row',()=>{
 const {c,actions}=setup();c.ssRows=[{s:'TEST'},{s:'test'}];c.syncSsSelection('TEST');c.submitSsSearch('chart');assert.equal(actions.length,0);
 c.ssRows=[{s:'TEST'},{s:'TEST'}];c.ssChoice='TEST';c.syncSsSelection('TEST');assert.equal(c.ssSel,-1);
});
test('explicit arrow selection survives asynchronous row reordering by identity',()=>{
 const {c,actions}=setup();c.ssRows=[{s:'ZZFIRST'},{s:'ZZSECOND'}];c.moveSsSelection(1);c.moveSsSelection(1);
 c.ssRows=[{s:'OTHER'},{s:'ZZSECOND'},{s:'ZZFIRST'}];c.syncSsSelection('US0000000000');c.submitSsSearch('compare');
 assert.equal(c.ssChoice,'ZZSECOND');assert.deepEqual(actions,[['ZZSECOND','compare']]);
});
test('a removed or empty selected row cannot trigger an earlier action',()=>{
 const {c,actions}=setup();c.ssChoice='MISSING';c.ssRows=[{s:'OTHER'}];c.syncSsSelection('x');c.submitSsSearch('chart');assert.equal(actions.length,0);
 c.ssRows=[];c.moveSsSelection(1);assert.equal(c.ssSel,-1);assert.equal(c.ssChoice,'');
});
test('keyboard highlighting uses result IDs and preserves comparison styling',()=>{
 const {c,elements}=setup();const toggles=[];elements.push(...[2,0,1].map(i=>({dataset:{i:String(i)},classList:{toggle:(name,state)=>toggles.push([i,name,state])}})));
 c.ssSel=0;c.paintSs();assert.deepEqual(toggles,[[2,'on',false],[0,'on',true],[1,'on',false]]);
});
test('source labels, metadata and quote-bearing IDs remain text in search rows',()=>{
 const {c}=setup(),value='<img src=x onerror="fixture=1">\'&';
 const html=c.ssRowHtml({s:value,label:value,name:value,extra:value},0);
 assert.doesNotMatch(html,/<img|data-more='<|data-more='[^']*'&/);assert.match(html,/&lt;img/);assert.match(html,/&#39;/);
 assert.doesNotThrow(()=>c.ssRowHtml({s:'TEST',name:{bad:true},extra:{bad:true}},0));
});
test('facet values remain text and missing or invalid counts stay unknown',()=>{
 const {c}=setup();c.ssFacets=[null,{provider:"x'<svg>",provider_name:'<img>',n:null},{provider:'zero',n:0},{provider:'bad',n:true}];
 const html=c.paintFacets();assert.doesNotMatch(html,/<svg|<img/);assert.match(html,/x&#39;&lt;svg&gt;/);assert.match(html,/>zero <b>0<\/b>/);assert.equal((html.match(/<b>\?<\/b>/g)||[]).length,2);
});
test('empty-result queries remain text and never advertise implicit routing',()=>{
 const {c}=setup();const html=c.paintSsList([],'chart','<img src=x>');assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img|Enter opens/);
});
test('non-exact results explain how to make an explicit selection',()=>{
 const {c}=setup();c.ssRows=[{s:'ZZFIRST'}];const html=c.paintSsList(c.ssRows,'chart','issuer');assert.match(html,/role=status/);assert.match(html,/Choose a specific result/);
});
