const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const root=path.join(__dirname,'..'),V=require('../jh-table-values.js');
const source=fs.readFileSync(path.join(root,'capex-pulse.html'),'utf8');
async function render(packet,error){
 const nodes=new Map(),calls=[];
 const get=id=>{if(!nodes.has(id))nodes.set(id,{value:'',innerHTML:'',textContent:''});return nodes.get(id);};
 const raw=JSON.stringify(packet),context=vm.createContext({console,document:{getElementById:get,querySelectorAll:()=>[]},encodeURIComponent,JHTableValues:{...V,load:async p=>{calls.push(p);if(error)throw error;return{packet,raw};}}});
 for(const m of source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))vm.runInContext(m[1],context);
 for(let i=0;i<3;i++)await new Promise(r=>setImmediate(r));return{get,calls,context,raw};
}
test('Capex renders the complete received population with typed values and inert source metadata',async()=>{
 const p={n:'<img src=x>',fails:0,market:{capex_ttm_b:false,yoy_pct:0},hyperscalers:{total_ttm_b:'',yoy_pct:' '},rows:[{ticker:'TEN',capex_ttm_b:'10',yoy_pct:'',intensity_pct:false,mc_b:0},{ticker:'TWO',capex_ttm_b:'2'},{ticker:'ZERO',capex_ttm_b:0,intensity_pct:0,sector:'<img src=x>'},{ticker:'BAD',capex_ttm_b:' ',yoy_pct:false},null],unknown:'retain'};
 const s=await render(p),h=s.get('board').innerHTML;assert.equal(s.calls[0],'/data/capex-pulse.json');assert.equal(s.calls.length,1);assert.equal(s.get('original').textContent,s.raw);assert.match(h,/4 of 4 received rows/);assert.match(h,/1 malformed/);assert.ok(!h.includes('<img'));assert.ok(!h.includes('NaN'));assert.ok(h.indexOf('TEN')<h.indexOf('TWO'));assert.match(h,/<td>0.00%<\/td>/);assert.match(s.get('kpis').textContent,/Source name count: Unavailable/);assert.match(s.get('kpis').textContent,/Source failures: 0/);assert.match(s.get('kpis').textContent,/Universe YoY: 0.0%/);
 assert.equal(vm.runInContext('fmtB(false)',s.context),'');assert.equal(vm.runInContext('fmtPct(" ")',s.context),'');assert.equal(vm.runInContext('cls(0)',s.context),'');
});
test('Capex failed, malformed and empty publications have different visible states',async()=>{
 const e=Error('Duplicate JSON key');e.original_text='{"x":"<img>","x":2}';const failure=await render(null,e);assert.match(failure.get('board').textContent,/Publication unavailable/);assert.equal(failure.get('original').textContent,e.original_text);
 const malformed=await render({rows:false});assert.match(malformed.get('board').textContent,/missing or malformed/);assert.equal(malformed.get('original').textContent,'{"rows":false}');
 const empty=await render({rows:[]});assert.match(empty.get('board').innerHTML,/0 of 0 received rows/);
});
test('Capex numeric sort leaves missing values last in both directions and remains filterable',async()=>{
 const s=await render({rows:[{ticker:'BAD',capex_ttm_b:false},{ticker:'TEN',capex_ttm_b:'10'},{ticker:'TWO',capex_ttm_b:'2'},{ticker:'ZERO',capex_ttm_b:0}]});
 const order=vm.runInContext('DIR=1; ROWS.slice().sort(cmp).map(r=>r.ticker).join(",")',s.context);assert.equal(order,'ZERO,TWO,TEN,BAD');
 s.get('q').value='TEN';s.get('q').oninput();assert.match(s.get('board').innerHTML,/1 of 4/);assert.ok(!s.get('board').innerHTML.includes('TWO'));
});
test('Capex original bytes survive and the startup syntax defect is reproduced by the predecessor',()=>{
 const raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-capex-desk.html.txt'));assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'f0562fcdd0820ef1e6f6a9498ad34324d5b3888617a681e7a260de2e13be4901');
 const inline=[...raw.toString().matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)][0][1];assert.throws(()=>new vm.Script(inline),SyntaxError);
 assert.match(source,/<label for="q">/);assert.match(source,/role="region"[^>]*tabindex="0"/);assert.match(source,/Capex pulse · JustHodl/);assert.ok(!/[\u00c2\u00c3\ufffd]/.test(source));assert.match(source,/V.bindSort/);
});
