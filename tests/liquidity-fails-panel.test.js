const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
const source=fs.readFileSync('jh-liq-fails-panel.js','utf8');
const row=(scope,values={})=>({scope_id:scope,unit:'usd_bn',as_of:'2026-09-16',ftd_bn:86,ftr_bn:87,combined_bn:173,gross_bn:173,...values});
async function render(sf,flow={},reject=null){
 const host={style:{},textContent:'',set innerHTML(value){throw Error('Untrusted source inserted as HTML');}},calls=[];
 class FixedDate extends Date {static now(){return Date.parse('2026-09-25T19:00:00Z');}}
 const context={window:{},Date:FixedDate,document:{getElementById:()=>host},fetch:async(url,options)=>{
  calls.push({url,options});if(reject&&url.includes(reject))throw Error('unavailable');
  return {ok:true,json:async()=>url.includes('settlement-fails')?sf:flow};
 }};
 vm.createContext(context);vm.runInContext(source,context);await new Promise(r=>setImmediate(r));return {text:host.textContent,calls};
}
test('including and excluding TIPS remain distinct with precision and real zero',async()=>{
 const {text,calls}=await render({headline:row('ust_ex_tips',{ftd_bn:0,ftr_bn:87.123,combined_bn:87.123}),treasury:row('treasury_incl_tips',{gross_bn:190.24})});
 assert.match(text,/excluding TIPS · 2026-09-16 · FTD \$0bn · FTR \$87.123bn · total \$87.123bn/);
 assert.match(text,/including TIPS · 2026-09-16.*total \$190.24bn/);
 assert(calls.every(c=>c.options.credentials==='omit'&&c.url.endsWith('?exact=1&nogen=1')));
});
test('missing or wrongly scoped ex-TIPS never inherits the Treasury gross population',async()=>{
 for(const headline of [undefined,row('treasury_incl_tips'),row('ust_ex_tips',{ftd_bn:null,ftr_bn:null,combined_bn:null})]){
  const {text}=await render({headline,treasury:row('treasury_incl_tips',{gross_bn:190.24})});
  const line=text.split('\n')[1];assert(!line.includes('$'));assert.match(text,/total \$190.24bn/);
 }
});
test('invalid units, coerced numbers and missing or future dates cannot display an amount',async()=>{
 for(const invalid of [{unit:'usd_mn'},{field_units:{ftd_bn:'usd_mn',ftr_bn:'usd_mn',combined_bn:'usd_mn'}},
  {as_of:'2026-02-30'},{as_of:'2099-01-01'},{as_of:'<img src=x onerror=alert(1)>'},
  {ftd_bn:'86',ftr_bn:true,combined_bn:-173}]){
  const {text}=await render({headline:row('ust_ex_tips',invalid)});assert(!text.includes('$'));assert(!text.includes('<img'));
 }
});
test('one failed feed preserves the available scoped feed and rejects unlabelled flow values',async()=>{
 const valid={pd_settlement_fails:row('treasury_incl_tips',{combined_bn:190.24})};
 assert.match((await render({},valid,'settlement-fails')).text,/including TIPS.*total \$190.24bn/);
 assert(!((await render({},{pd_settlement_fails:{combined_bn:190.24,unit:'usd_bn'}},'settlement-fails')).text.includes('$')));
 assert.match((await render({headline:row('ust_ex_tips',{as_of:'2026-08-01'})},{},'liquidity-flow')).text,/old observation/);
});
test('complete predecessor is retained and liquidity formula is not edited',()=>{
 const m=JSON.parse(fs.readFileSync('tests/fixtures/liquidity-fails-panel-migration.json'));const raw=fs.readFileSync(m.predecessor);
 assert.equal(raw.length,m.bytes);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),m.sha256);assert.equal(m.formula_changed,false);
});
