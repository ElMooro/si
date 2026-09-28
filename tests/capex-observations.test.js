const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const C=require('../jh-capex-observations.js');
const contract='capex-accounting-measurements.v1';
function issuer(ticker='AAA'){
 const original={symbol:ticker,date:'2026-06-30',period:'Q2',fiscalYear:'2027',calendarYear:'2026',reportedCurrency:'JPY',capitalExpenditure:-123,cik:'0001',filingDate:'2026-07-20',acceptedDate:'2026-07-20 09:00:00'};
 return {ticker,provider_response:[original],observations:[{source_row:0,original,issues:['explicit_calendar_quarter_unavailable']}],current_window:{status:'four_explicit_contiguous_quarters_unavailable'}};
}
function document(){const nodes=new Map();return {getElementById:id=>{if(!nodes.has(id))nodes.set(id,{innerHTML:'',textContent:'',value:'',hidden:false,open:false,disabled:false});return nodes.get(id);}};}

test('Capex keeps every source occurrence and a stable original coordinate across selection',()=>{
 const first=issuer(),second=issuer(),packet={measurement_contract:contract,rows:[null,first,second]},m=C.model(packet);
 assert.equal(m.issuers.length,2);assert.deepEqual(m.issuers.map(x=>x.index),[1,2]);assert.match(m.message,/2 received statement rows/);assert.match(m.message,/1 malformed/);
 const doc=document();C.mount(packet,doc);const select=doc.getElementById('capex-statement-issuer');
 assert.equal(select.disabled,false);assert.match(select.innerHTML,/rows\[1\]/);assert.match(select.innerHTML,/rows\[2\]/);
 select.value='2';select.onchange();assert.match(doc.getElementById('capex-statements').innerHTML,/\/rows\/2\/provider_response\/0/);
 const disclosure=doc.getElementById('capex-statement-record');assert.equal(disclosure.hidden,false);disclosure.open=true;disclosure.ontoggle();assert.equal(doc.getElementById('capex-statement-original').textContent,JSON.stringify(second,null,2));
 select.value='1';select.onchange();assert.equal(disclosure.open,false);assert.ok(!doc.getElementById('capex-statement-original').textContent.includes('capitalExpenditure'));
 select.value='';select.onchange();assert.equal(disclosure.hidden,true);
});

test('Capex reports signed local currency statements without manufacturing durations or annual calculations',()=>{
 const row=issuer(),html=C.statements(row,0);
 assert.match(html,/<td>-123<\/td>/);assert.match(html,/<td>JPY<\/td>/);assert.match(html,/<td>2026-06-30<\/td><td>Unavailable<\/td>/);
 assert.match(html,/<td>Q2<\/td><td>2027<\/td><td>2026<\/td>/);
 assert.match(html,/explicit_calendar_quarter_unavailable/);assert.match(html,/2026-07-20 09:00:00/);assert.ok(!html.includes('2026-04-01'));assert.ok(!html.includes('123.00 USD'));
 for(const value of [null,false,'123','',Number.POSITIVE_INFINITY,Number.MAX_SAFE_INTEGER+1]){
  const r=issuer();r.provider_response[0].capitalExpenditure=value;assert.ok(!C.statements(r,0).includes('<td>'+String(value)+'</td>'));
 }
 const zero=issuer();zero.provider_response[0].capitalExpenditure=0;assert.match(C.statements(zero,0),/<td>0<\/td>/);
});

test('Capex preserves malformed and unmatched originals, escapes every source field, and does not borrow issues',()=>{
 const row=issuer('<img src=x onerror=alert(1)>');row.provider_response[0].reportedCurrency='<script>';row.observations[0].issues=['<img>'];row.provider_response.push(null);
 let html=C.statements(row,3);assert.ok(!html.includes('<img'));assert.ok(!html.includes('<script>'));assert.match(html,/&lt;script&gt;/);assert.match(html,/Malformed statement at \/rows\/3\/provider_response\/1/);
 row.observations.push({...row.observations[0]});html=C.statements(row,3);assert.match(html,/Projection missing, ambiguous or inconsistent/);assert.ok(!html.includes('&lt;img&gt;'));
 row.observations=[{source_row:0,original:{},issues:[]}];assert.match(C.statements(row,3),/Projection missing, ambiguous or inconsistent/);
 row.provider_response=false;assert.match(C.statements(row,3),/Provider response missing or malformed/);
});

test('Legacy, malformed and empty packets cannot manufacture a selectable current statement',()=>{
 for(const p of [null,{rows:[issuer()]},{measurement_contract:contract,rows:false}]){const doc=document();C.mount(p,doc);assert.equal(doc.getElementById('capex-statement-issuer').disabled,true);}
 const doc=document();C.mount({measurement_contract:contract,rows:[]},doc);assert.equal(doc.getElementById('capex-statement-issuer').disabled,true);assert.match(doc.getElementById('capex-statement-status').textContent,/0 selectable/);
});

test('Capex keeps the whole predecessor and exposes a keyboard-accessible statement region',()=>{
 const prior=fs.readFileSync(path.join(__dirname,'fixtures/pre-capex-statements.html.txt'));
 assert.equal(crypto.createHash('sha256').update(prior).digest('hex'),'0d1523d2dd4f6dd5d80dddd4c6c5d420dc6d9c5dc99811348ae376c94b5b2af3');
 const html=fs.readFileSync(path.join(__dirname,'../capex-pulse.html'),'utf8');assert.match(html,/<label for="capex-statement-issuer">/);assert.match(html,/jh-capex-observations.js/);
 assert.match(C.statements(issuer(),0),/role="region"[^>]*tabindex="0"/);assert.match(html,/Annual window calculations/);
});
