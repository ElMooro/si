const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const v=require('../jh-freight-review.js'),now=Date.parse('2026-09-27T08:00:00Z');
const native=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/freight-calendar-browser.json'),'utf8'));
const legacy=()=>({engine_class:'physical_trade_slow_confirmation',version:'2.0.0',series:{},generated_at:'2026-09-26T11:50:00Z',composite:99,inflections:['LONG'],unknown:{preserved:[0,null,false]}});
const other=()=>({generated_at:'2026-09-26T10:40:00Z',all_fields:[0,null,false,'<img onerror=alert(1)>']});
class Element{
 constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.textContent='';this.value='';}
 appendChild(n){this.children.push(n);return n;}replaceChildren(...nodes){this.children=nodes;this.textContent='';}text(){return this.textContent+this.children.map(n=>n.text()).join(' ');}
}
function document(){const html=fs.readFileSync(path.join(__dirname,'../freight-pulse.html'),'utf8'),nodes=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element('div')]));return{createElement:tag=>new Element(tag),getElementById:id=>nodes.get(id)||null};}
const loader=p=>async kind=>kind==='freight'?p:other();

test('complete actual synthetic native output preserves six identities, unrounded Cass values and all monthly records',()=>{
 const p=native(),x=v.view(p,now);assert.equal(p._synthetic_fixture,true);assert.equal(p.engine,undefined);assert.equal(x.native,true);assert.equal(x.authority,false);assert.equal(x.observations,839);assert.equal(x.rows.length,6);
 const series=p.measurement_review.series;assert.equal(series.TSIFRGHT.yoy.percent,null);assert.equal(series.TSIFRGHT.yoy.status,'missing_comparison');assert.equal(series.TRUCKD11.yoy.status,'zero_denominator');assert.equal(series.RAILFRTCARLOADSD11.level,null);assert.equal(series.FRGSHPUSM649NCIS.six_month.annualized_percent,null);assert.equal(series.FRGSHPUSM649NCIS.six_month.annualization_status,'not_seasonally_adjusted');
 assert.notEqual(p.measurement_review.cass_index_ratio.current_ratio,Math.round(series.FRGEXPUSM649NCIS.level*100)/Math.round(series.FRGSHPUSM649NCIS.level*100));
});
test('growth, month identities, full histories and prior-only standardization must agree with original observations',()=>{
 const mutations=[p=>p.measurement_review.series.TSIFRGHT.level=999,p=>p.measurement_review.series.TSIFRGHT.yoy.percent=0,p=>p.measurement_review.series.TRUCKD11.yoy.previous_date='2025-07-01',p=>p.measurement_review.series.TRUCKD11.six_month.annualized_percent+=1,p=>p.measurement_review.series.FRGSHPUSM649NCIS.six_month.annualized_percent=1,p=>p.measurement_review.series.FRGSHPUSM649NCIS.prior_60_months.mean+=.001,p=>p.measurement_review.series.FRGSHPUSM649NCIS.prior_60_months.start='2021-09-01',p=>p.measurement_review.series.TSIFRGHT.prior_60_months.missing_or_invalid_dates=[],p=>p.measurement_review.series.TRUCKD11.observations[1].date=p.measurement_review.series.TRUCKD11.observations[0].date,p=>p.measurement_review.series.TRUCKD11.observations[0].value=999,p=>p.measurement_review.series.TRUCKD11.returned_rows--,p=>delete p.measurement_review.series.TRUCKD11];
 for(const mutate of mutations){const p=native();mutate(p);assert.throws(()=>v.view(p,now));}
 const p=native();p.composite=999999;p.inflections=['LONG'];assert.equal(v.view(p,now).authority,false);
});
test('Cass ratio cannot use rounded values, shifted dates or a changed denominator',()=>{
 for(const mutate of [r=>r.current_ratio+=.001,r=>r.current_date='2026-07-01',r=>r.observations.pop(),r=>r.observations[0].shipment_index=0,r=>r.unit='usd_per_shipment',r=>r.sizing_eligible=true]){const p=native();mutate(p.measurement_review.cass_index_ratio);assert.throws(()=>v.view(p,now));}
});
test('source units, contracts, clocks, retention and all compiler/authority declarations are required',()=>{
 const mutations=[p=>p.engine_class='other',p=>p.generated_at='2026-09-28T00:00:00Z',p=>p.generated_at='2026-02-30T00:00:00Z',p=>p.measurement_review.calculation_at='2026-09-25T00:00:00Z',p=>p.measurement_review.series.TRUCKD11.unit='USD',p=>p.measurement_review.series.TRUCKD11.metadata.seasonal_adjustment_short='NSA',p=>p.measurement_review.contract='new',p=>p.calls_eligible=true,p=>p.measurement_review.execution_eligible=true,p=>p.portfolio_action='LONG',p=>p.publication_context.point_in_time_verified=true,p=>p.publication_context.manifest.key='data/public.json',p=>delete p.publication_context.compiler_sha256['managed_secret.py'],p=>p.publication_context.provider_responses=1,p=>delete p.measurement_review];
 for(const mutate of mutations){const p=native();mutate(p);assert.throws(()=>v.view(p,now));}
 assert.equal(v.view(native(),now+2*86400000).overdue,true);
});
test('a constant decimal baseline stays zero variance; tiny nonzero values never become fabricated zero',()=>{
 const p=native(),s=p.measurement_review.series.TRUCKD11;s.observations.forEach(r=>{r.provider_value='0.1';r.value=.1;});s.level=.1;
 for(const field of ['yoy','six_month'])Object.assign(s[field],{current_level:.1,previous_level:.1,percent:0,status:'measured'});
 Object.assign(s.six_month,{annualized_percent:0,annualization_status:'measured'});Object.assign(s.prior_60_months,{mean:.1,population_sd:0,z:null,status:'zero_variance'});assert.equal(v.view(p,now).native,true);
 for(const raw of [true,false,null,'',' ','NaN','Infinity','1e400','1e-400',-1,'1_000'])assert.equal(v.number(raw),null);
 for(const raw of [0,'0','0.000','0e-999'])assert.equal(v.number(raw),0);assert.equal(v.number('1.038'),1.038);
});
test('DOM renders every source, exact dates, NSA withholding and all observation pages without HTML interpolation',async()=>{
 const p=native(),doc=document();await v.mount(doc,loader(p),now);assert.equal(doc.getElementById('freight-series').children[0].children[1].children.length,6);assert.match(doc.getElementById('freight-series').text(),/Withheld · NSA/);assert.match(doc.getElementById('freight-series').text(),/Unavailable/);assert.match(doc.getElementById('freight-count').textContent,/839/);
 assert.match(doc.getElementById('freight-definition').textContent,/2021-08-01 through 2026-07-01/);assert.match(doc.getElementById('freight-observation-count').textContent,/1–100 of 139/);doc.getElementById('freight-next').onclick();assert.match(doc.getElementById('freight-observation-count').textContent,/101–139 of 139/);
 const choice=doc.getElementById('freight-choice');choice.value='FRGSHPUSM649NCIS';choice.onchange();assert.match(doc.getElementById('freight-observation-count').textContent,/1–100 of 140/);
 for(const key of Object.keys(v.SOURCES)){const d=doc.getElementById(key+'-details');d.open=true;d.ontoggle();assert.deepEqual(JSON.parse(doc.getElementById(key+'-original').textContent),key==='freight'?p:other());}
 assert.match(doc.getElementById('air-original').textContent,/<img onerror=alert\(1\)>/);
});
test('legacy inputs remain inspectable but cannot populate the new measurements',async()=>{
 const p=legacy(),doc=document();await v.mount(doc,loader(p),now);assert.equal(v.view(p,now).native,false);assert.match(doc.getElementById('freight-count').textContent,/Legacy publication/);assert.match(doc.getElementById('freight-preservation').textContent,/11:50 UTC/);assert.equal(doc.getElementById('freight-series').children.length,0);assert.equal(doc.getElementById('freight-next').onclick,null);
 const d=doc.getElementById('freight-details');d.open=true;d.ontoggle();assert.deepEqual(JSON.parse(doc.getElementById('freight-original').textContent),p);
});
test('independent source failures cannot claim agreement or discard other complete packets',async()=>{
 const doc=document();await v.mount(doc,async key=>{if(key==='shipping')throw Error('HTTP 403');return key==='freight'?native():other();},now);assert.match(doc.getElementById('shipping-status').textContent,/Unavailable/);assert.equal(doc.getElementById('freight-series').children.length,1);assert.match(doc.getElementById('air-status').textContent,/2026-09-26/);assert.equal(doc.getElementById('shipping-details').ontoggle,null);
});
test('refresh clears measurements and stale callbacks; delayed old responses cannot repopulate the page',async()=>{
 const doc=document();await v.mount(doc,loader(native()),now);const oldNext=doc.getElementById('freight-next').onclick,oldChoice=doc.getElementById('freight-choice').onchange,oldRaw=doc.getElementById('freight-details').ontoggle;
 const resolvers=[];const first=v.mount(doc,()=>new Promise(resolve=>resolvers.push(resolve)),now);await v.mount(doc,async()=>{throw Error('current unavailable');},now);resolvers.forEach(fn=>fn(native()));await first;oldNext();oldChoice();oldRaw();
 assert.equal(doc.getElementById('freight-series').children.length,0);assert.equal(doc.getElementById('freight-next').onclick,null);assert.equal(doc.getElementById('freight-ratio').textContent,'Unavailable');assert.equal(doc.getElementById('freight-original').textContent,'Open to inspect the complete packet.');assert.match(doc.getElementById('freight-status').textContent,/current unavailable/);
});
test('whole-body same-origin reader rejects malformed, duplicate, overflow and timed-out responses',async()=>{
 const p=native(),raw=new TextEncoder().encode(JSON.stringify(p));let requested,index=0,cancelled=0;
 const fetcher=async(url,options)=>{requested={url,options};return{ok:true,body:{getReader:()=>({read:async()=>index<raw.length?{value:raw.slice(index,(index+=139)),done:false}:{done:true},cancel:async()=>{cancelled++;}})}};};
 assert.deepEqual(await v.load('freight',{fetcher}),p);assert.equal(requested.url,'/data/freight-pulse.json?exact=1&nogen=1');assert.equal(requested.options.redirect,'error');assert.equal(requested.options.cache,'no-store');assert.equal(cancelled,1);
 for(const bytes of [new TextEncoder().encode('{"x":[],"x":[]}'),new Uint8Array([255]),new TextEncoder().encode('{"x":1e400}'),new TextEncoder().encode('{"incomplete":')]){let i=0;await assert.rejects(v.load('air',{fetcher:async()=>({ok:true,body:{getReader:()=>({read:async()=>i++===0?{value:bytes,done:false}:{done:true},cancel:async()=>{}})}})}));}
 await assert.rejects(v.load('private'),/Undeclared/);await assert.rejects(v.load('trade',{fetcher:async()=>({ok:false})}),/unavailable/);await assert.rejects(v.load('boom',{timeout:5,fetcher:async()=>new Promise(()=>{})}),/timed out/);
});
test('whole original page is preserved; all five source packets survive without unsupported forecast UI',()=>{
 const crypto=require('node:crypto'),raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-research-freight-pulse.html.txt'));assert.equal(raw.length,12376);assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),'661c23e3652b9caf32f09129cde54e4b834075d44ae04b0ae77f0bd0739958fe');
 const html=fs.readFileSync(path.join(__dirname,'../freight-pulse.html'),'utf8'),js=fs.readFileSync(path.join(__dirname,'../jh-freight-review.js'),'utf8');assert.ok(!html.includes('s3.amazonaws.com'));assert.ok(!html.includes('jh-impact-strip'));assert.ok(!js.includes('.innerHTML'));assert.equal(Object.keys(v.SOURCES).length,5);assert.match(html,/jh-freight-review.js/);assert.match(html,/11:50 UTC/);assert.match(html,/not cargo value/);assert.match(html,/No missing source is interpreted as agreement/);
});
