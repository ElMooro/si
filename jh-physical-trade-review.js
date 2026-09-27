/* Source populations stay separate until country, period and exposure mappings are validated. */
(function(root){
 'use strict';
 const shipping=typeof module!=='undefined'&&module.exports?require('./jh-portwatch-review.js'):root.JHPortwatchReview;
 const cycle=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const finite=n=>typeof n==='number'&&Number.isFinite(n),fmt=n=>finite(n)?n.toLocaleString('en-US',{maximumFractionDigits:2}):'Unavailable';
 const text=v=>typeof v==='string'&&v.trim()?v:'Unavailable';
 function shippingRows(packet,now){
  const v=shipping.view(packet,now);
  return {...v,table:v.rows.map(r=>[r.family,r.name+' ['+r.entity_id+']',r.country,
   text(r.last_observation_date),v.native?fmt(r.observation_lag_days)+' days at calculation':'Unverified',
   v.native?r.source_field+' / '+r.unit:'Legacy measurement unverified',
   v.native?fmt(r.current_7d.mean):'Unavailable',v.native?r.current_7d.available_days+'/7':'Unavailable',
   v.native?fmt(r.prior_year_7d.mean):'Unavailable',v.native?r.prior_year_7d.available_days+'/7':'Unavailable'])};
 }
 function cycleRows(packet,now){
  const v=cycle.countryView(packet,now);
  return {...v,table:v.rows.map(r=>{const s=r.source;return [r.iso,r.reference?.name||'Unregistered',r.reference?.region||'Unregistered',
   s?'Present; definitions unqualified':'Missing country',fmt(s?.cli_level),text(s?.phase),
   cycle.date(s?.latest_date)!==null?s.latest_date:'Unavailable',text(s?.source)];})};
 }
 function element(doc,tag,value,parent){const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;if(parent)parent.appendChild(n);return n;}
 function table(doc,host,headers,rows){host.replaceChildren();const t=element(doc,'table',undefined,host),head=element(doc,'tr',undefined,element(doc,'thead',undefined,t));for(const name of headers){const th=element(doc,'th',name,head);th.scope='col';}const body=element(doc,'tbody',undefined,t);for(const row of rows){const tr=element(doc,'tr',undefined,body);for(const value of row)element(doc,'td',value,tr);}}
 const definitions={
  shipping:{load:()=>shipping.load(),view:shippingRows,headers:['Family','Entity identity','Country label','Last observation','Observation age','Exact field / unit','Current 7d calls/day','Current coverage','Prior-year 7d calls/day','Prior-year coverage'],preservation:shipping.preservation},
  cycle:{load:()=>cycle.load('current'),view:cycleRows,headers:['ISO','Country','Region','Source coverage','Synthetic index points','Model classification (not official)','Equity source date','Source label'],preservation:cycle.publicationStatus}
 };
 async function mount(doc,loaders={},now=Date.now()){
  if(!doc.getElementById('pt-review'))return;
  const generation=(doc._ptGeneration||0)+1;doc._ptGeneration=generation;
  function say(id,value){doc.getElementById(id).textContent=value;}
  function clear(kind){doc.getElementById('pt-'+kind+'-table').replaceChildren();doc.getElementById('pt-'+kind+'-search').oninput=null;for(const suffix of ['count','raw','coverage','preservation'])say('pt-'+kind+'-'+suffix,'Unavailable');}
  for(const kind of Object.keys(definitions)){clear(kind);say('pt-'+kind+'-status','Loading complete source…');}
  await Promise.all(Object.entries(definitions).map(async([kind,def])=>{
   try{
    const packet=await(loaders[kind]||def.load)();if(doc._ptGeneration!==generation)return;
    const view=def.view(packet,now),at=view.generated_at||view.at;
    say('pt-'+kind+'-status',at+' · '+(view.overdue?'publication overdue (>26h)':'publication less than 26h old')+' at page load. Source observation dates remain separate.');
    say('pt-'+kind+'-coverage',kind==='shipping'?(view.native?view.history_rows+' retained observation rows across '+view.rows.length+' entities. Vessel counts do not measure cargo weight.':view.rows.length+' legacy output entities. Calendar counts and complete observation history are not declared.'):
     view.present+' of '+view.expected+' configured countries present; '+(view.expected-view.present)+' missing; '+view.extras+' unregistered. Synthetic model outputs, not official cycle classifications.');
    say('pt-'+kind+'-preservation',def.preservation(packet));say('pt-'+kind+'-raw',JSON.stringify(packet,null,2));
    const input=doc.getElementById('pt-'+kind+'-search');input.value='';
    const render=query=>{if(doc._ptGeneration!==generation)return;const rows=view.table.filter(row=>row.join(' ').toLowerCase().includes(query.toLowerCase()));table(doc,doc.getElementById('pt-'+kind+'-table'),def.headers,rows);say('pt-'+kind+'-count',rows.length+' of '+view.table.length+' rows shown. No ranked financial signal.');};
    input.oninput=e=>render(e.target.value);render('');
   }catch(e){if(doc._ptGeneration!==generation)return;clear(kind);say('pt-'+kind+'-status','Source unavailable: '+e.message+'. No previous values substituted.');}
  }));
 }
 const api={shippingRows,cycleRows,mount};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 else{root.JHPhysicalTradeReview=api;if(root.document?.getElementById('pt-review')){mount(root.document);root.document.getElementById('pt-refresh').onclick=()=>mount(root.document);}}
})(typeof globalThis!=='undefined'?globalThis:this);
