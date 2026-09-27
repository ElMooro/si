/* Macro-page transport context: source populations, not overlapping signal votes. */
(function(root){
 'use strict';
 const freight=typeof module!=='undefined'&&module.exports?require('./jh-freight-review.js'):root.JHFreightReview;
 const shipping=typeof module!=='undefined'&&module.exports?require('./jh-portwatch-review.js'):root.JHPortwatchReview;
 const shared=typeof module!=='undefined'&&module.exports?require('./jh-business-cycle-review.js'):root.JHBusinessCycleReview;
 const PATHS={macro:'/data/macro-leads.json',asia:'/data/asia-leads.json',china:'/data/china-liquidity.json',geo:'/data/geopolitical-risk.json',bis:'/data/bis-crossborder.json',calendar:'/data/econ-calendar.json',freight:'/data/freight-pulse.json',shipping:'/data/portwatch.json'};
 // The retained legacy renderer interpolates HTML. Escape its complete copy;
 // original packets stay unmodified and are inspected through textContent.
 function legacy(value){
  const escape=s=>s.replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  if(typeof value==='string')return escape(value);if(Array.isArray(value))return value.map(legacy);
  if(value&&typeof value==='object'){const result={};for(const [k,v] of Object.entries(value))Object.defineProperty(result,escape(k),{value:legacy(v),enumerable:true,writable:true,configurable:true});return result;}return value;
 }
 function context(kind,p,now=Date.now()){
  if(kind==='freight'){
   const v=freight.view(p,now);return{at:v.at,overdue:v.overdue,native:v.native,authority:false,
    description:v.native?'6 monthly source identities; '+v.observations+' dated observations. Exact-month arithmetic is available on the freight research page.':'Legacy publication: exact-month measurements are pending the original 11:50 UTC run.',
    limitation:'TSI, Cass, truck and rail overlap. Composite scores and inflections do not supply independent votes, GDP forecasts or position instructions.'};
  }
  if(kind==='shipping'){
   const v=shipping.view(p,now);return{at:v.generated_at,overdue:v.overdue,native:v.native,authority:false,
    description:v.rows.length+' port/chokepoint identities. '+(v.native?v.history_rows+' retained observations; exact-calendar vessel-count context is available.':'Legacy publication: complete calendar comparisons are pending the original 11:20 UTC run.'),
    limitation:'Vessel counts are not national export values or cargo weights. Selected gateways, chokepoints, credit flows and bank-claim stocks cannot be counted as independent confirmation of a country slowdown.'};
  }
  throw Error('Unknown transport context');
 }
 async function load(kind,options={}){
  if(!Object.hasOwn(PATHS,kind))throw Error('Undeclared macro packet');
  const controller=new AbortController();let timer,reader;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Macro source request timed out'));},options.timeout||20000);});
  try{return await Promise.race([deadline,(async()=>{const response=await(options.fetcher||root.fetch.bind(root))(PATHS[kind]+'?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});if(!response.ok||!response.body?.getReader)throw Error('Macro source unavailable');reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>64*1024*1024)throw Error('Complete macro packet exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const p of parts){bytes.set(p,offset);offset+=p.byteLength;}const p=shared.strictJSON(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
   const at=shared.clock(p?.generated_at);if(!p||typeof p!=='object'||Array.isArray(p)||at===null||at>(options.now??Date.now()))throw Error('Valid nonfuture macro publication required');return p;})()]);
  }finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 async function mount(doc,loader=load,now=Date.now()){
  if(!doc.getElementById('macro-transport'))return;const generation=(doc._macroTransportGeneration||0)+1;doc._macroTransportGeneration=generation;const current=()=>doc._macroTransportGeneration===generation;
  const say=(kind,suffix,text)=>{doc.getElementById('transport-'+kind+'-'+suffix).textContent=text;};
  for(const kind of ['freight','shipping']){for(const suffix of ['status','description','limitation','original'])say(kind,suffix,'Unavailable');doc.getElementById('transport-'+kind+'-details').ontoggle=null;}
  await Promise.all(['freight','shipping'].map(async kind=>{
   try{const p=await loader(kind);if(!current())return;const v=context(kind,p,now);say(kind,'status',v.at+' · '+(v.overdue?'publication overdue (>26h)':'publication less than 26h old')+' at page load. Observation dates remain separate.');say(kind,'description',v.description);say(kind,'limitation',v.limitation);
    const details=doc.getElementById('transport-'+kind+'-details');let rendered=false;function original(){if(!current()||!details.open||rendered)return;rendered=true;say(kind,'original',JSON.stringify(p,null,2));}details.ontoggle=original;original();
   }catch(e){if(!current())return;say(kind,'status','Unavailable: '+e.message+'. No agreement or slowdown is inferred.');}
  }));
 }
 const api={PATHS,legacy,context,load,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;else{root.JHMacroTransport=api;if(root.document)mount(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
