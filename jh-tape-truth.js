/* Dated observation display contract. No signed-flow inference or freshness TTL. */
(function(root){
'use strict';
const CONTRACT='tape-truth-observations.v2', QUALIFICATION='tape-truth-qualification.v1', PROJECTION='tape-truth-projection.v1';
const unavailable='Tape observations unavailable — missing or unknown contract/publication clock.';
const reason='Directional calls and conviction withheld; these inputs do not establish aggressor direction or participant intent.';
const object=x=>x&&typeof x==='object'&&!Array.isArray(x);
const esc=x=>String(x==null?'':x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const finite=x=>typeof x==='number'&&Number.isFinite(x);
function dateParts(s){
 const m=/^(\d{4})-(\d{2})-(\d{2})/.exec(s);if(!m)return false;
 const y=+m[1],mo=+m[2],day=+m[3];if(y<1||mo<1||mo>12||day<1)return false;
 return day<=new Date(Date.UTC(y<100?y+400:y,mo,0)).getUTCDate();
}
function clock(value,kind,now=Date.now()){
 let status=value==null?'MISSING':'INVALID';
 if(typeof value==='string'&&dateParts(value)){
  if(kind==='date'&&/^\d{4}-\d{2}-\d{2}$/.test(value))status=value>new Date(now).toISOString().slice(0,10)?'FUTURE':'VALID';
  if(kind==='timestamp'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)){
   const h=+value.slice(11,13),m=+value.slice(14,16),s=+value.slice(17,19),n=Date.parse(value);
   if(h<24&&m<60&&s<60&&Number.isFinite(n))status=n>now?'FUTURE':'VALID';
  }
 }
 return {value,kind,status};
}
function qualified(q){return object(q)&&q.contract===QUALIFICATION&&q.status==='WITHHELD'&&q.calls_eligible===false&&q.sizing_eligible===false&&q.freshness==='UNKNOWN';}
function clocks(s,now){return {cvd:clock(s.cvd?.last_day,'date',now),short_vol:clock(s.short_vol?.observation_date,'date',now),gex:clock(s.gex?.source_timestamp,'timestamp',now)};}
function view(packet,ticker,now=Date.now()){
 if(!object(packet)||packet.measurement_contract!==CONTRACT||packet.status!=='RESEARCH'||!qualified(packet.qualification)||clock(packet.generated_at,'timestamp',now).status!=='VALID')return {available:false};
 const s=ticker==='_SPX'?packet.gex_index:packet.symbols?.[ticker];
 if(!object(s)||!qualified(s.qualification))return {available:false};
 return {available:true,observations:s,clocks:clocks(s,now),publication:packet.generated_at};
}
function projection(p,now=Date.now()){
 if(!object(p)||p.contract!==PROJECTION||p.availability!=='AVAILABLE'||p.source_key!=='data/tape-truth.json'||!qualified(p.qualification)||clock(p.source_generated_at,'timestamp',now).status!=='VALID'||!object(p.observations))return {available:false};
 return {available:true,observations:p.observations,clocks:clocks(p.observations,now),publication:p.source_generated_at};
}
function stamp(c){return c.status==='VALID'?c.value:c.status.toLowerCase()+' source date/time';}
function summary(v){
 if(!v.available)return unavailable;
 return reason+' Published '+v.publication+'; CVD ledger session: '+stamp(v.clocks.cvd)+'; FINRA file date: '+stamp(v.clocks.short_vol)+'; GEX provider timestamp: '+stamp(v.clocks.gex)+'. Freshness UNKNOWN: no authoritative observation SLA.';
}
function legText(v,key){return v.available?stamp(v.clocks[key])+' · freshness UNKNOWN':'unavailable';}
function fmt(x,d=2){return finite(x)?x.toLocaleString('en-US',{maximumFractionDigits:d}):'—';}
function bindTicker(render){
 function boot(){
  if(root.__JH_TICKER_BUS){root.__JH_TICKER_BUS.subscribe(render);if(!root.__JH_TICKER_BUS.current())render('');}
  else {const q=new URLSearchParams(root.location.search);render((q.get('ticker')||q.get('t')||'').toUpperCase());}
 }
 if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
}
const api={CONTRACT,QUALIFICATION,PROJECTION,unavailable,reason,esc,finite,clock,view,projection,summary,legText,fmt,bindTicker};
root.JHTapeTruth=api;if(typeof module==='object'&&module.exports)module.exports=api;
})(typeof window==='object'?window:globalThis);
