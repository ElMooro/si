/* jh-reskin-skip */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.JHYieldCurve=api;})(typeof globalThis!=='undefined'?globalThis:this,function(){
 'use strict';
 const months={'1M':1,'3M':3,'6M':6,'1Y':12,'2Y':24,'3Y':36,'5Y':60,'7Y':84,'10Y':120,'20Y':240,'30Y':360};
 function points(data,real=false){
  if(!data||typeof data!=='object')return [];
  return Object.entries(months).flatMap(([tenor,x])=>{
   const item=data[tenor+(real?'_REAL':'')],value=item&&typeof item==='object'?(real?item.value_pct:item.value):item;
   return typeof value==='number'&&Number.isFinite(value)?[{tenor,x,y:value}]:[];
  });
 }
 function validLegacy(packet,now=Date.now()){
  const published=packet?.generated_at,day=packet?.as_of_date;
  if(typeof published!=='string'||!/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(published)||typeof day!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(day))return false;
  const t=Date.parse(published),d=Date.parse(day+'T00:00:00Z');
  const localDay=Date.parse(published.slice(0,10)+'T00:00:00Z');
  if(!Number.isFinite(t)||!Number.isFinite(d)||!Number.isFinite(localDay)||new Date(d).toISOString().slice(0,10)!==day||new Date(localDay).toISOString().slice(0,10)!==published.slice(0,10)||day>new Date(t).toISOString().slice(0,10))return false;
  const age=now-t,observationDays=Math.floor(now/864e5)-Math.floor(d/864e5);
  return packet.methodology_version==='dated-curve.v2'&&packet.quality?.status==='fresh'&&age>=0&&age<=48*36e5&&observationDays>=0&&observationDays<=7;
 }
 async function json(url,timeoutMs=12000,fetcher=fetch){
  const controller=new AbortController();let timer;
  try{
   return await Promise.race([
    (async()=>{const response=await fetcher(url,{cache:'no-store',signal:controller.signal});if(!response.ok)throw new Error('Research response unavailable');return await response.json();})(),
    new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new Error('Research response timed out'));},timeoutMs);})
   ]);
  }finally{clearTimeout(timer);}
 }
 return {points,validLegacy,json};
});
