/* Original FINRA trading flows and market-source coverage; no squeeze inference. */
(function(root){
 'use strict';
 const CONTRACT='microcap-flow-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER?v:null;
 const decimal=v=>typeof v==='string'&&/^(0|[1-9]\d{0,29})(?:\.\d{1,12})?$/.test(v)?v:null;
 function source(ref){
  if(ref===undefined)return null;
  const r=obj(ref);if(Object.keys(r).sort().join(',')!=='bytes,format,key,sha256'||!['json','txt'].includes(r.format)||typeof r.sha256!=='string'||!/^[a-f0-9]{64}$/.test(r.sha256)||!Number.isInteger(r.bytes)||r.bytes<0||r.bytes>8*1024*1024||r.key!=='data/microcap-float-squeeze/sources/'+r.sha256+'.'+r.format)throw Error('Invalid declared public source identity');
  return {href:'/'+r.key,bytes:r.bytes,sha256:r.sha256,ref:r};
 }
 async function loadSource(ref,options={}){
  const declared=source(ref);if(!declared)throw Error('Original source reference required');
  const controller=new AbortController();let reader,timer;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Original source timed out'));},options.timeout??15000);});
  try{return await Promise.race([deadline,(async()=>{
   const response=await(options.fetcher||root.fetch.bind(root))(declared.href+'?exact=1&nogen=1',{cache:'no-store',credentials:'omit',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Whole original unavailable (HTTP '+response.status+')');
   reader=response.body.getReader();const chunks=[];let count=0;
   for(;;){const{done,value}=await reader.read();if(done)break;count+=value.byteLength;if(count>declared.bytes)throw Error('Original source byte count differs');chunks.push(value);}
   if(count!==declared.bytes)throw Error('Original source byte count differs');
   const bytes=new Uint8Array(count);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
   const digest=Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',bytes)),v=>v.toString(16).padStart(2,'0')).join('');
   if(digest!==declared.sha256)throw Error('Original source SHA-256 differs');
   return {raw:new TextDecoder('utf-8',{fatal:true}).decode(bytes),...declared};
  })()]);}finally{clearTimeout(timer);controller.abort();if(reader)reader.cancel().catch(()=>{});}
 }
 function model(p){
  const out={native:p.measurement_contract===CONTRACT,flows:[],requests:[],files:[]};
  if(out.native){
   for(const key of ['request_records','finra_acquisitions'])if(!Array.isArray(p[key]))throw Error('Complete source population required: '+key);
   if(!Array.isArray(obj(p.finra_file_coverage).files))throw Error('Whole FINRA file coverage required');
   const originals=p.finra_acquisitions.map(a=>source(obj(a).original_ref));
   p.finra_file_coverage.files.forEach((v,i)=>{const f=obj(v),a=obj(p.finra_acquisitions[f.acquisition_index]);out.files.push({index:i,ticker:'',date:text(f.observation_date),status:text(f.status),count:number(f.reported_rows),matched:number(f.matched_rows),raw:{coverage:v,acquisition:a},sources:[originals[f.acquisition_index]].filter(Boolean),pointer:'/finra_file_coverage/files/'+i});});
   p.request_records.forEach((value,requestIndex)=>{
    const r=obj(value);for(const key of ['acquisitions','finra_observations'])if(!Array.isArray(r[key]))throw Error('Complete request population required: '+key);
    const refs=r.acquisitions.map(a=>source(obj(a).original_ref)).filter(Boolean),price=obj(r.price_evidence),averages=obj(price.averages);
    out.requests.push({index:requestIndex,ticker:text(r.ticker),date:text(r.latest_parsed_finra_date),count:number(price.records),matched:r.finra_observations.length,
     average30:number(obj(averages['30']).reported_volume_mean),average60:number(obj(averages['60']).reported_volume_mean),
     status:r.acquisitions.map(a=>text(obj(a).endpoint)+': '+text(obj(a).status)).join(' / '),raw:value,sources:refs,pointer:'/request_records/'+requestIndex});
    r.finra_observations.forEach((value,i)=>{const f=obj(value),ref=originals[f.acquisition_index];if(!ref)throw Error('Matched FINRA row requires its complete original file');
     out.flows.push({index:out.flows.length,ticker:text(r.ticker),date:text(f.observation_date),short:decimal(f.short_volume_shares),exempt:decimal(f.short_exempt_volume_shares),total:decimal(f.total_volume_shares),pct:decimal(f.short_volume_pct),
      facilities:Array.isArray(f.facilities)?f.facilities.join(', '):text(f.source_fields&&f.source_fields.Market),status:'Reported daily flow; short interest and float unknown',raw:value,sources:[ref],pointer:'/request_records/'+requestIndex+'/finra_observations/'+i});
    });
   });return out;
  }
  for(const[key,values]of [['all_qualifying',p.all_qualifying],['summary/top_25_overall',obj(p.summary).top_25_overall],['summary/tier_s',obj(p.summary).tier_s]]){
   if(values!==undefined&&!Array.isArray(values))throw Error('Malformed legacy population: '+key);
   (values||[]).forEach((value,i)=>{const r=obj(value);out.flows.push({index:out.flows.length,ticker:typeof value==='string'?value:text(r.symbol),date:'',short:null,exempt:null,total:null,pct:null,facilities:'Unverified',
    status:'Legacy '+key+'; float, days-to-cover and squeeze claims unverified',raw:value,sources:[],pointer:'/'+key+'/'+i});});
  }return out;
 }
 function summary(p){const m=model(p);return {native:m.native,message:m.native?m.requests.length+' request records and '+m.flows.length+' matched FINRA flow occurrences. No qualified squeeze signal.':m.flows.length+' legacy occurrences retained. Float, short-interest and squeeze claims remain unverified.',generated_at:text(p.generated_at)||'unavailable'};}
 function compareDecimal(a,b,key,direction){
  const x=decimal(a[key]),y=decimal(b[key]);if(x===null||y===null)return x===null&&y===null?a.index-b.index:x===null?1:-1;
  const integer=v=>{const[n,f='']=v.split('.');return BigInt(n)*10n**12n+BigInt(f.padEnd(12,'0'));},u=integer(x),v=integer(y);
  return (u<v?-1:u>v?1:0)*direction||a.index-b.index;
 }
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={flows:[],requests:[],files:[]},page=0,key='index',direction=1,sourceRequest=0;
  async function inspect(ref,pointer){
   const request=++sourceRequest;$('source-original').textContent='';$('source-status').textContent='Verifying whole original for '+pointer;
   $('source-status').scrollIntoView({block:'center'});
   try{const received=await loadSource(ref);if(request!==sourceRequest)return;
    $('source-status').textContent='Verified '+received.bytes+' bytes; SHA-256 '+received.sha256+'; '+pointer;
    $('source-original').textContent=received.raw;
   }catch(e){if(request!==sourceRequest)return;$('source-status').textContent='Original source unavailable: '+e.message;}
  }
  function render(){
   sourceRequest++;$('source-status').textContent='Select Inspect whole original in a row to verify and view its source file.';$('source-original').textContent='';const links=[];
   const mode=$('mode').value||'flows',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const rows=all.filter(r=>[r.ticker,r.date,r.status,r.facilities,r.pointer].join(' ').toLowerCase().includes(query));
   const numbers=['index','count','matched','average30','average60'],decimals=['short','exempt','total','pct'];rows.sort((a,b)=>decimals.includes(key)?compareDecimal(a,b,key,direction):api.compare(a,b,key,direction,numbers.includes(key)?'number':'text'));
   const pages=Math.max(1,Math.ceil(rows.length/100));page=Math.min(Math.max(page,0),pages-1);const shown=rows.slice(page*100,(page+1)*100);
   $('rows').textContent=rows.length+' matching / '+all.length+' received '+mode+' occurrences. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const cols=mode==='requests'?[['index','Request occurrence'],['ticker','Requested ticker'],['count','Price rows'],['matched','FINRA rows'],['average30','30-row volume mean'],['average60','60-row volume mean'],['date','Latest parsed FINRA date'],['status','Acquisition outcomes']]:mode==='files'?
    [['index','File occurrence'],['date','Requested observation date'],['count','Reported rows'],['matched','Matched literal symbols'],['status','File status']]:
    [['index','Occurrence'],['ticker','Literal ticker'],['date','Observation date'],['short','Short volume, shares'],['exempt','Included exempt shares'],['total','Total volume, shares'],['pct','Short volume %'],['facilities','Facilities'],['status','Qualification']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Complete source and record</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='index'?r.index+1:numbers.includes(k)?(api.format(r[k],mode==='requests'&&k.startsWith('average')?2:0)||'Unavailable'):decimals.includes(k)?r[k]??'Unavailable':r[k])+'</td>').join('')+'<td>'+r.sources.map(s=>'<button type="button" data-source-index="'+(links.push({ref:s.ref,pointer:r.pointer})-1)+'">Inspect whole original</button><br><a target="_blank" rel="noopener" href="'+esc(s.href)+'">Whole original ('+s.bytes+' bytes)</a><details><summary>Source SHA-256</summary><code>'+s.sha256+'</code></details>').join('')+'<details><summary>Inspect '+esc(r.pointer)+'</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   $('board').querySelectorAll('button[data-source-index]').forEach(button=>{button.onclick=()=>{const value=links[Number(button.dataset.sourceIndex)];if(value)inspect(value.ref,value.pointer);};});
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{
   const received=await api.load('/data/microcap-float-squeeze.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);const s=summary(p);
   $('status').textContent=s.message+' Generated '+s.generated_at+'. Generation time is not the observation date.';
   $('coverage').textContent='Every published occurrence is retained. Duplicate universe memberships repeat observations and are not independent evidence. Full source files retain unmatched symbols and every EOD row. Missing or invalid files remain explicit.';render();
  }catch(e){$('status').textContent='Stored market-flow research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,summary,start,source,loadSource,compareDecimal,escape:esc};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHMicrocapObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
