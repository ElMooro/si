/* Original option contract/bar observations and separate FINRA trading flow. */
(function(root){
 'use strict';
 const CONTRACT='options-flow-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER?v:null;
 const decimal=v=>typeof v==='string'&&/^(0|[1-9]\d{0,29})(?:\.\d{1,12})?$/.test(v)?v:null;
 function source(ref){
  if(ref===undefined)return null;
  const r=obj(ref);if(Object.keys(r).sort().join(',')!=='bytes,format,key,sha256'||!['json','txt'].includes(r.format)||typeof r.sha256!=='string'||!/^[a-f0-9]{64}$/.test(r.sha256)||!Number.isInteger(r.bytes)||r.bytes<0||r.bytes>8*1024*1024||r.key!=='data/options-flow-scanner/sources/'+r.sha256+'.'+r.format)throw Error('Invalid declared public source identity');
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
  const out={native:p.measurement_contract===CONTRACT,bars:[],contracts:[],daily:[],flows:[],requests:[],files:[]};
  function add(mode,v,raw,sources,pointer){out[mode].push({index:out[mode].length,ticker:'',contract:'',date:'',volume:null,call:null,put:null,ratio:null,pct:null,status:'',...v,raw,sources:sources.filter(Boolean),pointer});}
  if(out.native){
   if(!Array.isArray(p.request_records)||!Array.isArray(p.finra_acquisitions)||!Array.isArray(obj(p.finra_file_coverage).files))throw Error('Complete received populations required');
   const originals=p.finra_acquisitions.map(a=>source(obj(a).original_ref));
   p.finra_file_coverage.files.forEach((f,i)=>add('files',{date:text(f.observation_date),status:text(f.status)+'; rows '+String(f.reported_rows??'unavailable')},{coverage:f,acquisition:p.finra_acquisitions[i]},[originals[i]],'/finra_file_coverage/files/'+i));
   p.request_records.forEach((r,ri)=>{
    const a=obj(r.acquisitions),chain=obj(r.contract_population),base='/request_records/'+ri;
    for(const x of [a.contract_pages,a.bar_requests,chain.records,chain.selected_record_indices,r.bar_populations,r.daily_observations,r.finra_observations])if(!Array.isArray(x))throw Error('Incomplete option population');
    const q=source(obj(a.quote).original_ref),pages=a.contract_pages.map(a=>source(obj(a).original_ref)),bars=a.bar_requests.map(a=>source(obj(a).original_ref));
    add('requests',{ticker:text(r.ticker),status:text(obj(a.quote).status)+'; '+text(chain.stop_reason)+'; '+a.bar_requests.length+' selected-contract outcomes'},r,[q,...pages,...bars],base);
    chain.records.forEach((c,i)=>add('contracts',{ticker:text(r.ticker),contract:text(c.contract_id),date:text(c.expiration_date),status:Array.isArray(c.identity_issues)&&c.identity_issues.length?c.identity_issues.join('; '):'Current received contract; historical membership unverified'},c,[pages[c.page_index]],base+'/contract_population/records/'+i));
    r.bar_populations.forEach((g,gi)=>{if(!Array.isArray(g.records))throw Error('Whole bar population required');g.records.forEach((b,bi)=>add('bars',{ticker:text(r.ticker),contract:text(b.contract_id),date:text(b.observation_date),volume:decimal(b.reported_volume_contracts),status:Array.isArray(b.issues)&&b.issues.length?b.issues.join('; '):'Reported bar; direction unknown'},b,[bars[gi]],base+'/bar_populations/'+gi+'/records/'+bi));});
    r.daily_observations.forEach((d,i)=>add('daily',{ticker:text(r.ticker),date:text(d.observation_date),call:decimal(d.call_volume_contracts),put:decimal(d.put_volume_contracts),ratio:decimal(d.call_put_volume_ratio),status:text(d.scope)},d,bars,base+'/daily_observations/'+i));
    r.finra_observations.forEach((f,i)=>add('flows',{ticker:text(r.ticker),date:text(f.observation_date),volume:decimal(f.short_volume_shares),call:decimal(f.short_exempt_volume_shares),put:decimal(f.total_volume_shares),pct:decimal(f.short_volume_pct),status:'Daily off-exchange trading flow; exempt included in short'},f,[originals[f.acquisition_index]],base+'/finra_observations/'+i));
   });return out;
  }
  if(!Array.isArray(p.all_qualifying)&&!Array.isArray(obj(p.summary).top_25_overall)&&!Array.isArray(obj(p.summary).tier_a))throw Error('Dedicated options scanner contract unavailable');
  for(const[key,values]of [['all_qualifying',p.all_qualifying],['summary/top_25_overall',obj(p.summary).top_25_overall],['summary/tier_a',obj(p.summary).tier_a]]){
   if(values!==undefined&&!Array.isArray(values))throw Error('Malformed legacy population');
   (values||[]).forEach((r,i)=>add('bars',{ticker:typeof r==='string'?r:text(obj(r).symbol),status:'Legacy '+key+'; score, tier and direction unqualified'},r,[],'/'+key+'/'+i));
  }return out;
 }
 function summary(p){const m=model(p);return {native:m.native,message:m.native?m.requests.length+' request records, '+m.contracts.length+' returned contracts and '+m.bars.length+' bars. No qualified directional signal.':m.bars.length+' legacy occurrences retained. Scores, tiers and direction remain unqualified.',generated_at:text(p.generated_at)||'unavailable'};}
 function compareDecimal(a,b,key,direction){
  const x=decimal(a[key]),y=decimal(b[key]);if(x===null||y===null)return x===null&&y===null?a.index-b.index:x===null?1:-1;
  const integer=v=>{const[n,f='']=v.split('.');return BigInt(n)*10n**12n+BigInt(f.padEnd(12,'0'));},u=integer(x),v=integer(y);
  return (u<v?-1:u>v?1:0)*direction||a.index-b.index;
 }
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={bars:[],contracts:[],daily:[],flows:[],requests:[],files:[]},page=0,key='index',direction=1,sourceRequest=0;
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
   const mode=$('mode').value||'bars',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const rows=all.filter(r=>[r.ticker,r.contract,r.date,r.status,r.pointer].join(' ').toLowerCase().includes(query));
   const numbers=['index'],decimals=['volume','call','put','ratio','pct'];rows.sort((a,b)=>decimals.includes(key)?compareDecimal(a,b,key,direction):api.compare(a,b,key,direction,numbers.includes(key)?'number':'text'));
   const pages=Math.max(1,Math.ceil(rows.length/100));page=Math.min(Math.max(page,0),pages-1);const shown=rows.slice(page*100,(page+1)*100);
   $('rows').textContent=rows.length+' matching / '+all.length+' received '+mode+' occurrences. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const cols=mode==='daily'?[['index','Occurrence'],['ticker','Underlying'],['date','Observation date'],['call','Selected call volume, contracts'],['put','Selected put volume, contracts'],['ratio','Call / put volume ratio'],['status','Coverage scope']]:mode==='flows'?
    [['index','Occurrence'],['ticker','Literal symbol'],['date','Observation date'],['volume','Short volume, shares'],['call','Included exempt shares'],['put','Total volume, shares'],['pct','Short volume %'],['status','Scope']]:
    [['index','Occurrence'],['ticker','Underlying'],['contract','Contract identifier'],['date',mode==='contracts'?'Expiration date':'Observation date'],['volume','Reported volume, contracts'],['status','Acquisition / qualification']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Complete source and record</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='index'?r.index+1:numbers.includes(k)?(api.format(r[k],mode==='requests'&&k.startsWith('average')?2:0)||'Unavailable'):decimals.includes(k)?r[k]??'Unavailable':r[k])+'</td>').join('')+'<td>'+r.sources.map(s=>'<button type="button" data-source-index="'+(links.push({ref:s.ref,pointer:r.pointer})-1)+'">Inspect whole original</button><br><a target="_blank" rel="noopener" href="'+esc(s.href)+'">Whole original ('+s.bytes+' bytes)</a><details><summary>Source SHA-256</summary><code>'+s.sha256+'</code></details>').join('')+'<details><summary>Inspect '+esc(r.pointer)+'</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   $('board').querySelectorAll('button[data-source-index]').forEach(button=>{button.onclick=()=>{const value=links[Number(button.dataset.sourceIndex)];if(value)inspect(value.ref,value.pointer);};});
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{
   const received=await api.load('/data/options-flow-scanner.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);const s=summary(p);
   $('status').textContent=s.message+' Generated '+s.generated_at+'. Generation time is not the observation date.';
   $('coverage').textContent='Every published occurrence is retained. Duplicate universe memberships repeat observations and are not independent evidence. Original responses retain every contract, bar and FINRA row. Current contract selection is not a historical universe. Missing or invalid responses remain explicit.';render();
  }catch(e){$('status').textContent='Stored options research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,summary,start,source,loadSource,compareDecimal,escape:esc};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHOptionObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
