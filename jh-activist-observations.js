/* Complete SEC ownership-feed occurrences with explicit role and source gaps. */
(function(root){
 'use strict';
 const CONTRACT='ownership-filing-observations.v1',obj=v=>v&&typeof v==='object'&&!Array.isArray(v)?v:{},text=v=>typeof v==='string'?v:'';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=v=>typeof v==='number'&&Number.isFinite(v)&&Math.abs(v)<=Number.MAX_SAFE_INTEGER?v:null;
 const decimal=v=>typeof v==='string'&&/^(0|[1-9]\d{0,29})(?:\.\d{1,12})?$/.test(v)?v:null;
 function source(ref){
  if(ref===undefined)return null;
  const r=obj(ref);if(Object.keys(r).sort().join(',')!=='bytes,format,key,sha256'||!['json','xml'].includes(r.format)||typeof r.sha256!=='string'||!/^[a-f0-9]{64}$/.test(r.sha256)||!Number.isInteger(r.bytes)||r.bytes<0||r.bytes>8*1024*1024||r.key!=='data/activist-filings/sources/'+r.sha256+'.'+r.format)throw Error('Invalid declared public source identity');
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
  p=obj(p);const out={native:p.measurement_contract===CONTRACT,entries:[],groups:[],feeds:[],mapping:[],universe:[]};
  function add(mode,v,raw,sources,pointer){out[mode].push({index:out[mode].length,accession:'',name:'',cik:'',role:'',form:'',date:'',ticker:'',status:'',...v,raw,sources:sources.filter(Boolean),pointer});}
  if(out.native){
   for(const k of ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','independent_evidence_eligible','private_state_read_or_written'])if(p[k]!==false)throw Error('Research permissions invalid');
   if(p.call!==null)throw Error('Research call must abstain');
   const a=obj(p.acquisitions),mapping=obj(p.mapping_population),universe=obj(p.universe_population);
   for(const v of [p.entry_occurrences,p.filing_groups,p.feed_coverage,a.feeds,mapping.records,universe.occurrences])if(!Array.isArray(v))throw Error('Complete filing populations required');
   if(a.feeds.length!==8||p.feed_coverage.length!==8)throw Error('All declared feed outcomes required');
   const feeds=a.feeds.map(v=>source(obj(v).original_ref)),map=source(obj(a.mapping).original_ref),members=source(obj(a.universe).original_ref);
   p.entry_occurrences.forEach((r,i)=>{if(!Array.isArray(r.issues)||!Number.isInteger(r.feed_index)||r.feed_index<0||r.feed_index>=8)throw Error('Entry source coordinate invalid');
    add('entries',{accession:text(r.accession),name:text(r.name)||text(r.atom_title),cik:text(r.cik),role:text(r.role),form:text(r.reported_form),date:text(r.feed_updated_at),status:r.issues.length?r.issues.join('; '):'Explicit Atom role; filing acceptance time unverified'},r,[feeds[r.feed_index]],'/entry_occurrences/'+i);});
   p.filing_groups.forEach((g,i)=>{if(!Array.isArray(g.parties)||!Array.isArray(g.current_ticker_candidates)||!Array.isArray(g.occurrence_indices)||!Array.isArray(g.identity_issues))throw Error('Complete role group required');
    const originals=g.occurrence_indices.map(n=>{if(!Number.isInteger(n)||n<0||n>=p.entry_occurrences.length)throw Error('Group source coordinate invalid');return feeds[p.entry_occurrences[n].feed_index];});
    add('groups',{accession:text(g.accession),name:g.parties.map(x=>text(x.role)+': '+(Array.isArray(x.names)?x.names.join(' / '):'')).join('; '),cik:text(g.subject_cik),form:text(g.canonical_form),ticker:g.current_ticker_candidates.join(', '),status:text(g.role_group_status)+(g.identity_issues.length?'; '+g.identity_issues.join('; '):'')},g,[...new Set(originals),map,members],'/filing_groups/'+i);});
   p.feed_coverage.forEach((f,i)=>add('feeds',{form:text(f.requested_form),date:text(f.feed_updated_at),status:text(f.status)+'; returned entries '+String(f.returned_entries??'unavailable')},{coverage:f,acquisition:a.feeds[i]},[feeds[i]],'/feed_coverage/'+i));
   mapping.records.forEach((v,i)=>add('mapping',{ticker:text(v.ticker),cik:text(v.cik),name:text(obj(v.raw).title),status:v.mapping_row_valid===true?'Reported current mapping; historical identity unverified':'Invalid mapping row retained'},v,[map],'/mapping_population/records/'+i));
   universe.occurrences.forEach((v,i)=>add('universe',{ticker:text(v.literal_symbol),status:'Original universe occurrence; duplicates retained'},v,[members],'/universe_population/occurrences/'+i));
   return out;
  }
  if(!Array.isArray(p.all_filings))throw Error('Dedicated ownership filing packet unavailable');
  const populations=[['all_filings',p.all_filings],...Object.entries(obj(p.summary)).map(([k,v])=>['summary/'+k,v])];
  for(const [key,values] of populations){if(!Array.isArray(values))throw Error('Malformed legacy population');values.forEach((v,i)=>{const r=obj(v);add('entries',{accession:text(r.accession),name:text(r.filer_name),cik:text(r.subject_cik),form:text(r.form_type),date:text(r.filing_date),ticker:text(r.subject_ticker)||text(r.ticker),status:'Legacy occurrence; roles, coverage, scores and intent unqualified'},v,[],'/'+key+'/'+i);});}
  return out;
 }
 function summary(p){const m=model(p);return{native:m.native,message:m.native?m.entries.length+' received Atom occurrences; '+m.groups.length+' accession groups; '+m.feeds.length+' feed outcomes. No qualified ownership or activist signal.':m.entries.length+' legacy occurrences retained. Feed success, identity and investment tiers are unqualified.',generated_at:text(p.generated_at)||'unavailable'};}
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let data={entries:[],groups:[],feeds:[],mapping:[],universe:[]},page=0,key='index',direction=1,sourceRequest=0;
  async function inspect(ref,pointer){
   const request=++sourceRequest;$('source-original').textContent='';$('source-status').textContent='Verifying whole original for '+pointer;$('source-status').scrollIntoView({block:'center'});
   try{const received=await loadSource(ref);if(request!==sourceRequest)return;$('source-status').textContent='Verified '+received.bytes+' bytes; SHA-256 '+received.sha256+'; '+pointer;$('source-original').textContent=received.raw;}
   catch(e){if(request===sourceRequest)$('source-status').textContent='Original source unavailable: '+e.message;}
  }
  function render(){
   sourceRequest++;$('source-status').textContent='Select Inspect whole original to verify the complete source file.';$('source-original').textContent='';const links=[];
   const mode=$('mode').value||'entries',all=data[mode]||[],query=$('q').value.trim().toLowerCase();
   const rows=all.filter(r=>[r.accession,r.name,r.cik,r.role,r.form,r.date,r.ticker,r.status,r.pointer].join(' ').toLowerCase().includes(query));
   rows.sort((a,b)=>api.compare(a,b,key,direction,key==='index'?'number':'text')||a.index-b.index);
   const pages=Math.max(1,Math.ceil(rows.length/100));page=Math.min(Math.max(page,0),pages-1);const shown=rows.slice(page*100,(page+1)*100);
   $('rows').textContent=rows.length+' matching / '+all.length+' received '+mode+' occurrences. Page '+(page+1)+' of '+pages+'.';$('previous').disabled=page===0;$('next').disabled=page+1===pages;
   const cols=mode==='mapping'||mode==='universe'?[['index','Occurrence'],['ticker','Current literal ticker'],['cik','Reported CIK'],['name','Reported name'],['status','Scope']]:mode==='feeds'?[['index','Query'],['form','Requested form'],['date','Feed updated timestamp'],['status','Acquisition coverage']]:
    [['index','Occurrence'],['accession','Reported accession'],['form','Form'],['role','Explicit role'],['name','Reported names'],['cik','CIK'],['date','Atom updated / legacy date'],['ticker','Current ticker candidates'],['status','Identity and scope']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Complete source and record</th></tr></thead><tbody>'+shown.map(r=>'<tr>'+cols.map(([k])=>'<td>'+esc(k==='index'?r.index+1:r[k]||'Unavailable')+'</td>').join('')+'<td>'+r.sources.map(s=>'<button type="button" data-source-index="'+(links.push({ref:s.ref,pointer:r.pointer})-1)+'">Inspect whole original</button><details><summary>Source SHA-256</summary><code>'+s.sha256+'</code></details>').join('')+'<details><summary>Inspect '+esc(r.pointer)+'</summary><pre>'+esc(JSON.stringify(r.raw,null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   $('board').querySelectorAll('button[data-source-index]').forEach(button=>{button.onclick=()=>{const value=links[Number(button.dataset.sourceIndex)];if(value)inspect(value.ref,value.pointer);};});
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;page=0;render();});
  }
  $('q').oninput=()=>{page=0;render();};$('mode').onchange=()=>{page=0;key='index';direction=1;render();};$('previous').onclick=()=>{page--;render();};$('next').onclick=()=>{page++;render();};
  (async()=>{try{const received=await api.load('/data/activist-filings.json'),p=received.packet;$('original').textContent=received.raw;data=model(p);const s=summary(p);
   $('status').textContent=s.message+' Generated '+s.generated_at+'. Collection time is not a filing acceptance or ownership-event timestamp.';
   $('coverage').textContent='Every received entry, co-filer, mapping and universe occurrence remains inspectable. Repeated entries, amendments and share classes do not create independent investors. A current-feed snapshot does not establish complete historical coverage.';render();
  }catch(e){$('status').textContent='Stored ownership research unavailable: '+e.message;$('board').textContent='No verified display population';$('rows').textContent='';$('previous').disabled=true;$('next').disabled=true;if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,model,summary,start,source,loadSource,escape:esc};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHActivistObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
