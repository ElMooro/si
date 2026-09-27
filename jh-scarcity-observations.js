/* Complete donor display; source labels do not establish forecasts or independence. */
(function(root){
 'use strict';
 const CONTRACT='scarcity-donor-observations.v1',obj=x=>x&&typeof x==='object'&&!Array.isArray(x)?x:{};
 const text=x=>typeof x==='string'?x:'';
 const escape=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function rows(packet){
  if(packet.measurement_contract===CONTRACT){
   if(!Array.isArray(packet.donor_occurrences)||!Array.isArray(packet.source_inventory))throw Error('Complete donor populations required');
   return packet.donor_occurrences.map((value,i)=>{
    const r=obj(value),raw=obj(r.raw),source=packet.source_inventory.find(s=>obj(s).key===r.source_key)||{};
    return {ticker:text(r.ticker)||text(r.declared_symbol),theme:text(r.declared_theme),source:text(r.source_key),coordinate:text(r.source_pointer),
     generated:text(source.producer_generated_at),meaning:[raw.description,raw.edge,raw.why,raw.interpretation].filter(v=>typeof v==='string').join(' / '),
     original:r.raw,identity:r.source_original,index:i,status:'Upstream meaning and independence unverified'};
   });
  }
  const out=[];
  for(const key of ['vertical_tightness','stealth_shortage_board','prime_setups']){
   if(packet[key]!==undefined&&!Array.isArray(packet[key]))throw Error('Malformed legacy population: '+key);
   (packet[key]||[]).forEach((value,i)=>{const r=obj(value);out.push({ticker:text(r.ticker),theme:text(r.theme_etf)||text(r.vertical),source:'Legacy '+key,
    coordinate:'/'+key+'/'+i,generated:text(packet.generated_at),meaning:text(r.why),original:value,index:out.length,status:'Legacy score and shortage classification unverified'});});
  }
  return out;
 }
 function start(){
  const api=root.JHTableValues,$=id=>root.document.getElementById(id);let all=[],key='index',direction=1;
  function render(){
   const query=$('q').value.trim().toLowerCase(),filtered=all.filter(r=>[r.ticker,r.theme,r.source,r.coordinate,r.meaning].join(' ').toLowerCase().includes(query));
   filtered.sort((a,b)=>api.compare(a,b,key,direction,key==='index'?'number':'text'));
   $('rows').textContent=filtered.length+' shown / '+all.length+' received occurrences. Repeated rows are not independent evidence.';
   const cols=[['index','Occurrence'],['ticker','Reported ticker / symbol'],['theme','Reported theme'],['source','Donor'],['generated','Donor generated at'],['coordinate','Source coordinate']];
   $('board').innerHTML='<table><thead><tr>'+cols.map(([k,label])=>'<th data-k="'+k+'">'+label+'</th>').join('')+'<th scope="col">Reported meaning and full record</th></tr></thead><tbody>'+filtered.map(r=>'<tr>'+cols.map(([k])=>'<td>'+escape(k==='index'?r.index+1:r[k])+'</td>').join('')+'<td>'+escape(r.meaning)+'<p>'+escape(r.status)+'</p><details><summary>Inspect this source occurrence</summary><pre>'+escape(JSON.stringify({source_original:r.identity,raw:r.original},null,2))+'</pre></details></td></tr>').join('')+'</tbody></table>';
   api.bindSort($('board').querySelectorAll('th[data-k]'),key,direction,k=>{direction=key===k?-direction:1;key=k;render();});
  }
  $('q').oninput=render;
  (async()=>{try{
   const received=await api.load('/data/scarcity-radar.json'),p=received.packet;$('original').textContent=received.raw;all=rows(p);
   const native=p.measurement_contract===CONTRACT;
   $('status').textContent=(native?'Research observations; no qualified shortage forecast or position-size signal. ':'Legacy publication: scores and investment claims are unverified. ')+
     'Packet generated: '+(text(p.generated_at)||'unavailable')+'. This is not an observation date.';
   if(native){
    $('sources').innerHTML='<table><thead><tr><th scope="col">Donor</th><th scope="col">Acquisition</th><th scope="col">Reported producer clock</th><th scope="col">Original identity and source populations</th></tr></thead><tbody>'+p.source_inventory.map(value=>{const r=obj(value),c=obj(r.capture);return '<tr><td>'+escape(r.label)+'<br>'+escape(r.key)+'</td><td>'+escape(r.status)+'</td><td>'+escape(r.producer_generated_at)+'<br>'+escape(r.producer_clock_status)+'</td><td><details><summary>Inspect acquisition evidence</summary><pre>'+escape(JSON.stringify({capture:c,populations:r.populations,source_period_freshness:r.source_period_freshness,model_qualification:r.model_qualification},null,2))+'</pre></details></td></tr>';}).join('')+'</tbody></table>';
   }else $('sources').textContent='This legacy packet does not retain complete donor identities. Its full published rows are available below; historical missing rows cannot be reconstructed.';
   render();
  }catch(e){all=[];$('status').textContent='Stored research unavailable: '+e.message;$('board').textContent='No verified display population';$('sources').textContent='Unavailable';$('rows').textContent='';if(e.original_text!==undefined)$('original').textContent=e.original_text;}})();
 }
 const api={CONTRACT,rows,start,escape};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHScarcityObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
