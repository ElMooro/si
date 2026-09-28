/* Report individual provider rows without inferring their fiscal duration. */
(function(root){
 'use strict';
 const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const text=x=>typeof x==='string'&&x.trim()?x:'Unavailable';
 const amount=x=>typeof x==='number'&&Number.isFinite(x)&&Math.abs(x)<=Number.MAX_SAFE_INTEGER?String(x):'Unavailable';
 function model(packet){
  if(packet?.measurement_contract!=='capex-accounting-measurements.v1')return {available:false,issuers:[],message:'Individual statement observations are unavailable in this legacy publication. Inspect the complete received packet.'};
  if(!Array.isArray(packet.rows))return {available:false,issuers:[],message:'Issuer population missing or malformed. Inspect the complete received packet.'};
  const issuers=packet.rows.map((row,index)=>({row,index})).filter(x=>object(x.row));
  const count=issuers.reduce((n,x)=>n+(Array.isArray(x.row.provider_response)?x.row.provider_response.length:0),0);
  return {available:true,issuers,message:issuers.length+' selectable issuer occurrences; '+count+' received statement rows. '+(packet.rows.length-issuers.length)+' malformed issuer occurrences remain in the complete packet. Repeated tickers remain separate source records.'};
 }
 function statements(row,index){
  const base='/rows/'+index,source=row.provider_response;
  let html='<p class="sub">'+esc(text(row.ticker))+' · '+esc(base)+'. Reported signs and currencies are preserved. These individual statements do not establish an annual total. Filing and acceptance dates are provider-reported, not verified first availability.</p>';
  if(!Array.isArray(source))return html+'<p>Provider response missing or malformed at '+base+'/provider_response. Inspect the parsed issuer and complete packet.</p>';
  html+='<p class="sub">'+source.length+' received statement occurrences. A fiscal-quarter label alone does not prove a duration or make amounts additive.</p>';
  if(!source.length)return html;
  html+='<div class="statement-scroll" role="region" aria-label="Individual capital expenditure statements" tabindex="0"><table><thead><tr>';
  for(const title of ['Reported period end','Explicit start','Fiscal period tag','Fiscal year tag','Calendar year tag','Currency','Capital expenditure (reported sign)','Reported filing date','Reported acceptance','CIK','Producer validation issues','Original source coordinate'])html+='<th scope="col">'+title+'</th>';
  html+='</tr></thead><tbody>';
  source.forEach((original,i)=>{
   const pointer=base+'/provider_response/'+i;
   if(!object(original)){html+='<tr><td colspan="12">Malformed statement at '+pointer+'; retained in the parsed issuer and complete packet.</td></tr>';return;}
   const projections=Array.isArray(row.observations)?row.observations.filter(x=>object(x)&&x.source_row===i):[];
   const matched=projections.length===1&&JSON.stringify(projections[0].original)===JSON.stringify(original);
   const issues=matched&&Array.isArray(projections[0].issues)?(projections[0].issues.length?projections[0].issues.map(x=>typeof x==='string'?x:JSON.stringify(x)).join('; '):'None reported; original filing and first availability unverified'):'Projection missing, ambiguous or inconsistent; original row retained';
   const year=x=>typeof x==='number'?amount(x):text(x);
   const cells=[text(original.date),text(original.startDate),text(original.period),year(original.fiscalYear),year(original.calendarYear),text(original.reportedCurrency),amount(original.capitalExpenditure),text(original.filingDate??original.fillingDate),text(original.acceptedDate),text(original.cik),issues,pointer];
   html+='<tr>'+cells.map(x=>'<td>'+esc(x)+'</td>').join('')+'</tr>';
  });
  return html+'</tbody></table></div>';
 }
 function mount(packet,doc=root.document){
  const select=doc.getElementById('capex-statement-issuer'),status=doc.getElementById('capex-statement-status'),board=doc.getElementById('capex-statements'),detail=doc.getElementById('capex-statement-record'),original=doc.getElementById('capex-statement-original');
  const data=model(packet);let selected=null;
  status.textContent=data.message;select.disabled=!data.available||!data.issuers.length;
  select.innerHTML='<option value="">Choose an issuer occurrence</option>'+data.issuers.map(({row,index})=>'<option value="'+index+'">'+esc(text(row.ticker))+' · rows['+index+']</option>').join('');
  select.value='';detail.hidden=true;detail.open=false;
  board.textContent=data.available?'Choose an issuer to inspect its reported statements.':'No individual statement view is available.';
  original.textContent='Choose an issuer.';
  select.onchange=()=>{
   selected=data.issuers.find(x=>String(x.index)===select.value)||null;
   detail.open=false;detail.hidden=!selected;original.textContent='Open to inspect the selected parsed issuer record.';
   if(!selected){board.textContent='Choose an issuer to inspect its reported statements.';return;}
   board.innerHTML=statements(selected.row,selected.index);
  };
  detail.ontoggle=()=>{if(detail.open&&selected)original.textContent=JSON.stringify(selected.row,null,2);};
 }
 const api={model,statements,mount};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.JHCapexObservations=api;
})(typeof globalThis!=='undefined'?globalThis:this);
