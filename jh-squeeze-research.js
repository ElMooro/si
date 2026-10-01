(function(root){
 'use strict';
 const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
 const INPUTS={finra:'data/finra-short.json',short_interest:'data/short-interest-tickers.json',catalyst:'data/catalyst-calendar.json'};
 const LABELS={finra:'FINRA daily short volume',short_interest:'Reported settlement positions',catalyst:'Catalyst calendar'};
 const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
 const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 function view(packet){
  const unavailable={accepted:false,publication:null,inputs:[],reason:'Squeeze research is unavailable or uses an unqualified legacy contract. No squeeze forecast or trade is established.'};
  if(!object(packet)||packet.contract!=='squeeze-pretrigger-research.v1'||packet.state!=='UNQUALIFIED'||packet.portfolio_action!=='WAIT'||packet.call!==null||FLAGS.some(k=>packet[k]!==false)||packet.independent_investment_votes!==0||!object(packet.inputs))return unavailable;
  const inputs=[];
  for(const [name,path] of Object.entries(INPUTS)){
   const row=packet.inputs[name];
   if(!object(row)||row.artifact!==path||FLAGS.some(k=>row[k]!==false)||row.identity_verified!==false||row.observation_freshness_verified!==false||!['parsed','malformed','reported_error','unavailable'].includes(row.read_status))return unavailable;
   let digest=null,bytes=null;
   if(row.read_status!=='unavailable'){
    if(typeof row.body_sha256!=='string'||!/^[a-f0-9]{64}$/.test(row.body_sha256)||!Number.isSafeInteger(row.body_bytes)||row.body_bytes<0||row.body_bytes>16*1024*1024)return unavailable;
    digest=row.body_sha256;bytes=row.body_bytes;
   }
   inputs.push({label:LABELS[name],path,status:row.read_status,digest,bytes});
  }
  const stamp=packet.generated_at;
  const date=typeof stamp==='string'?stamp.slice(0,10):'';
  const day=new Date(date+'T00:00:00Z');
  const publication=typeof stamp==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(stamp)&&Number(date.slice(0,4))>0&&Number.isFinite(day.getTime())&&day.toISOString().slice(0,10)===date&&Number.isFinite(Date.parse(stamp))?stamp:null;
  return {accepted:true,publication,inputs,reason:'Squeeze probability, expected returns and trade sizing are unqualified. WAIT means abstain; unavailable forecasts do not mean zero setups or a quiet market.'};
 }
 function render(packet){
  const value=view(packet);
  const rows=value.inputs.map(row=>'<li style="margin:10px 0;overflow-wrap:anywhere"><a href="/'+row.path+'">'+escape(row.label)+'</a>: '+escape(row.status==='parsed'?'Received and parsed; freshness unverified':row.status.replaceAll('_',' '))+(row.digest?'<br><small>Received '+row.bytes+' bytes · SHA-256 <code>'+row.digest+'</code></small>':'')+'</li>').join('');
  return '<section class="jh-squeeze-research" aria-label="Squeeze research status" style="min-width:0;grid-column:1/-1;overflow-wrap:anywhere;line-height:1.55;padding:12px"><h3>WAIT · Squeeze forecast unqualified</h3><p>'+escape(value.reason)+'</p>'+(value.accepted?'<p>Publication: '+escape(value.publication??'Unavailable')+'. This is not an observation date.</p><details><summary>Inputs and interpretation</summary><ul>'+rows+'</ul><p>Daily short volume is a trade-flow measure. Settlement short interest is a position measure. Neither establishes borrow utilization, float, covering demand or squeeze probability. Received-body identifiers support the engine audit trail; provider identity and measurement quality remain unverified.</p></details>':'')+'<p>No qualified forecast, position size or execution instruction.</p></section>';
 }
 const api={view,render};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHSqueezeResearch=api;
})(typeof window==='object'?window:globalThis);
