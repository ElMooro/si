(function(root){
  'use strict';
  const FLAGS=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number=v=>typeof v==='number'&&Number.isFinite(v)?v:null;
  const shares=v=>Number.isSafeInteger(v)&&v>=0?v:null;
  function day(v){
    if(typeof v!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(v)||Number(v.slice(0,4))<1)return null;
    const d=new Date(v+'T00:00:00Z');return Number.isFinite(d.getTime())&&d.toISOString().slice(0,10)===v?v:null;
  }
  function view(row,meta,ticker){
    const blank={status:'unavailable',reason:'Short-position context is unavailable.',shares:null,ratio:null,reportedRatio:null,date:null,ticker:null,pointer:null,digest:null};
    if(!object(meta)||meta.artifact!=='data/short-interest-tickers.json'||meta.read_status!=='parsed'||meta.context_status!=='descriptive_only'||meta.source_contract!=='short-interest-tickers.v1'||meta.context_contract!=='short-position-consumer-context.v1')return blank;
    if(!object(row)||FLAGS.some(k=>row[k]!==false||meta[k]!==false)||row.observation_freshness_verified!==false||row.identity_verified!==false)return blank;
    if(typeof ticker!=='string'||row.ticker!==ticker.toUpperCase()||!/^([A-Z0-9][A-Z0-9.:-]{0,31})$/.test(row.ticker))return blank;
    if(typeof row.reported_ticker_key!=='string'||!/^[A-Za-z0-9][A-Za-z0-9.:-]{0,31}$/.test(row.reported_ticker_key)||row.reported_ticker_key.toUpperCase()!==row.ticker||row.source_row!=='/by_ticker/'+row.reported_ticker_key)return blank;
    if(typeof meta.body_sha256!=='string'||!/^[a-f0-9]{64}$/.test(meta.body_sha256)||!Number.isSafeInteger(meta.body_bytes)||meta.body_bytes<=0||meta.body_bytes>16*1024*1024)return blank;
    const r=number(row.days_to_cover),raw=number(row.reported_days_to_cover),reconstructed=number(row.reported_reconstructed_ratio);
    const provider=row.days_to_cover_basis==='producer_reconciled_provider_display'&&['matches_reconstructed_rounded_ratio','provider_display_floor_one'].includes(row.reported_dtc_status)&&r===raw;
    const rebuilt=row.days_to_cover_basis==='producer_reconstructed_ratio'&&['provider_999_99_convention_unconfirmed','provider_differs_from_reconstructed_ratio'].includes(row.reported_dtc_status)&&r===reconstructed;
    return {status:'descriptive',reason:(row.latest_reported===false?'Explicitly historical settlement row. ':'Settlement positions. ')+'Freshness and security identity are not independently verified.',shares:shares(row.short_interest_shares),ratio:r!==null&&r>=0&&(provider||rebuilt)?r:null,reportedRatio:raw!==null&&raw>=0?raw:null,date:day(row.settlement_date),ticker:row.ticker,pointer:row.source_row,digest:meta.body_sha256};
  }
  function render(row,meta,ticker){
    const v=view(row,meta,ticker);
    const start='<section class="jh-short-position" aria-label="Short-position context"><h5>Reported short positions</h5>';
    if(v.status!=='descriptive')return start+'<p>'+esc(v.reason)+'</p></section>';
    const count=v.shares===null?'Unavailable':v.shares.toLocaleString('en-US')+' shares';
    const ratio=v.ratio===null?'Unavailable':v.ratio.toLocaleString('en-US',{maximumSignificantDigits:8})+' volume-days';
    const discrepancy=v.reportedRatio!==null&&v.ratio!==null&&v.reportedRatio!==v.ratio?'<p>Provider display: '+esc(v.reportedRatio)+'. The producer-reconciled ratio is shown above.</p>':'';
    return start+'<dl><dt>Settlement date</dt><dd>'+esc(v.date??'Unavailable')+'</dd><dt>Reported short interest</dt><dd>'+esc(count)+'</dd><dt>Short interest / reported average daily volume</dt><dd>'+esc(ratio)+'</dd></dl>'+discrepancy+'<p>'+esc(v.reason)+'</p><details><summary>Source and limitations</summary><p>These positions do not measure percentage of float, borrow utilization, covering demand, or squeeze probability.</p><p><a href="/data/short-interest-tickers.json">Reported source packet</a><br>Row: <code>'+esc(v.pointer)+'</code><br>Received body SHA-256: <code>'+esc(v.digest)+'</code></p><p>The digest identifies the received body; this consumer does not retain an immutable original. No Calls, forecast, sizing or execution authority.</p></details></section>';
  }
  const api={view,render};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHShortPosition=api;
})(typeof window==='object'?window:globalThis);
