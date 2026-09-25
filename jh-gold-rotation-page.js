(function(root){
 'use strict';const A=root.JHOptionResearch,G=root.JHGoldRotationResearch;
 function boot(){const host=document.querySelector('[data-gold-rotation]');if(!host)return;
  const q=s=>host.querySelector(s),params=new URLSearchParams(location.search),pin=params.has('run')?params.get('run'):null;
  let packet=null,controller=null,epoch=0,page=0,calculation=null;
  const regions=['record','coverage','identity','returns','history','sources','ratio','ratio-history','ratio-basis','page','scenario'];
  const stamp=s=>typeof s==='string'?s.replace('T',' ').replace(/(?:Z|\+00:00)$/,' UTC'):'Unreported';
  const fmt=G.format,selected=()=>[q('[data-gr-symbol]').value,q('[data-gr-basis]').value];
  function clearCalculation(){calculation=null;q('[data-gr-scenario]').textContent='';}
  function select(){if(!packet)return;clearCalculation();const [symbol,kind]=selected(),asset=G.selection(packet,symbol,kind),history=asset.history.slice().reverse(),n=Math.max(1,Math.ceil(history.length/25));page=Math.min(page,n-1);
   q('[data-gr-record]').innerHTML='Compiled '+A.esc(stamp(packet.generated_at))+' · <a href="'+A.esc(G.recordedUrl(packet,symbol,kind))+'">Keep this exact record and selection</a>';
   q('[data-gr-identity]').textContent=symbol+' · '+(asset.provider_title||'Identity unavailable')+' · '+(asset.isin||'Unverified ISIN')+' · '+(asset.currency||'Unverified currency')+' · observation '+(asset.observation_date||'unavailable')+' · first publication time unknown';
   q('[data-gr-returns]').innerHTML=A.table(['Intervals','Change, %','First observation','Last observation','Available / required'],Object.entries(asset.returns?.[kind]||{}).map(([n,v])=>[n,fmt(v.value_decimal),v.start_date,v.end_date,v.observations_available+' / '+v.observations_required]),'Dated price changes');
   const fields=kind==='full'?['open','high','low','close','volume','vwap']:kind==='light'?['price','volume']:['adjOpen','adjHigh','adjLow','adjClose','volume'];
   q('[data-gr-history]').innerHTML=A.table(['Observation date','Original row index',...fields],history.slice(page*25,page*25+25).map(row=>{const source=row.source_rows[kind];return [row.date,source?.source_row_index??'Missing',...fields.map(f=>fmt(source?.values[f]))];}),'Recorded '+symbol+' observations · prices in USD, volume in shares');
   q('[data-gr-page]').textContent='Page '+(page+1)+' of '+n+' · '+history.length+' union dates';q('[data-gr-older]').disabled=page+1>=n;q('[data-gr-newer]').disabled=page===0;
   q('[data-gr-sources]').innerHTML=A.table(['Endpoint','Acquired at, UTC','HTTP status','Retained original hash'],Object.entries(asset.originals).map(([k,v])=>[k,stamp(v.received_at),v.http_status??'Unavailable',v.original?.sha256??'Unavailable']),'Recorded source captures');
   const basis=kind==='dividend-adjusted'?'dividend-adjusted':'full',ratio=packet.ratios[basis];
   q('[data-gr-ratio-basis]').textContent='Ratio basis: '+basis+'. '+(kind==='light'?'Legacy light is available for inspection; ratio calculations use the separate full endpoint. ':'')+ratio.coverage.matched_dates+' matched observations of '+ratio.coverage.union_dates+' union dates. No verified market-calendar completeness.';
   const rows=[['Latest SPY / GLD',fmt(ratio.latest?.value_decimal),ratio.latest?.date??'Unavailable'],...Object.entries(ratio.moving_averages).map(([n,v])=>[n+'-observation mean',fmt(v.value_decimal),v.start_date+' → '+v.end_date]),['Z-score, standard deviations',fmt(ratio.zscore.value_decimal),ratio.zscore.start_date+' → '+ratio.zscore.end_date],['Five-interval persistence',({1:'Every interval rising','-1':'Every interval falling',0:'No strict direction'})[ratio.persistence.direction]??'Incomplete',ratio.persistence.observation_dates.join(' · ')]];
   q('[data-gr-ratio]').innerHTML=A.table(['Measurement','Value','Dates'],rows,'Descriptive ratio statistics');
   q('[data-gr-ratio-history]').innerHTML=A.table(['Date','SPY price','GLD price','SPY / GLD','SPY source row','GLD source row'],ratio.history.slice(-25).reverse().map(v=>[v.date,fmt(v.numerator_decimal),fmt(v.denominator_decimal),fmt(v.value_decimal),v.spy_row_index??'Missing',v.gld_row_index??'Missing']),'Matched dated ratios');
   q('[data-gr-calculate]').disabled=kind!=='full'||asset.latest?.full==null;
   if(kind!=='full')q('[data-gr-scenario]').textContent='Select Reported full close to calculate an assumed share-price effect.';
  }
  async function refresh(){const mine=++epoch;controller?.abort();controller=new AbortController();packet=null;page=0;clearCalculation();
   for(const name of regions)q('[data-gr-'+name+']').textContent='';for(const name of ['export','calculate','older','newer'])q('[data-gr-'+name+']').disabled=true;
   q('[data-gr-status]').textContent='Checking the recorded publication and qualified compiler identities…';
   try{const result=pin!==null?await G.recordedRun(pin,root.fetch.bind(root),controller.signal):await G.verified(JSON.parse((await G.load(G.CURRENT,root.fetch.bind(root),controller.signal)).text),root.fetch.bind(root),controller.signal);
    if(mine!==epoch)return;packet=result;
    const symbol=params.get('symbol')??'GLD',basis=params.get('basis')??'full';G.selection(packet,symbol,basis);q('[data-gr-symbol]').value=symbol;q('[data-gr-basis]').value=basis;
    q('[data-gr-coverage]').innerHTML=A.table(['Recorded coverage','Count'],[['Funds with history',packet.quality.instruments_with_history+' / 8'],['Retained source endpoints',packet.quality.source_endpoints_retained+' / 32'],['Qualified investment votes',0]],'Coverage at compilation');
    select();q('[data-gr-export]').disabled=false;q('[data-gr-status]').textContent='Recorded publication and qualified compiler bytes verified. Dated observations; no qualified allocation signal.';
   }catch(error){if(mine!==epoch||error.name==='AbortError')return;packet=null;clearCalculation();for(const name of regions)q('[data-gr-'+name+']').textContent='';q('[data-gr-status]').textContent=error.message+'. No latest or legacy substitute was used.';}
  }
  q('[data-gr-symbol]').onchange=q('[data-gr-basis]').onchange=()=>{page=0;select();};
  q('[data-gr-older]').onclick=()=>{page++;select();};q('[data-gr-newer]').onclick=()=>{page=Math.max(0,page-1);select();};
  for(const name of ['shares','change','cost'])q('[data-gr-'+name+']').oninput=clearCalculation;
  q('[data-gr-form]').onsubmit=e=>{e.preventDefault();clearCalculation();if(!packet||q('[data-gr-calculate]').disabled)return;
   try{calculation=G.scenario(packet,...selected(),q('[data-gr-shares]').value.trim(),q('[data-gr-change]').value.trim(),q('[data-gr-cost]').value.trim());
    q('[data-gr-scenario]').innerHTML=A.table(['Explicit assumption result','USD'],[['Dated reference exposure',fmt(calculation.exposure_usd)],['Assumed price effect',fmt(calculation.price_effect_usd)],['Effect after assumed costs',fmt(calculation.net_effect_usd)]],'Hypothetical exposure effect')+'<p>Reference: '+A.esc(calculation.symbol+' on '+calculation.observation_date)+'. '+A.esc(calculation.formula)+'.</p>';
   }catch(error){q('[data-gr-scenario]').textContent=error.message;}
  };
  q('[data-gr-refresh]').onclick=refresh;q('[data-gr-export]').onclick=()=>{if(!packet||q('[data-gr-export]').disabled)return;
   const payload={...G.evidence(packet),selection:{symbol:selected()[0],basis:selected()[1]},assumed_exposure:calculation};
   const url=URL.createObjectURL(new Blob([JSON.stringify(payload,null,2)+'\n'],{type:'application/json'})),a=document.createElement('a');a.href=url;a.download='gold-equity-research-evidence.json';a.hidden=true;document.body.appendChild(a);
   try{a.click();}finally{a.remove();root.setTimeout(()=>URL.revokeObjectURL(url),1000);}
  };refresh();
 }
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot);else boot();
})(window);
