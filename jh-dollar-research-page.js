(function(root){
  'use strict';
  async function boot(host){
    const D=root.JHDollarResearch,A=root.JHOptionResearch,q=s=>host.querySelector(s),fetcher=root.fetch.bind(root),params=new URL(root.location.href).searchParams;
    const pinned=params.has('run')?params.get('run'):null;let sid=params.get('series')||'DTWEXBGS',packet=null,epoch=0,controller=null,result=null,page=0;
    const fields=['initial','return_pct','entry','future','cost'];
    function clearScenario(reset=false){result=null;q('[data-dr-result]').textContent='';if(reset)for(const name of fields)q('[name='+name+']').value='';}
    function clear(){packet=null;clearScenario(true);for(const name of ['detail','chart','comparisons','rows','source','derived','dependencies','recorded','scenario-note'])q('[data-dr-'+name+']').textContent='';
      for(const name of ['series','export','calculate','previous','next','row'])q('[data-dr-'+name+']').disabled=true;}
    function renderRows(){const row=D.selected(packet,sid),rows=row.history.slice(page*100,(page+1)*100);
      q('[data-dr-rows]').innerHTML=A.table(['Observation date','Original row','Reported value','Unit'],rows.map(p=>[p.date,p.original_row_index,D.exact(p.exact_value),row.unit]),'Rows '+(rows.length?page*100+1:0)+'–'+(page*100+rows.length)+' of '+row.history.length);
      q('[data-dr-previous]').disabled=page===0;q('[data-dr-next]').disabled=(page+1)*100>=row.history.length;}
    function sourceDetail(){const row=D.selected(packet,sid),index=Number(q('[data-dr-row]').value),point=row.history[index];
      if(!point){q('[data-dr-source]').textContent='No dated source row available.';return;}
      q('[data-dr-source]').innerHTML='<h3>Original observation '+point.original_row_index+'</h3><p>'+A.esc(point.date)+' · '+A.esc(D.exact(point.exact_value))+' '+A.esc(row.unit)+'</p>'+A.table(['Evidence property','Recorded identity'],[
        ['Series',sid],['Definition title',row.source_title],['Source acquisition',row.acquired_at],['Provider metadata update',row.provider_updated_at??'Not reported'],
        ['Publication time','Not established by acquisition time'],['Original observation location','/observations/'+point.original_row_index],
        ['Observation response SHA-256',row.evidence.observations?.sha256??'Unavailable'],['Definition response SHA-256',row.evidence.definition?.sha256??'Unavailable']], 'Inspect the retained provider response')+
        '<p><a href="'+A.esc(row.source_url)+'" target="_blank" rel="noopener">Official FRED series and definition</a>. Complete original responses are retained for runner replay. The export includes their exact keys, digests and row positions.</p>';
    }
    function renderDerived(){const d=packet.derived,m=d.us_germany_monthly.latest,n=d.net_liquidity;q('[data-dr-derived]').innerHTML=
      A.table(['Dated comparison','Value','Scope'],[
        ['US 10-year minus 2-year yield',D.formatted(d.treasury_curve.difference_bps.exact_value)+' bps',d.treasury_curve.available?d.treasury_curve.left.date:'Identical eligible observation dates required'],
        ['US minus German monthly yield',m?D.formatted(m.difference_bps.exact_value)+' bps':'Unavailable',m?m.reference_month+'; '+m.numeric_us_rows+' numeric US observations':'No matched closed month'],
        ['WALCL − TGA − RRP',D.exact(n.net_decimal)+' USD millions','Mixed dates: Wednesday assets, weekly-average TGA and daily reverse repo']], 'Descriptive calculations with their actual scopes')+
      '<p>'+A.esc(d.us_germany_monthly.limitation)+'</p>'+A.table(['Liquidity component','Original value','Original unit','Observation date','Multiplier to USD millions'],Object.entries(n.components).map(([id,v])=>[id,D.exact(v.value_decimal),v.unit??'Unavailable',v.date??'Unavailable',v.multiplier_to_usd_millions]),'Each signed liquidity leg remains separate')+
      '<details><summary>Matched monthly US / German rate history</summary>'+A.table(['Month','US daily mean %','German monthly %','Difference bps','Numeric US rows','Coverage'],d.us_germany_monthly.trail.map(v=>[v.reference_month,D.formatted(v.us_daily_mean.exact_value),D.exact(v.german.exact_value),D.formatted(v.difference_bps.exact_value),v.numeric_us_rows,v.available?'Minimum sample and returned span met; market calendar unverified':'Insufficient comparable observations']),'Closed calendar months; different sovereign yield definitions')+'</details>';
    }
    function inspect(){if(!packet)return;clearScenario(true);page=0;sid=q('[data-dr-series]').value;const row=D.selected(packet,sid);
      q('[data-dr-detail]').innerHTML='<h2>'+A.esc(row.label)+'</h2><p class="dr-value">'+A.esc(D.exact(row.latest_observation?.exact_value))+' <span>'+A.esc(row.unit)+'</span></p><p>Observation '+A.esc(row.latest_observation?.date??'unavailable')+' · '+A.esc(row.frequency==='D'?'Daily':row.frequency==='W'?'Weekly':'Monthly')+' · <span data-dr-review>'+A.esc(D.review(row))+'</span>.</p>'+
        (row.quote?'<p>Original quote: '+A.esc(row.quote.numerator)+' per '+A.esc(row.quote.denominator)+'. USD per '+A.esc(row.quote.currency)+': '+A.esc(D.formatted(row.quote.usd_per_foreign_unit.exact_value))+'. '+A.esc(row.quote.currency)+' per USD: '+A.esc(D.formatted(row.quote.foreign_units_per_usd.exact_value))+'.</p>':'')+
        '<p>Acquired '+A.esc(row.acquired_at??'unavailable')+'; source review due '+A.esc(row.source_review_due_at??'unknown')+'. This is a recorded observation, not a live executable price.</p>';
      q('[data-dr-chart]').innerHTML=D.chart(packet,sid);q('[data-dr-comparisons]').innerHTML=D.comparisonsView(packet,sid);renderRows();
      q('[data-dr-row]').innerHTML=row.history.map((v,i)=>'<option value="'+i+'">'+v.date+' · original row '+v.original_row_index+'</option>').join('');
      q('[data-dr-row]').disabled=!row.history.length;q('[data-dr-row]').value=String(row.history.length-1);sourceDetail();
      q('[data-dr-recorded]').innerHTML='<a href="'+A.esc(D.recordedUrl(packet,sid))+'">Keep this exact recorded run and series</a> · Compiled '+A.esc(packet.generated_at)+'; canonical source compiled '+A.esc(packet.source_generated_at)+'.';
      const currency=D.FX[sid]?.[0];q('[data-dr-calculate]').disabled=!currency||!row.latest_observation||row.latest_observation.exact_value===null;
      q('[data-dr-scenario-note]').textContent=currency?'Local value and return refer to '+currency+' assets. Enter both assumed exchange rates as USD per one '+currency+', even if the source quote above uses the inverse. Costs and results are USD.':'Choose a bilateral currency series to calculate an explicit local-asset exposure scenario.';
      q('[data-dr-export]').disabled=false;
    }
    async function refresh(){const token=++epoch;controller?.abort();controller=new AbortController();clear();q('[data-dr-status]').textContent='Verifying recorded Dollar run, input, output and compiler artifacts…';
      try{const p=pinned!==null?await D.recordedRun(pinned,fetcher,controller.signal):await D.verifyPacket(JSON.parse((await D.load(D.CURRENT,fetcher,controller.signal)).text),fetcher,controller.signal);
        if(token!==epoch)return;if(!D.SERIES.includes(sid))throw Error('Requested series is outside this recorded universe');packet=p;
        q('[data-dr-series]').innerHTML=D.SERIES.map(id=>'<option value="'+id+'">'+A.esc(id+' · '+p.series[id].label)+'</option>').join('');q('[data-dr-series]').value=sid;q('[data-dr-series]').disabled=false;
        q('[data-dr-status]').textContent='Recorded artifacts verified. '+p.quality.available_original_histories+' of '+p.quality.declared_series+' source histories; no qualified investment votes.';
        renderDerived();q('[data-dr-dependencies]').innerHTML=A.table(['Source family','Series'],Object.entries(p.dependency_graph.series_families).map(([key,ids])=>[key,ids.join(', ')]),'Shared source families; not independent confirmations')+
          p.dependency_graph.known_overlap.map(v=>'<p>'+A.esc(v.members.join(', '))+': '+A.esc(v.basis)+'</p>').join('')+
          '<details><summary>Retained context and prior publications</summary><p>The previous Dollar packet, complete legacy history and seven context packets remain retained with exact digests. Context does not become an extra vote.</p><pre>'+A.esc(JSON.stringify({contexts:p.retained_contexts,predecessors:p.retained_predecessors},null,2))+'</pre></details>';inspect();
      }catch(error){if(token===epoch&&error.name!=='AbortError'){clear();q('[data-dr-status]').textContent='Recorded evidence unavailable: '+error.message+'. No latest or legacy substitute was used.';}}
    }
    q('[data-dr-series]').onchange=inspect;q('[data-dr-row]').onchange=sourceDetail;q('[data-dr-previous]').onclick=()=>{page--;renderRows();};q('[data-dr-next]').onclick=()=>{page++;renderRows();};
    q('[data-dr-refresh]').textContent=pinned!==null?'Recheck this recorded run':'Refresh and verify';q('[data-dr-refresh]').onclick=()=>void refresh();
    for(const name of fields)q('[name='+name+']').oninput=()=>clearScenario();
    q('[data-dr-scenario]').onsubmit=event=>{event.preventDefault();clearScenario();if(!packet||q('[data-dr-calculate]').disabled)return;
      try{result=D.scenario(packet,sid,Object.fromEntries(fields.map(name=>[name,q('[name='+name+']').value.trim()])));
        q('[data-dr-result]').innerHTML='<div class="or-result"><strong>Net assumed change: '+A.esc(result.net_change_usd)+' USD</strong>'+A.table(['Effect','USD'],[
          ['Initial assumed USD value',result.initial_usd],['Local asset return at initial FX rate',result.local_return_effect_usd],['FX change on initial local value',result.fx_effect_usd],['Local return / FX interaction',result.interaction_effect_usd],['Assumed total costs',result.cost_usd]],'Exact decimal scenario attribution')+'<p>'+A.esc(result.formula)+'</p><p>'+A.esc(result.scope)+'</p></div>';
      }catch(error){q('[data-dr-result]').textContent=error.message;}};
    q('[data-dr-export]').onclick=()=>{if(!packet||q('[data-dr-export]').disabled)return;const blob=new Blob([JSON.stringify({...D.evidence(packet,sid),scenario:result},null,2)+'\n'],{type:'application/json'}),url=URL.createObjectURL(blob),a=document.createElement('a');
      a.href=url;a.download='justhodl-'+sid+'-recorded-dollar.json';a.click();URL.revokeObjectURL(url);};
    root.setInterval(()=>{const node=q('[data-dr-review]');if(node&&packet)node.textContent=D.review(packet.series[sid]);},60000);await refresh();
  }
  if(root.document)root.document.querySelectorAll('[data-dollar-research]').forEach(host=>void boot(host));
})(typeof window!=='undefined'?window:globalThis);
