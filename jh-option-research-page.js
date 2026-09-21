(function(root){
  'use strict';
  async function boot(host){
    const A=root.JHOptionResearch;if(!A||!host)return;
    const q=s=>host.querySelector(s),fetcher=root.fetch.bind(root),form=q('[data-or-scenario]'),result=q('[data-or-scenario-output]');
    let packet=null,currentChain=null,rows=[],selected=null,controller=null,publication=0,selection=0,pageRequest=0;
    const params=new URL(root.location.href).searchParams,requested=params.get('underlying'),pinned=params.get('run');let symbol=A.ticker(requested)?requested:'SPY';
    if(pinned!==null)q('[data-or-refresh]').textContent='Recheck recorded publication';
    let lastReview='';
    function invalidate(){selected=null;form.reset();q('[data-or-scenario-selection]').textContent='Select a verified standard contract above.';result.textContent='Source or selection changed; select a contract and recalculate your hypothetical scenario.';}
    function clear(){
      currentChain=null;rows=[];invalidate();
      for(const name of ['chain','dates','records','row-detail'])q('[data-or-'+name+']').textContent='';
      q('[data-or-page]').innerHTML='<option>No source page loaded</option>';q('[data-or-page]').disabled=true;
      q('[data-or-prev]').disabled=q('[data-or-next]').disabled=true;
    }
    form.oninput=()=>{result.textContent='Assumptions changed; recalculate the hypothetical payoff.';};
    form.onsubmit=event=>{
      event.preventDefault();
      try{
        const assumptions=Object.fromEntries(['quantity','premium','spot','cost'].map(name=>[name,form.elements[name].value.trim()]));
        const out=A.scenario(packet,selected,assumptions);
        result.innerHTML='<div class="or-result"><p>'+A.esc(out.contract_id)+' · expiration '+A.esc(out.expiration_date)+'</p><p>Hypothetical net payoff: <strong>USD '+A.exact(out.net_usd)+'</strong>.</p>'+A.table(
          ['Cashflow','USD'],[['Premium paid / received',A.exact(out.premium_cash_usd)],['Expiration intrinsic value',A.exact(out.intrinsic_value_usd)],['Assumed costs',A.exact(out.costs_usd)],['Maximum loss within this payoff model',out.maximum_loss_usd===null?'Unbounded':A.exact(out.maximum_loss_usd)]],'Hypothetical payoff components')+
          '<p>'+A.esc(out.formula)+'</p><p>'+A.esc(out.scope)+'</p><p class="or-digest">Evidence run '+A.esc(out.run.manifest_key)+'</p><button type="button" data-or-export>Download assumptions and result</button></div>';
        q('[data-or-export]').onclick=()=>{
          const blob=new Blob([JSON.stringify({...out,calculated_at:new Date().toISOString()},null,2)],{type:'application/json'}),url=root.URL.createObjectURL(blob),a=document.createElement('a');
          a.href=url;a.download='justhodl-option-scenario-'+out.contract_id.replace(/[^A-Za-z0-9.-]/g,'_')+'.json';a.click();root.setTimeout(()=>root.URL.revokeObjectURL(url),1000);
        };
      }catch(error){result.textContent=error.message;}
    };
    function inventory(){
      if(!packet)return;q('[data-or-inventory]').innerHTML=A.inventory(packet,q('[data-or-query]').value);
      q('[data-or-inventory]').querySelectorAll('[data-or-pick]').forEach(button=>{button.onclick=()=>{
        symbol=button.dataset.orPick;q('[data-or-symbol]').value=symbol;void inspect();q('[data-or-chain]').scrollIntoView({block:'start',behavior:'smooth'});
      };});
    }
    function renderRows(){
      q('[data-or-records]').innerHTML=A.rowTable(rows,q('[data-or-contract-query]').value);
      q('[data-or-records]').querySelectorAll('[data-or-row]').forEach(button=>{button.onclick=()=>{
        selected=rows.find(row=>row.evidence.row_index===Number(button.dataset.orRow));
        if(!selected)return;form.reset();q('[data-or-row-detail]').innerHTML=A.rowView(selected);
        q('[data-or-scenario-selection]').textContent=(selected.contract_id||'Malformed row')+' · '+(selected.identity_eligible?'valid contract identity':'identity is unqualified; scenario unavailable');
        result.textContent='Contract selected. Enter your assumptions and calculate a hypothetical payoff.';
        q('[data-or-row-detail]').scrollIntoView({block:'start',behavior:'smooth'});
      };});
    }
    async function showPage(){
      const token=++pageRequest,chosen=selection,version=publication,c=currentChain,index=Number(q('[data-or-page]').value);
      rows=[];invalidate();q('[data-or-row-detail]').textContent='Select a contract to inspect every field.';
      q('[data-or-records]').textContent='Verifying the original record page…';
      q('[data-or-prev]').disabled=index<=0;q('[data-or-next]').disabled=!c||index+1>=c.record_blocks.length;
      if(!c||!c.record_blocks.length){q('[data-or-records]').textContent='No validated contract rows are available from this capture.';return;}
      try{
        const loaded=await A.records(c,index,fetcher,controller.signal);
        if(token!==pageRequest||chosen!==selection||version!==publication||c!==currentChain)return;
        rows=loaded;renderRows();
      }catch(error){if(token===pageRequest&&chosen===selection&&version===publication&&error.name!=='AbortError')q('[data-or-records]').textContent=error.message;}
    }
    async function inspect(){
      const token=++selection,version=publication,expected=packet,t=symbol;pageRequest++;clear();
      q('[data-or-chain]').textContent='Verifying the captured chain and page inventory…';
      try{
        if(packet)q('[data-or-replay]').innerHTML='<p><a href="'+A.esc(A.recordedUrl(packet,t))+'">Link to this recorded capture</a> · <a href="/option-chain-research.html?underlying='+A.esc(t)+'">Latest capture for '+A.esc(t)+'</a></p>'+
          '<p><a href="/'+A.esc(packet.replay.manifest_key)+'">Open this exact run manifest</a></p><p class="or-digest">Output SHA-256 '+A.esc(packet.replay.output_sha256)+'</p>';
        const c=await A.chain(expected,t,fetcher,controller.signal);
        if(token!==selection||version!==publication||expected!==packet)return;
        currentChain=c;q('[data-or-chain]').innerHTML=A.chainView(c);q('[data-or-dates]').innerHTML=A.dateGroups(c);
        q('[data-or-page]').innerHTML=c.record_blocks.length?c.record_blocks.map((b,i)=>'<option value="'+i+'">Page '+b.source_page+' · '+b.rows+' rows</option>').join(''):'<option>No validated records</option>';
        q('[data-or-page]').disabled=!c.record_blocks.length;q('[data-or-contract-query]').value='';await showPage();
      }catch(error){if(token===selection&&version===publication&&error.name!=='AbortError')q('[data-or-chain]').textContent=error.message;}
    }
    function overview(){
      if(!packet)return;q('[data-or-overview]').innerHTML=A.overview(packet);lastReview=Date.now()>=Date.parse(packet.quality.acquisition_review_due_at)?'overdue':'within';
    }
    async function refresh(){
      const version=++publication;selection++;pageRequest++;controller?.abort();controller=new AbortController();packet=null;clear();
      q('[data-or-status]').textContent=pinned!==null?'Verifying the requested recorded capture…':'Verifying the latest publication and its retained output…';q('[data-or-symbol]').disabled=true;
      for(const name of ['overview','inventory','universe','replay'])q('[data-or-'+name+']').textContent='';
      try{
        const verified=pinned!==null?await A.recordedRun(pinned,fetcher,controller.signal):await A.verifyPacket((await A.load(A.CURRENT,fetcher,controller.signal)).doc,fetcher,controller.signal);
        if(version!==publication)return;packet=verified;if(!packet.chains[symbol])symbol=packet.universe.selected[0];
        q('[data-or-symbol]').innerHTML=packet.universe.selected.map(t=>'<option value="'+t+'">'+t+'</option>').join('');q('[data-or-symbol]').value=symbol;q('[data-or-symbol]').disabled=false;
        overview();inventory();q('[data-or-universe]').innerHTML='<p>'+A.esc(packet.universe.scope)+'</p><p>'+A.esc(packet.universe.selection_rule)+'</p><div class="or-universe-list">Deferred: '+A.esc(packet.universe.deferred.join(', ')||'None')+'</div>'+
          A.table(['Underlying','Selection evidence'],packet.universe.selected.map(t=>[t,packet.universe.origins[t].map(o=>o.kind==='public_baseline_continuity'?'Previously published watchlist':o.source_key+' '+o.pointer).join('; ')]),'Watchlist provenance');
        q('[data-or-status]').textContent=pinned!==null?'Recorded publication verified. This view stays on the capture from '+packet.generated_at+'.':'Publication verified. Inspect a captured chain below.';await inspect();
      }catch(error){if(version===publication&&error.name!=='AbortError'){
        q('[data-or-status]').textContent=error.message+' — previously displayed calculations have been cleared.';
        if(pinned!==null)q('[data-or-replay]').innerHTML='<p><a href="/option-chain-research.html">Open the latest publication</a></p>';
      }}
    }
    q('[data-or-refresh]').onclick=()=>void refresh();q('[data-or-query]').oninput=inventory;
    q('[data-or-symbol]').onchange=()=>{symbol=q('[data-or-symbol]').value;void inspect();};
    q('[data-or-page]').onchange=()=>void showPage();q('[data-or-contract-query]').oninput=renderRows;
    q('[data-or-prev]').onclick=()=>{q('[data-or-page]').value=String(Math.max(0,Number(q('[data-or-page]').value)-1));void showPage();};
    q('[data-or-next]').onclick=()=>{if(currentChain){q('[data-or-page]').value=String(Math.min(currentChain.record_blocks.length-1,Number(q('[data-or-page]').value)+1));void showPage();}};
    root.setInterval(()=>{if(packet&&(Date.now()>=Date.parse(packet.quality.acquisition_review_due_at)?'overdue':'within')!==lastReview)overview();},30000);
    await refresh();
  }
  root.JHOptionResearchBoot=boot;
  if(typeof document!=='undefined'){
    const start=()=>document.querySelectorAll('[data-option-research]').forEach(host=>{void boot(host);});
    if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start,{once:true});else start();
  }
})(typeof window!=='undefined'?window:globalThis);
