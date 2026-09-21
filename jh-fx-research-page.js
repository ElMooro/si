(function(root){
  'use strict';
  async function boot(host){
    const F=root.JHFXResearch,A=root.JHOptionResearch,q=s=>host.querySelector(s),fetcher=root.fetch.bind(root),params=new URL(root.location.href).searchParams;
    const pinned=params.has('run')?params.get('run'):null;let pair=params.get('pair')||'EUR_USD',packet=null,epoch=0,controller=null,result=null,page=0;
    const fields=['quantity','entry','future','cost'];
    function clearScenario(reset=false){result=null;q('[data-fx-result]').textContent='';if(reset)for(const k of fields)q('[name='+k+']').value='';}
    function clearPair(){clearScenario(true);q('[data-fx-export]').disabled=true;q('[data-fx-calculate]').disabled=true;q('[data-fx-row]').disabled=true;
      for(const s of ['detail','comparisons','rows','evidence','recorded','scenario-note'])q('[data-fx-'+s+']').textContent='';
      q('[data-fx-previous]').disabled=true;q('[data-fx-next]').disabled=true;}
    function rowDetail(){
      if(!packet)return;const proof=F.evidence(packet,pair),index=Number(q('[data-fx-row]').value),r=proof.rows[index];
      if(!r){q('[data-fx-evidence]').textContent='Choose an original row.';return;}
      const f=packet.pairs[pair],units={o:f.price_unit,h:f.price_unit,l:f.price_unit,c:f.price_unit,v:'Provider v; trade-volume unit unqualified',n:'Provider n; executed-trade count unqualified',vw:'Provider weighted price; basis unqualified',t:'Unix milliseconds at window start'};
      q('[data-fx-evidence]').innerHTML='<h3>Original row '+r.ordinal+'</h3><p>Acquired '+A.esc(r.source.acquired_at)+' · Page '+r.source.page+' · '+A.esc(r.source.pointer)+'</p>'+
        '<p class="or-digest">Original SHA-256 '+A.esc(r.source.original.sha256)+'</p>'+A.table(['Field','Reported value','State','Unit / definition'],Object.entries(r.values).map(([k,v])=>[k,A.exact(v.decimal),v.state,units[k]]),'Every captured numeric field')+
        '<p>Window start '+A.esc(r.window_start_utc??'unknown')+'. Quote time, close time and bar finality remain unverified. '+A.esc(r.issues.length?r.issues.join('; '):'No structural row issue detected.')+'</p>';
    }
    function renderRows(){const rows=F.evidence(packet,pair).rows;q('[data-fx-rows]').innerHTML=F.rowsView(packet,pair,page*100);
      q('[data-fx-previous]').disabled=page===0;q('[data-fx-next]').disabled=(page+1)*100>=rows.length;}
    async function inspect(){
      const token=++epoch;controller?.abort();controller=new AbortController();clearPair();page=0;pair=q('[data-fx-pair]').value;
      if(!packet)return;q('[data-fx-status]').textContent='Verifying '+pair+' original row blocks and exact comparisons…';
      try{const rows=await F.records(packet,pair,fetcher,controller.signal);if(token!==epoch)return;const f=packet.pairs[pair];
        q('[data-fx-status]').textContent='Recorded publication and selected row blocks verified. Compilation '+packet.generated_at+'.';
        q('[data-fx-detail]').innerHTML='<h2>'+A.esc(pair.replace('_',' / '))+'</h2><p>'+A.esc(f.price_unit)+' · <span data-fx-review>'+A.esc(F.review(f))+'</span>.</p>'+A.table(['Evidence property','Recorded value'],[
          ['Capture completed',f.source_capture_completed_at],['Acquisition review due',f.source_review_due_at],['Requested window',f.request_window.from+' to '+f.request_window.to],
          ['Returned rows',f.coverage.returned_rows],['Usable positive closes',f.coverage.positive_close_rows],['Returned pagination',f.coverage.pagination_complete?'Complete':'Incomplete: '+f.coverage.stop],
          ['Full calendar coverage','Not independently verified'],['Quote / close time / finality','Not independently verified']],'Source coverage and clocks');
        q('[data-fx-comparisons]').innerHTML=F.comparisonsView(packet,pair);renderRows();
        q('[data-fx-row]').innerHTML=rows.map(r=>'<option value="'+r.ordinal+'">Row '+r.ordinal+' · '+A.esc(r.window_start_utc??'Unknown clock')+'</option>').join('');
        q('[data-fx-row]').disabled=!rows.length;q('[data-fx-row]').value=String(f.latest_reported_row?.ordinal??0);rowDetail();
        q('[data-fx-recorded]').innerHTML='<a href="'+A.esc(F.recordedUrl(packet,pair))+'">Keep this exact capture and pair</a>';
        q('[data-fx-export]').disabled=false;q('[data-fx-calculate]').disabled=['XAU','XAG'].includes(f.base_code)||!f.latest_reported_row?.positive_close_usable_as_reported;
        q('[data-fx-scenario-note]').textContent=['XAU','XAG'].includes(f.base_code)?'Metal quantity units are unverified. The exposure calculator is available for currency pairs.':
          'Signed units are '+f.base_code+'. Both assumed rates are '+f.price_unit+'. Costs and the result are in '+f.quote_code+'.';
      }catch(error){if(token===epoch&&error.name!=='AbortError'){clearPair();q('[data-fx-status]').textContent='Selected evidence unavailable: '+error.message;}}
    }
    async function refresh(){const token=++epoch;controller?.abort();controller=new AbortController();packet=null;clearPair();q('[data-fx-pair]').disabled=true;
      q('[data-fx-status]').textContent='Verifying the recorded FX publication…';
      try{const p=pinned!==null?await F.recordedRun(pinned,fetcher,controller.signal):await F.verifyPacket((await F.load(F.CURRENT,fetcher,controller.signal)).doc,fetcher,controller.signal);
        if(token!==epoch)return;packet=p;q('[data-fx-pair]').innerHTML=F.PAIRS.map(v=>'<option value="'+v+'">'+v.replace('_',' / ')+'</option>').join('');q('[data-fx-pair]').disabled=false;
        if(!F.PAIRS.includes(pair))throw Error('Requested pair is outside this captured universe');q('[data-fx-pair]').value=pair;await inspect();
      }catch(error){if(token===epoch&&error.name!=='AbortError')q('[data-fx-status]').textContent='Recorded evidence unavailable: '+error.message+'. No latest or legacy substitute was used.';}}
    q('[data-fx-pair]').onchange=()=>void inspect();q('[data-fx-row]').onchange=rowDetail;
    q('[data-fx-previous]').onclick=()=>{page--;renderRows();};q('[data-fx-next]').onclick=()=>{page++;renderRows();};
    q('[data-fx-refresh]').textContent=pinned!==null?'Recheck this recorded capture':'Refresh and verify';q('[data-fx-refresh]').onclick=()=>void refresh();
    for(const k of fields)q('[name='+k+']').oninput=()=>clearScenario();
    q('[data-fx-scenario]').onsubmit=event=>{event.preventDefault();clearScenario();if(q('[data-fx-calculate]').disabled)return;
      try{result=F.scenario(packet,pair,Object.fromEntries(fields.map(k=>[k,q('[name='+k+']').value.trim()])));
        q('[data-fx-result]').innerHTML='<div class="or-result"><strong>Net change: '+A.esc(A.exact(result.net_quote))+' '+A.esc(result.quote_currency)+'</strong><p>Gross '+A.esc(A.exact(result.gross_quote))+'; assumed total costs '+A.esc(A.exact(result.cost_quote))+'.</p><p>'+A.esc(result.formula)+'</p><p>'+A.esc(result.scope)+'</p></div>';
      }catch(error){q('[data-fx-result]').textContent=error.message;}};
    q('[data-fx-export]').onclick=()=>{if(!packet||q('[data-fx-export]').disabled)return;
      const blob=new Blob([JSON.stringify({...F.evidence(packet,pair),scenario:result},null,2)+'\n'],{type:'application/json'}),url=URL.createObjectURL(blob),a=document.createElement('a');
      a.href=url;a.download='justhodl-'+pair+'-recorded-fx.json';a.click();URL.revokeObjectURL(url);};
    root.setInterval(()=>{const node=q('[data-fx-review]');if(node&&packet?.pairs[pair])node.textContent=F.review(packet.pairs[pair]);},60000);
    await refresh();
  }
  if(root.document)root.document.querySelectorAll('[data-fx-research]').forEach(host=>void boot(host));
})(typeof window!=='undefined'?window:globalThis);
