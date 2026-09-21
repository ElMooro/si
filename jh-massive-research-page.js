(function(root){
  'use strict';
  async function boot(host){
    const M=root.JHMassiveResearch,A=root.JHOptionResearch,q=s=>host.querySelector(s),fetcher=root.fetch.bind(root);
    const params=new URL(root.location.href).searchParams,pinned=params.has('run')?params.get('run'):null;
    let packet=null,epoch=0,controller=null,symbol=params.has('symbol')?params.get('symbol').trim().toUpperCase():'SPY';
    function inspect(){
      q('[data-mr-detail]').textContent='';q('[data-mr-export]').disabled=true;
      if(!packet)return;
      symbol=q('[data-mr-symbol]').value.trim().toUpperCase();
      if(!Object.hasOwn(packet.instruments,symbol)){q('[data-mr-detail]').textContent='Choose an instrument present in this recorded composition.';return;}
      q('[data-mr-detail]').innerHTML=M.instrumentView(packet,symbol);
      q('[data-mr-recorded]').innerHTML='<a href="'+A.esc(M.recordedUrl(packet,symbol))+'">Keep this exact composition and instrument</a>';
      q('[data-mr-export]').disabled=false;
    }
    async function refresh(){
      const token=++epoch;controller?.abort();controller=new AbortController();packet=null;
      for(const selector of ['[data-mr-sources]','[data-mr-families]','[data-mr-shared]','[data-mr-detail]','[data-mr-contexts]','[data-mr-recorded]'])q(selector).textContent='';
      q('[data-mr-export]').disabled=true;q('[data-mr-symbol]').disabled=true;
      q('[data-mr-status]').textContent='Verifying the composition and its recorded parent outputs…';
      try{
        const p=pinned!==null?await M.recordedRun(pinned,fetcher,controller.signal):await M.verifyPacket((await M.load(M.CURRENT,fetcher,controller.signal)).doc,fetcher,controller.signal);
        if(token!==epoch)return;packet=p;
        q('[data-mr-status]').textContent='Recorded composition and parent outputs verified. Compiled '+p.generated_at+'. Source observation dates remain separate below.';
        q('[data-mr-sources]').innerHTML=M.sourcesView(p);q('[data-mr-families]').innerHTML=M.familiesView(p);q('[data-mr-shared]').innerHTML=M.sharedView(p);
        const names=Object.keys(p.instruments).sort();q('[data-mr-names]').innerHTML=names.map(t=>'<option value="'+A.esc(t)+'">').join('');
        q('[data-mr-symbol]').disabled=false;q('[data-mr-symbol]').value=symbol;
        q('[data-mr-contexts]').innerHTML=A.table(['Retained context','Qualification','Original reported publication','Reported status'],
          Object.entries(p.contexts).map(([key,row])=>[key,row.status,row.reported_generated_at??'Unknown',row.reported_status??'Unspecified']),
          'Whole legacy contexts are preserved without ranking authority');
        inspect();
      }catch(error){if(token===epoch&&error.name!=='AbortError')q('[data-mr-status]').textContent='Evidence verification unavailable: '+error.message+'. No legacy rank is substituted.';}
    }
    q('[data-mr-form]').onsubmit=event=>{event.preventDefault();inspect();};
    q('[data-mr-symbol]').oninput=()=>{q('[data-mr-detail]').textContent='Select this instrument to inspect its recorded evidence.';q('[data-mr-export]').disabled=true;q('[data-mr-recorded]').textContent='';};
    q('[data-mr-refresh]').textContent=pinned!==null?'Recheck this recorded composition':'Refresh and verify';
    q('[data-mr-refresh]').onclick=()=>void refresh();
    q('[data-mr-export]').onclick=()=>{
      if(!packet||q('[data-mr-export]').disabled)return;
      const blob=new Blob([JSON.stringify(M.exportEvidence(packet,symbol),null,2)+'\n'],{type:'application/json'}),url=URL.createObjectURL(blob),a=document.createElement('a');
      a.href=url;a.download='justhodl-'+symbol.replaceAll(':','-')+'-recorded-evidence.json';a.click();URL.revokeObjectURL(url);
    };
    root.setInterval(()=>{if(packet){q('[data-mr-sources]').innerHTML=M.sourcesView(packet);if(!q('[data-mr-export]').disabled)q('[data-mr-detail]').innerHTML=M.instrumentView(packet,symbol);}},30000);
    await refresh();
  }
  root.JHMassiveResearchBoot=boot;
  if(typeof document!=='undefined'){
    const start=()=>document.querySelectorAll('[data-market-evidence]').forEach(host=>void boot(host));
    if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start,{once:true});else start();
  }
})(typeof window!=='undefined'?window:globalThis);
