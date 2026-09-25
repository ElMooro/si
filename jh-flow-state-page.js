(function(root){
 'use strict';const A=root.JHOptionResearch,F=root.JHFlowStateResearch;
 function boot(){const host=document.querySelector('[data-flow-state]');if(!host)return;
  const q=s=>host.querySelector(s),params=new URLSearchParams(location.search),pin=params.has('run')?params.get('run'):null;
  let packet=null,controller=null,epoch=0;
  const regions=['record','summary','categories','overlap','monthly','parents','shared','dollar','contexts'];
  const value=F.formatAmount;
  const stamp=s=>typeof s==='string'?s.replace('T',' ').replace(/\.\d+(?:Z|\+00:00)$/,' UTC').replace(/(?:Z|\+00:00)$/,' UTC'):'Unreported';
  function render(p){const evidence=F.evidence(p),etf=evidence.parents['data/etf-true-flows.json'];
   const coverage=Number.isSafeInteger(etf.quality?.native_histories)&&Number.isSafeInteger(etf.quality?.configured_funds)?etf.quality.native_histories+' of '+etf.quality.configured_funds+' configured funds':'Not reported in source';
   q('[data-fs-record]').innerHTML='Compiled '+A.esc(stamp(p.generated_at))+' · <a href="'+A.esc(F.recordedUrl(p))+'">Keep this exact recorded composition</a>';
   q('[data-fs-summary]').innerHTML=A.table(['Evidence','Recorded coverage'],[
    ['Native parent publications',Object.keys(p.parents).length+' verified'],['Issuer histories',coverage],
    ['Category estimates',p.asset_class_rotation.length+' categories; coverage subtotals'],['Monthly transaction windows',p.monthly_transactions.length],['Qualified investment votes','0']], 'Composition coverage');
   q('[data-fs-categories]').innerHTML=A.table(['Category','Estimate, USD','Direction','Period','Covered / configured'],p.asset_class_rotation.map(r=>[
    r.category.replaceAll('_',' '),value(r.value_decimal),r.direction.replaceAll('_',' '),r.period?r.period.start_date+' → '+r.period.end_date:'Unavailable',r.covered_count+' / '+r.configured_count]),'Five-observation issuer estimates');
   q('[data-fs-overlap]').innerHTML=p.category_overlap.length?A.table(['Fund','Categories'],p.category_overlap.map(r=>[r.ticker,r.categories.join(', ')]),'Repeated category membership'):'<p>No covered fund appears in multiple categories in this record. Incomplete coverage still prevents a whole-market flow total.</p>';
   q('[data-fs-monthly]').innerHTML=A.table(['Asset class','Window','Value, USD bn','Observation months','Status'],p.monthly_transactions.map(r=>[
    r.asset_class.replaceAll('_',' '),r.period==='latest'?'Latest month':'12 months',value(r.value_decimal),r.months.length?r.months[0].slice(0,7)+' → '+r.months.at(-1).slice(0,7):'Unavailable',r.status.replaceAll('_',' ')]),'Dated TIC transaction windows');
   q('[data-fs-parents]').innerHTML=A.table(['Recorded source output','Parent compiled at, UTC','Source quality'],Object.entries(p.parents).map(([key,row])=>[
    '<a href="/'+A.esc(row.output.key)+'">'+A.esc(F.SOURCES[key][3])+'</a>',stamp(row.generated_at),row.quality?.status??'See recorded source']), 'Exact parent publications',true);
   q('[data-fs-shared]').innerHTML=A.table(['Source identity','Parents','Independent confirmation'],p.dependency_graph.shared_roots.map(r=>[
    r.source_identity,r.parents.map(k=>F.SOURCES[k]?.[3]??k).join(' · '),'No']), 'Shared measurements');
   const dollar=p.parents['data/dollar-radar.json'].replay.manifest_key.split('/').pop().slice(0,-5);
   q('[data-fs-dollar]').innerHTML='<a href="/dollar.html?series=DEXUSEU&amp;run='+dollar+'">Open the recorded EUR / USD exposure calculator</a>';
   q('[data-fs-contexts]').innerHTML=A.table(['Retained context','Reported compilation','Status'],p.retained_contexts.map(r=>[
    r.source_key,stamp(r.generated_at),'No promoted measurement']), 'Retained unqualified inputs');
  }
  async function refresh(){const mine=++epoch;controller?.abort();controller=new AbortController();packet=null;q('[data-fs-export]').disabled=true;
   for(const name of regions)q('[data-fs-'+name+']').textContent='';q('[data-fs-status]').textContent='Verifying recorded composition and parent evidence…';
   try{const result=pin!==null?await F.recordedRun(pin,root.fetch.bind(root),controller.signal):await F.verifyPacket(JSON.parse((await F.load(F.CURRENT,root.fetch.bind(root),controller.signal)).text),root.fetch.bind(root),controller.signal);
    if(mine!==epoch)return;packet=result;render(packet);q('[data-fs-export]').disabled=false;q('[data-fs-status]').textContent='Recorded composition and five parent publications verified. Dated evidence; no qualified investment votes.';
   }catch(error){if(mine!==epoch||error.name==='AbortError')return;packet=null;for(const name of regions)q('[data-fs-'+name+']').textContent='';q('[data-fs-status]').textContent=error.message+'. No latest or legacy substitute was used.';}
  }
  q('[data-fs-refresh]').onclick=refresh;q('[data-fs-export]').onclick=()=>{if(!packet||q('[data-fs-export]').disabled)return;
   const blob=new Blob([JSON.stringify(F.evidence(packet),null,2)+'\n'],{type:'application/json'}),url=URL.createObjectURL(blob),a=document.createElement('a');
   a.href=url;a.download='cross-asset-flow-evidence.json';a.hidden=true;document.body.appendChild(a);
   try{a.click();}finally{a.remove();root.setTimeout(()=>URL.revokeObjectURL(url),1000);}};
  refresh();
 }
 if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot);else boot();
})(window);
