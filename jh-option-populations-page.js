(function(root){
  'use strict';
  async function boot(host){
    const A=root.JHOptionResearch,P=root.JHOptionPopulations;if(!A||!P||!host)return;
    const q=s=>host.querySelector(s),fetcher=root.fetch.bind(root),params=new URL(root.location.href).searchParams;
    let packet=null,chain=null,groups=new Map(),blockCache=new Map(),controller=null,generation=0,selection=0,pageToken=0,detailToken=0;
    let symbol=A.ticker(params.get('underlying'))?params.get('underlying'):'SPY',expiry=params.get('expiry')||'',page=0;
    const pinned=params.get('run'),captureDefault=host.dataset.optionPopulations==='capture-expiry';let first=true,lastReview=null;
    function clearDetails(){detailToken++;q('[data-op-evidence]').textContent='Select an expiry/strike group to inspect its original contracts.';}
    function clear(){chain=null;groups=new Map();blockCache=new Map();page=0;clearDetails();for(const name of ['summary','groups','page-info'])q('[data-op-'+name+']').textContent='';q('[data-op-expiry]').disabled=true;q('[data-op-prev]').disabled=q('[data-op-next]').disabled=true;}
    function links(){if(packet)q('[data-op-replay]').innerHTML='<p><a href="'+A.esc(P.recordedUrl(packet,symbol,expiry))+'">Link to this recorded population</a> · <a href="/gex/?underlying='+A.esc(symbol)+'">Latest population for '+A.esc(symbol)+'</a></p><p><a href="/'+A.esc(packet.replay.manifest_key)+'">Exact population run manifest</a> · <a href="/'+A.esc(packet.source_run.manifest_key)+'">Exact source capture manifest</a></p><p class="or-digest">Population output SHA-256 '+A.esc(packet.replay.output_sha256)+'</p>';}
    async function details(group){
      const token=++detailToken,chosen=selection,version=generation;
      q('[data-op-evidence]').textContent='Verifying contributing contract pages…';q('[data-op-evidence]').scrollIntoView({block:'start',behavior:'smooth'});
      try{const items=await P.sourceRows(group,fetcher,controller.signal);if(token!==detailToken||chosen!==selection||version!==generation)return;q('[data-op-evidence]').innerHTML=P.evidence(items);}
      catch(error){if(token===detailToken&&error.name!=='AbortError')q('[data-op-evidence]').textContent=error.message;}
    }
    async function show(){
      const token=++pageToken,chosen=selection,version=generation,c=chain,cache=blockCache;groups=new Map();clearDetails();
      q('[data-op-groups]').textContent='Verifying retained expiry/strike groups…';
      if(!c)return;
      const ids=P.indices(c,expiry),pageCount=Math.ceil(ids.length/100);page=Math.max(0,Math.min(page,Math.max(0,pageCount-1)));
      const chosenIds=ids.slice(page*100,page*100+100),blocks=[...new Set(chosenIds.map(i=>c.group_blocks.findIndex(b=>i>=b.offset&&i<b.offset+b.groups)))];
      q('[data-op-prev]').disabled=page===0;q('[data-op-next]').disabled=page+1>=pageCount;
      q('[data-op-page-info]').textContent=ids.length?(expiry?'Expiry '+expiry:'All captured expiries')+' · '+ids.length+' groups · page '+(page+1)+' of '+pageCount:'No contracts at this expiration date in the captured population. No nearby expiry is substituted.';
      links();
      try{
        // At most 100 displayed groups; fetch only their bounded blocks.
        for(let start=0;start<blocks.length;start+=4)await Promise.all(blocks.slice(start,start+4).map(async i=>{if(!cache.has(i))cache.set(i,await P.groupBlock(c,i,fetcher,controller.signal));}));
        if(token!==pageToken||chosen!==selection||version!==generation||c!==chain)return;
        for(const i of chosenIds){const b=c.group_blocks.findIndex(x=>i>=x.offset&&i<x.offset+x.groups);groups.set(i,cache.get(b)[i-c.group_blocks[b].offset]);}
        q('[data-op-groups]').innerHTML=ids.length?P.groupTable([...groups.values()]):'';
        q('[data-op-groups]').querySelectorAll('[data-op-group]').forEach(button=>{button.onclick=()=>{const group=groups.get(Number(button.dataset.opGroup));if(group)void details(group);};});
      }catch(error){if(token===pageToken&&chosen===selection&&version===generation&&error.name!=='AbortError')q('[data-op-groups]').textContent=error.message;}
    }
    async function inspect(){
      const token=++selection,version=generation,p=packet,t=symbol;pageToken++;clear();
      q('[data-op-summary]').textContent='Verifying the population and its source membership…';
      try{
        const c=await P.chain(p,t,fetcher,controller.signal);if(token!==selection||version!==generation||p!==packet)return;chain=c;
        q('[data-op-summary]').innerHTML=P.summary(c);
        if(first&&captureDefault&&!params.has('expiry'))expiry=P.captureDate(p);first=false;
        const dates=P.expiries(c);if(expiry&&!dates.includes(expiry))dates.push(expiry);dates.sort();
        q('[data-op-expiry]').innerHTML='<option value="">All captured expiries</option>'+dates.map(date=>'<option value="'+A.esc(date)+'">'+A.esc(date)+(date===P.captureDate(p)?' · capture date (New York)':'')+'</option>').join('');
        q('[data-op-expiry]').value=expiry;q('[data-op-expiry]').disabled=false;await show();
      }catch(error){if(token===selection&&version===generation&&error.name!=='AbortError')q('[data-op-summary]').textContent=error.message;}
    }
    function overview(){
      if(!packet)return;lastReview=Date.now()>=Date.parse(packet.quality.acquisition_review_due_at);q('[data-op-overview]').innerHTML=P.overview(packet);
      q('[data-op-overview]').querySelectorAll('[data-op-pick]').forEach(button=>{button.onclick=()=>{symbol=button.dataset.opPick;q('[data-op-symbol]').value=symbol;void inspect();q('[data-op-summary]').scrollIntoView({block:'start',behavior:'smooth'});};});
    }
    async function refresh(){
      const version=++generation;selection++;pageToken++;controller?.abort();controller=new AbortController();packet=null;clear();
      q('[data-op-status]').textContent='Verifying publication, retained output and source capture…';q('[data-op-symbol]').disabled=true;q('[data-op-overview]').textContent='';q('[data-op-replay]').textContent='';
      try{
        if(expiry&&!/^\d{4}-\d{2}-\d{2}$/.test(expiry))throw Error('Choose an exact YYYY-MM-DD expiry');
        const p=pinned!==null?await P.recordedRun(pinned,fetcher,controller.signal):await P.verifyPacket((await P.load(P.CURRENT,fetcher,controller.signal)).doc,fetcher,controller.signal);
        if(version!==generation)return;packet=p;if(!p.universe.includes(symbol))symbol=p.universe[0];
        q('[data-op-symbol]').innerHTML=p.universe.map(t=>'<option>'+t+'</option>').join('');q('[data-op-symbol]').value=symbol;q('[data-op-symbol]').disabled=false;
        overview();q('[data-op-status]').textContent=(pinned!==null?'Recorded population':'Population publication')+' verified. Source capture '+p.source_capture_completed_at+'.';await inspect();
      }catch(error){if(version===generation&&error.name!=='AbortError'){q('[data-op-status]').textContent=error.message+' — no legacy dealer regime is substituted.';q('[data-op-replay]').innerHTML='<a href="/option-chain-research.html">Open captured contract research</a>';}}
    }
    q('[data-op-refresh]').textContent=pinned!==null?'Recheck recorded population':'Refresh population';q('[data-op-refresh]').onclick=()=>void refresh();
    q('[data-op-symbol]').onchange=()=>{symbol=q('[data-op-symbol]').value;void inspect();};
    q('[data-op-expiry]').onchange=()=>{expiry=q('[data-op-expiry]').value;page=0;void show();};
    q('[data-op-prev]').onclick=()=>{page--;void show();};q('[data-op-next]').onclick=()=>{page++;void show();};
    root.setInterval(()=>{if(packet&&(Date.now()>=Date.parse(packet.quality.acquisition_review_due_at))!==lastReview)overview();},30000);
    await refresh();
  }
  root.JHOptionPopulationsBoot=boot;
  if(typeof document!=='undefined'){
    const start=()=>document.querySelectorAll('[data-option-populations]').forEach(host=>{void boot(host);});
    if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',start,{once:true});else start();
  }
})(typeof window!=='undefined'?window:globalThis);
