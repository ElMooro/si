(function(root){
  'use strict';
  async function boot(){
    const target=document.querySelector('[data-option-research-hub]'),A=root.JHOptionResearch;if(!target||!A)return;
    const stamp=document.getElementById('f-flow');
    target.textContent='Verifying the captured option-chain publication…';
    try{
      const fetcher=root.fetch.bind(root),{doc}=await A.load(A.CURRENT,fetcher),p=await A.verifyPacket(doc,fetcher),q=p.quality;
      target.innerHTML='<p><b>'+A.esc(q.selected_underlyings)+' underlyings · '+A.esc(q.returned_rows.toLocaleString('en-US'))+' captured contract rows</b></p>'+
        '<p>Complete returned pagination where verified; exact fields, units, missingness and source row references.</p><p><a href="/option-chain-research.html">Open the verified chain explorer and hypothetical payoff calculator →</a></p>'+
        '<p>OI, IV and Greek observation clocks are not supplied. No trade direction or size is inferred.</p>';
      if(stamp)stamp.textContent='Capture '+p.generated_at;
    }catch(error){target.textContent=error.message+' — no legacy directional alert is substituted.';if(stamp)stamp.textContent='Source unavailable';}
  }
  if(typeof document!=='undefined'){
    if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>{void boot();},{once:true});else void boot();
  }
})(typeof window!=='undefined'?window:globalThis);
