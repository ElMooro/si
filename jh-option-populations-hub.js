(function(root){
  'use strict';
  async function boot(){
    const target=document.querySelector('[data-option-populations-hub]'),A=root.JHOptionResearch,P=root.JHOptionPopulations;if(!target||!A||!P)return;
    target.textContent='Verifying captured populations and their original source…';
    try{
      const fetcher=root.fetch.bind(root),p=await P.verifyPacket((await P.load(P.CURRENT,fetcher)).doc,fetcher);
      target.innerHTML='<p><b>'+A.esc(p.universe.length)+' underlyings · '+A.esc(p.quality.counts.identity_eligible_rows.toLocaleString('en-US'))+' valid captured contract identities</b></p><p>Source capture '+A.esc(p.source_capture_completed_at)+'. OI and Greek observation dates are not supplied.</p><p><a href="'+A.esc(P.recordedUrl(p,'SPY'))+'">Inspect this recorded population, expiry groups and contributing contracts →</a> · <a href="/0dte/">Capture-date expiries</a></p><p>Call and put coefficients stay separate. Dealer signs, flip levels, support/resistance and squeeze rankings are not inferred.</p>';
    }catch(error){target.textContent=error.message+' — no legacy dealer regime is substituted.';}
  }
  if(typeof document!=='undefined'){if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',()=>{void boot();},{once:true});else void boot();}
})(typeof window!=='undefined'?window:globalThis);
