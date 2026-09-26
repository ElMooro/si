/* Verified FI/FX entry point; full original-source research has its own page. */
(function(root){
  'use strict';
  async function mount(doc,api=root.JHFIFXResearch,fetcher=root.fetch,crypto=root.crypto){
    const status=doc.getElementById('jh-fifx-status');if(!status||!api)return null;
    let packet=null,timer;
    function render(){if(packet)status.textContent='Verified '+packet.generated_at+' · '+api.IDS.filter(s=>api.fresh(packet,s)).length+' / 18 current descriptive sources · zero qualified investment votes';}
    try{
      const p=JSON.parse(new TextDecoder().decode(await api.bytes('/data/fifx-vol.json?exact=1&nogen=1',fetcher,2*1024*1024,15000)));
      if(p.contract!=='fifx-vol-research.v1'){status.textContent='Native source-replay publication pending · historical packet '+(p.generated_at||'date unavailable');return null;}
      await api.verifyView(p,fetcher,crypto);packet=p;render();timer=setInterval(render,30000);
    }catch(_){status.textContent='Source verification unavailable · no current FI/FX recommendation';}
    return {render,destroy(){clearInterval(timer);packet=null;}};
  }
  if(typeof module!=='undefined'&&module.exports)module.exports={mount};
  if(root.document)mount(root.document);
})(typeof globalThis!=='undefined'?globalThis:this);
