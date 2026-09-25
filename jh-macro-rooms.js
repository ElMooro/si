/* jh-reskin-skip */
(function(root){
  'use strict';
  const A=root.JHOptionResearch||(typeof require==='function'?require('./jh-option-research.js'):null);
  const D=root.JHDollarResearch||(typeof require==='function'?require('./jh-dollar-research.js'):null);
  function render(packet,now=Date.now()){
    if(Date.parse(packet.generated_at)>now||Date.parse(packet.source_generated_at)>now)throw Error('Future research clock');
    // selected() requires the complete retained publication to have passed
    // Dollar's output/input/compiler bindings before any value is rendered.
    const rows=D.SERIES.map(sid=>{
      const row=D.selected(packet,sid),point=row.latest_observation,quote=row.quote;
      const units=quote?quote.numerator+' per '+quote.denominator:row.unit;
      return '<tr><td><a href="'+A.esc(D.recordedUrl(packet,sid))+'">'+sid+'</a><small>'+A.esc(row.label)+'</small></td>'+
        '<td>'+A.esc(D.exact(point?.exact_value))+'<small>'+A.esc(units)+'</small></td>'+
        '<td>'+A.esc(point?.date??'Unavailable')+'<small>'+A.esc(row.frequency)+' · acquired '+A.esc(row.acquired_at??'unavailable')+'</small></td>'+
        '<td>'+A.esc(D.review(row,now))+'<small><a href="'+A.esc(D.recordedUrl(packet,sid))+'">Dates, history, source rows and scenario</a></small></td></tr>';
    }).join('');
    return '<p>Recorded '+A.esc(packet.generated_at)+' · canonical source '+A.esc(packet.source_generated_at)+'. Retained artifact hashes and output binding checked in this browser; original arithmetic is checked on the runner.</p>'+
      '<div class="rooms-scroll" tabindex="0" role="region" aria-label="Dated Dollar and FX observations"><table><thead><tr><th>Series / definition</th><th>Retained observation / unit</th><th>Observation / acquisition</th><th>Source status / evidence</th></tr></thead><tbody>'+rows+'</tbody></table></div>'+
      '<p>These are recorded observations, not executable quotes. Each drill-down opens this exact run and series, with explicit FX exposure assumptions. No pressure score or directional recommendation is inferred.</p>';
  }
  function install(win){
    const host=win.document.getElementById('rooms-dollar');if(!host)return;
    let packet=null,lastMarkup=null,epoch=0;
    function draw(){if(!packet)return;const html=render(packet);if(html!==lastMarkup){host.innerHTML=html;lastMarkup=html;}}
    async function load(){
      const own=++epoch;
      try{
        const fetcher=(url,options)=>win.fetch(url,{...options,credentials:'omit'});
        const item=await D.load(D.CURRENT,fetcher),p=await D.verifyPacket(JSON.parse(item.text),fetcher);
        if(own!==epoch)return;packet=p;draw();
      }catch(_){if(own!==epoch)return;packet=null;lastMarkup=null;
        host.textContent='Dollar research unavailable: the current publication could not be matched to its recorded evidence. Earlier values have been cleared. Open the source desk for retained runs.';}
    }
    load();win.setInterval(load,300000);win.setInterval(draw,60000);
  }
  const api={render,install};if(typeof module==='object'&&module.exports)module.exports=api;
  if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',()=>install(root));else install(root);}
})(typeof window==='object'?window:globalThis);
