/* The public monitor reads only the source-map engine's reviewed projection. */
(function (global) {
  'use strict';
  function validProjection(doc) {
    return doc?.schema_version === 'public-source-map.v1' && doc.engine === 'justhodl-source-map' &&
      doc.publication?.scope === 'PUBLIC_MARKET_SOURCE_METADATA' && doc.publication.contains_private_data === false &&
      doc.publication.raw_source_text_private === true && doc.publication.raw_diagnostics_private === true;
  }
  function render(doc, target) {
    const el=id=>target.getElementById(id), n=(tag,text)=>{const e=target.createElement(tag);e.textContent=String(text??'—');return e;};
    const num=v=>typeof v==='number'&&Number.isFinite(v)?v:null;
    if(!validProjection(doc)) {
      for(const id of ['stats','diag','prog','agency','econ','latest','tops','newsrc'])el(id).replaceChildren(n('p','Public source metadata unavailable: waiting for the verified projection. Raw browser content is private.'));
      return false;
    }
    el('stats').replaceChildren(...[['Sourced rows',doc.symbols_with_source],['Public market references',doc.public_symbol_count],['Unmapped source rows',doc.unmapped_source_rows],['Input status',doc.input_status],['Input received',doc.input_generated_at],['Projection generated',doc.generated_at]].map(([k,v])=>{const card=n('div','');card.className='card';card.append(n('div',k),n('strong',v));return card;}));
    el('diag').textContent='Raw source text, descriptions, browser diagnostics and watchlists are private. This page has no authenticated route for that content. Only permitted numeric progress is shown below.';
    const pr=doc.harvest_progress||{};
    el('prog').replaceChildren(...[['Walked',pr.walked],['Total',pr.total],['Progress %',pr.pct],['Agency rows walked',pr.tier1_done],['Rate / min',pr.rate_per_min],['ETA hours',pr.eta_hours],['Matched',pr.matched],['Elapsed seconds',pr.elapsed_s]].map(([label,value])=>n('p',label+': '+(num(value)??'—'))));
    const table=(headers,rows)=>{const t=n('table',''),head=n('tr','');headers.forEach(h=>head.append(n('th',h)));t.append(head);rows.forEach(values=>{const r=n('tr','');values.forEach(value=>r.append(n('td',value)));t.append(r);});return t;};
    const sources=Object.entries(doc.cleaned_sources||{}).sort((a,b)=>String(b[1]?.updated??'').localeCompare(String(a[1]?.updated??'')));
    el('latest').replaceChildren(n('p',`Summary: ${Math.min(sources.length,15)} of ${sources.length} public references. All returned rows are in the complete engine inspector.`),table(['Market reference','Classified source family','Received'],sources.slice(0,15).map(([key,value])=>[key,value?.source_family,value?.updated])));
    el('tops').replaceChildren(table(['Source family','Rows'],Object.entries(doc.known_families||{}).sort((a,b)=>b[1]-a[1])));
    el('agency').replaceChildren(n('p',doc.classification_method),n('p','Macro attributed: '+(num(doc.macro_attributed)??'—')+'; unmatched: '+(num(doc.macro_unattributed)??'—')),table(['Agency family','Rows'],Object.entries(doc.agency_families||{})));
    el('econ').replaceChildren(table(['Source family','Symbols'],(Array.isArray(doc.economics_agencies)?doc.economics_agencies:[]).map(row=>[row.source_family,num(row.n_symbols)])));
    el('newsrc').textContent=(num(doc.unmapped_source_rows)??'—')+' rows have an unmapped source family. Raw names and examples are withheld. '+(num(doc.withheld_symbol_count)??'—')+' identifiers did not pass the public market-reference filter.';
    return true;
  }
  async function load() {
    try {
      const response=await global.fetch('/data/source-map.json?v='+Date.now(),{cache:'no-store'});
      const doc=response.ok?await response.json():null;
      render(doc,global.document);
    } catch (_) {render(null,global.document);}
  }
  const api={validProjection,render,load};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  else {global.load=load;let cd=30;global.setInterval(()=>{cd--;if(cd<=0){cd=30;load();}global.document.getElementById('cd').textContent=String(cd);},1000);load();}
})(typeof window!=='undefined'?window:globalThis);
