/* Chart network routing only. No watchlist or storage mutations. */
(function () {
  'use strict';
  if (window.__jhWarehouseRouting) return;
  window.__jhWarehouseRouting = true;
  const original = window.fetch.bind(window);
  const proxy = 'https://justhodl-data-proxy.raafouis.workers.dev';
  const tvHost = 'nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws';
  let maps;
  function read(path) {
    return original('/data/'+path, {cache:'no-cache'}).then(r => {
      if (!r.ok) throw new Error('Resolver unavailable');
      return r.json();
    });
  }
  function load() {
    if (!maps) maps = Promise.all([read('tv-symbol-resolver.json'), read('symbol-map.json')]).catch(e => {maps=null;throw e;});
    return maps;
  }
  function resolve(sym, resolver, smap) {
    if ((resolver.licensed_econ_skip || []).includes(sym)) return {id:'SKIP'};
    const exact = (resolver.exact || {})[sym];
    if (exact && exact.id) return exact;
    const m = (smap.map || {})[sym];
    if (m && m.id) return {id:m.source === 'FRED' ? 'FRED:'+String(m.id).replace(/^FRED:/i,'') : m.id, engine:String(m.source || '').toLowerCase()};
    const parts = /^([^:]+):(.+)$/.exec(sym);
    const p = parts && (resolver.prefix || {})[parts[1]];
    if (p && p.engine === 'fred') return {id:'FRED:'+parts[2],engine:'fred'};
    if (p && p.strip) return {id:parts[2],engine:p.engine};
    if (p && p.engine === 'yahoo' && /^[A-Z]{6}$/.test(parts[2])) return {id:parts[2]+'=X',engine:'yahoo'};
    if (/^[A-Z][A-Z0-9.\-]{0,11}$/.test(sym)) return {id:sym,engine:'equity'};
    return null;
  }
  function json(doc, status = 200) { return new Response(JSON.stringify(doc), {status,headers:{'Content-Type':'application/json','X-JH-Route':'resolved-series'}}); }
  window.fetch = async function (input, init) {
    const url = new URL(typeof input === 'string' || input instanceof URL ? String(input) : input.url, location.href);
    if (url.hostname !== tvHost) return original(input,init);
    // The legacy diagnostic beacon has no series request to resolve.
    if (url.searchParams.has('diag')) return new Response(null,{status:204});
    const sym = (url.searchParams.get('sym') || '').trim();
    if (!sym) return json({error:'symbol required',points:[]},400);
    // Failure to read the resolver is not evidence that a symbol is unmapped.
    const [resolver, smap] = await load();
    const r = resolve(sym,resolver,smap);
    if (!r) return original(input,init);
    if (r.id === 'SKIP' || String(r.id).startsWith('COMPUTE:')) return json({id:r.id,points:[],status:'HELD'});
    const route = r.engine === 'yahoo'
      ? '/yf-ohlc?symbol='+encodeURIComponent(r.id)+'&range=max&interval=1d'
      : '/series?id='+encodeURIComponent(r.id);
    const response = await original(proxy+route,init);
    if (!response.ok) return json({id:r.id,points:[],error:'resolved series unavailable'},response.status);
    const doc = await response.json();
    const points = doc.obs || doc.points || (doc.bars || []).map(b=>[new Date(b.time*1000).toISOString().slice(0,10),b.close]);
    return json({...doc,id:r.id,points});
  };
})();
