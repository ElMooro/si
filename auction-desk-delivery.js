/* Treasury desk delivery integrity. This does not certify provider originals. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.JHAuctionDelivery = api;
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';
  const CONTRACT = 'auction-desk-delivery.v1', PREFIX = 'data/auction-desk-delivery/';
  const FLAGS = ['original_source_verified','historical_point_in_time_verified','forecast_eligible','calls_eligible','sizing_eligible','execution_eligible'];
  const MAX = 128 * 1024 * 1024, INITIAL = 4 * 1024 * 1024, ROWS = 8 * 1024 * 1024;
  const ORIGINS = ['https://justhodl-data-proxy.raafouis.workers.dev/', 'https://justhodl-dashboard-live.s3.amazonaws.com/'];
  const object = x => x !== null && typeof x === 'object' && !Array.isArray(x);
  const same = (a,b) => JSON.stringify(canonical(a)) === JSON.stringify(canonical(b));
  function canonical(value) {
    if (Array.isArray(value)) return value.map(canonical);
    if (object(value)) return Object.fromEntries(Object.keys(value).sort().map(k => [k,canonical(value[k])]));
    return value;
  }
  function clock(value) {
    if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))) throw Error('Publication time is invalid');
    return Date.parse(value);
  }
  function authority(value) {
    if (!object(value) || value.contract !== CONTRACT || FLAGS.some(k => value[k] !== false)) throw Error('Research-only delivery contract required');
  }
  function reference(ref, category) {
    if (!object(ref) || !same(Object.keys(ref).sort(), ['bytes','encoding','key','sha256']) || !Number.isSafeInteger(ref.bytes) || ref.bytes <= 0 || ref.bytes > MAX || !/^[a-f0-9]{64}$/.test(ref.sha256)) throw Error('Invalid delivery reference');
    const kinds = category ? [category] : ['snapshots','rows','sections','views','manifests'];
    const suffix = ref.encoding === 'json' ? '.json' : ref.encoding === 'gzip' ? '.json.gz' : null;
    if (!suffix || !kinds.some(k => ref.key === PREFIX+k+'/'+ref.sha256+suffix) || (ref.encoding === 'gzip' && !ref.key.startsWith(PREFIX+'snapshots/'))) throw Error('Evidence path is outside the owned namespace');
    if ((category === 'views' || category === 'manifests') && ref.bytes > INITIAL || ref.key.startsWith(PREFIX+'rows/') && ref.bytes > ROWS) throw Error('Evidence exceeds its complete-byte limit');
    return ref;
  }
  function packet(doc) {
    if (!object(doc) || doc.engine !== 'justhodl-auction-desk') throw Error('Not a Treasury desk packet');
    if (clock(doc.generated_at) > Date.now()+300000) throw Error('Publication time is in the future');
    for (const k of ['today','buybacks','calendar','reactions','freshness','decision']) if (!object(doc[k])) throw Error('Desk section missing: '+k);
    for (const rows of [doc.auctions,doc.recent_days,doc.today.auctions,doc.today.buybacks,doc.buybacks.operations,doc.calendar.auctions,doc.calendar.buybacks]) {
      if (!Array.isArray(rows) || rows.some(row => !object(row))) throw Error('Invalid operation array');
    }
    if (doc.decision.call !== null || doc.decision.sizing_eligible !== false) throw Error('Desk measurements cannot authorize a position');
    return doc;
  }
  async function bytes(url, limit, options) {
    const controller = new AbortController();
    let timer, reader;
    const task = (async () => {
      const response = await options.fetch(url, {signal:controller.signal, credentials:'omit', cache:options.mutable ? 'no-store' : 'default'});
      if (controller.signal.aborted) throw Error('Request timed out');
      if (!response.ok) throw Error('Delivery HTTP '+response.status);
      if (Number(response.headers?.get('content-length')) > limit) throw Error('Response exceeds its byte limit');
      if (!response.body?.getReader) throw Error('Bounded response streaming unavailable');
      reader = response.body.getReader();
      const chunks = []; let length = 0;
      for (;;) {
        const part = await reader.read();
        if (controller.signal.aborted) throw Error('Request timed out');
        if (part.done) break;
        length += part.value.byteLength;
        if (length > limit) throw Error('Response exceeds its byte limit');
        chunks.push(part.value);
      }
      const out = new Uint8Array(length); let offset = 0;
      for (const chunk of chunks) {out.set(chunk,offset); offset += chunk.byteLength;}
      return out;
    })();
    try {
      return await Promise.race([task, new Promise((_,reject) => {timer = setTimeout(() => reject(Error('Delivery deadline exceeded')), options.timeout);})]);
    } finally {
      clearTimeout(timer); controller.abort();
      if (reader) {try {Promise.resolve(reader.cancel()).catch(() => {});} catch (_) { /* Closed stream. */ }}
    }
  }
  const parse = raw => JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
  function createClient(options = {}) {
    const fetcher = options.fetch || globalThis.fetch.bind(globalThis), crypto = options.crypto || globalThis.crypto;
    const timeout = options.timeout || 12000;
    const contexts = new WeakMap(), cache = new Map();
    async function get(origin,key,limit,mutable=false) {
      // The proxy intentionally ignores arbitrary cache-busting parameters.
      // Its existing exact read route bypasses mutable cache and cold generation.
      return bytes(origin+key+(mutable ? '?exact=1&nogen=1' : ''),limit,{fetch:fetcher,timeout,mutable});
    }
    async function artifact(origin,ref,category) {
      reference(ref,category);
      if (ref.encoding !== 'json') throw Error('Complete gzip snapshots are downloaded, not auto-loaded');
      const key = origin+ref.key;
      if (cache.has(key)) {
        const saved = cache.get(key);
        if (!same(saved.ref,ref)) throw Error('Cached evidence reference differs');
        return saved.doc;
      }
      const raw = await get(origin,ref.key,ref.bytes);
      if (raw.byteLength !== ref.bytes) throw Error('Evidence length differs');
      if (!crypto?.subtle) throw Error('Evidence checksum verification unavailable');
      const hash = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw))).map(x=>x.toString(16).padStart(2,'0')).join('');
      if (hash !== ref.sha256) throw Error('Evidence checksum differs');
      const doc = parse(raw);
      // Bound the cache to avoid accumulating old 8 MB comparison populations.
      if (cache.size >= 6) cache.delete(cache.keys().next().value);
      cache.set(key,{ref:structuredClone(ref),doc}); return doc;
    }
    async function load() {
      const problems = [];
      for (const origin of ORIGINS) {
        try {
          const locator = parse(await get(origin,'data/auction-desk-view.json',INITIAL,true));
          authority(locator); clock(locator.generated_at);
          const manifest = await artifact(origin,locator.manifest,'manifests');
          authority(manifest);
          if (!same(manifest.view,locator.view) || manifest.generated_at !== locator.generated_at || !/^[a-f0-9]{64}$/.test(manifest.projection_compiler_sha256)) throw Error('Manifest publication differs');
          reference(manifest.source_packet,'snapshots');
          if (!Array.isArray(manifest.row_chunks) || !object(manifest.sections)) throw Error('Complete evidence inventory required');
          manifest.row_chunks.forEach(ref => reference(ref,'rows'));
          for (const name of ['buyback_program','reactions','composite_history']) reference(manifest.sections[name],'sections');
          const view = packet(await artifact(origin,locator.view,'views'));
          authority(view.delivery);
          if (view.generated_at !== locator.generated_at || !same(view.delivery.source_packet,manifest.source_packet) || view.delivery.row_chunks !== manifest.row_chunks.length) throw Error('View publication differs');
          const inventory = new Set(manifest.row_chunks.map(ref=>ref.key));
          for (const [kind,rows] of [['auction',[...view.auctions,...view.today.auctions]],['buyback',[...view.buybacks.operations,...view.today.buybacks]]]) {
            for (const row of rows) {
              const detail = row.delivery_detail;
              if (row.delivery_contract !== CONTRACT || !object(detail) || detail.kind !== kind || !Number.isSafeInteger(detail.index) || detail.index < 0 || !inventory.has(detail.artifact?.key) || !manifest.row_chunks.some(ref=>same(ref,detail.artifact))) throw Error('Unbound operation detail');
            }
          }
          if (!same(view.buybacks.program?.delivery_detail,manifest.sections.buyback_program) || !same(view.reactions.delivery_detail,manifest.sections.reactions) || !same(view.composite_history?.delivery_detail,manifest.sections.composite_history)) throw Error('Unbound section detail');
          contexts.set(view,{origin,manifest});
          return view;
        } catch (error) {problems.push(error.message);}
      }
      // Older complete packets remain usable only within the initial byte bound.
      // This path neither downloads a huge packet nor invents an empty snapshot.
      for (const origin of ORIGINS) {
        try {
          const view = packet(parse(await get(origin,'data/auction-desk.json',INITIAL,true)));
          if (view.delivery) throw Error('A projection requires its bound manifest');
          return view;
        } catch (error) {problems.push(error.message);}
      }
      throw Error('Desk publication unavailable: '+[...new Set(problems)].join('; '));
    }
    async function detail(view,row) {
      if (!row.delivery_detail) return row;
      const ctx = contexts.get(view), bound = row.delivery_detail;
      if (!ctx || ![...view.auctions,...view.today.auctions,...view.buybacks.operations,...view.today.buybacks].includes(row)) throw Error('Detail is not part of the displayed snapshot');
      const doc = await artifact(ctx.origin,bound.artifact,'rows');
      if (doc.contract !== CONTRACT || doc.kind !== bound.kind || !Array.isArray(doc.rows) || !object(doc.rows[bound.index])) throw Error('Invalid retained operation');
      const full = doc.rows[bound.index];
      const excluded = bound.kind === 'auction' ? ['grading_inputs','bidder_share_inputs'] : ['measurement_inputs','normalization_inputs'];
      const projected = Object.fromEntries(Object.entries(full).filter(([k]) => !excluded.includes(k)));
      projected.delivery_detail = bound; projected.delivery_contract = CONTRACT;
      if (bound.kind === 'auction') {
        projected.grading_contract = full.grading_inputs?.contract ?? null;
        projected.grading_status = full.grading_inputs?.status ?? null;
        projected.grading_score = full.grading_inputs?.score ?? null;
      } else {
        projected.normalized_metric_summary = Object.fromEntries(Object.entries(full.normalization_inputs?.fields || {}).map(([k,v]) => [k,Object.fromEntries(['status','value','unit'].map(n=>[n,v[n]??null]))]));
        projected.measurement_summary = Object.fromEntries(Object.entries(full.measurement_inputs || {}).filter(([k])=>!['input','size_comparison'].includes(k)));
        projected.measurement_summary.size_comparison = Object.fromEntries(Object.entries(full.measurement_inputs?.size_comparison || {}).filter(([k])=>k!=='prior'));
      }
      if (!same(projected,row)) throw Error('Retained operation differs from the displayed measurement');
      return full;
    }
    async function program(view) {
      const ctx = contexts.get(view);
      if (!ctx) return view.buybacks.program;
      const doc = await artifact(ctx.origin,ctx.manifest.sections.buyback_program,'sections');
      if (doc.contract !== CONTRACT || doc.section !== 'buyback_program' || !object(doc.value)) throw Error('Invalid retained program');
      const projected = Object.fromEntries(Object.entries(doc.value).filter(([k])=>k!=='aggregate_inputs'));
      const aggregate = doc.value.aggregate_inputs || {};
      projected.aggregate_summary = Object.fromEntries(Object.entries(aggregate).filter(([k])=>!['windows','by_bucket'].includes(k)));
      projected.aggregate_summary.windows = Object.fromEntries(Object.entries(aggregate.windows || {}).map(([k,v])=>[k,Object.fromEntries(Object.entries(v).filter(([n])=>n!=='inputs'))]));
      projected.delivery_detail = ctx.manifest.sections.buyback_program;
      if (!same(projected,view.buybacks.program)) throw Error('Retained program differs from the displayed aggregate');
      return doc.value;
    }
    function source(view) {
      const ctx = contexts.get(view);
      return ctx ? {url:ctx.origin+ctx.manifest.source_packet.key,bytes:ctx.manifest.source_packet.bytes,sha256:ctx.manifest.source_packet.sha256} : null;
    }
    return {load,detail,program,source};
  }
  return {CONTRACT,FLAGS,reference,packet,bytes,createClient};
});
