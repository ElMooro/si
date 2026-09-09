/* Reviewed provider catalogue and selected series output traversal.
 * Catalogue: justhodl-provider-catalog. ECB/Eurostat series: justhodl-series-extractor.
 * Tier-0 flow inventory: ops_5047..5052, not a Lambda-owned output.
 */
(function (global) {
'use strict';
const CATALOG_ENGINE = 'justhodl-provider-catalog';
const SERIES_ENGINE = 'justhodl-series-extractor';
const SERIES_PROVIDERS = new Set(['ecb', 'eurostat']);
function integer(n) { return Number.isSafeInteger(n) && n >= 0; }
function slug(value) {
  if (typeof value !== 'string' || !/^[a-z0-9][a-z0-9-]{0,63}$/.test(value)) throw new Error('Choose a valid provider from the Data page.');
  return value;
}
function flow(value) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}$/.test(value) || value.includes('..')) throw new Error('This dataset identifier is outside the reviewed series path contract.');
  return value;
}
function object(doc) { return doc && typeof doc === 'object' && !Array.isArray(doc); }
function pageNumber(n, count) {
  if (!integer(n) || !integer(count) || n >= count) throw new Error('Page is outside the published manifest.');
  return n;
}
function receipt(key, producer, document) {
  return {key, producer, received_at: new Date().toISOString(), availability: 'Current returned output; not an immutable or point-in-time certified snapshot.', document};
}
function createClient(provider, request) {
  provider = slug(provider);
  request = request || global.fetch.bind(global);
  const base = '/data/providers/' + provider;
  async function json(key) {
    const response = await request(key, {cache: 'no-store'});
    if (!response.ok) { const error = new Error('Output unavailable (HTTP ' + response.status + '): ' + key); error.status = response.status; throw error; }
    const doc = await response.json();
    if (!object(doc)) throw new Error('Invalid object response: ' + key);
    return doc;
  }
  function seriesOnly() { if (!SERIES_PROVIDERS.has(provider)) throw new Error('No series-extractor contract is reviewed for this provider.'); }
  return {
    provider, hasSeries: SERIES_PROVIDERS.has(provider),
    async catalog() {
      const key = base + '.json', d = await json(key);
      if (d.slug !== provider || !integer(d.n_pages) || !Array.isArray(d.keys)) throw new Error('Catalogue identity or page manifest is invalid.');
      return receipt(key, CATALOG_ENGINE, d);
    },
    async catalogPage(n, manifest) {
      pageNumber(n, manifest.n_pages);
      const key = base + '/page-' + String(n).padStart(3, '0') + '.json', d = await json(key);
      if (d.page !== n || !Array.isArray(d.keys)) throw new Error('Catalogue page does not match the requested page.');
      return receipt(key, CATALOG_ENGINE, d);
    },
    async seriesManifest() {
      seriesOnly();
      const key = base + '/series-manifest.json', d = await json(key);
      if (d.provider !== provider || !integer(d.n_pages)) throw new Error('Series manifest identity or page count is invalid.');
      return receipt(key, SERIES_ENGINE, d);
    },
    async seriesPage(n, manifest) {
      seriesOnly(); pageNumber(n, manifest.n_pages);
      const key = base + '/series/page-' + String(n).padStart(4, '0') + '.json', d = await json(key);
      if (d.page !== n || !Array.isArray(d.rows) || d.count !== d.rows.length) throw new Error('Series page identity or row count is invalid.');
      return receipt(key, SERIES_ENGINE, d);
    },
    async flowIndex() {
      seriesOnly();
      const key = '/data/index/' + provider + '/flows.json.gz', d = await json(key);
      if (!object(d.flows)) throw new Error('Flow inventory is unavailable or malformed.');
      return receipt(key, 'Ops-generated flow inventory (ops_5047–5052); no Lambda writer association', d);
    },
    async blocks(name) {
      seriesOnly(); name = flow(name);
      const key = '/data/index/' + provider + '/t1/' + name + '.blocks.json', d = await json(key);
      if (d.provider !== provider || d.flow !== name || d.entry_schema !== 2 || !integer(d.n) || !integer(d.bytes) || !Array.isArray(d.blocks)) throw new Error('Series block manifest identity or schema is invalid.');
      let bytes = 0, count = 0;
      for (const b of d.blocks) {
        if (!object(b) || typeof b.k !== 'string' || b.o !== bytes || !integer(b.c) || b.c < 1 || !integer(b.n) || b.n < 1) throw new Error('Series block offsets or counts are invalid.');
        bytes += b.c; count += b.n;
      }
      if (bytes !== d.bytes || count !== d.n) throw new Error('Series block manifest totals do not reconcile.');
      return receipt(key, SERIES_ENGINE, d);
    },
    async block(n, manifest, seriesManifest) {
      seriesOnly(); flow(manifest.flow);
      if (manifest.provider !== provider || manifest.entry_schema !== 2) throw new Error('Block manifest belongs to another provider or schema.');
      pageNumber(n, manifest.blocks.length);
      const b = manifest.blocks[n], end = b.o + b.c - 1;
      const key = '/data/index/' + provider + '/t1/' + manifest.flow + '.jsonl';
      const response = await request(key, {cache: 'no-store', headers: {Range: 'bytes=' + b.o + '-' + end}});
      if (response.status !== 206) throw new Error('Server did not return the requested byte range; no full-file body was read.');
      if (response.headers.get('Content-Range') !== 'bytes ' + b.o + '-' + end + '/' + manifest.bytes) throw new Error('Returned byte range differs from the block manifest.');
      const buffer = await response.arrayBuffer();
      if (buffer.byteLength !== b.c) throw new Error('Returned block byte count differs from the manifest.');
      const text = new TextDecoder('utf-8', {fatal: true}).decode(buffer);
      if (!text.endsWith('\n')) throw new Error('Series block ends with an incomplete row.');
      const rows = text.slice(0, -1).split('\n').map(line => JSON.parse(line));
      if (rows.length !== b.n || rows.some(r => !object(r) || typeof r.id !== 'string' || !integer(r.p) || r.p >= seriesManifest.n_pages) || rows[0].id !== b.k) throw new Error('Series block row count, boundary or source-page pointer is invalid.');
      return receipt(key, SERIES_ENGINE, {selected_block: n, byte_range: response.headers.get('Content-Range'), rows, source_pages: [...new Set(rows.map(r => r.p))].sort((a, b) => a - b), completeness: 'All rows in this selected index block. Open an original source page for fields omitted by the compact index.'});
    }
  };
}
function inspect(container, record) {
  container.replaceChildren();
  if (!global.JHDataInspector) { container.textContent = 'The complete data viewer is unavailable. Refresh to retry.'; return; }
  global.JHDataInspector.inspect(container, record, record.producer + ' → ' + record.key);
}
function newestOnly() {
  let revision = 0;
  return async (load, render, failure) => {
    const current = ++revision;
    try { const value = await load(); if (current === revision) render(value); }
    catch (error) { if (current === revision) failure(error); }
  };
}
function start() {
  const $ = id => document.getElementById(id);
  if (!$('provider-app')) return;
  const errorAt = id => error => { $(id).textContent = error.message; };
  let client;
  try { client = createClient(new URLSearchParams(location.search).get('p')); }
  catch (error) { errorAt('status')(error); return; }
  let catalog, series, files = [], flowIndex, selectedBlocks, searchPage = 0;
  const catalogRun = newestOnly(), pageRun = newestOnly(), blockRun = newestOnly(), flowRun = newestOnly();
  function textCell(row, value) { const td = document.createElement('td'); td.textContent = value == null ? '—' : String(value); row.append(td); }
  function renderFiles() {
    $('files').replaceChildren();
    const query = $('file-filter').value.trim().toLowerCase();
    const status = $('file-status').value;
    let shown = 0;
    for (const item of files) {
      if (query && !String(item.key).toLowerCase().includes(query)) continue;
      const state = item.status || (item.missing ? 'missing' : 'unknown');
      if (status && state !== status) continue;
      const row = document.createElement('tr');
      [item.key, state, item.bytes, item.age_h, (item.engines || []).join(', ')].forEach(value => textCell(row, value));
      $('files').append(row); shown++;
    }
    $('file-count').textContent = shown + ' of ' + files.length + ' keys in this selected page. Filters apply to this page only.';
  }
  function showCatalog(record) { files = record.document.keys; renderFiles(); inspect($('catalog-data'), record); }
  $('file-filter').addEventListener('input', renderFiles);
  $('file-status').addEventListener('change', renderFiles);
  $('catalog-load').onclick = () => catalogRun(async () => {
    const n = Number($('catalog-page').value);
    return n === -1 ? catalog : client.catalogPage(n, catalog.document);
  }, record => { showCatalog(record); $('status').textContent = 'Selected catalogue output loaded.'; }, errorAt('status'));
  $('series-load').onclick = () => {
    $('series-data').replaceChildren(); $('series-status').textContent = 'Loading original source page…';
    return pageRun(() => client.seriesPage(Number($('series-page').value), series.document), record => {
    inspect($('series-data'), record); $('series-status').textContent = 'All ' + record.document.rows.length + ' rows and fields from original page ' + record.document.page + ' are available below.';
    }, errorAt('series-status'));
  };
  function loadBlock() {
    if (!selectedBlocks) return;
    $('block-status').textContent = 'Loading selected byte range…';
    $('block-data').replaceChildren(); $('source-pages').replaceChildren();
    blockRun(() => client.block(Number($('block-number').value), selectedBlocks.document, series.document), record => {
      inspect($('block-data'), record); $('block-status').textContent = 'Complete selected index block. Source page numbers below open the original records.';
      $('source-pages').replaceChildren();
      for (const n of record.document.source_pages) {
        const button = document.createElement('button'); button.textContent = 'Open original page ' + n;
        button.onclick = () => { $('series-page').value = n; $('series-load').click(); $('series-data').scrollIntoView({behavior: 'smooth'}); };
        $('source-pages').append(button);
      }
    }, errorAt('block-status'));
  }
  $('block-load').onclick = loadBlock;
  async function selectFlow(name) {
    selectedBlocks = null;
    // Invalidate any older in-flight block before a new dataset is selected.
    blockRun(async () => null, () => {}, () => {});
    $('block-data').replaceChildren(); $('source-pages').replaceChildren(); $('block-controls').hidden = true;
    $('block-status').textContent = 'Loading block manifest for ' + name + '…';
    const range = flowIndex.document.flows[name];
    if (range && integer(range.lo) && integer(range.hi) && range.lo <= range.hi && range.hi < series.document.n_pages) {
      $('series-page').value = range.lo;
      $('series-status').textContent = name + ' is indexed across original pages ' + range.lo + '–' + range.hi + '. Every published page is accessible with the page control.';
    }
    flowRun(() => client.blocks(name), record => {
      selectedBlocks = record; inspect($('block-manifest-data'), record);
      $('block-status').textContent = record.document.blocks.length + ' blocks / ' + record.document.n + ' compact index entries. Each block and every original page can be opened.';
      if (!record.document.blocks.length) return;
      $('block-controls').hidden = false; $('block-number').value = 0; $('block-number').max = record.document.blocks.length - 1;
      $('block-range').textContent = '0–' + (record.document.blocks.length - 1); loadBlock();
    }, error => { $('block-manifest-data').replaceChildren(); $('block-status').textContent = error.message + ' Original series pages remain available; no partial block is shown.'; });
  }
  function search() {
    $('flows').replaceChildren();
    const query = $('flow-search').value.trim().toLowerCase();
    if (!flowIndex || query.length < 2) { $('flow-count').textContent = 'Type at least 2 characters to search dataset identifiers.'; $('flow-prev').disabled = $('flow-next').disabled = true; return; }
    const hits = Object.keys(flowIndex.document.flows).filter(name => name.toLowerCase().includes(query)).sort();
    const pages = Math.max(1, Math.ceil(hits.length / 50)); searchPage = Math.min(searchPage, pages - 1);
    for (const name of hits.slice(searchPage * 50, (searchPage + 1) * 50)) {
      const item = document.createElement('li'), button = document.createElement('button'); button.textContent = name; button.onclick = () => selectFlow(name); item.append(button); $('flows').append(item);
    }
    $('flow-count').textContent = hits.length + ' matches · page ' + (searchPage + 1) + ' of ' + pages + '. All matches are reachable.';
    $('flow-prev').disabled = searchPage === 0; $('flow-next').disabled = searchPage + 1 >= pages;
  }
  $('flow-search').addEventListener('input', () => { searchPage = 0; search(); });
  $('flow-prev').onclick = () => { searchPage--; search(); };
  $('flow-next').onclick = () => { searchPage++; search(); };
  client.catalog().then(record => {
    catalog = record; const d = record.document;
    $('name').textContent = d.name || client.provider; document.title = (d.name || client.provider) + ' — Data — JustHodl.AI';
    $('provider-meta').textContent = 'API: ' + (d.api || 'unknown') + ' · Referenced collectors: ' + (d.engines || []).join(', ');
    $('status').textContent = 'Catalogue producer: ' + CATALOG_ENGINE + '. ' + d.n_keys + ' indexed keys; ' + d.n_pages + ' continuation pages. Counts describe the published catalogue, not verified source freshness.';
    $('catalog-page').max = d.n_pages - 1; $('catalog-range').textContent = '-1 (manifest first keys)' + (d.n_pages ? ', then 0–' + (d.n_pages - 1) : '; no continuation pages');
    $('catalog-load').disabled = false; showCatalog(record); inspect($('catalog-manifest-data'), record);
    if (!client.hasSeries) { $('series-note').textContent = 'This provider has no reviewed series-extractor page contract. All catalogue pages and returned metadata remain available above.'; return; }
    $('series-note').textContent = 'Series producer: ' + SERIES_ENGINE + '. Original source pages preserve all returned fields; compact block indexes contain selected series attributes only.';
    client.seriesManifest().then(record => {
      series = record; inspect($('series-manifest-data'), record);
      $('series-controls').hidden = false; $('series-page').max = record.document.n_pages - 1;
      $('series-range').textContent = record.document.n_pages ? '0–' + (record.document.n_pages - 1) : 'No published pages'; $('series-load').disabled = !record.document.n_pages;
      client.flowIndex().then(record => { flowIndex = record; inspect($('flow-index-data'), record); search(); }).catch(errorAt('flow-count'));
    }).catch(errorAt('series-status'));
  }).catch(errorAt('status'));
}
const api = {createClient, inspect, newestOnly, slug, flow, start};
if (typeof module !== 'undefined' && module.exports) module.exports = api;
global.JHProviderData = api;
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
}
})(typeof window !== 'undefined' ? window : globalThis);
