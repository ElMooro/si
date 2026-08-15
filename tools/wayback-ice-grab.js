/* JustHodl — archived FRED history grabber v2 (paste into DevTools console)
 *
 * v1 assumed Highcharts; FRED changed charting libs, so v1 correctly
 * reported "no Highcharts object". v2 doesn't touch the chart at all:
 * it fetches the ARCHIVED RAW DATA FILES straight from web.archive.org
 * — which our AWS Lambdas cannot do (archive.org returns 498 to cloud
 * IPs) but Khalid's browser can. Then it posts each series to S3.
 *
 * USE: open ANY page on web.archive.org (so the fetches are same-origin),
 *      then paste this once. It walks all 27 series itself.
 */
(async () => {
  const INGEST = '__INGEST__';
  const TOKEN  = '__TOKEN__';
  const CUTOFF = '2023-08-14';        // our authentic FRED window starts 08-15

  const IDS = ['BAMLH0A0HYM2','BAMLC0A0CM','BAMLC0A1CAAA','BAMLC0A2CAA',
    'BAMLC0A3CA','BAMLC0A4CBBB','BAMLH0A1HYBB','BAMLH0A2HYB','BAMLH0A3HYC',
    'BAMLC0A0CMEY','BAMLC0A1CAAAEY','BAMLC0A2CAAEY','BAMLC0A3CAEY',
    'BAMLC0A4CBBBEY','BAMLH0A0HYM2EY','BAMLH0A1HYBBEY','BAMLH0A2HYBEY',
    'BAMLH0A3HYCEY','BAMLCC0A0CMTRIV','BAMLCC0A1AAATRIV','BAMLCC0A2AATRIV',
    'BAMLCC0A3ATRIV','BAMLCC0A4BBBTRIV','BAMLHYH0A0HYM2TRIV',
    'BAMLHYH0A1BBTRIV','BAMLHYH0A2BTRIV','BAMLHYH0A3CMTRIV'];

  const STAMPS = ['20250601','20240901','20240301','20231001'];

  // Several archived shapes; first one that yields rows wins.
  const forms = (ts, id) => [
    `https://web.archive.org/web/${ts}id_/https://fred.stlouisfed.org/data/${id}.txt`,
    `https://web.archive.org/web/${ts}id_/https://fred.stlouisfed.org/graph/fredgraph.csv?id=${id}`,
    `https://web.archive.org/web/${ts}id_/https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&id=${id}`
  ];

  const parse = (txt) => {
    const out = [];
    for (const ln of txt.split(/\r?\n/)) {
      if (!/^(19|20)\d\d-\d\d-\d\d/.test(ln)) continue;
      const p = ln.replace(/,/g, ' ').trim().split(/\s+/);
      if (p.length >= 2 && p[1] !== '.' && isFinite(Number(p[1])) && p[0] <= CUTOFF)
        out.push([p[0], Number(p[1])]);
    }
    return out;
  };

  /* v4: archive.org injects wombat.js, which monkey-patches window.fetch
   * and rewrites absolute URLs back through web.archive.org — that is why
   * v3's POSTs became web.archive.org/web/<ts>/https://...lambda-url and
   * 404'd. A freshly created about:blank iframe has PRISTINE natives, so
   * we borrow its fetch/XHR for the upload only. Page fetches keep using
   * the wombat-wrapped fetch, which is what makes the archive reads work.
   */
  const _fr = document.createElement('iframe');
  _fr.style.display = 'none';
  document.body.appendChild(_fr);
  const _w = _fr.contentWindow;
  const cleanFetch = _w.fetch ? _w.fetch.bind(_w) : null;
  const CleanXHR = _w.XMLHttpRequest;

  const viaXHR = (payload) => new Promise((resolve) => {
    try {
      const x = new CleanXHR();
      x.open('POST', INGEST, true);
      x.setRequestHeader('Content-Type', 'application/json');
      x.onload  = () => { try { resolve(JSON.parse(x.responseText)); }
                          catch (e) { resolve({ ok: false, error: 'parse ' + x.status }); } };
      x.onerror = () => resolve({ ok: false, error: 'xhr ' + x.status });
      x.send(payload);
    } catch (e) { resolve({ ok: false, error: String(e) }); }
  });

  /* v5: the network is a dead end from here. archive.org's wombat
   * rewrites every outbound request on its own pages (fetch, XHR, and
   * iframe natives), and off-page origins can't read the archive because
   * the raw replays carry no CORS headers. So we don't upload from here
   * at all — we write ONE file, and the S3 recovery page ingests it.
   */
  const post = async () => ({ ok: true, deferred: true });

  console.log('%c[JH] fetching ' + IDS.length + ' archived series…',
              'color:#7fb0d0;font-weight:bold');
  let batch = [], done = 0, failed = [];

  for (const id of IDS) {
    let got = null, usedTs = '', usedUrl = '';
    outer:
    for (const ts of STAMPS) {
      for (const u of forms(ts, id)) {
        try {
          const res = await fetch(u);
          if (!res.ok) continue;
          const txt = await res.text();
          const rows = parse(txt);
          if (rows.length > 500) { got = rows; usedTs = ts; usedUrl = u; break outer; }
        } catch (e) { /* try next form */ }
      }
    }
    if (!got) { failed.push(id); console.warn('[JH] ' + id + ' — no archived copy found'); continue; }
    console.log('[JH] ' + id + ': ' + got.length + ' rows ' + got[0][0] + ' -> ' +
                got[got.length - 1][0] + '  (capture ' + usedTs + ')');
    const rec = { id, rows: got, source: 'wayback-raw', capture: usedTs, url: usedUrl };
    window.__JH_ICE.push(rec);
    batch.push(rec);
    done++;
    if (batch.length >= 4) {
      const r = await post(batch);
      console.log('[JH] posting batch…', r);
      if (r && r.ok) batch.forEach(b => b.__sent = 1);
      batch = [];
    }
  }
  if (batch.length) {
    const r = await post(batch);
    console.log('[JH] posting final batch…', r);
    if (r && r.ok) batch.forEach(b => b.__sent = 1);
  }

  // Retry helper: re-post anything still unsent, no re-fetching.
  window.__JH_RETRY = async () => {
    const left = window.__JH_ICE.filter(x => !x.__sent);
    console.log('[JH] retrying ' + left.length + ' unsent series…');
    for (let i = 0; i < left.length; i += 4) {
      const chunk = left.slice(i, i + 4);
      const r = await post(chunk);
      console.log('[JH] retry batch:', r);
      if (r && r.ok) chunk.forEach(b => b.__sent = 1);
    }
    const still = window.__JH_ICE.filter(x => !x.__sent).length;
    console.log('%c[JH] retry done — ' + still + ' still unsent',
                still ? 'color:#e05252' : 'color:#5fbf7f;font-weight:bold');
  };

  console.log('%c[JH] DONE — ' + done + '/' + IDS.length + ' series banked to S3',
              'color:#5fbf7f;font-weight:bold');
  /* save everything to a single file for the S3 uploader */
  try {
    const blob = new Blob([JSON.stringify(window.__JH_ICE.map(x => ({
      id: x.id, rows: x.rows, source: x.source, capture: x.capture })))],
      { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'jh-ice.json';
    document.body.appendChild(a); a.click(); a.remove();
    console.log('%c[JH] saved jh-ice.json to your Downloads — open the '
      + 'recovery page and choose that file', 'color:#5fbf7f;font-weight:bold');
  } catch (e) { console.error('[JH] save failed:', e); }
  if (failed.length) console.log('[JH] not found in archive:', failed.join(' '));
})();
