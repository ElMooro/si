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

  const post = async (batch) => {
    const r = await fetch(INGEST, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: TOKEN, kind: 'series', series: batch })
    }).then(x => x.json()).catch(e => ({ ok: false, error: String(e) }));
    return r;
  };

  if (!/^https:\/\//.test(INGEST)) {
    console.error('[JH] ingest URL not injected — aborting rather than '
                + 'posting into the void'); return;
  }
  // v3: keep everything in memory first. A failed POST must never cost
  // a re-fetch of 27 series — window.__JH_ICE survives, and
  // window.__JH_RETRY() re-posts whatever has not landed.
  window.__JH_ICE = window.__JH_ICE || [];
  const HAVE = new Set(window.__JH_ICE.map(x => x.id));
  console.log('%c[JH] fetching ' + IDS.length + ' archived series…',
              'color:#7fb0d0;font-weight:bold');
  let batch = [], done = 0, failed = [];

  for (const id of IDS) {
    if (HAVE.has(id)) { done++; continue; }   // already collected
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
  const unsent = window.__JH_ICE.filter(x => !x.__sent).length;
  if (unsent) console.log('%c[JH] ' + unsent + ' collected but UNSENT — '
    + 'run  __JH_RETRY()  to post them (no re-fetch needed)',
    'color:#e0b552;font-weight:bold');
  if (failed.length) console.log('[JH] not found in archive:', failed.join(' '));
})();
