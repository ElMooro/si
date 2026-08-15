/* JustHodl — post collected series, bypassing archive.org's wombat.
 *
 * web.archive.org injects wombat.js, which monkey-patches window.fetch
 * and rewrites EVERY absolute URL to route back through web.archive.org
 * — so our POST became web.archive.org/web/<ts>/https://...lambda-url...
 * and 404'd. Nothing was wrong with the data or the endpoint.
 *
 * Fix: pull a PRISTINE fetch/XHR out of a fresh same-origin iframe.
 * wombat patches the top window; a newly-created about:blank frame has
 * untouched natives. XHR fallback if fetch is patched there too.
 *
 * Requires window.__JH_ICE (already populated by the grabber). Paste
 * this in the SAME tab — do not reload, or the data is lost.
 */
(async () => {
  const INGEST = '__INGEST__';
  const TOKEN  = '__TOKEN__';

  const data = window.__JH_ICE || [];
  if (!data.length) {
    console.error('[JH] window.__JH_ICE is empty — run the grabber first, same tab');
    return;
  }
  const todo = data.filter(x => !x.__sent);
  console.log('[JH] ' + todo.length + ' series to post (of ' + data.length + ' collected)');
  if (!todo.length) { console.log('[JH] nothing pending'); return; }

  /* ── pristine transport from a fresh iframe ───────────────────────── */
  const fr = document.createElement('iframe');
  fr.style.display = 'none';
  document.body.appendChild(fr);
  const w = fr.contentWindow;
  const cleanFetch = w.fetch ? w.fetch.bind(w) : null;
  const CleanXHR = w.XMLHttpRequest;

  const viaXHR = (payload) => new Promise((resolve) => {
    try {
      const x = new CleanXHR();
      x.open('POST', INGEST, true);
      x.setRequestHeader('Content-Type', 'application/json');
      x.onload = () => { try { resolve(JSON.parse(x.responseText)); }
                         catch (e) { resolve({ ok: false, error: 'parse: ' + x.status }); } };
      x.onerror = () => resolve({ ok: false, error: 'xhr error ' + x.status });
      x.send(payload);
    } catch (e) { resolve({ ok: false, error: String(e) }); }
  });

  const send = async (batch) => {
    const payload = JSON.stringify({ token: TOKEN, kind: 'series',
      series: batch.map(b => ({ id: b.id, rows: b.rows, source: b.source,
                                capture: b.capture, url: b.url })) });
    if (cleanFetch) {
      try {
        const r = await cleanFetch(INGEST, { method: 'POST',
          headers: { 'Content-Type': 'application/json' }, body: payload });
        return await r.json();
      } catch (e) { /* fall through to XHR */ }
    }
    return await viaXHR(payload);
  };

  let sent = 0;
  for (let i = 0; i < todo.length; i += 3) {
    const chunk = todo.slice(i, i + 3);
    const r = await send(chunk);
    console.log('[JH] batch ' + chunk.map(c => c.id).join(',') + ' ->', r);
    if (r && r.ok) { chunk.forEach(c => c.__sent = 1); sent += chunk.length; }
  }
  fr.remove();

  const left = data.filter(x => !x.__sent).length;
  console.log('%c[JH] posted ' + sent + ' series · ' + left + ' still pending',
              left ? 'color:#e05252;font-weight:bold' : 'color:#5fbf7f;font-weight:bold');
})();
