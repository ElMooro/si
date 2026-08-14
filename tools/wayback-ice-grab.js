/* JustHodl — archived FRED history grabber (paste into DevTools console)
 *
 * Why this exists: every automated egress we have is blocked — archive.org
 * returns 498 to AWS Lambda, TradingView refuses the socket on TLS
 * fingerprint, investing.com 403s. Khalid's browser is not blocked. This
 * runs there, pulls the history out of the archived FRED chart, and POSTs
 * it straight into S3 via the existing authenticated ingest endpoint.
 * No files, no downloads, no uploads.
 *
 * USE: open an archived FRED series page on web.archive.org, click MAX,
 * wait for the chart, then paste this and press Enter.
 */
(async () => {
  const INGEST = 'https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws/';
  const TOKEN  = '__TOKEN__';           // injected below by the setup line
  const CUTOFF = '2023-08-14';          // our banked FRED window starts 08-15

  const id = (location.href.match(/\/series\/([A-Za-z0-9]+)/i) || [])[1]
          || (location.href.match(/[?&]id=([A-Za-z0-9]+)/i) || [])[1];
  if (!id) { console.error('[JH] no series id in URL'); return; }

  const capture = (location.href.match(/web\/(\d{8,14})/) || [])[1] || '';

  /* ── find the chart ──────────────────────────────────────────────── */
  const charts = (window.Highcharts && Highcharts.charts || []).filter(Boolean);
  if (!charts.length) {
    console.error('[JH] no Highcharts object — let the chart finish loading, click MAX, retry');
    return;
  }
  let picked = null, best = 0;
  charts.forEach(c => (c.series || []).forEach(s => {
    const n = (s.options && s.options.data ? s.options.data.length : 0)
            || (s.processedXData ? s.processedXData.length : 0)
            || (s.data ? s.data.length : 0);
    if (n > best) { best = n; picked = s; }
  }));
  if (!picked) { console.error('[JH] no data series found'); return; }

  /* ── extract points, tolerating every Highcharts point shape ─────── */
  const iso = t => {
    const d = new Date(Number(t));
    return isFinite(d) ? d.toISOString().slice(0, 10) : null;
  };
  const rows = new Map();
  const opts = picked.options || {};
  const start = opts.pointStart != null ? opts.pointStart
              : (picked.userOptions || {}).pointStart;
  const step  = opts.pointInterval != null ? opts.pointInterval
              : (picked.userOptions || {}).pointInterval;

  const raw = (opts.data && opts.data.length) ? opts.data
            : (picked.processedXData
               ? picked.processedXData.map((x, i) => [x, picked.processedYData[i]])
               : (picked.data || []).map(p => [p.x, p.y]));

  raw.forEach((p, i) => {
    let x = null, y = null;
    if (Array.isArray(p))            { x = p[0]; y = p[1]; }
    else if (p && typeof p === 'object') { x = p.x; y = (p.y != null ? p.y : p.value); }
    else if (typeof p === 'number' && start != null && step != null) { x = start + i * step; y = p; }
    const d = iso(x);
    if (d && y != null && isFinite(Number(y)) && d <= CUTOFF) rows.set(d, Number(y));
  });

  const obs = [...rows.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  console.log('[JH] series:', id, '· capture:', capture,
              '· points:', raw.length, '· usable (<=' + CUTOFF + '):', obs.length);
  if (obs.length) console.log('[JH] range:', obs[0][0], '->', obs[obs.length - 1][0]);
  if (obs.length < 50) { console.error('[JH] too few points — click MAX and let it load fully'); return; }

  /* ── post straight to S3 ─────────────────────────────────────────── */
  const res = await fetch(INGEST, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token: TOKEN, kind: 'series', series: [{
      id: id, rows: obs, source: 'wayback-highcharts',
      capture: capture, url: location.href }] })
  }).then(r => r.json()).catch(e => ({ ok: false, error: String(e) }));

  console.log('[JH] ingest:', res);
  if (res && res.ok) {
    console.log('%c[JH] ' + id + ' BANKED — ' + obs.length + ' observations in S3',
                'color:#5fbf7f;font-weight:bold');
  } else {
    console.error('[JH] ingest failed:', res);
  }
})();
