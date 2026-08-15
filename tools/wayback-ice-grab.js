/* JustHodl — archived FRED history grabber (v6, clean rewrite)
 *
 * Fetches pre-truncation ICE BofA history from web.archive.org and
 * saves it as ONE local file. Upload happens on a separate page
 * (ice-recover.html) because archive.org's wombat.js rewrites every
 * outbound request made from its own pages, and the raw replay files
 * carry no CORS headers so an off-origin page can't read them either.
 * A downloaded file is the only channel neither restriction touches.
 *
 * USE: open any page on web.archive.org, paste this once, wait for it
 * to finish. It saves jh-ice.json to your Downloads folder.
 */
(async function () {
  'use strict';
  var CUTOFF = '2023-08-14';   // our authentic FRED window starts 08-15
  var IDS = [
    'BAMLH0A0HYM2', 'BAMLC0A0CM', 'BAMLC0A1CAAA', 'BAMLC0A2CAA',
    'BAMLC0A3CA', 'BAMLC0A4CBBB', 'BAMLH0A1HYBB', 'BAMLH0A2HYB',
    'BAMLH0A3HYC', 'BAMLC0A0CMEY', 'BAMLC0A1CAAAEY', 'BAMLC0A2CAAEY',
    'BAMLC0A3CAEY', 'BAMLC0A4CBBBEY', 'BAMLH0A0HYM2EY',
    'BAMLH0A1HYBBEY', 'BAMLH0A2HYBEY', 'BAMLH0A3HYCEY',
    'BAMLCC0A0CMTRIV', 'BAMLCC0A1AAATRIV', 'BAMLCC0A2AATRIV',
    'BAMLCC0A3ATRIV', 'BAMLCC0A4BBBTRIV', 'BAMLHYH0A0HYM2TRIV',
    'BAMLHYH0A1BBTRIV', 'BAMLHYH0A2BTRIV', 'BAMLHYH0A3CMTRIV'
  ];
  var STAMPS = ['20250601', '20240901', '20240301', '20231001'];

  function urlForms(ts, id) {
    return [
      'https://web.archive.org/web/' + ts + 'id_/https://' +
        'fred.stlouisfed.org/data/' + id + '.txt',
      'https://web.archive.org/web/' + ts + 'id_/https://' +
        'fred.stlouisfed.org/graph/fredgraph.csv?id=' + id
    ];
  }

  function parseRows(text) {
    var lines = text.split(/\r?\n/);
    var out = [];
    for (var i = 0; i < lines.length; i++) {
      var ln = lines[i];
      if (!/^(19|20)\d\d-\d\d-\d\d/.test(ln)) continue;
      var parts = ln.replace(/,/g, ' ').trim().split(/\s+/);
      if (parts.length < 2) continue;
      var v = parts[1];
      if (v === '.' || v === '' || !isFinite(Number(v))) continue;
      if (parts[0] > CUTOFF) continue;
      out.push([parts[0], Number(v)]);
    }
    return out;
  }

  async function fetchOne(id) {
    for (var s = 0; s < STAMPS.length; s++) {
      var forms = urlForms(STAMPS[s], id);
      for (var f = 0; f < forms.length; f++) {
        try {
          var res = await fetch(forms[f]);
          if (!res || !res.ok) continue;
          var text = await res.text();
          var rows = parseRows(text);
          if (rows.length > 500) {
            return { rows: rows, capture: STAMPS[s], url: forms[f] };
          }
        } catch (err) { /* try next form/stamp */ }
      }
    }
    return null;
  }

  console.log('%c[JH] fetching ' + IDS.length + ' archived series…',
              'color:#7fb0d0;font-weight:bold');

  var collected = [];
  var failed = [];

  for (var i = 0; i < IDS.length; i++) {
    var id = IDS[i];
    var result = await fetchOne(id);
    if (!result) {
      failed.push(id);
      console.warn('[JH] ' + id + ' — no archived copy found');
      continue;
    }
    var first = result.rows[0][0];
    var last = result.rows[result.rows.length - 1][0];
    console.log('[JH] ' + id + ': ' + result.rows.length + ' rows  ' +
                first + ' -> ' + last + '  (capture ' +
                result.capture + ')');
    collected.push({ id: id, rows: result.rows,
                     source: 'wayback-raw', capture: result.capture });
  }

  console.log('%c[JH] fetch complete — ' + collected.length + '/' +
              IDS.length + ' series collected',
              'color:#7fb0d0;font-weight:bold');

  if (!collected.length) {
    console.error('[JH] nothing collected — nothing to save');
    return;
  }

  try {
    var blob = new Blob([JSON.stringify(collected)],
                        { type: 'application/json' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'jh-ice.json';
    document.body.appendChild(a);
    a.click();
    a.remove();
    console.log('%c[JH] saved jh-ice.json (' +
                Math.round(blob.size / 1024) + ' KB) — open the ' +
                'recovery page and choose this file',
                'color:#5fbf7f;font-weight:bold');
  } catch (err) {
    console.error('[JH] could not save file:', err);
  }

  if (failed.length) {
    console.log('[JH] not found in archive:', failed.join(' '));
  }
})();
