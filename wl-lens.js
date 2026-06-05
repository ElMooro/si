/* wl-lens.js — "watchlist as a lens": filter any signal page down to the
 * tickers on one of your watchlists. Reads the same localStorage the Chart Pro
 * watchlist writes (jh_custom_watchlists + jh_favorites), so it works with no
 * backend. Drop into any page:
 *
 *   <script src="/wl-lens.js"></script>
 *   <script>WLLens.mount({ rowSelector: '.signal-row', tickerAttr: 'data-ticker' });</script>
 *
 * Or, if rows expose their ticker as text, pass a tickerFn.
 * Honors ?watchlist=<id> in the URL for deep-linking (e.g. /radar?watchlist=holdings).
 */
(function () {
  if (window.WLLens) return;

  function readLists() {
    const lists = [];
    try {
      const favs = JSON.parse(localStorage.getItem('jh_favorites') || '[]');
      if (favs && favs.length) lists.push({ id: 'favorites', name: '★ Favorites', tickers: favs });
    } catch (e) {}
    try {
      const wl = JSON.parse(localStorage.getItem('jh_custom_watchlists') || '{}');
      Object.keys(wl).forEach(id => {
        const l = wl[id];
        if (l && Array.isArray(l.tickers)) lists.push({ id, name: l.name || id, tickers: l.tickers });
      });
    } catch (e) {}
    return lists;
  }

  let CFG = null, ACTIVE = null;

  function tickerOf(row) {
    if (CFG.tickerFn) return (CFG.tickerFn(row) || '').toUpperCase();
    if (CFG.tickerAttr && row.getAttribute(CFG.tickerAttr)) return row.getAttribute(CFG.tickerAttr).toUpperCase();
    // heuristic: first all-caps 1-5 letter token in the row text
    const m = (row.textContent || '').match(/\b[A-Z]{1,5}\b/);
    return m ? m[0] : '';
  }

  function applyFilter() {
    const rows = document.querySelectorAll(CFG.rowSelector);
    if (!ACTIVE) { rows.forEach(r => { r.style.display = ''; }); updateCount(rows.length, rows.length); return; }
    const set = new Set(ACTIVE.tickers.map(t => t.toUpperCase()));
    let shown = 0;
    rows.forEach(r => {
      const t = tickerOf(r);
      const on = t && set.has(t);
      r.style.display = on ? '' : 'none';
      if (on) shown++;
    });
    updateCount(shown, rows.length);
  }

  function updateCount(shown, total) {
    const el = document.getElementById('wllens-count');
    if (el) el.textContent = ACTIVE ? `${shown} of ${total} · ${ACTIVE.name}` : `${total} (all)`;
  }

  function buildBar() {
    const lists = readLists();
    const bar = document.createElement('div');
    bar.id = 'wllens-bar';
    bar.innerHTML =
      '<span class="wll-label">🔭 Filter by watchlist:</span>' +
      '<select id="wllens-select"><option value="">All (no filter)</option>' +
      lists.map(l => `<option value="${l.id}">${l.name} (${l.tickers.length})</option>`).join('') +
      '</select>' +
      '<span class="wll-count" id="wllens-count"></span>' +
      (lists.length ? '' : '<span class="wll-empty">No saved watchlists yet — build one in Chart Pro.</span>');
    return { bar, lists };
  }

  function injectCSS() {
    if (document.getElementById('wllens-css')) return;
    const s = document.createElement('style'); s.id = 'wllens-css';
    s.textContent = [
      '#wllens-bar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:9px 14px;margin:0 0 12px;background:#0c1018;border:1px solid #1c2433;border-radius:10px;font-family:ui-monospace,Menlo,monospace;font-size:12px;color:#a8b3c7}',
      '.wll-label{color:#22d3ee;font-weight:700}',
      '#wllens-select{background:#0f1420;color:#e1e8f4;border:1px solid #2a3550;border-radius:6px;padding:5px 9px;font-family:inherit;font-size:12px;cursor:pointer}',
      '.wll-count{color:#6f7b91;margin-left:auto}',
      '.wll-empty{color:#6f7b91;font-style:italic}',
    ].join('');
    document.head.appendChild(s);
  }

  function mount(cfg) {
    CFG = Object.assign({ rowSelector: '.signal-row', tickerAttr: 'data-ticker', mountTarget: null }, cfg || {});
    injectCSS();
    const { bar, lists } = buildBar();
    // place the bar
    let host = CFG.mountTarget ? document.querySelector(CFG.mountTarget) : null;
    if (host) host.prepend(bar);
    else {
      const firstRow = document.querySelector(CFG.rowSelector);
      const anchor = firstRow ? firstRow.parentElement : document.body;
      anchor.parentElement ? anchor.parentElement.insertBefore(bar, anchor) : document.body.prepend(bar);
    }
    const sel = document.getElementById('wllens-select');
    sel.addEventListener('change', () => {
      ACTIVE = sel.value ? lists.find(l => l.id === sel.value) : null;
      // reflect in URL
      const u = new URL(location.href);
      if (ACTIVE) u.searchParams.set('watchlist', ACTIVE.id); else u.searchParams.delete('watchlist');
      history.replaceState(null, '', u);
      applyFilter();
    });
    // honor ?watchlist= deep link
    const dl = new URLSearchParams(location.search).get('watchlist');
    if (dl) { const l = lists.find(x => x.id === dl); if (l) { sel.value = dl; ACTIVE = l; } }
    applyFilter();
    // re-apply if the page re-renders rows (observe the row container)
    const cont = document.querySelector(CFG.rowSelector)?.parentElement;
    if (cont && window.MutationObserver) {
      let t = null;
      new MutationObserver(() => { clearTimeout(t); t = setTimeout(applyFilter, 150); }).observe(cont, { childList: true });
    }
    return { apply: applyFilter };
  }

  window.WLLens = { mount, readLists };
})();
