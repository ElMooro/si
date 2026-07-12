/**
 * JustHodl TV Notes — Content Script
 *
 * Runs on every tradingview.com page. Key advantages over bookmarklet:
 *  - NOT restricted by TradingView's Content Security Policy
 *  - Full same-origin fetch access (session cookies included automatically)
 *  - Persistent across navigation (re-runs via auto-start)
 *  - DOM + localStorage + API access simultaneously
 *
 * Harvest phases:
 *  1. Bulk API probe (8 endpoint patterns — fastest path)
 *  2. Watchlist enumeration → per-symbol notes pull
 *  3. Chart layout text annotations
 *  4. localStorage scan (notes stored client-side)
 *  5. DOM scraping (catches notes visible in the Notes panel)
 *  6. Passive fetch/XHR intercept (catches anything TV loads dynamically)
 */

(function () {
  const WATCHLISTS = [];  // ops 3158: [{id,name,symbols[]}]
  'use strict';
  if (window.__JH_EXT_V1) return;
  window.__JH_EXT_V1 = true;

  // ── Note store ────────────────────────────────────────────────────────────
  const STORE   = new Map();   // id → note object
  const TICKERS = new Set();   // symbols seen
  let   isHarvesting = false;
  let   overlay      = null;
  let   progressPct  = 0;

  // ── Helpers ───────────────────────────────────────────────────────────────
  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  function hashId(sym, ts, text) {
    let h = 0;
    const s = `${sym}|${ts}|${String(text).slice(0, 160)}`;
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
    return `tv-${h.toString(36)}`;
  }

  function keep(sym, text, title, ts) {
    text = String(text || '').trim();
    if (text.length < 2) return false;
    sym = String(sym || 'UNTAGGED').toUpperCase().replace(/[^A-Z0-9:._-]/g, '').slice(0, 30) || 'UNTAGGED';
    if (typeof ts === 'string') { try { ts = new Date(ts).getTime(); } catch (e) { ts = 0; } }
    if (!ts || isNaN(ts)) ts = Date.now();
    const id = hashId(sym, ts, text);
    if (!STORE.has(id)) {
      STORE.set(id, { symbol: sym, text: `[TV:${sym}] ${text}`.slice(0, 7500),
                      title: String(title || '').slice(0, 200), created: ts });
      TICKERS.add(sym);
      updateUI();
      return true;
    }
    return false;
  }

  function mine(obj, hint, depth) {
    if (!obj || depth > 8) return;
    if (Array.isArray(obj)) { obj.forEach(x => mine(x, hint, depth + 1)); return; }
    if (typeof obj !== 'object') return;
    const sym  = obj.symbol || obj.ticker || obj.s || obj.symbol_full || hint;
    const text = obj.text   || obj.note   || obj.content || obj.body || obj.description;
    const hasId = obj.id != null || obj.created != null || obj.created_at != null || obj.updated_at != null;
    if (text && typeof text === 'string' && text.length > 1 && hasId) {
      const ts = obj.created_at || obj.created || obj.updated_at || obj.updated;
      keep(sym, text, obj.title || obj.name || obj.subject, ts);
    }
    for (const k of Object.keys(obj)) {
      if (obj[k] && typeof obj[k] === 'object') mine(obj[k], sym || hint, depth + 1);
    }
  }

  // ── TV same-origin API fetch (cookies auto-included) ─────────────────────
  async function tvGet(path, params) {
    const qs = params ? '?' + new URLSearchParams(params).toString() : '';
    try {
      const r = await fetch(`https://www.tradingview.com${path}${qs}`, {
        credentials: 'include',
        headers: { 'Accept': 'application/json, text/plain, */*',
                   'X-Requested-With': 'XMLHttpRequest',
                   'Referer': 'https://www.tradingview.com/' },
      });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }

  // ── Phase 1: bulk notes (no symbol filter) ────────────────────────────────
  const BULK_ENDPOINTS = [
    ['/note-manager/api/notes/',     { limit: 9999 }],
    ['/note-manager/api/notes/',     { page_size: 9999, offset: 0 }],
    ['/api/v1/text_notes/',          { limit: 9999 }],
    ['/api/v1/text-notes/',          { limit: 9999 }],
    ['/textnotes/list/',             {}],
    ['/api/v2/notes/',               { limit: 9999 }],
    ['/api/v2/chart-notes/',         { limit: 9999 }],
    ['/note-manager/api/',           {}],
    ['/note-manager/api/notes/list/',{}],
    ['/pine-notes/list/',            { limit: 9999 }],
  ];

  async function phaseBulk() {
    setStatus('Probing TradingView notes API…', 5);
    for (const [path, params] of BULK_ENDPOINTS) {
      const d = await tvGet(path, params);
      if (d) {
        mine(d, null, 0);
        if (STORE.size > 0) {
          setStatus(`Bulk endpoint hit — ${STORE.size} notes`, 20);
          return true;
        }
      }
      await sleep(80);
    }
    return false;
  }

  // ── Phase 2: watchlist enumeration ────────────────────────────────────────
  async function getWatchlistSymbols() {
    setStatus('Loading your watchlists…', 22);
    const syms = new Set();
    const endpoints = [
      ['/api/v2/lists/',      { limit: 200, include_list_symbols: 1 }],
      ['/api/v2/lists/',      { limit: 200 }],
      ['/lists/',             {}],
      ['/api/v1/lists/',      {}],
      ['/api/v2/watchlists/', {}],
    ];
    for (const [path, params] of endpoints) {
      const d = await tvGet(path, params);
      if (!d) { await sleep(100); continue; }
      const arr = d.data || d.lists || d.watchlists || d.results || (Array.isArray(d) ? d : []);
      // ops 3158: keep full membership — watchlists ARE the predictive unit
      try {
        (arr || []).forEach(l => {
          const syms = (l.symbols || l.list_symbols || l.items || [])
            .map(x => typeof x === 'string' ? x : (x && (x.symbol || x.s || x.name)) || '')
            .filter(Boolean).slice(0, 500);
          if (l && (l.name || l.id) && syms.length)
            WATCHLISTS.push({ id: String(l.id || l.name), name: String(l.name || l.id).slice(0, 120),
                              symbols: syms, color: l.color || null });
        });
      } catch (e) {}
      for (const lst of (Array.isArray(arr) ? arr : [])) {
        if (typeof lst !== 'object') continue;
        const items = lst.symbols || lst.items || lst.data || lst.list_symbols || [];
        for (const it of (Array.isArray(items) ? items : [])) {
          const s = typeof it === 'string' ? it : (it.symbol || it.s || it.ticker || '');
          if (s && s.length < 30) syms.add(s.toUpperCase());
        }
        // some lists embed symbol count, fetch symbols separately
        if (lst.id && !items.length) {
          const ld = await tvGet(`/api/v2/lists/${lst.id}/`, {});
          if (ld) {
            const si = ld.symbols || ld.data || [];
            for (const it of (Array.isArray(si) ? si : [])) {
              const s = typeof it === 'string' ? it : (it.symbol || it.s || '');
              if (s) syms.add(s.toUpperCase());
            }
          }
          await sleep(80);
        }
      }
      if (syms.size > 0) break;
      await sleep(100);
    }
    setStatus(`Watchlists: ${syms.size} unique symbols`, 28);
    return [...syms];
  }

  // ── Phase 3: per-symbol notes pull ────────────────────────────────────────
  const SYM_ENDPOINTS = (s) => [
    ['/note-manager/api/notes/',  { symbol: s, limit: 500 }],
    ['/note-manager/api/notes/',  { symbol_id: s, page_size: 500 }],
    ['/api/v1/text_notes/',       { symbol: s, limit: 500 }],
    ['/api/v1/text-notes/',       { symbol: s, limit: 500 }],
    ['/textnotes/list/',          { symbol: s }],
    [`/api/v2/symbols/${encodeURIComponent(s)}/notes/`, null],
    ['/api/v1/symbols/notes/',    { symbol: s }],
  ];

  async function pullForSymbol(sym) {
    for (const [path, params] of SYM_ENDPOINTS(sym)) {
      const d = await tvGet(path, params);
      if (d) { mine(d, sym, 0); return; }
      await sleep(40);
    }
  }

  // ── Phase 4: chart layouts (text drawings as notes) ───────────────────────
  async function phaseChartLayouts() {
    setStatus('Scanning chart layouts…');
    const d = await tvGet('/api/v2/chart-layouts/', { sort: 'recent', limit: 100 });
    if (!d) return;
    const arr = d.data || d.layouts || (Array.isArray(d) ? d : []);
    let scanned = 0;
    for (const layout of (Array.isArray(arr) ? arr.slice(0, 50) : [])) {
      const id  = layout.id  || layout.chart_id;
      const sym = layout.symbol || layout.name;
      if (id) {
        const content = await tvGet(`/api/v2/chart-layouts/${id}/content/`) ||
                        await tvGet(`/api/v2/chart-layouts/${id}/`);
        if (content) mine(content, sym, 0);
        scanned++;
        await sleep(120);
      }
    }
    if (scanned) setStatus(`Chart layouts: ${scanned} scanned`);
  }

  // ── Phase 5: localStorage ──────────────────────────────────────────────────
  function phaseLocalStorage() {
    try {
      for (const k of Object.keys(localStorage)) {
        if (k.length > 80) continue;
        if (!/note|annot|text|memo/i.test(k)) continue;
        try { mine(JSON.parse(localStorage.getItem(k)), null, 0); } catch (e) {}
      }
    } catch (e) {}
  }

  // ── Phase 6: DOM scrape (catches notes visible in the UI right now) ────────
  function phaseDom() {
    const selectors = [
      '[class*="note-text"]', '[class*="noteText"]', '[class*="NoteText"]',
      '[class*="note-content"]', '[class*="NoteContent"]',
      '[class*="notes-list"] [class*="text"]',
      '[class*="NotesWidget"] p', '[class*="notes-widget"] p',
      '[class*="note-item"]', '[class*="noteItem"]',
      '[data-note-text]',  '[class*="symbol-note"]',
      '[class*="textNote"]', '[class*="text-note"]',
    ].join(', ');
    try {
      document.querySelectorAll(selectors).forEach(el => {
        const text = el.textContent?.trim() || '';
        if (text.length < 3 || text.length > 20000) return;
        const container = el.closest('[data-symbol], [data-ticker], [class*="symbol-header"]');
        const sym = container?.dataset?.symbol || container?.dataset?.ticker
          || el.closest('[data-symbol]')?.dataset?.symbol
          || pageSymbol() || 'UNTAGGED';
        keep(sym, text, '', Date.now() - Math.random() * 1000);
      });
    } catch (e) {}
  }

  function pageSymbol() {
    const m = location.pathname.match(/\/symbols?\/([A-Z0-9:._-]{2,25})/i)
           || location.search.match(/[?&]symbol=([A-Z0-9:._%-]{2,25})/i);
    return m ? decodeURIComponent(m[1]).toUpperCase() : null;
  }

  // ── Passive fetch intercept ────────────────────────────────────────────────
  // Catches note-related API responses that TV makes on its own
  const _fetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    const url = typeof input === 'string' ? input : (input?.url || '');
    const p   = _fetch(input, init);
    if (/note|annot/i.test(url) && !/notif|notice|annotate/i.test(url)) {
      p.then(r => {
        try { r.clone().json().then(j => mine(j, null, 0)).catch(() => {}); } catch (e) {}
      }).catch(() => {});
    }
    return p;
  };
  // XHR intercept
  const _xhrOpen = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (method, url) {
    if (/note|annot/i.test(String(url)) && !/notif|notice/i.test(String(url))) {
      this.addEventListener('load', function () {
        try { mine(JSON.parse(this.responseText), null, 0); } catch (e) {}
      });
    }
    return _xhrOpen.apply(this, arguments);
  };

  // ── DOM observer (continuously scrape as notes load) ─────────────────────
  const mutObs = new MutationObserver(() => {
    if (overlay && overlay.style.display !== 'none') phaseDom();
  });
  mutObs.observe(document.body, { childList: true, subtree: true });

  // ── Overlay UI ────────────────────────────────────────────────────────────
  function buildOverlay() {
    if (document.getElementById('__jh_ext')) return;
    const d = document.createElement('div');
    d.id = '__jh_ext';
    d.style.cssText = `
      position:fixed;z-index:2147483647;bottom:16px;right:16px;
      background:#0C0B09;color:#e8e2d4;
      border:2px solid #F0B429;border-radius:10px;
      padding:14px 16px;
      font:12px/1.55 "IBM Plex Mono",ui-monospace,monospace;
      width:330px;
      box-shadow:0 8px 40px rgba(0,0,0,.9);
    `.replace(/\n\s+/g, '');
    d.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <b style="color:#F0B429;font-size:13px">⚡ JustHodl · TV Notes</b>
        <button id="__jh_close" style="background:none;border:none;color:#8a836f;cursor:pointer;font-size:18px;line-height:1;padding:0">×</button>
      </div>
      <div id="__jh_cnt" style="color:#F0B429;font-weight:bold;font-size:15px;margin-bottom:4px">Starting harvest…</div>
      <div id="__jh_st"  style="color:#8a836f;font-size:10px;margin-bottom:8px;min-height:14px"></div>
      <div style="background:#1a1a0f;border-radius:3px;height:5px;margin-bottom:10px;overflow:hidden">
        <div id="__jh_pb" style="background:linear-gradient(90deg,#b8892e,#F0B429);height:5px;border-radius:3px;width:0%;transition:width .4s ease"></div>
      </div>
      <button id="__jh_up" style="background:#F0B429;color:#0C0B09;border:none;border-radius:5px;padding:9px;font-weight:bold;cursor:pointer;width:100%;font-size:12px;font-family:inherit;margin-bottom:6px;opacity:.5" disabled>
        Harvesting…
      </button>
      <div id="__jh_msg" style="font-size:11px;min-height:16px;color:#6fce8a;line-height:1.4"></div>
    `.trim();
    document.body.appendChild(d);
    overlay = d;
    document.getElementById('__jh_close').addEventListener('click', () => { d.style.display = 'none'; });
    document.getElementById('__jh_up').addEventListener('click', doUpload);
  }

  function updateUI() {
    const c = document.getElementById('__jh_cnt');
    const u = document.getElementById('__jh_up');
    if (c) c.textContent = `${STORE.size} notes · ${TICKERS.size} tickers`;
    if (u && !u.disabled) u.textContent = `UPLOAD ${STORE.size} NOTES TO BRAIN`;
    // notify background for badge update
    try { chrome.runtime.sendMessage({ action: 'harvest_update', count: STORE.size, tickers: TICKERS.size }); }
    catch (e) {}
  }

  function setStatus(msg, pct) {
    const s  = document.getElementById('__jh_st');
    const pb = document.getElementById('__jh_pb');
    if (s) s.textContent = msg;
    if (pb && pct != null) { progressPct = pct; pb.style.width = `${pct}%`; }
    else if (pb) pb.style.width = `${Math.min(progressPct + 2, 95)}%`;
  }

  function setMsg(msg, color) {
    const m = document.getElementById('__jh_msg');
    if (m) { m.textContent = msg; m.style.color = color || '#6fce8a'; }
  }

  function enableUpload() {
    const u = document.getElementById('__jh_up');
    if (u) {
      u.disabled = false;
      u.style.opacity = '1';
      u.textContent = `UPLOAD ${STORE.size} NOTES TO BRAIN`;
    }
  }

  // ── Upload via background ─────────────────────────────────────────────────
  async function doUpload() {
    const all = [...STORE.values()];
    if (!all.length) { setMsg('No notes captured yet — wait for harvest to complete', '#F0B429'); return; }
    const u = document.getElementById('__jh_up');
    if (u) { u.disabled = true; u.style.opacity = '.5'; u.textContent = `Uploading ${all.length} notes…`; }
    setMsg(`Uploading ${all.length} notes to Brain…`, '#F0B429');
    try {
      const result = await chrome.runtime.sendMessage({ action: 'upload', notes: all, watchlists: WATCHLISTS });
      if (result?.ok || result?.brain_upserted > 0) {
        setMsg(`✅ ${result.brain_upserted} notes → Brain · ${result.watchlists_saved||0} watchlists → tracker (${TICKERS.size} tickers)`, '#6fce8a');
        if (u) { u.disabled = false; u.style.opacity = '1'; u.textContent = 'Upload complete — DONE'; }
      } else {
        setMsg(`❌ Upload failed: ${result?.error || 'check background console'}`, '#E07A6A');
        if (u) { u.disabled = false; u.style.opacity = '1'; }
        updateUI();
      }
    } catch (e) {
      setMsg(`❌ Error: ${e.message}`, '#E07A6A');
      if (u) { u.disabled = false; u.style.opacity = '1'; }
    }
  }

  // ── Main harvest orchestration ─────────────────────────────────────────────
  async function harvest() {
    if (isHarvesting) return;
    isHarvesting = true;
    buildOverlay();
    STORE.clear();
    TICKERS.clear();
    progressPct = 0;
    setStatus('Starting…', 2);

    // Phase 1 — bulk API
    const bulkHit = await phaseBulk();
    const afterBulk = STORE.size;

    // Phase 2 — watchlists
    const symbols = await getWatchlistSymbols();

    // Phase 3 — per-symbol (especially for tickers bulk missed)
    const covered = new Set([...STORE.values()].map(n => n.symbol));
    const uncovered = symbols.filter(s => !covered.has(s));

    if (uncovered.length > 0) {
      setStatus(`Per-ticker sweep: 0 / ${uncovered.length}…`, 30);
      for (let i = 0; i < uncovered.length; i++) {
        await pullForSymbol(uncovered[i]);
        const pct = 30 + Math.round((i + 1) / uncovered.length * 45);
        if ((i + 1) % 5 === 0 || i === uncovered.length - 1) {
          setStatus(`Per-ticker: ${i + 1}/${uncovered.length} · ${STORE.size} notes`, pct);
        }
        await sleep(90); // gentle throttle
      }
    }

    // Also sweep ALL symbols (even ones bulk already got — per-symbol may have more)
    if (symbols.length > 0 && bulkHit && STORE.size > 0) {
      setStatus('Cross-sweeping all watchlist symbols…', 78);
      for (let i = 0; i < Math.min(symbols.length, 300); i++) {
        await pullForSymbol(symbols[i]);
        await sleep(80);
      }
    }

    setStatus('Scanning chart layouts…', 82);
    await phaseChartLayouts();

    setStatus('Scanning localStorage…', 90);
    phaseLocalStorage();

    setStatus('Scraping Notes panel…', 93);
    phaseDom();

    setStatus(`Complete — ${STORE.size} notes from ${TICKERS.size} tickers`, 100);

    // Signal completion
    try {
      chrome.runtime.sendMessage({ action: 'harvest_complete', count: STORE.size, tickers: TICKERS.size, watchlists: WATCHLISTS.length });
    } catch (e) {}

    enableUpload();
    isHarvesting = false;

    if (STORE.size === 0) {
      setMsg(
        'No notes found via API. Scroll through your Notes panel (All Notes) — ' +
        'every note that loads gets captured automatically.',
        '#F0B429'
      );
    }
  }

  // ── Message listener (from popup) ─────────────────────────────────────────
  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (msg.action === 'start_harvest') {
      harvest().then(() => sendResponse({ ok: true, count: STORE.size })).catch(e => sendResponse({ ok: false, error: e.message }));
      return true;
    }
    if (msg.action === 'get_status') {
      sendResponse({ count: STORE.size, tickers: TICKERS.size, harvesting: isHarvesting });
      return true;
    }
    if (msg.action === 'show_overlay') {
      if (overlay) { overlay.style.display = 'block'; }
      else { buildOverlay(); }
      sendResponse({ ok: true });
    }
  });

  // ── Auto-start ────────────────────────────────────────────────────────────
  // Wait for page to settle, then begin
  setTimeout(harvest, 2000);

})();
