/* symbol-bar.js — universal ticker launcher. Type a ticker on ANY page and
 * instantly open it in Chart Pro (with signal overlays), the Stock Analyzer,
 * or any signal page. Makes every ticker one keystroke from everything.
 *
 *   <script src="/symbol-bar.js"></script>
 * Auto-mounts a compact launcher (press "t" or click the floating button).
 * Also exposes window.SymbolBar.go(ticker) and renders "open in chart" deep
 * links: /chart-pro.html?symbol=XYZ&layers=insider,gex,catalyst
 */
(function () {
  if (window.SymbolBar) return;

  // Destinations for a resolved ticker.
  const DESTS = [
    { label: 'Chart Pro (signals)', icon: '📈', url: t => `/chart-pro.html?symbol=${t}&layers=insider,flow,catalyst,dma,analyst` },
    { label: 'Stock Analyzer',      icon: '🔬', url: t => `/stock/?symbol=${t}` },
    { label: 'Deep-Value Overlap',  icon: '⭐', url: t => `/deep-value-overlap.html?focus=${t}` },
    { label: 'Backlog / RPO',       icon: '📦', url: t => `/backlog.html?focus=${t}` },
    { label: 'Capital Flow',        icon: '💰', url: t => `/capital-flow.html?focus=${t}` },
    { label: 'My Portfolio',        icon: '🧠', url: t => `/my-portfolio.html?add=${t}` },
  ];

  function injectCSS() {
    if (document.getElementById('symbar-css')) return;
    const s = document.createElement('style'); s.id = 'symbar-css';
    s.textContent = [
      '#symbar-fab{position:fixed;bottom:16px;right:70px;z-index:9997;background:#0c1018;border:1px solid #2a3550;color:#22d3ee;font-family:ui-monospace,Menlo,monospace;font-size:12px;font-weight:700;padding:7px 11px;border-radius:8px;cursor:pointer;box-shadow:0 4px 16px rgba(0,0,0,.4)}',
      '#symbar-fab:hover{border-color:#22d3ee}',
      '#symbar-ov{position:fixed;inset:0;background:rgba(4,6,10,0.6);backdrop-filter:blur(3px);z-index:99998;display:none;align-items:flex-start;justify-content:center}',
      '#symbar-ov.on{display:flex}',
      '#symbar-box{margin-top:13vh;width:min(440px,92vw);background:#0c1018;border:1px solid #2a3550;border-radius:14px;overflow:hidden;font-family:-apple-system,system-ui,sans-serif;box-shadow:0 24px 80px rgba(0,0,0,.6)}',
      '#symbar-in{width:100%;background:transparent;border:none;outline:none;color:#e1e8f4;font-size:20px;font-weight:700;padding:18px 20px;border-bottom:1px solid #1c2433;text-transform:uppercase;font-family:ui-monospace,Menlo,monospace}',
      '#symbar-in::placeholder{color:#6f7b91;text-transform:none;font-weight:400}',
      '.symbar-dest{display:flex;align-items:center;gap:12px;padding:12px 18px;cursor:pointer;color:#a8b3c7;font-size:14px}',
      '.symbar-dest:hover,.symbar-dest.sel{background:#131929;color:#fff}',
      '.symbar-dest .ic{width:22px;text-align:center}',
      '.symbar-dest .u{margin-left:auto;font-family:ui-monospace,Menlo,monospace;font-size:10px;color:#6f7b91}',
      '#symbar-foot{padding:8px 18px;border-top:1px solid #1c2433;font-family:ui-monospace,Menlo,monospace;font-size:9.5px;color:#6f7b91}',
    ].join('');
    document.head.appendChild(s);
  }

  let sel = 0, ov = null;
  function dests(t) { return DESTS.map(d => ({ ...d, href: d.url(t) })); }

  function render() {
    const t = (document.getElementById('symbar-in').value || '').trim().toUpperCase();
    const list = document.getElementById('symbar-list');
    if (!t) { list.innerHTML = '<div class="symbar-dest" style="color:#6f7b91">Type a ticker…</div>'; return; }
    list.innerHTML = dests(t).map((d, i) =>
      `<div class="symbar-dest${i === sel ? ' sel' : ''}" data-href="${d.href}"><span class="ic">${d.icon}</span><span>${d.label} · ${t}</span><span class="u">${d.href.split('?')[0]}</span></div>`
    ).join('');
    list.querySelectorAll('.symbar-dest').forEach(el => el.addEventListener('click', () => { if (el.dataset.href) location.href = el.dataset.href; }));
  }
  function paint() { document.querySelectorAll('.symbar-dest').forEach((el, i) => el.classList.toggle('sel', i === sel)); }

  function open_() {
    ov.classList.add('on'); sel = 0;
    const inp = document.getElementById('symbar-in'); inp.value = ''; render();
    setTimeout(() => inp.focus(), 30);
  }
  function close_() { ov.classList.remove('on'); }

  function go(t) { if (t) location.href = DESTS[0].url(t.toUpperCase()); }

  function build() {
    injectCSS();
    const fab = document.createElement('div'); fab.id = 'symbar-fab'; fab.textContent = '🔎 Ticker';
    fab.title = 'Jump to any ticker (press "t")'; fab.onclick = open_;
    document.body.appendChild(fab);
    ov = document.createElement('div'); ov.id = 'symbar-ov';
    ov.innerHTML = '<div id="symbar-box"><input id="symbar-in" placeholder="Type a ticker (e.g. NVDA)…" autocomplete="off" spellcheck="false"><div id="symbar-list"></div><div id="symbar-foot">↑↓ choose · ↵ open · esc close · "t" anywhere</div></div>';
    document.body.appendChild(ov);
    ov.addEventListener('click', e => { if (e.target === ov) close_(); });
    const inp = document.getElementById('symbar-in');
    inp.addEventListener('input', () => { sel = 0; render(); });
    inp.addEventListener('keydown', e => {
      if (e.key === 'ArrowDown') { e.preventDefault(); sel = Math.min(sel + 1, DESTS.length - 1); paint(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); sel = Math.max(sel - 1, 0); paint(); }
      else if (e.key === 'Enter') { e.preventDefault(); const t = inp.value.trim().toUpperCase(); if (t) location.href = DESTS[sel].url(t); }
      else if (e.key === 'Escape') close_();
    });
    document.addEventListener('keydown', e => {
      const tag = (e.target.tagName || '').toLowerCase();
      if (e.key === 't' && !ov.classList.contains('on') && tag !== 'input' && tag !== 'textarea' && !e.target.isContentEditable && !e.metaKey && !e.ctrlKey) {
        e.preventDefault(); open_();
      }
    });
  }

  window.SymbolBar = { go, open: open_ };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', build); else build();
})();
