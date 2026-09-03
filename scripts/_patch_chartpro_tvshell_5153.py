"""Patch chart-pro.html: TradingView shell + Data Explorer (ops 5153).

Everything is additive: the existing drawers, watchlist, info tabs, search, toolbar and
chart engines stay exactly as they are; the TV shell moves their DOM nodes into a docked
right rail at init (handlers survive DOM moves) and can be switched off (classic layout)
from the header. A theme toggle (TradingView light / JustHodl dark) drives the CSS
variables and the LightweightCharts palette.
"""
import re
from pathlib import Path

P = Path("/root/work/si/chart-pro.html")
s = P.read_text(encoding="utf-8")


def rep(old, new, label, count=1):
    global s
    n = s.count(old)
    assert n == count, "anchor %r found %d (want %d)" % (label, n, count)
    s = s.replace(old, new)


# ─────────────────────────── CSS: theme + TV shell + explorer
rep(""".hs-sources { margin-top: 2px; display: flex; flex-wrap: wrap; gap: 6px; }""", """/* ═══ ops 5153: TradingView light theme (JustHodl dark stays the other option) ═══ */
html[data-theme="light"] {
  --bg: #ffffff; --bg-1: #f8f9fd; --bg-2: #f0f3fa; --bg-3: #e8ecf5; --border: #e0e3eb; --border-bright: #d1d4dc;
  --fg-0: #131722; --fg-1: #1e222d; --fg-2: #434651; --fg-3: #787b86; --fg-4: #9598a1;
  --cyan: #2962ff; --green: #089981; --red: #f23645; --amber: #ff9800; --violet: #7e57c2;
}
html[data-theme="light"] body { background: var(--bg); color: var(--fg-1); }
html[data-theme="light"] .header { background: var(--bg); border-bottom-color: var(--border); }
html[data-theme="light"] .left-sidebar, html[data-theme="light"] .right-sidebar { box-shadow: 0 0 0 1px var(--border); }
html[data-theme="light"] .native-chart-wrap { background: var(--bg); }
/* ═══ TradingView shell ═══ */
body.tv-shell .main-area { right: calc(var(--tv-rail-w, 340px) + 44px); }
body.tv-shell.tv-rail-collapsed .main-area { right: 44px; }
body.tv-shell .edge-tab { display: none; }
body.tv-shell .left-sidebar, body.tv-shell .right-sidebar { display: none !important; }
.tv-rail { position: absolute; top: 0; right: 44px; bottom: 0; width: var(--tv-rail-w, 340px); background: var(--bg-1); border-left: 1px solid var(--border); display: none; flex-direction: column; z-index: 30; }
body.tv-shell .tv-rail { display: flex; }
body.tv-shell.tv-rail-collapsed .tv-rail { display: none; }
.tv-rail-tabs { display: flex; align-items: center; gap: 2px; padding: 6px 8px; border-bottom: 1px solid var(--border); }
.tv-rail-tab { font-family: var(--font-mono); font-size: 10.5px; font-weight: 700; letter-spacing: .5px; padding: 5px 10px; border-radius: 6px; color: var(--fg-3); cursor: pointer; }
.tv-rail-tab.active { background: var(--bg-3); color: var(--fg-0); }
.tv-rail-body { flex: 1; overflow: hidden; display: flex; flex-direction: column; }
.tv-rail-panel { display: none; flex: 1; flex-direction: column; overflow: hidden; }
.tv-rail-panel.active { display: flex; }
.tv-rail-panel .left-sidebar-host, .tv-rail-panel .right-sidebar-host { display: flex; flex-direction: column; flex: 1; overflow: hidden; }
.tv-rail-panel .left-sidebar-host > .left-sidebar, .tv-rail-panel .right-sidebar-host > .right-sidebar { position: static !important; transform: none !important; width: auto !important; display: flex !important; box-shadow: none !important; border: none !important; flex: 1; min-height: 0; }
.tv-rail-panel .drawer-close { display: none; }
.tv-symcard { border-top: 1px solid var(--border); padding: 10px 12px; font-size: 12px; color: var(--fg-2); background: var(--bg-1); max-height: 46%; overflow: auto; }
.tv-symcard .sc-name { font-weight: 700; color: var(--fg-0); font-size: 13px; display: flex; align-items: center; gap: 8px; }
.tv-symcard .sc-sub { color: var(--fg-3); font-size: 11px; margin-top: 2px; }
.tv-symcard .sc-price { font-size: 26px; font-weight: 700; color: var(--fg-0); margin-top: 8px; font-variant-numeric: tabular-nums; }
.tv-symcard .sc-price small { font-size: 12px; color: var(--fg-3); margin-left: 4px; }
.tv-symcard .sc-chg { font-size: 13px; font-weight: 600; margin-left: 8px; }
.tv-symcard .sc-status { font-size: 11px; margin-top: 4px; }
.tv-symcard .sc-range { margin-top: 10px; }
.tv-symcard .sc-range-lbl { display: flex; justify-content: space-between; font-size: 10.5px; color: var(--fg-3); font-family: var(--font-mono); }
.tv-symcard .sc-range-lbl b { color: var(--fg-1); }
.tv-symcard .sc-bar { position: relative; height: 4px; background: var(--bg-3); border-radius: 2px; margin-top: 4px; }
.tv-symcard .sc-bar i { position: absolute; top: -3px; width: 2px; height: 10px; background: var(--fg-1); border-radius: 1px; }
.tv-symcard .sc-bar b { position: absolute; left: 0; top: 0; height: 4px; background: var(--cyan); border-radius: 2px; opacity: .55; }
.tv-symcard .sc-meta { margin-top: 10px; font-family: var(--font-mono); font-size: 10.5px; color: var(--fg-3); line-height: 1.7; }
.tv-symcard .sc-meta b { color: var(--fg-1); font-weight: 600; }
.tv-iconrail { position: absolute; top: 0; right: 0; bottom: 0; width: 44px; background: var(--bg-1); border-left: 1px solid var(--border); display: none; flex-direction: column; align-items: center; padding-top: 6px; gap: 4px; z-index: 31; }
body.tv-shell .tv-iconrail { display: flex; }
.tv-ico { width: 34px; height: 34px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 16px; cursor: pointer; color: var(--fg-3); position: relative; }
.tv-ico:hover { background: var(--bg-3); color: var(--fg-0); }
.tv-ico.active { background: var(--bg-3); color: var(--cyan); }
.tv-ico .tv-ico-lbl { position: absolute; right: 42px; top: 50%; transform: translateY(-50%); background: var(--bg-2); border: 1px solid var(--border); color: var(--fg-1); font-family: var(--font-mono); font-size: 10px; padding: 3px 7px; border-radius: 5px; white-space: nowrap; display: none; }
.tv-ico:hover .tv-ico-lbl { display: block; }
.tv-ico .tv-badge { position: absolute; top: 2px; right: 2px; min-width: 14px; height: 14px; border-radius: 7px; background: var(--red); color: #fff; font-size: 9px; font-weight: 700; display: flex; align-items: center; justify-content: center; padding: 0 3px; }
.tv-rangebar { display: flex; align-items: center; gap: 2px; padding: 4px 10px; border-top: 1px solid var(--border); background: var(--bg-1); font-family: var(--font-mono); font-size: 10.5px; }
.tv-rangebar button { border: none; background: transparent; color: var(--fg-3); padding: 3px 8px; border-radius: 5px; cursor: pointer; font-family: inherit; font-size: inherit; font-weight: 600; }
.tv-rangebar button:hover { color: var(--fg-0); background: var(--bg-3); }
.tv-rangebar button.active { color: var(--cyan); }
.tv-rangebar .tv-range-right { margin-left: auto; color: var(--fg-4); display: flex; gap: 10px; align-items: center; }
.tv-rangebar .tv-range-right span { cursor: pointer; }
.tv-rangebar .tv-range-right span:hover { color: var(--fg-0); }
/* ═══ Data Explorer ═══ */
.dx-modal { position: fixed; inset: 0; background: rgba(0,0,0,0.55); z-index: 1200; display: none; align-items: center; justify-content: center; }
.dx-modal.open { display: flex; }
.dx-box { width: min(1360px, 96vw); height: min(860px, 92vh); background: var(--bg); border: 1px solid var(--border-bright); border-radius: 12px; display: grid; grid-template-columns: 260px 1fr 320px; grid-template-rows: auto auto 1fr auto; overflow: hidden; box-shadow: 0 30px 80px rgba(0,0,0,.5); }
.dx-head { grid-column: 1 / -1; display: flex; align-items: center; gap: 10px; padding: 10px 14px; border-bottom: 1px solid var(--border); }
.dx-head input { flex: 1; background: var(--bg-2); border: 1px solid var(--border); color: var(--fg-0); border-radius: 8px; padding: 9px 12px; font-size: 13px; outline: none; }
.dx-head input:focus { border-color: var(--cyan); }
.dx-head .dx-title { font-family: var(--font-mono); font-size: 11px; font-weight: 700; color: var(--fg-3); letter-spacing: 1px; white-space: nowrap; }
.dx-chips { grid-column: 1 / -1; display: flex; flex-wrap: wrap; gap: 5px; padding: 8px 14px; border-bottom: 1px solid var(--border); background: var(--bg-1); max-height: 78px; overflow: auto; }
.dx-chip { font-family: var(--font-mono); font-size: 10px; padding: 2px 8px; border-radius: 10px; border: 1px solid var(--border); color: var(--fg-2); cursor: pointer; white-space: nowrap; }
.dx-chip b { color: var(--fg-0); }
.dx-chip.on { background: var(--cyan); color: #fff; border-color: var(--cyan); }
.dx-chip.on b { color: #fff; }
.dx-tree { border-right: 1px solid var(--border); overflow: auto; background: var(--bg-1); }
.dx-prov { display: grid; grid-template-columns: 1fr auto; gap: 6px; align-items: center; padding: 7px 12px; cursor: pointer; border-bottom: 1px solid var(--border); font-size: 12px; }
.dx-prov:hover, .dx-prov.on { background: var(--bg-3); }
.dx-prov .dx-prov-n { font-family: var(--font-mono); font-size: 10px; color: var(--fg-3); text-align: right; }
.dx-prov .dx-prov-name { color: var(--fg-1); font-weight: 600; }
.dx-prov .dx-prov-sub { font-family: var(--font-mono); font-size: 9.5px; color: var(--fg-4); margin-top: 1px; }
.dx-list { overflow: auto; position: relative; }
.dx-row { display: grid; grid-template-columns: minmax(90px, 200px) 1fr 72px 120px 26px; gap: 10px; align-items: center; padding: 6px 14px; border-bottom: 1px solid var(--border); cursor: pointer; font-size: 12px; }
.dx-row:hover { background: var(--bg-2); }
.dx-row.sel { background: var(--bg-3); }
.dx-row .dx-id { font-family: var(--font-mono); font-size: 10.5px; color: var(--violet); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dx-row .dx-id.dataset { color: var(--amber); }
.dx-row .dx-id.instrument { color: var(--cyan); }
.dx-row .dx-name { color: var(--fg-1); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dx-row .dx-name small { color: var(--fg-4); margin-left: 6px; font-family: var(--font-mono); font-size: 9.5px; }
.dx-row .dx-freq { font-family: var(--font-mono); font-size: 10px; color: var(--fg-3); }
.dx-row .dx-span { font-family: var(--font-mono); font-size: 10px; color: var(--fg-3); text-align: right; white-space: nowrap; }
.dx-crumb { padding: 8px 14px; font-family: var(--font-mono); font-size: 10.5px; color: var(--fg-3); border-bottom: 1px solid var(--border); display: flex; gap: 8px; align-items: center; background: var(--bg-1); }
.dx-crumb a { color: var(--cyan); cursor: pointer; }
.dx-info { border-left: 1px solid var(--border); padding: 14px; overflow: auto; font-size: 12px; color: var(--fg-2); background: var(--bg-1); }
.dx-info h4 { margin: 0 0 6px; color: var(--fg-0); font-size: 14px; }
.dx-info .dx-kv { display: grid; grid-template-columns: 90px 1fr; gap: 3px 8px; font-family: var(--font-mono); font-size: 10.5px; margin-top: 8px; }
.dx-info .dx-kv span:nth-child(odd) { color: var(--fg-4); }
.dx-info .dx-actions { margin-top: 14px; display: flex; gap: 8px; }
.dx-info .dx-actions button { flex: 1; padding: 8px 10px; border-radius: 7px; border: 1px solid var(--border); background: var(--bg-2); color: var(--fg-0); cursor: pointer; font-weight: 600; font-size: 12px; }
.dx-info .dx-actions button.primary { background: var(--cyan); border-color: var(--cyan); color: #fff; }
.dx-foot { grid-column: 1 / -1; display: flex; justify-content: space-between; padding: 7px 14px; border-top: 1px solid var(--border); font-family: var(--font-mono); font-size: 10px; color: var(--fg-4); }
.dx-empty { padding: 30px; text-align: center; color: var(--fg-4); font-size: 12.5px; }
.tv-rail .dx-mini { flex: 1; overflow: auto; }
.tv-rail .dx-mini .dx-prov { padding: 6px 12px; }
.hs-sources { margin-top: 2px; display: flex; flex-wrap: wrap; gap: 6px; }""", "css")

# ─────────────────────────── markup: rail, icon strip, explorer modal (inside .app so positioning works)
rep("""  <!-- ─── LEFT SIDEBAR: Watchlists (TradingView-style) ─── -->
  <aside class="left-sidebar" id="left-sidebar">""", """  <!-- ops 5153: TradingView shell — docked right rail (Watchlist · Data · Info) + icon strip; the classic drawers stay in the DOM and are moved in at init -->
  <aside class="tv-rail" id="tv-rail">
    <div class="tv-rail-tabs">
      <span class="tv-rail-tab active" data-rail="watchlist">WATCHLIST</span>
      <span class="tv-rail-tab" data-rail="data">DATA</span>
      <span class="tv-rail-tab" data-rail="info">INFO</span>
      <span style="margin-left:auto" class="tv-rail-tab" id="tv-rail-collapse" title="Collapse panel">⟩</span>
    </div>
    <div class="tv-rail-body">
      <div class="tv-rail-panel active" data-rail-panel="watchlist"><div class="left-sidebar-host" id="tv-wl-host"></div><div class="tv-symcard" id="tv-symcard"></div></div>
      <div class="tv-rail-panel" data-rail-panel="data"><div class="dx-mini" id="dx-mini"></div></div>
      <div class="tv-rail-panel" data-rail-panel="info"><div class="right-sidebar-host" id="tv-info-host"></div></div>
    </div>
  </aside>
  <div class="tv-iconrail" id="tv-iconrail">
    <div class="tv-ico active" data-ico="watchlist" title="Watchlist">☰<span class="tv-ico-lbl">Watchlist</span></div>
    <div class="tv-ico" data-ico="data" title="Data Explorer — every provider, dataset and series">🗂<span class="tv-ico-lbl">Data Explorer</span></div>
    <div class="tv-ico" data-ico="info" title="Overview · Intel · Notes · News · Fundamentals · Ticket">ℹ<span class="tv-ico-lbl">Info</span></div>
    <div class="tv-ico" data-ico="alerts" title="Alerts">🔔<span class="tv-ico-lbl">Alerts</span></div>
    <div class="tv-ico" data-ico="explorer" title="Open the full Data Explorer (Ctrl+E)">⌕<span class="tv-ico-lbl">Explore all data</span></div>
    <div style="flex:1"></div>
    <div class="tv-ico" data-ico="theme" title="Light / dark">◐<span class="tv-ico-lbl">Theme</span></div>
    <div class="tv-ico" data-ico="classic" title="Classic JustHodl layout">⇆<span class="tv-ico-lbl">Classic layout</span></div>
  </div>
  <div class="dx-modal" id="dx-modal">
    <div class="dx-box">
      <div class="dx-head">
        <span class="dx-title">DATA EXPLORER</span>
        <input type="text" id="dx-search" placeholder="Search every provider, dataset and series — or pick a provider on the left. Double-click charts, + adds to the watchlist." autocomplete="off" />
        <button class="modal-close" id="dx-close">&times;</button>
      </div>
      <div class="dx-chips" id="dx-chips"></div>
      <div class="dx-tree" id="dx-tree"></div>
      <div style="display:flex;flex-direction:column;overflow:hidden"><div class="dx-crumb" id="dx-crumb">every provider</div><div class="dx-list" id="dx-list"><div class="dx-empty">Pick a provider or type to search.</div></div></div>
      <div class="dx-info" id="dx-info"><div class="dx-empty">Select a row to see what this data is, where it comes from and how much history the warehouse holds.</div></div>
      <div class="dx-foot"><span id="dx-count"></span><span>↑↓ move · ↵ chart · <b>+</b> add · Esc close</span></div>
    </div>
  </div>

  <!-- ─── LEFT SIDEBAR: Watchlists (TradingView-style) ─── -->
  <aside class="left-sidebar" id="left-sidebar">""", "markup")

# ─────────────────────────── JS: TVShell + DataExplorer (before jhYahooBars, after SymDir/DatasetBrowser)
rep("""async function jhYahooBars(sym, days) {""", r"""// ─── ops 5153: TradingView shell (docked rail, symbol card, icon strip, theme, range strip) ─────────
class TVShell {
  static on = false;
  static init() {
    const want = (localStorage.getItem('jh_tv_shell') || '1') === '1';
    const theme = localStorage.getItem('jh_theme') || 'light';
    this.setTheme(theme, false);
    if (want) this.enable();
    document.querySelectorAll('.tv-rail-tab[data-rail]').forEach(t => t.addEventListener('click', () => this.showPanel(t.dataset.rail)));
    const col = document.getElementById('tv-rail-collapse'); if (col) col.addEventListener('click', () => this.collapse(true));
    document.querySelectorAll('.tv-ico[data-ico]').forEach(ic => ic.addEventListener('click', () => {
      const k = ic.dataset.ico;
      if (k === 'theme') { this.setTheme((document.documentElement.dataset.theme === 'light') ? 'dark' : 'light', true); return; }
      if (k === 'classic') { this.disable(); return; }
      if (k === 'explorer') { DataExplorer.open(); return; }
      if (k === 'alerts') { const b = document.getElementById('alerts-btn') || document.querySelector('[data-open="alerts"]'); if (b) b.click(); else this.showPanel('info'); return; }
      this.collapse(false); this.showPanel(k);
    }));
    document.addEventListener('keydown', (e) => { if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'e') { e.preventDefault(); DataExplorer.open(); } });
    this.buildRangeBar();
  }
  static enable() {
    const wlHost = document.getElementById('tv-wl-host'), infoHost = document.getElementById('tv-info-host');
    const left = document.getElementById('left-sidebar'), right = document.getElementById('right-sidebar');
    if (!wlHost || !infoHost || !left || !right) return;
    if (!left.dataset.home) { left.dataset.home = 'app'; right.dataset.home = 'app'; }
    wlHost.appendChild(left); infoHost.appendChild(right);
    left.classList.add('open'); right.classList.add('open');
    document.body.classList.add('tv-shell'); this.on = true;
    localStorage.setItem('jh_tv_shell', '1');
    try { UI.refreshWatchlist(); } catch (e) {}
    this.renderSymCard();
    DataExplorer.renderMini();
    setTimeout(() => window.dispatchEvent(new Event('resize')), 60);
  }
  static disable() {
    const app = document.querySelector('.app'), left = document.getElementById('left-sidebar'), right = document.getElementById('right-sidebar');
    if (app && left && right) { app.appendChild(left); app.appendChild(right); left.classList.remove('open'); right.classList.remove('open'); }
    document.body.classList.remove('tv-shell'); this.on = false;
    localStorage.setItem('jh_tv_shell', '0');
    try { window.jhSetDrawers && window.jhSetDrawers(true, false); } catch (e) {}
    setTimeout(() => window.dispatchEvent(new Event('resize')), 60);
  }
  static collapse(v) { document.body.classList.toggle('tv-rail-collapsed', !!v); document.querySelectorAll('.tv-ico[data-ico]').forEach(i => { if (v && ['watchlist', 'data', 'info'].includes(i.dataset.ico)) i.classList.remove('active'); }); setTimeout(() => window.dispatchEvent(new Event('resize')), 60); }
  static showPanel(k) {
    document.querySelectorAll('.tv-rail-tab[data-rail]').forEach(t => t.classList.toggle('active', t.dataset.rail === k));
    document.querySelectorAll('.tv-rail-panel').forEach(p => p.classList.toggle('active', p.dataset.railPanel === k));
    document.querySelectorAll('.tv-ico[data-ico]').forEach(i => i.classList.toggle('active', i.dataset.ico === k));
    if (k === 'data') DataExplorer.renderMini();
    if (k === 'watchlist') this.renderSymCard();
  }
  static setTheme(t, persist) {
    document.documentElement.dataset.theme = t;
    if (persist) { localStorage.setItem('jh_theme', t); const a = State.activeTicker; if (a) { State.activeTicker = '__r'; ChartController.loadTicker(a); } }
  }
  static palette() {
    const light = document.documentElement.dataset.theme === 'light';
    return light
      ? { bg: '#ffffff', text: '#787b86', grid: '#f0f3fa', border: '#e0e3eb', up: '#089981', dn: '#f23645', line: '#2962ff', areaTop: 'rgba(41,98,255,0.22)', areaBot: 'rgba(41,98,255,0.02)' }
      : { bg: '#0a0e14', text: '#a8b3c7', grid: '#1c2433', border: '#1c2433', up: '#26ffaf', dn: '#ff5577', line: '#22d3ee', areaTop: 'rgba(34,211,238,0.25)', areaBot: 'rgba(34,211,238,0.02)' };
  }
  static async renderSymCard() {
    const el = document.getElementById('tv-symcard'); if (!el || !this.on) return;
    const t = State.activeTicker; if (!t || t === '__r') { el.innerHTML = ''; return; }
    const isSeries = !!(jhSeriesProvider(t) || (t.includes(':') && !/^(NASDAQ|NYSE|AMEX)/.test(t)) || /^FRED:/i.test(t));
    const meta = SymDir.meta(t) || {};
    const q = State.quotes[t] || {};
    const price = q.price != null ? q.price : null;
    const chg = q.changePct;
    const name = meta.name || (State.knownNames && State.knownNames[t]) || '';
    const prov = jhSeriesProvider(t) ? SymDir.provLabel(jhSeriesProvider(t)) : (t.includes(':') ? t.split(':')[0] : 'Stock');
    let ranges = '';
    if (!isSeries) {
      try {
        const f = await FundamentalsService.get(t);
        if (f && f.yearHigh && f.yearLow && price != null) {
          const pct = Math.max(0, Math.min(1, (price - f.yearLow) / (f.yearHigh - f.yearLow)));
          ranges = `<div class="sc-range"><div class="sc-range-lbl"><b>${f.yearLow.toFixed(2)}</b><span>52WK RANGE</span><b>${f.yearHigh.toFixed(2)}</b></div><div class="sc-bar"><b style="width:${(pct*100).toFixed(1)}%"></b><i style="left:${(pct*100).toFixed(1)}%"></i></div></div>`;
        }
        if (f && f.dayHigh && f.dayLow && price != null) {
          const pct = Math.max(0, Math.min(1, (price - f.dayLow) / (f.dayHigh - f.dayLow)));
          ranges = `<div class="sc-range"><div class="sc-range-lbl"><b>${f.dayLow.toFixed(2)}</b><span>DAY'S RANGE</span><b>${f.dayHigh.toFixed(2)}</b></div><div class="sc-bar"><b style="width:${(pct*100).toFixed(1)}%"></b><i style="left:${(pct*100).toFixed(1)}%"></i></div></div>` + ranges;
        }
      } catch (e) {}
    }
    const priceTxt = price == null ? '—' : (isSeries ? jhFmtVal(price) : '$' + Number(price).toFixed(2));
    const unit = isSeries && (q.unit || meta.unit) ? `<small>${UI.esc(String(q.unit || meta.unit).slice(0, 14))}</small>` : '';
    el.innerHTML = `<div class="sc-name">${UI.esc(t)} <span class="hs-prov ${jhSeriesProvider(t) === 'fred' ? 'fred' : 'instrument'}">${UI.esc(prov)}</span></div>
      <div class="sc-sub">${UI.esc(name || '')}</div>
      <div><span class="sc-price">${priceTxt}${unit}</span><span class="sc-chg" style="color:${chg == null ? 'var(--fg-3)' : (chg >= 0 ? 'var(--green)' : 'var(--red)')}">${chg == null ? '' : (chg >= 0 ? '+' : '') + Number(chg).toFixed(2) + '%'}</span></div>
      <div class="sc-status" style="color:${isSeries ? 'var(--fg-3)' : 'var(--green)'}">${isSeries ? ('● last obs ' + UI.esc(q.lastDate || meta.last || '')) : '● Market data · JustHodl warehouse'}</div>
      ${ranges}
      <div class="sc-meta">${isSeries ? `first <b>${UI.esc(meta.first || '—')}</b> · last <b>${UI.esc(meta.last || q.lastDate || '—')}</b> · <b>${UI.esc(meta.freq || '')}</b>${q.n ? ' · <b>' + Number(q.n).toLocaleString() + '</b> obs' : ''}` : ''}${q.yoy != null ? `<br>MoM <b>${q.mom != null ? (q.mom >= 0 ? '+' : '') + q.mom.toFixed(2) + '%' : '—'}</b> · QoQ <b>${q.qoq != null ? (q.qoq >= 0 ? '+' : '') + q.qoq.toFixed(2) + '%' : '—'}</b> · YoY <b>${(q.yoy >= 0 ? '+' : '') + q.yoy.toFixed(2)}%</b>` : ''}</div>`;
  }
  static buildRangeBar() {
    const main = document.querySelector('.main-area'); if (!main || document.getElementById('tv-rangebar')) return;
    const bar = document.createElement('div'); bar.className = 'tv-rangebar'; bar.id = 'tv-rangebar';
    const ranges = [['1D', 1], ['5D', 5], ['1M', 31], ['3M', 92], ['6M', 183], ['YTD', 'ytd'], ['1Y', 366], ['5Y', 1830], ['All', 'all']];
    bar.innerHTML = ranges.map(([l, d]) => `<button data-range="${d}">${l}</button>`).join('') + `<div class="tv-range-right"><span id="tv-range-log" title="Logarithmic price scale">log</span><span id="tv-range-fit" title="Fit all bars">⌂</span><span id="tv-range-clock"></span></div>`;
    main.appendChild(bar);
    bar.querySelectorAll('button[data-range]').forEach(b => b.addEventListener('click', () => {
      bar.querySelectorAll('button').forEach(x => x.classList.remove('active')); b.classList.add('active');
      const c = ChartSync.charts.get(State.activeChartPane); if (!c) return;
      const r = b.dataset.range;
      if (r === 'all') { c.timeScale().fitContent(); return; }
      const vr = c.timeScale().getVisibleRange(); const to = (vr && vr.to) || new Date().toISOString().slice(0, 10);
      const toD = new Date(String(to).slice(0, 10) + 'T00:00:00Z');
      const from = r === 'ytd' ? new Date(Date.UTC(toD.getUTCFullYear(), 0, 1)) : new Date(toD.getTime() - Number(r) * 86400000);
      try { c.timeScale().setVisibleRange({ from: from.toISOString().slice(0, 10), to: String(to).slice(0, 10) }); } catch (e) {}
    }));
    document.getElementById('tv-range-fit').addEventListener('click', () => { const c = ChartSync.charts.get(State.activeChartPane); if (c) c.timeScale().fitContent(); });
    document.getElementById('tv-range-log').addEventListener('click', () => { const c = ChartSync.charts.get(State.activeChartPane); if (!c) return; State._log = !State._log; c.priceScale('right').applyOptions({ mode: State._log ? 1 : 0 }); document.getElementById('tv-range-log').style.color = State._log ? 'var(--cyan)' : ''; });
    const clock = () => { const el = document.getElementById('tv-range-clock'); if (el) el.textContent = new Date().toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour12: false }) + ' (ET)'; };
    clock(); setInterval(clock, 1000);
  }
}

// ─── ops 5153: Data Explorer — every provider → every dataset/series on data.html, inside chart-pro ─────
class DataExplorer {
  static providers = null; static prov = null; static q = ''; static rows = []; static sel = -1; static _t = null; static _seq = 0; static total = 0;
  static async loadProviders() {
    if (this.providers) return this.providers;
    const d = await fetch(`${PROXY}/explorer`).then(r => r.ok ? r.json() : { providers: [] }).catch(() => ({ providers: [] }));
    this.providers = d.providers || []; this._hub = d;
    return this.providers;
  }
  static init() {
    const inp = document.getElementById('dx-search'); if (!inp) return;
    inp.addEventListener('input', () => { this.q = inp.value; clearTimeout(this._t); this._t = setTimeout(() => this.load(), 240); });
    inp.addEventListener('keydown', (e) => this.onKey(e));
    document.getElementById('dx-close').addEventListener('click', () => this.close());
    document.getElementById('dx-modal').addEventListener('click', (e) => { if (e.target.id === 'dx-modal') this.close(); });
    document.getElementById('dx-list').addEventListener('keydown', (e) => this.onKey(e));
  }
  static async open(prov) {
    document.getElementById('dx-modal').classList.add('open');
    await this.loadProviders();
    this.renderTree();
    if (prov) this.pickProvider(prov); else if (!this.rows.length) this.load();
    setTimeout(() => document.getElementById('dx-search').focus(), 40);
  }
  static close() { document.getElementById('dx-modal').classList.remove('open'); }
  static fmtN(n) { return n == null ? '' : Number(n).toLocaleString(); }
  static renderTree() {
    const tree = document.getElementById('dx-tree'); const chips = document.getElementById('dx-chips');
    const ps = this.providers || [];
    tree.innerHTML = `<div class="dx-prov ${this.prov ? '' : 'on'}" data-prov=""><div><div class="dx-prov-name">All providers</div><div class="dx-prov-sub">${ps.length} sources · ${this.fmtN(this._hub && this._hub.directory_docs)} searchable docs</div></div><div class="dx-prov-n">${this.fmtN(this._hub && this._hub.totals && this._hub.totals.gb)} GB</div></div>` +
      ps.map(p => { const c = p.in_directory || {}; const n = (c.series || 0) + (c.dataset || 0) + (c.instrument || 0); return `<div class="dx-prov ${this.prov === p.slug ? 'on' : ''}" data-prov="${UI.esc(p.slug)}" title="${UI.esc(p.note || '')}"><div><div class="dx-prov-name">${UI.esc(p.name || p.slug)}</div><div class="dx-prov-sub">${UI.esc(p.api || '')}${p.freshest_h != null ? ' · ' + p.freshest_h + 'h' : ''}</div></div><div class="dx-prov-n">${n ? this.fmtN(n) : (p.datasets ? this.fmtN(p.datasets) + ' files' : '')}${p.slug === 'eurostat' ? '<br>+564M via browse' : (p.slug === 'ecb' ? '<br>+3.2M via browse' : '')}</div></div>`; }).join('');
    tree.querySelectorAll('.dx-prov').forEach(el => el.addEventListener('click', () => this.pickProvider(el.dataset.prov || null)));
    chips.innerHTML = `<span class="dx-chip ${this.prov ? '' : 'on'}" data-prov="">all</span>` + ps.slice(0, 40).map(p => { const c = p.in_directory || {}; const n = (c.series || 0) + (c.dataset || 0) + (c.instrument || 0); return `<span class="dx-chip ${this.prov === p.slug ? 'on' : ''}" data-prov="${UI.esc(p.slug)}">${UI.esc(p.name || p.slug)} <b>${n ? this.fmtN(n) : this.fmtN(p.datasets || 0)}</b></span>`; }).join('');
    chips.querySelectorAll('.dx-chip').forEach(el => el.addEventListener('click', () => this.pickProvider(el.dataset.prov || null)));
  }
  static pickProvider(slug) { this.prov = slug || null; this.renderTree(); this.load(); }
  static async load(offset = 0) {
    const seq = ++this._seq; const list = document.getElementById('dx-list'); const crumb = document.getElementById('dx-crumb');
    list.innerHTML = '<div class="dx-empty">Loading…</div>';
    let d;
    if (this.prov) d = await fetch(`${PROXY}/explorer?provider=${encodeURIComponent(this.prov)}&q=${encodeURIComponent(this.q)}&offset=${offset}&limit=300`).then(r => r.json()).catch(e => ({ rows: [], error: String(e) }));
    else if (this.q.trim()) d = await SymDir.query(this.q.trim(), 100);
    else d = { rows: [], total: 0, hint: 'Pick a provider on the left, or type to search everything.' };
    if (seq !== this._seq) return;
    this.rows = d.rows || []; this.total = d.total != null ? d.total : this.rows.length; this.sel = this.rows.length ? 0 : -1; this._offset = offset;
    SymDir.noteRows(this.rows);
    const pName = this.prov ? ((this.providers || []).find(p => p.slug === this.prov) || {}).name || this.prov : 'every provider';
    crumb.innerHTML = `<a data-home="1">Data</a> › <b style="color:var(--fg-1)">${UI.esc(pName)}</b>${this.q ? ` › "${UI.esc(this.q)}"` : ''}${d.hub && d.hub.datasets ? ` <span style="color:var(--fg-4)">· ${this.fmtN(d.hub.datasets)} files · ${d.hub.total_mb ? (d.hub.total_mb / 1024).toFixed(1) + ' GB' : ''}${d.hub.freshest_h != null ? ' · fresh ' + d.hub.freshest_h + 'h' : ''}</span>` : ''}${d.series_level_via_browse ? ' <span style="color:var(--amber)">· datasets open to their series</span>' : ''}`;
    crumb.querySelector('[data-home]').addEventListener('click', () => this.pickProvider(null));
    document.getElementById('dx-count').textContent = `${this.fmtN(this.rows.length)} shown · ${this.fmtN(this.total)} total${offset ? ' · from ' + this.fmtN(offset) : ''}`;
    if (!this.rows.length) { list.innerHTML = `<div class="dx-empty">${UI.esc(d.error || d.hint || 'Nothing here — try a different word or provider.')}</div>`; return; }
    list.innerHTML = this.rows.map((r, i) => this.rowHtml(r, i)).join('') + (this.total > offset + this.rows.length ? `<div class="dx-empty"><button class="tf-btn" style="color:var(--cyan)" id="dx-more">Load more (${this.fmtN(this.total - offset - this.rows.length)} left)</button></div>` : '');
    list.querySelectorAll('.dx-row').forEach(el => {
      el.addEventListener('click', () => { this.select(+el.dataset.i); });
      el.addEventListener('dblclick', () => { this.chart(+el.dataset.i); });
    });
    list.querySelectorAll('.hs-add').forEach(b => b.addEventListener('click', (ev) => { ev.stopPropagation(); const r = this.rows[+b.closest('.dx-row').dataset.i]; if (SymDir.addToWatchlist(r.id, r) === 'ok') { b.classList.add('added'); b.textContent = '✓'; } }));
    const more = document.getElementById('dx-more'); if (more) more.addEventListener('click', () => this.append(offset + this.rows.length));
    this.select(0);
  }
  static async append(offset) {
    const d = await fetch(`${PROXY}/explorer?provider=${encodeURIComponent(this.prov)}&q=${encodeURIComponent(this.q)}&offset=${offset}&limit=300`).then(r => r.json()).catch(() => ({ rows: [] }));
    const start = this.rows.length; this.rows = this.rows.concat(d.rows || []); SymDir.noteRows(d.rows || []);
    const list = document.getElementById('dx-list'); const more = document.getElementById('dx-more'); if (more) more.parentElement.remove();
    list.insertAdjacentHTML('beforeend', (d.rows || []).map((r, i) => this.rowHtml(r, start + i)).join('') + (this.total > this.rows.length ? `<div class="dx-empty"><button class="tf-btn" style="color:var(--cyan)" id="dx-more">Load more (${this.fmtN(this.total - this.rows.length)} left)</button></div>` : ''));
    list.querySelectorAll('.dx-row').forEach(el => { if (+el.dataset.i >= start) { el.addEventListener('click', () => this.select(+el.dataset.i)); el.addEventListener('dblclick', () => this.chart(+el.dataset.i)); } });
    list.querySelectorAll('.hs-add').forEach(b => { if (!b.dataset.wired) { b.dataset.wired = '1'; b.addEventListener('click', (ev) => { ev.stopPropagation(); const r = this.rows[+b.closest('.dx-row').dataset.i]; if (SymDir.addToWatchlist(r.id, r) === 'ok') { b.classList.add('added'); b.textContent = '✓'; } }); } });
    const m2 = document.getElementById('dx-more'); if (m2) m2.addEventListener('click', () => this.append(this.rows.length));
    document.getElementById('dx-count').textContent = `${this.fmtN(this.rows.length)} shown · ${this.fmtN(this.total)} total`;
  }
  static rowHtml(r, i) {
    const kind = r.kind || 'series';
    const span = (r.first || r.last) ? `${(r.first || '?').slice(0, 4)}→${(r.last || '?').slice(0, 7)}` : '';
    const n = r.n ? ` · ${this.fmtN(r.n)}${kind === 'dataset' ? ' series' : ''}` : '';
    return `<div class="dx-row ${i === this.sel ? 'sel' : ''}" data-i="${i}" title="${UI.esc(r.id)}"><span class="dx-id ${kind}">${UI.esc(r.symbol || r.id)}</span><span class="dx-name">${UI.esc(r.name || '')}<small>${UI.esc(r.provider_name || r.provider || '')}${kind === 'dataset' ? ' · dataset' : ''}</small></span><span class="dx-freq">${UI.esc(r.freq || '')}${r.unit ? ' · ' + UI.esc(String(r.unit).slice(0, 10)) : ''}</span><span class="dx-span">${UI.esc(span)}${n}</span>${kind === 'dataset' ? '<span></span>' : `<button class="hs-add" data-add="${UI.esc(r.id)}" title="Add to watchlist">+</button>`}</div>`;
  }
  static select(i) {
    this.sel = i; const r = this.rows[i]; const info = document.getElementById('dx-info');
    document.querySelectorAll('#dx-list .dx-row').forEach(el => el.classList.toggle('sel', +el.dataset.i === i));
    if (!r) { info.innerHTML = '<div class="dx-empty">Select a row.</div>'; return; }
    const kind = r.kind || 'series';
    const meta = SymDir.rowMeta(r);
    info.innerHTML = `<h4>${UI.esc(r.symbol || r.id)}</h4><div style="color:var(--fg-1)">${UI.esc(r.name || '')}</div>
      <div class="dx-kv"><span>id</span><span style="word-break:break-all">${UI.esc(r.id)}</span><span>source</span><span>${UI.esc(r.provider_name || r.provider || '')}${r.ex ? ' · ' + UI.esc(r.ex) : ''}</span><span>kind</span><span>${kind}${r.type ? ' · ' + UI.esc(r.type) : ''}</span>${r.freq ? `<span>cadence</span><span>${UI.esc(r.freq)}</span>` : ''}${r.unit ? `<span>unit</span><span>${UI.esc(String(r.unit))}</span>` : ''}${(r.first || r.last) ? `<span>history</span><span>${UI.esc(r.first || '?')} → ${UI.esc(r.last || '?')}</span>` : ''}${r.n ? `<span>${kind === 'dataset' ? 'series' : 'obs'}</span><span>${this.fmtN(r.n)}</span>` : ''}${r.last_value != null ? `<span>last value</span><span>${jhFmtVal(Number(r.last_value))}</span>` : ''}${meta ? `<span>meta</span><span>${UI.esc(meta)}</span>` : ''}${Array.isArray(r.sources) && r.sources.length ? `<span>warehouse</span><span>${r.sources.map(x => UI.esc(x.provider_name + ' ' + x.id.split(':').slice(1).join(':'))).join('<br>')}</span>` : ''}</div>
      <div class="dx-actions">${kind === 'dataset' ? `<button class="primary" data-act="browse">Open ${r.n ? this.fmtN(r.n) + ' ' : ''}series ›</button>` : `<button class="primary" data-act="chart">Chart (double-click)</button><button data-act="add">+ Watchlist</button>`}</div>`;
    info.querySelectorAll('[data-act]').forEach(b => b.addEventListener('click', () => { const a = b.dataset.act; if (a === 'chart') this.chart(i); else if (a === 'add') { if (SymDir.addToWatchlist(r.id, r) === 'ok') b.textContent = '✓ added'; } else if (a === 'browse') { this.close(); DatasetBrowser.open(r.id, r.name); } }));
  }
  static chart(i) {
    const r = this.rows[i]; if (!r) return;
    if ((r.kind || 'series') === 'dataset') { this.close(); DatasetBrowser.open(r.id, r.name); return; }
    SymDir.remember(r); WatchlistManager.rememberSymbol(r.id, r.kind === 'instrument' ? 'equity' : 'series');
    ChartController.loadTicker(r.id); this.close();
  }
  static onKey(e) {
    if (e.key === 'ArrowDown') { e.preventDefault(); this.select(Math.min(this.rows.length - 1, this.sel + 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); this.select(Math.max(0, this.sel - 1)); }
    else if (e.key === 'Enter') { e.preventDefault(); this.chart(this.sel); }
    else if (e.key === '+' && this.rows[this.sel]) { e.preventDefault(); SymDir.addToWatchlist(this.rows[this.sel].id, this.rows[this.sel]); }
    else if (e.key === 'Escape') { this.close(); }
    const el = document.querySelector(`#dx-list .dx-row[data-i="${this.sel}"]`); if (el) el.scrollIntoView({ block: 'nearest' });
  }
  static async renderMini() {
    const host = document.getElementById('dx-mini'); if (!host) return;
    await this.loadProviders();
    const ps = this.providers || [];
    host.innerHTML = `<div class="dx-crumb" style="cursor:pointer" id="dx-mini-open">⌕ Open the full Data Explorer (Ctrl+E) · ${ps.length} providers</div>` +
      ps.map(p => { const c = p.in_directory || {}; const n = (c.series || 0) + (c.dataset || 0) + (c.instrument || 0); return `<div class="dx-prov" data-prov="${UI.esc(p.slug)}"><div><div class="dx-prov-name">${UI.esc(p.name || p.slug)}</div><div class="dx-prov-sub">${UI.esc(p.api || '')}${p.freshest_h != null ? ' · ' + p.freshest_h + 'h' : ''}</div></div><div class="dx-prov-n">${n ? this.fmtN(n) : this.fmtN(p.datasets || 0)}</div></div>`; }).join('');
    document.getElementById('dx-mini-open').addEventListener('click', () => this.open());
    host.querySelectorAll('.dx-prov').forEach(el => el.addEventListener('click', () => this.open(el.dataset.prov)));
  }
}

async function jhYahooBars(sym, days) {""", "classes")

# ─────────────────────────── init hooks + symbol card refresh on load + theme-aware chart palette
rep("""  DatasetBrowser.init(); SymDir.loadInstruments();  // ops 5117: symbol directory (instant instruments + dataset browser)""",
    """  DatasetBrowser.init(); SymDir.loadInstruments();  // ops 5117: symbol directory (instant instruments + dataset browser)
  DataExplorer.init(); TVShell.init();               // ops 5153: TradingView shell + Data Explorer""", "init")
rep("""    State.activeTicker = ticker;
    ChartTabs.setActiveTabSymbol(ticker);""", """    State.activeTicker = ticker;
    ChartTabs.setActiveTabSymbol(ticker);
    setTimeout(() => { try { TVShell.renderSymCard(); } catch (e) {} }, 900);""", "symcard on load")
# LightweightCharts palettes follow the theme (the seven native chart builders share these literals)
s = s.replace("layout: { background: { color: '#0a0e14' }, textColor: '#a8b3c7', fontFamily: \"'IBM Plex Mono',monospace\" },",
              "layout: { background: { color: TVShell.palette().bg }, textColor: TVShell.palette().text, fontFamily: \"'IBM Plex Mono',monospace\" },")
s = s.replace("grid: { vertLines: { color: '#1c2433' }, horzLines: { color: '#1c2433' } },",
              "grid: { vertLines: { color: TVShell.palette().grid }, horzLines: { color: TVShell.palette().grid } },")
s = s.replace("rightPriceScale: { borderColor: '#1c2433' }, timeScale: { borderColor: '#1c2433', minBarSpacing: 0.001 }",
              "rightPriceScale: { borderColor: TVShell.palette().border }, timeScale: { borderColor: TVShell.palette().border, minBarSpacing: 0.001 }")
s = s.replace("timeScale: { borderColor: '#1c2433', timeVisible: intraday, minBarSpacing: 0.001 },",
              "timeScale: { borderColor: TVShell.palette().border, timeVisible: intraday, minBarSpacing: 0.001 },")
s = s.replace("timeScale: { borderColor: '#1c2433', minBarSpacing: 0.001 }, crosshair: { mode: 1 },",
              "timeScale: { borderColor: TVShell.palette().border, minBarSpacing: 0.001 }, crosshair: { mode: 1 },")
# main series colours follow the theme too
rep("""    const ct = State.chartType || 'candles';
    const up = '#26ffaf', dn = '#ff5577';""", """    const ct = State.chartType || 'candles';
    const pal = TVShell.palette(); const up = pal.up, dn = pal.dn;""", "mainSeries palette")
rep("""    else if (ct === 'line') { series = chart.addLineSeries({ color: opts.lineColor || '#22d3ee', lineWidth: 2 }); series.setData(bars.map(b => ({ time: b.time, value: b.close }))); }
    else if (ct === 'area') { series = chart.addAreaSeries({ lineColor: '#22d3ee', topColor: 'rgba(34,211,238,0.25)', bottomColor: 'rgba(34,211,238,0.02)', lineWidth: 2 }); series.setData(bars.map(b => ({ time: b.time, value: b.close }))); }""",
    """    else if (ct === 'line') { series = chart.addLineSeries({ color: opts.lineColor || pal.line, lineWidth: 2 }); series.setData(bars.map(b => ({ time: b.time, value: b.close }))); }
    else if (ct === 'area') { series = chart.addAreaSeries({ lineColor: pal.line, topColor: pal.areaTop, bottomColor: pal.areaBot, lineWidth: 2 }); series.setData(bars.map(b => ({ time: b.time, value: b.close }))); }""", "mainSeries line/area")
rep("""        const area = chart.addAreaSeries({ lineColor: '#22d3ee', topColor: 'rgba(34,211,238,0.25)', bottomColor: 'rgba(34,211,238,0.02)', lineWidth: 2 });
        area.setData(pts);""", """        const pal2 = TVShell.palette();
        const area = chart.addAreaSeries({ lineColor: pal2.line, topColor: pal2.areaTop, bottomColor: pal2.areaBot, lineWidth: 2 });
        area.setData(pts);""", "series area palette")
# Escape closes the explorer too
rep("""      MacroData.close(); DatasetBrowser.close();""", """      MacroData.close(); DatasetBrowser.close(); DataExplorer.close();""", "escape")
# the classic drawer helper must be harmless under the shell (nodes live in the rail)
rep("""  function setLeft(open) { leftDrawer.classList.toggle('open', open); edgeLeft.classList.toggle('hidden', open); try { localStorage.setItem('jh_wl_open', open ? '1' : '0'); } catch (e) {} if (open) { try { UI.refreshWatchlist(); } catch (e) {} } }""",
    """  function setLeft(open) { if (document.body.classList.contains('tv-shell')) { TVShell.collapse(false); TVShell.showPanel('watchlist'); return; } leftDrawer.classList.toggle('open', open); edgeLeft.classList.toggle('hidden', open); try { localStorage.setItem('jh_wl_open', open ? '1' : '0'); } catch (e) {} if (open) { try { UI.refreshWatchlist(); } catch (e) {} } }""", "setLeft shell")
rep("""  function setRight(open) { rightDrawer.classList.toggle('open', open); edgeRight.classList.toggle('hidden', open); }""",
    """  function setRight(open) { if (document.body.classList.contains('tv-shell')) { TVShell.collapse(false); TVShell.showPanel('info'); return; } rightDrawer.classList.toggle('open', open); edgeRight.classList.toggle('hidden', open); }""", "setRight shell")

P.write_text(s, encoding="utf-8")
print("chart-pro.html: TradingView shell + Data Explorer patched (%d bytes)" % len(s))
