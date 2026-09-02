"""Patch chart-pro.html for the symbol directory (ops 5116/5117).

Every replacement is anchored on exact existing text and asserted, so a drift
in the page fails loudly instead of silently shipping half a feature.
"""
import re
import sys
from pathlib import Path

P = Path("/root/work/si/chart-pro.html")
s = P.read_text(encoding="utf-8")
orig_len = len(s)


def rep(old, new, count=1, label=""):
    global s
    n = s.count(old)
    assert n == count, "anchor %r found %d times (want %d)" % (label or old[:60], n, count)
    s = s.replace(old, new)


# ───────────────────────── 1. CSS
rep(""".hs-foot { padding: 7px 12px; border-top: 1px solid var(--border); font-family: var(--font-mono); font-size: 9px; color: var(--fg-4); display: flex; gap: 14px; }
""", """.hs-foot { padding: 7px 12px; border-top: 1px solid var(--border); font-family: var(--font-mono); font-size: 9px; color: var(--fg-4); display: flex; gap: 14px; }
/* ops 5117: symbol directory rows (every ticker + every data series) */
.hs-row.series, .hs-row.dataset { grid-template-columns: minmax(64px, 190px) 1fr auto auto; }
.hs-row.instrument { grid-template-columns: 64px 1fr auto auto; }
.hs-tk.series { color: var(--violet); font-size: 10.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.hs-tk.dataset { color: var(--amber); font-size: 10.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.hs-name small { color: var(--fg-4); font-family: var(--font-mono); font-size: 9px; margin-left: 6px; }
.hs-prov { font-family: var(--font-mono); font-size: 8.5px; padding: 1px 5px; border-radius: 3px; border: 1px solid var(--border); color: var(--fg-3); white-space: nowrap; }
.hs-prov.fred { color: var(--violet); border-color: var(--violet); }
.hs-prov.eurostat, .hs-prov.ecb { color: #7cc4ff; border-color: #7cc4ff; }
.hs-prov.instrument { color: var(--cyan); border-color: var(--cyan); }
.hs-prov.dataset { color: var(--amber); border-color: var(--amber); }
.hs-add { width: 22px; height: 22px; border-radius: 5px; border: 1px solid var(--border); background: var(--bg-2); color: var(--fg-2); font-family: var(--font-mono); font-size: 14px; line-height: 1; cursor: pointer; opacity: 0.45; display: flex; align-items: center; justify-content: center; }
.hs-row:hover .hs-add, .hs-row.sel .hs-add { opacity: 1; }
.hs-add:hover { color: var(--bg); background: var(--green); border-color: var(--green); }
.hs-add.added { color: var(--green); border-color: var(--green); opacity: 1; }
.wl-symbol.series { font-size: 10.5px; max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; display: inline-block; vertical-align: bottom; color: var(--violet); }
.wl-last.series { font-size: 10px; }
.dsb-facets { display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 14px; border-bottom: 1px solid var(--border); max-height: 132px; overflow-y: auto; }
.dsb-fgroup { display: inline-flex; align-items: center; gap: 4px; flex-wrap: wrap; padding: 2px 6px; border: 1px dashed var(--border); border-radius: 6px; }
.dsb-fgroup b { font-family: var(--font-mono); font-size: 9px; color: var(--fg-4); text-transform: uppercase; }
.dsb-chip { font-family: var(--font-mono); font-size: 9.5px; padding: 1px 6px; border-radius: 10px; border: 1px solid var(--border); color: var(--fg-2); cursor: pointer; background: var(--bg-2); }
.dsb-chip:hover { border-color: var(--cyan); color: var(--cyan); }
.dsb-chip.on { background: var(--cyan); color: var(--bg); border-color: var(--cyan); }
.dsb-row { display: grid; grid-template-columns: minmax(120px, 260px) 1fr 70px 110px 26px; gap: 10px; align-items: center; padding: 6px 14px; cursor: pointer; border-bottom: 1px solid var(--border); }
.dsb-row:hover { background: var(--bg-3); }
.dsb-row .k { font-family: var(--font-mono); font-size: 10.5px; color: var(--violet); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dsb-row .nm { font-size: 11.5px; color: var(--fg-2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dsb-row .v { font-family: var(--font-mono); font-size: 10.5px; color: var(--fg-1); text-align: right; }
.dsb-row .rng { font-family: var(--font-mono); font-size: 9.5px; color: var(--fg-4); text-align: right; white-space: nowrap; }
.dsb-title { font-family: var(--font-mono); font-size: 11px; color: var(--amber); padding: 0 10px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 40%; }
.dsb-hint { padding: 6px 14px; font-family: var(--font-mono); font-size: 9.5px; color: var(--fg-4); }
""", label="css")

# ───────────────────────── 2. dataset browser modal markup (before the add-symbol modal)
rep("""<div class="search-modal" id="add-symbol-modal">""", """<!-- ops 5117: dataset browser — every series inside a dataset (Tier-1 for the 567M-series index) -->
<div class="search-modal" id="dsb-modal">
  <div class="search-modal-content" style="width:min(1120px,94vw)">
    <div class="search-modal-header">
      <span class="dsb-title" id="dsb-title">dataset</span>
      <input type="text" id="dsb-input" placeholder="Filter series — type dimension codes (DE, M, CLV10_MEUR), a vector, a country…" autocomplete="off" />
      <button class="modal-close" id="dsb-close">&times;</button>
    </div>
    <div class="dsb-facets" id="dsb-facets"></div>
    <div class="dsb-hint" id="dsb-hint"></div>
    <div class="search-modal-body" id="dsb-body"><div class="search-empty">Loading series…</div></div>
    <div class="search-footer">
      <span>click = chart full history &middot; <b>+</b> = add to watchlist &middot; <kbd>Esc</kbd> close</span>
      <span id="dsb-count"></span>
    </div>
  </div>
</div>

<div class="search-modal" id="add-symbol-modal">""", label="dsb modal")

# ───────────────────────── 3. State: symbolMeta + persistence
rep("""  knownSymbols: {},      // {ticker: {kind, ts}} — per-user searched/opened symbols (grows the index)
""", """  knownSymbols: {},      // {ticker: {kind, ts}} — per-user searched/opened symbols (grows the index)
  symbolMeta: {},        // ops 5117: {id: {name, provider, unit, freq, first, last, kind}} — directory metadata for series ids
""", label="state")
rep("""      const known = localStorage.getItem('jh_known_symbols');
      if (known) State.knownSymbols = JSON.parse(known);
""", """      const known = localStorage.getItem('jh_known_symbols');
      if (known) State.knownSymbols = JSON.parse(known);
      const smeta = localStorage.getItem('jh_symbol_meta');
      if (smeta) State.symbolMeta = JSON.parse(smeta);
""", label="load meta")
rep("""      localStorage.setItem('jh_known_symbols', JSON.stringify(State.knownSymbols || {}));
""", """      localStorage.setItem('jh_known_symbols', JSON.stringify(State.knownSymbols || {}));
      try { const sm = State.symbolMeta || {}; const ks = Object.keys(sm); if (ks.length > 3000) ks.slice(0, ks.length - 3000).forEach(k => delete sm[k]); localStorage.setItem('jh_symbol_meta', JSON.stringify(sm)); } catch (e) {}
""", label="save meta")
rep("""        if (data.knownSymbols) State.knownSymbols = data.knownSymbols;
""", """        if (data.knownSymbols) State.knownSymbols = data.knownSymbols;
        if (data.symbolMeta) Object.assign(State.symbolMeta, data.symbolMeta);
""", label="cloud load meta")
rep("""          knownSymbols: State.knownSymbols,
          alertRules: State.alertRules,""", """          knownSymbols: State.knownSymbols,
          symbolMeta: State.symbolMeta,
          alertRules: State.alertRules,""", label="cloud sync meta")

# ───────────────────────── 4. helpers + SymDir + DatasetBrowser (after jhChartable)
rep("""async function jhYahooBars(sym, days) {""", r"""// ─── SYMBOL DIRECTORY (ops 5117: justhodl-symdir — every ticker + every data series) ────
// A series id is `provider:rest` for one of the warehouse providers below. Everything else
// (bare tickers, EXCHANGE:SYMBOL) stays on the equity / TradingView path. Ids are
// case-sensitive (census slugs, FiscalData datasets, StatCan vectors), so they are never
// upper-cased by the watchlist or the chart.
const JH_SERIES_PROVIDERS = new Set(['fred','te','eurostat','ecb','nyfed','ofr','ofr-hfm','ofr-bsrm','boj','statcan','worldbank','treasury','boe','census','bls','oecd','bis','imf','cboe']);
function jhSeriesProvider(id) {
  if (!id || typeof id !== 'string' || !id.includes(':')) return null;
  const p = id.split(':')[0].toLowerCase();
  return JH_SERIES_PROVIDERS.has(p) ? p : null;
}
function jhCanonId(id) { id = (id || '').trim(); if (!id) return id; return jhSeriesProvider(id) ? id : id.toUpperCase(); }
function jhFmtVal(v) {
  if (v == null || !isFinite(v)) return '—';
  const a = Math.abs(v);
  if (a >= 1e12) return (v / 1e12).toFixed(2) + 'T';
  if (a >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (a >= 1000) return v.toLocaleString(undefined, { maximumFractionDigits: 1 });
  if (a >= 1) return v.toFixed(2);
  return Number(v.toPrecision(3)).toString();
}
function jhToast(msg) {
  let el = document.getElementById('jh-toast');
  if (!el) { el = document.createElement('div'); el.id = 'jh-toast'; el.style.cssText = 'position:fixed;bottom:22px;left:50%;transform:translateX(-50%);background:var(--bg-1);border:1px solid var(--green);color:var(--fg-0);padding:8px 14px;border-radius:8px;font:12px var(--font-mono);z-index:9999;box-shadow:0 8px 30px rgba(0,0,0,.5);display:none'; document.body.appendChild(el); }
  el.textContent = msg; el.style.display = 'block'; clearTimeout(el._t); el._t = setTimeout(() => { el.style.display = 'none'; }, 2200);
}
class SymDir {
  static instruments = null; static _instrLoading = null; static _cache = new Map(); static _seriesCache = new Map();
  static loadInstruments() {
    if (this.instruments) return Promise.resolve(this.instruments);
    if (this._instrLoading) return this._instrLoading;
    this._instrLoading = fetch(`${PROXY}/data/symdir/instruments.json.gz`).then(r => r.ok ? r.json() : null).then(d => {
      const rows = (d && d.rows) || [];
      this.instruments = rows.map(r => ({ symbol: r[0], name: r[1] || '', exchange: r[2] || '', type: r[3] || '', market: r[4] || '', pop: r[5] || 0, symU: (r[0] || '').toUpperCase(), nameU: (r[1] || '').toUpperCase() }));
      return this.instruments;
    }).catch(() => { this.instruments = []; return this.instruments; });
    return this._instrLoading;
  }
  // instant, client-side: symbol prefix > name prefix > name contains, weighted by popularity
  static localInstruments(q, limit = 8) {
    if (!this.instruments) { this.loadInstruments(); return []; }
    const Q = q.toUpperCase().trim(); if (!Q) return [];
    const bare = Q.includes(':') ? Q.split(':').pop() : Q;
    const out = [];
    for (const r of this.instruments) {
      let sc = 0;
      if (r.symU === Q || r.symU === bare) sc = 1000;
      else if (r.symU.startsWith(bare)) sc = 500 - (r.symU.length - bare.length) * 3;
      else if (r.symU.endsWith(':' + bare)) sc = 480;
      else if (r.nameU.startsWith(Q)) sc = 300;
      else if (Q.length >= 3 && r.nameU.includes(Q)) sc = 150;
      if (!sc) continue;
      sc += r.pop * 100; if (r.market === 'stocks') sc += 20; else if (r.market === 'otc') sc -= 60;
      out.push({ sc, r });
    }
    out.sort((a, b) => b.sc - a.sc);
    return out.slice(0, limit).map(x => x.r);
  }
  static query(q, limit = 30) {
    const key = q.toLowerCase().trim() + '|' + limit;
    if (this._cache.has(key)) return this._cache.get(key);
    const p = fetch(`${PROXY}/symsearch?q=${encodeURIComponent(q)}&limit=${limit}`).then(r => r.ok ? r.json() : { rows: [] }).catch(() => ({ rows: [], failed: true }));
    this._cache.set(key, p); if (this._cache.size > 400) this._cache.delete(this._cache.keys().next().value);
    return p;
  }
  static browse(ds, q = '', limit = 300, offset = 0) {
    return fetch(`${PROXY}/browse?ds=${encodeURIComponent(ds)}&q=${encodeURIComponent(q)}&limit=${limit}&offset=${offset}`).then(r => r.ok ? r.json() : { rows: [], error: 'HTTP ' + r.status }).catch(e => ({ rows: [], error: String(e) }));
  }
  static series(id, nocache = false) {
    const key = id + (nocache ? '|nc' : '');
    if (!nocache && this._seriesCache.has(key)) return this._seriesCache.get(key);
    const p = fetch(`${PROXY}/series?id=${encodeURIComponent(id)}${nocache ? '&nocache=1' : ''}`).then(r => r.json()).catch(e => ({ obs: [], error: String(e) }));
    this._seriesCache.set(key, p); if (this._seriesCache.size > 60) this._seriesCache.delete(this._seriesCache.keys().next().value);
    p.then(d => { if (d && d.error && !(d.obs || []).length) this._seriesCache.delete(key); });
    return p;
  }
  static quotes(ids) {
    if (!ids.length) return Promise.resolve({});
    return fetch(`${PROXY}/quote?ids=${encodeURIComponent(ids.join(','))}`).then(r => r.ok ? r.json() : { quotes: {} }).then(d => d.quotes || {}).catch(() => ({}));
  }
  static remember(r) {
    if (!r || !r.id) return;
    State.symbolMeta = State.symbolMeta || {};
    State.symbolMeta[r.id] = { name: r.name || '', provider: r.provider || jhSeriesProvider(r.id), unit: r.unit || null, freq: r.freq || null, first: r.first || null, last: r.last || null, kind: r.kind || 'series' };
  }
  static meta(id) { return (State.symbolMeta || {})[id] || null; }
  static provLabel(p) { return ({ fred: 'FRED', eurostat: 'Eurostat', ecb: 'ECB', oecd: 'OECD', bis: 'BIS', imf: 'IMF', boj: 'BoJ', statcan: 'StatCan', worldbank: 'World Bank', nyfed: 'NY Fed', ofr: 'OFR', 'ofr-hfm': 'OFR HFM', 'ofr-bsrm': 'OFR BSRM', te: 'TE mirror', treasury: 'FiscalData', boe: 'BoE', census: 'Census', bls: 'BLS', cboe: 'Cboe', tv: 'TradingView', instrument: 'Market' })[p] || (p || '').toUpperCase(); }
  static rowMeta(r) {
    const bits = [];
    if (r.freq) bits.push(r.freq);
    if (r.unit) bits.push(String(r.unit).slice(0, 18));
    if (r.first || r.last) bits.push(`${(r.first || '?').slice(0, 7)}→${(r.last || '?').slice(0, 7)}`);
    if (r.kind === 'dataset' && r.n) bits.push(Number(r.n).toLocaleString() + ' series');
    return bits.join(' · ');
  }
  // TradingView-style "+": add to the open custom watchlist, else to Favorites
  static addToWatchlist(id, meta) {
    id = jhCanonId(id);
    if (meta) this.remember({ ...meta, id });
    const active = State.activeWatchlistId;
    if (active && active.startsWith('custom_') && State.customWatchlists[active]) {
      if (State.customWatchlists[active].tickers.includes(id)) { jhToast(`${id} is already in ${State.customWatchlists[active].name}`); return 'dup'; }
      WatchlistManager.addTicker(active, id);
      jhToast(`+ ${id} → ${State.customWatchlists[active].name}`);
    } else {
      const ids = Object.keys(State.customWatchlists || {});
      if (ids.length === 1) { WatchlistManager.addTicker(ids[0], id); jhToast(`+ ${id} → ${State.customWatchlists[ids[0]].name}`); }
      else { State.favorites[id] = true; WatchlistManager.syncToCloud(); jhToast(`★ ${id} → Favorites (open a custom list to add there)`); }
    }
    WatchlistManager.rememberSymbol(id, jhSeriesProvider(id) ? 'series' : 'equity');
    UI.refreshWatchlist();
    return 'ok';
  }
  static rowHtml(r, i, sel, opts = {}) {
    const isSeries = r.kind === 'series', isDs = r.kind === 'dataset';
    const cls = isSeries ? 'series' : (isDs ? 'dataset' : 'instrument');
    const sym = r.symbol || r.id;
    const provCls = r.provider === 'fred' ? 'fred' : (r.provider === 'eurostat' || r.provider === 'ecb') ? 'eurostat' : (isDs ? 'dataset' : 'instrument');
    const meta = this.rowMeta(r);
    const tag = isDs ? `<span class="hs-prov dataset">${UI.esc(this.provLabel(r.provider))} · browse ›</span>` : `<span class="hs-prov ${provCls}">${UI.esc(this.provLabel(r.provider))}${r.ex && r.kind === 'instrument' ? ' · ' + UI.esc(r.ex) : ''}</span>`;
    const add = isDs ? '<span></span>' : `<button class="hs-add" data-add="${UI.esc(r.id)}" title="Add to watchlist">+</button>`;
    return `<div class="hs-row ${cls} ${i === sel ? 'sel' : ''}" data-ticker="${UI.esc(r.id)}" data-i="${i}" data-kind="${cls}" title="${UI.esc(r.id)}">
      <span class="hs-tk ${cls}">${UI.esc(sym)}</span>
      <span class="hs-name">${UI.esc(r.name || '')}${meta ? `<small>${UI.esc(meta)}</small>` : ''}</span>
      ${tag}${add}</div>`;
  }
  static wireRows(container, onPick) {
    container.querySelectorAll('.hs-add').forEach(btn => btn.addEventListener('click', (ev) => {
      ev.stopPropagation();
      const id = btn.dataset.add; const row = btn.closest('.hs-row');
      const meta = (this._lastRows || {})[id];
      if (this.addToWatchlist(id, meta) === 'ok') { btn.classList.add('added'); btn.textContent = '✓'; }
    }));
    container.querySelectorAll('.hs-row').forEach(el => el.addEventListener('click', () => onPick(el)));
  }
  static noteRows(rows) { this._lastRows = this._lastRows || {}; rows.forEach(r => { if (r && r.id) this._lastRows[r.id] = r; }); }
}

// ─── DATASET BROWSER (series inside a dataset — Eurostat/ECB flows, StatCan cubes, WB indicators, BoJ DBs…) ──
class DatasetBrowser {
  static ds = null; static q = ''; static _t = null; static _seq = 0; static rows = []; static chips = [];
  static init() {
    const input = document.getElementById('dsb-input'); if (!input) return;
    input.addEventListener('input', () => { this.q = input.value; this.chips = []; clearTimeout(this._t); this._t = setTimeout(() => this.load(), 260); });
    document.getElementById('dsb-close').addEventListener('click', () => this.close());
    document.getElementById('dsb-modal').addEventListener('click', (e) => { if (e.target.id === 'dsb-modal') this.close(); });
  }
  static open(ds, title) {
    this.ds = ds; this.q = ''; this.chips = [];
    document.getElementById('dsb-title').textContent = (title ? title + ' · ' : '') + ds;
    const input = document.getElementById('dsb-input'); input.value = '';
    document.getElementById('dsb-facets').innerHTML = '';
    document.getElementById('dsb-hint').textContent = '';
    document.getElementById('dsb-body').innerHTML = '<div class="search-empty">Loading series…</div>';
    document.getElementById('dsb-modal').classList.add('open');
    setTimeout(() => input.focus(), 40);
    this.load();
  }
  static close() { document.getElementById('dsb-modal').classList.remove('open'); this.ds = null; }
  static async load() {
    if (!this.ds) return;
    const seq = ++this._seq;
    const q = [this.q.trim(), ...this.chips].filter(Boolean).join(' ').trim();
    const d = await SymDir.browse(this.ds, q, 300, 0);
    if (seq !== this._seq || !this.ds) return;
    this.rows = d.rows || [];
    SymDir.noteRows(this.rows);
    const total = d.total != null ? Number(d.total).toLocaleString() : '?';
    document.getElementById('dsb-count').textContent = `${(d.matched != null ? d.matched : this.rows.length).toLocaleString()} matched · ${total} series in dataset${d.scanned ? ` · scanned ${Number(d.scanned).toLocaleString()}` : ''}${d.truncated ? ' (partial scan — narrow with codes)' : ''}`;
    document.getElementById('dsb-hint').textContent = d.error ? ('⚠ ' + d.error) : (d.hint || '');
    // facets: dimension → top values (from the scanned sample)
    const fac = d.facets || {};
    const fh = Object.entries(fac).filter(([k, v]) => v && v.length > 1 && v.length < 400).slice(0, 10).map(([k, v]) =>
      `<span class="dsb-fgroup"><b>${UI.esc(k)}</b>${v.slice(0, 12).map(([val, n]) => `<span class="dsb-chip ${this.chips.includes(val) ? 'on' : ''}" data-chip="${UI.esc(val)}" title="${n} series">${UI.esc(val)}</span>`).join('')}</span>`).join('');
    document.getElementById('dsb-facets').innerHTML = fh;
    document.getElementById('dsb-facets').querySelectorAll('.dsb-chip').forEach(ch => ch.addEventListener('click', () => {
      const v = ch.dataset.chip; const i = this.chips.indexOf(v); if (i >= 0) this.chips.splice(i, 1); else this.chips = [v]; this.load();
    }));
    const body = document.getElementById('dsb-body');
    if (!this.rows.length) { body.innerHTML = `<div class="search-empty">${d.error ? 'Could not list this dataset.' : 'No series match — try a dimension code (geo like DE, frequency like M) or clear the filter.'}</div>`; return; }
    body.innerHTML = this.rows.map((r, i) => `<div class="dsb-row" data-i="${i}" title="${UI.esc(r.id)}">
        <span class="k">${UI.esc(r.symbol || r.id)}</span>
        <span class="nm">${UI.esc(r.name || '')}${r.geo ? ` <small style="color:var(--fg-4)">· ${UI.esc(r.geo)}</small>` : ''}</span>
        <span class="v">${r.last_value != null ? jhFmtVal(Number(r.last_value)) : ''}</span>
        <span class="rng">${UI.esc((r.first || '').slice(0, 7))}${r.first || r.last ? '→' : ''}${UI.esc((r.last || '').slice(0, 7))}${r.n ? ` · ${r.n}` : ''}</span>
        <button class="hs-add" data-add="${UI.esc(r.id)}" title="Add to watchlist">+</button></div>`).join('');
    body.querySelectorAll('.hs-add').forEach(btn => btn.addEventListener('click', (ev) => {
      ev.stopPropagation(); const r = this.rows[+btn.closest('.dsb-row').dataset.i];
      if (SymDir.addToWatchlist(r.id, r) === 'ok') { btn.classList.add('added'); btn.textContent = '✓'; }
    }));
    body.querySelectorAll('.dsb-row').forEach(el => el.addEventListener('click', () => {
      const r = this.rows[+el.dataset.i]; if (!r) return;
      SymDir.remember(r); WatchlistManager.rememberSymbol(r.id, 'series'); ChartController.loadTicker(r.id); this.close();
    }));
  }
}

async function jhYahooBars(sym, days) {""", label="symdir classes")

# ───────────────────────── 5. HeaderSearch → directory-driven
rep("""    this._seq++; const seq = this._seq;
    // Instant local JustHodl results
    const local = UniverseSearch.localSearch(q).map(r => ({ ticker: r.ticker, name: r.name, kind: 'justhodl', sources: r.sources }));
    this._groups = { justhodl: local, equity: [], macro: [] };
    this.render(true);
    // Remote: TradingView universe + FRED, each renders as it returns
    const enc = encodeURIComponent(q);
    fetch(`${PROXY}/tv-search?text=${enc}`).then(r => r.ok ? r.json() : {symbols:[]}).then(d => {
      if (seq !== this._seq) return;
      const lt = new Set(local.map(l => l.ticker));
      this._groups.equity = (d.symbols || []).filter(s => !lt.has((s.symbol||'').toUpperCase()))
        .filter(s => jhChartable(s))
        .map(s => ({ ticker: s.symbol, name: s.description, kind: 'equity', type: s.type, exchange: s.exchange, full: s.full }));
      const qU = q.toUpperCase();
      const hfav = jhFavs().filter(f => f.ticker.toUpperCase().includes(qU) || (f.name||'').toUpperCase().includes(qU) || (f.display||'').toUpperCase().includes(qU)).slice(0, 5);
      if (hfav.length) {
        const hs = new Set(hfav.map(f => f.ticker));
        this._groups.equity = this._groups.equity.filter(r => !hs.has(r.ticker));
        this._groups.favorites = hfav;
      }
      this.render();
    }).catch(()=>{});
    fetch(`${PROXY}/fred-search?text=${enc}`).then(r => r.ok ? r.json() : {series:[]}).then(d => {
      if (seq !== this._seq) return;
      this._groups.macro = (d.series || []).map(s => ({ ticker: 'FRED:' + s.id, display: s.id, name: s.title, kind: 'macro', units: s.units, frequency: s.frequency }));
      this.render();
    }).catch(()=>{});
  }
""", """    this._seq++; const seq = this._seq;
    // Instant local: JustHodl signals + the instrument directory (every Polygon/finviz/TV symbol, client-side)
    const local = UniverseSearch.localSearch(q).map(r => ({ ticker: r.ticker, name: r.name, kind: 'justhodl', sources: r.sources }));
    const lt = new Set(local.map(l => l.ticker));
    const instr = SymDir.localInstruments(q, 8).filter(r => !lt.has(r.symbol.toUpperCase()))
      .map(r => ({ id: r.symbol, symbol: r.symbol, name: r.name, kind: 'instrument', provider: r.market === 'tv' ? 'tv' : 'instrument', ex: r.exchange, type: r.type, market: r.market }));
    const qU = q.toUpperCase();
    const hfav = jhFavs().filter(f => f.ticker.toUpperCase().includes(qU) || (f.name||'').toUpperCase().includes(qU) || (f.display||'').toUpperCase().includes(qU)).slice(0, 5);
    this._groups = { favorites: hfav, justhodl: local, instruments: instr, series: [], datasets: [], equity: [] };
    this.render(true);
    if (!SymDir.instruments) SymDir.loadInstruments().then(() => { if (seq === this._seq) this.onInput(q); });
    // Server: the symbol directory (every data series + dataset across every provider), then TradingView for what's left
    SymDir.query(q, 40).then(d => {
      if (seq !== this._seq) return;
      const rows = d.rows || [];
      SymDir.noteRows(rows);
      const seen = new Set([...this._groups.instruments.map(r => r.id.toUpperCase()), ...lt]);
      const sh = (d.series_hits && d.series_hits.rows) || [];
      SymDir.noteRows(sh);
      this._groups.series = [...sh, ...rows.filter(r => r.kind === 'series')].filter(r => !seen.has(r.id.toUpperCase())).slice(0, 14);
      this._groups.datasets = rows.filter(r => r.kind === 'dataset').slice(0, 6);
      rows.filter(r => r.kind === 'instrument' && !seen.has(r.id.toUpperCase())).slice(0, 4).forEach(r => { this._groups.instruments.push(r); seen.add(r.id.toUpperCase()); });
      this._suggest = d.suggest || [];
      this._failed = !!d.failed;
      this.render();
      if (d.failed) {   // directory unreachable → legacy FRED full-text search so macro never goes dark
        fetch(`${PROXY}/fred-search?text=${encodeURIComponent(q)}`).then(r => r.ok ? r.json() : {series:[]}).then(f => {
          if (seq !== this._seq) return;
          this._groups.series = (f.series || []).map(s => ({ id: 'fred:' + s.id, symbol: s.id, name: s.title, kind: 'series', provider: 'fred', unit: s.units, freq: s.frequency }));
          this.render();
        }).catch(()=>{});
      }
    });
    fetch(`${PROXY}/tv-search?text=${encodeURIComponent(q)}`).then(r => r.ok ? r.json() : {symbols:[]}).then(d => {
      if (seq !== this._seq) return;
      const have = new Set([...lt, ...this._groups.instruments.map(r => r.id.toUpperCase())]);
      this._groups.equity = (d.symbols || []).filter(s => !have.has((s.symbol||'').toUpperCase()) && !have.has((s.full||'').toUpperCase()))
        .filter(s => jhChartable(s)).slice(0, 6)
        .map(s => ({ ticker: s.symbol, name: s.description, kind: 'equity', type: s.type, exchange: s.exchange, full: s.full }));
      this.render();
    }).catch(()=>{});
  }
""", label="headersearch onInput")

rep("""  static render(partial) {
    const dd = document.getElementById('hsearch-dropdown');
    const g = this._groups || { justhodl: [], equity: [], macro: [] };
    g.favorites = g.favorites || [];
    this.results = [...g.favorites, ...g.justhodl, ...g.equity, ...g.macro];
    dd.classList.add('open');
    if (!this.results.length) { dd.innerHTML = `<div class="hs-empty">${partial ? 'Searching…' : 'No matches'}</div>`; return; }
    let idx = 0; let html = '';
    const section = (label, items, fn) => { if (!items.length) return ''; let s = `<div class="hs-group">${label}</div>`; items.forEach(it => { s += fn(it, idx); idx++; }); return s; };
    html += section('★ Favorites', g.favorites, (r, i) => {
      return `<div class="hs-row ${i===this.sel?'sel':''}" data-ticker="${UI.esc(r.ticker)}" data-i="${i}" data-full="${UI.esc(r.full||'')}"><span class="hs-tk" style="color:var(--amber)">${UI.esc(r.display || r.ticker)}</span><span class="hs-name">${UI.esc(r.name||'')}</span><span class="hs-tag">${UI.esc(r.kind||'fav')}</span></div>`;
    });
    html += section('★ Your JustHodl signals', g.justhodl, (r, i) => {
      const badges = (r.sources||[]).slice(0,3).map(x => `<span class="hs-badge">${x}</span>`).join('');
      return `<div class="hs-row ${i===this.sel?'sel':''}" data-ticker="${UI.esc(r.ticker)}" data-i="${i}"><span class="hs-tk">${UI.esc(r.ticker)}</span><span class="hs-name">${UI.esc(r.name||'')}<span class="hs-badges">${badges}</span></span><span class="hs-tag signal">signal</span></div>`;
    });
    html += section('US Equities · ETFs · Indices · Forex', g.equity, (r, i) => {
      const tc = (r.type||'').toLowerCase();
      return `<div class="hs-row ${i===this.sel?'sel':''}" data-ticker="${UI.esc(r.ticker)}" data-i="${i}" data-full="${UI.esc(r.full||'')}" data-type="${UI.esc(tc)}"><span class="hs-tk">${UI.esc(r.ticker)}</span><span class="hs-name">${UI.esc(r.name||'')}</span><span class="hs-tag ${tc}">${UI.esc((r.type||'eq'))} ${UI.esc(r.exchange||'')}</span></div>`;
    });
    html += section('Macro · FRED economic series', g.macro, (r, i) => {
      return `<div class="hs-row ${i===this.sel?'sel':''}" data-ticker="${UI.esc(r.ticker)}" data-i="${i}"><span class="hs-tk macro">${UI.esc(r.display||r.ticker)}</span><span class="hs-name">${UI.esc(r.name||'')}</span><span class="hs-tag macro">${UI.esc(r.frequency||'macro')}</span></div>`;
    });
    html += `<div class="hs-foot"><span>↑↓ navigate</span><span>↵ load</span><span>esc close</span><span>${this.results.length} results</span></div>`;
    dd.innerHTML = html;
    dd.querySelectorAll('.hs-row').forEach(el => el.addEventListener('click', () => this.pick(+el.dataset.i)));
  }
  
  static pick(i) {
    const r = this.results[i];
    if (!r) return;
    if (r.kind === 'equity' && r.type) State.symbolType[r.ticker] = r.type;
    if (r.kind === 'equity' && r.full && r.full.includes(':') && !/^[A-Z]{1,5}$/.test(r.ticker)) State.symbolResolution[r.ticker] = r.full;
    WatchlistManager.rememberSymbol(r.ticker, r.kind);
    ChartController.loadTicker(r.ticker);
    this.close();
    const input = document.getElementById('search-input'); if (input) input.blur();
  }
""", """  static render(partial) {
    const dd = document.getElementById('hsearch-dropdown');
    const g = this._groups || { favorites: [], justhodl: [], instruments: [], series: [], datasets: [], equity: [] };
    ['favorites', 'justhodl', 'instruments', 'series', 'datasets', 'equity'].forEach(k => { g[k] = g[k] || []; });
    this.results = [...g.favorites, ...g.justhodl, ...g.instruments, ...g.equity, ...g.series, ...g.datasets];
    dd.classList.add('open');
    if (!this.results.length) {
      const sug = (this._suggest || []).length && !partial ? `<div class="hs-foot"><span>did you mean:</span>${this._suggest.slice(0, 6).map(t => `<span class="dsb-chip" data-sug="${UI.esc(t)}">${UI.esc(t)}</span>`).join('')}</div>` : '';
      dd.innerHTML = `<div class="hs-empty">${partial ? 'Searching every ticker and data series…' : (this._failed ? 'Symbol directory unreachable — showing TradingView + FRED only' : 'No matches')}</div>${sug}`;
      dd.querySelectorAll('[data-sug]').forEach(el => el.addEventListener('click', () => { const inp = document.getElementById('search-input'); inp.value = el.dataset.sug; this.onInput(inp.value); }));
      return;
    }
    let idx = 0; let html = '';
    const section = (label, items, fn) => { if (!items.length) return ''; let s = `<div class="hs-group">${label}</div>`; items.forEach(it => { s += fn(it, idx); idx++; }); return s; };
    html += section('★ Favorites', g.favorites, (r, i) => {
      return `<div class="hs-row ${i===this.sel?'sel':''}" data-ticker="${UI.esc(r.ticker)}" data-i="${i}" data-full="${UI.esc(r.full||'')}"><span class="hs-tk" style="color:var(--amber)">${UI.esc(r.display || r.ticker)}</span><span class="hs-name">${UI.esc(r.name||'')}</span><span class="hs-tag">${UI.esc(r.kind||'fav')}</span></div>`;
    });
    html += section('★ Your JustHodl signals', g.justhodl, (r, i) => {
      const badges = (r.sources||[]).slice(0,3).map(x => `<span class="hs-badge">${x}</span>`).join('');
      return `<div class="hs-row ${i===this.sel?'sel':''}" data-ticker="${UI.esc(r.ticker)}" data-i="${i}"><span class="hs-tk">${UI.esc(r.ticker)}</span><span class="hs-name">${UI.esc(r.name||'')}<span class="hs-badges">${badges}</span></span><span class="hs-tag signal">signal</span></div>`;
    });
    html += section('Stocks · ETFs · Indices · Crypto · FX', g.instruments, (r, i) => SymDir.rowHtml(r, i, this.sel));
    html += section('TradingView universe', g.equity, (r, i) => {
      const tc = (r.type||'').toLowerCase();
      return `<div class="hs-row ${i===this.sel?'sel':''}" data-ticker="${UI.esc(r.ticker)}" data-i="${i}" data-full="${UI.esc(r.full||'')}" data-type="${UI.esc(tc)}"><span class="hs-tk">${UI.esc(r.ticker)}</span><span class="hs-name">${UI.esc(r.name||'')}</span><span class="hs-tag ${tc}">${UI.esc((r.type||'eq'))} ${UI.esc(r.exchange||'')}</span></div>`;
    });
    html += section('Data series · FRED · ECB · Eurostat · BoJ · StatCan · World Bank · NY Fed · OFR · BLS · Census …', g.series, (r, i) => SymDir.rowHtml(r, i, this.sel));
    html += section('Datasets · open to browse every series inside', g.datasets, (r, i) => SymDir.rowHtml(r, i, this.sel));
    html += `<div class="hs-foot"><span>↑↓ navigate</span><span>↵ load</span><span>+ add to watchlist</span><span>esc close</span><span>${this.results.length} results${partial ? ' · searching…' : ''}</span></div>`;
    dd.innerHTML = html;
    SymDir.wireRows(dd, (el) => this.pick(+el.dataset.i));
  }
  
  static pick(i) {
    const r = this.results[i];
    if (!r) return;
    if (r.kind === 'dataset') { DatasetBrowser.open(r.id, r.name); this.close(); return; }
    const id = r.ticker || r.id;
    if (r.kind === 'equity' && r.type) State.symbolType[id] = r.type;
    if (r.kind === 'equity' && r.full && r.full.includes(':') && !/^[A-Z]{1,5}$/.test(id)) State.symbolResolution[id] = r.full;
    if (r.kind === 'instrument' && r.type) State.symbolType[id] = String(r.type).toLowerCase();
    if (r.kind === 'series') SymDir.remember(r);
    WatchlistManager.rememberSymbol(id, r.kind === 'series' ? 'series' : (r.kind || 'equity'));
    ChartController.loadTicker(id);
    this.close();
    const input = document.getElementById('search-input'); if (input) input.blur();
  }
""", label="headersearch render/pick")

rep("""      else { const q = input.value.trim().toUpperCase(); if (q) { WatchlistManager.rememberSymbol(q, 'equity'); ChartController.loadTicker(q); this.close(); input.blur(); } }""",
    """      else { const q = jhCanonId(input.value); if (q) { WatchlistManager.rememberSymbol(q, jhSeriesProvider(q) ? 'series' : 'equity'); ChartController.loadTicker(q); this.close(); input.blur(); } }""", label="headersearch enter")

# ───────────────────────── 6. UniverseSearch (Cmd-K modal): directory groups
rep("""    // Parallel remote: each source renders as soon as it returns
    this.debounceTimer = setTimeout(() => {
      const q = encodeURIComponent(query);
      fetch(`${PROXY}/tv-search?text=${q}`).then(r => r.ok ? r.json() : {symbols:[]})""", """    // Instant instruments from the directory (client-side)
    this._groups.equity = SymDir.localInstruments(query, 8).filter(r => !local.some(l => l.ticker === r.symbol.toUpperCase()))
      .map(r => ({ ticker: r.symbol, name: r.name, kind: 'equity', type: r.type, exchange: r.exchange, full: r.symbol }));
    this.renderGrouped(this._groups, true);
    // Parallel remote: each source renders as soon as it returns
    this.debounceTimer = setTimeout(() => {
      const q = encodeURIComponent(query);
      SymDir.query(query, 30).then(d => {
        if (seq !== this._seq) return;
        const rows = d.rows || []; SymDir.noteRows(rows);
        const sh = (d.series_hits && d.series_hits.rows) || []; SymDir.noteRows(sh);
        this._groups.macro = [...sh, ...rows.filter(r => r.kind === 'series')].slice(0, 12).map(r => ({ ticker: r.id, display: r.symbol || r.id, name: r.name, kind: 'macro', units: r.unit, frequency: r.freq, provider: r.provider, _row: r }));
        this._groups.datasets = rows.filter(r => r.kind === 'dataset').slice(0, 6).map(r => ({ ticker: r.id, display: r.symbol || r.id, name: r.name, kind: 'dataset', provider: r.provider, n: r.n, _row: r }));
        this.renderGrouped(this._groups);
      });
      fetch(`${PROXY}/tv-search?text=${q}`).then(r => r.ok ? r.json() : {symbols:[]})""", label="universesearch remote")
rep("""          this._groups.equity = (d.symbols || [])
            .filter(s => !localTickers.has((s.symbol||'').toUpperCase()))
            .filter(s => jhChartable(s))
            .map(s => ({ ticker: s.symbol, name: s.description, kind: 'equity', type: s.type, exchange: s.exchange, full: s.full }));
          this.renderGrouped(this._groups);
        }).catch(()=>{});
      fetch(`${PROXY}/fred-search?text=${q}`).then(r => r.ok ? r.json() : {series:[]})
        .then(d => {
          if (seq !== this._seq) return; // stale
          this._groups.macro = (d.series || [])
            .map(s => ({ ticker: 'FRED:' + s.id, display: s.id, name: s.title, kind: 'macro', units: s.units, frequency: s.frequency }));
          this.renderGrouped(this._groups);
        }).catch(()=>{});
    }, 200);""", """          const have = new Set([...localTickers, ...(this._groups.equity || []).map(r => (r.ticker || '').toUpperCase())]);
          const tv = (d.symbols || [])
            .filter(s => !have.has((s.symbol||'').toUpperCase()) && !have.has((s.full||'').toUpperCase()))
            .filter(s => jhChartable(s)).slice(0, 8)
            .map(s => ({ ticker: s.symbol, name: s.description, kind: 'equity', type: s.type, exchange: s.exchange, full: s.full }));
          this._groups.equity = [...(this._groups.equity || []), ...tv];
          this.renderGrouped(this._groups);
        }).catch(()=>{});
    }, 200);""", label="universesearch tv merge")
rep("""    for (const g of ['justhodl', 'equity', 'macro']) groups[g] = (groups[g] || []).filter(r => !favSet2.has(r.ticker));
    this.currentResults = [...groups.favorites, ...groups.recents, ...groups.justhodl, ...groups.equity, ...groups.macro];""",
    """    for (const g of ['justhodl', 'equity', 'macro', 'datasets']) groups[g] = (groups[g] || []).filter(r => !favSet2.has(r.ticker));
    this.currentResults = [...groups.favorites, ...groups.recents, ...groups.justhodl, ...groups.equity, ...groups.macro, ...groups.datasets];""", label="universesearch results")
rep("""    html += section('Macro · FRED economic series', groups.macro, (r, i) => {
      return `<div class="search-result us-row ${i===this.focusIdx?'focus':''}" data-ticker="${UI.esc(r.ticker)}" data-kind="macro">
        <span class="sr-ticker" style="color:var(--violet)">${UI.esc(r.display||r.ticker)}</span>
        <span class="sr-name">${UI.esc(r.name||'')}</span>
        <span class="sr-price">${UI.esc(r.units||'')} · ${UI.esc(r.frequency||'')}</span>
        ${star(r, i)}<span class="sr-chg">→</span></div>`;
    });
    body.innerHTML = html;""", """    html += section('Data series · every provider (FRED · ECB · Eurostat · BoJ · StatCan · World Bank · NY Fed · OFR · BLS …)', groups.macro, (r, i) => {
      return `<div class="search-result us-row ${i===this.focusIdx?'focus':''}" data-ticker="${UI.esc(r.ticker)}" data-kind="macro" title="${UI.esc(r.ticker)}">
        <span class="sr-ticker" style="color:var(--violet);font-size:10.5px">${UI.esc(r.display||r.ticker)}</span>
        <span class="sr-name">${UI.esc(r.name||'')}</span>
        <span class="sr-price">${UI.esc(SymDir.provLabel(r.provider))} · ${UI.esc(r.units||'')} · ${UI.esc(r.frequency||'')}</span>
        ${star(r, i)}<button class="hs-add" data-add="${UI.esc(r.ticker)}" title="Add to watchlist">+</button></div>`;
    });
    html += section('Datasets · open to browse every series inside', groups.datasets || [], (r, i) => {
      return `<div class="search-result us-row ${i===this.focusIdx?'focus':''}" data-ticker="${UI.esc(r.ticker)}" data-kind="dataset" title="${UI.esc(r.ticker)}">
        <span class="sr-ticker" style="color:var(--amber);font-size:10.5px">${UI.esc(r.display||r.ticker)}</span>
        <span class="sr-name">${UI.esc(r.name||'')}</span>
        <span class="sr-price">${UI.esc(SymDir.provLabel(r.provider))}${r.n ? ' · ' + Number(r.n).toLocaleString() + ' series' : ''} · browse ›</span>
        <span class="sr-chg">›</span></div>`;
    });
    body.innerHTML = html;
    body.querySelectorAll('.hs-add').forEach(btn => btn.addEventListener('click', (ev) => {
      ev.stopPropagation(); const it = this.currentResults.find(x => x.ticker === btn.dataset.add);
      if (SymDir.addToWatchlist(btn.dataset.add, it && it._row) === 'ok') { btn.classList.add('added'); btn.textContent = '✓'; }
    }));""", label="universesearch render macro")
rep("""  // Select → ENRICH: cache resolution, load chart (right engine), pull all intel
  static select(ticker, kind, full) {
    if (kind === 'equity' && full && full.includes(':')) {""", """  // Select → ENRICH: cache resolution, load chart (right engine), pull all intel
  static select(ticker, kind, full) {
    if (kind === 'dataset') { const m = this.currentResults.find(r => r.ticker === ticker); DatasetBrowser.open(ticker, m && m.name); this.close(); return; }
    if (kind === 'macro') { const m = this.currentResults.find(r => r.ticker === ticker); if (m && m._row) SymDir.remember(m._row); }
    if (kind === 'equity' && full && full.includes(':')) {""", label="universesearch select")

# ───────────────────────── 7. AddSymbol modal: directory results (series + instruments)
rep("""  static async tvSearch(query) {
    try {
      const r = await fetch(`${PROXY}/tv-search?text=${encodeURIComponent(query)}&type=${this.activeFilter}`);
      if (!r.ok) return;
      const data = await r.json();
      let symbols = data.symbols || [];
      const local = this.localSearch(query);
      const seen = new Set(local.map(s => s.symbol));
      const merged = [...local, ...symbols.filter(s => !seen.has(s.symbol))];
      this.renderResults(merged);
    } catch (e) { console.warn('TV search failed', e); }
  }""", """  static async tvSearch(query) {
    try {
      const local = this.localSearch(query);
      const seen = new Set(local.map(s => s.symbol));
      const instr = SymDir.localInstruments(query, 8).filter(r => !seen.has(r.symbol))
        .map(r => { seen.add(r.symbol); return { symbol: r.symbol, description: r.name, type: r.type || 'stock', exchange: r.exchange, full: r.symbol }; });
      const wantData = !this.activeFilter || this.activeFilter === 'data';
      const [tvRes, dirRes] = await Promise.all([
        this.activeFilter === 'data' ? Promise.resolve({ symbols: [] }) : fetch(`${PROXY}/tv-search?text=${encodeURIComponent(query)}&type=${this.activeFilter}`).then(r => r.ok ? r.json() : { symbols: [] }).catch(() => ({ symbols: [] })),
        wantData ? SymDir.query(query, 30) : Promise.resolve({ rows: [] }),
      ]);
      const tv = (tvRes.symbols || []).filter(s => !seen.has(s.symbol));
      const rows = (dirRes.rows || []); SymDir.noteRows(rows);
      const data = rows.filter(r => r.kind === 'series').slice(0, 14).map(r => ({ symbol: r.id, description: r.name, type: 'data', exchange: SymDir.provLabel(r.provider), full: r.id, _row: r }));
      const merged = [...local, ...instr, ...(this.activeFilter === 'data' ? data : [...tv, ...data])];
      this.renderResults(merged);
    } catch (e) { console.warn('symbol search failed', e); }
  }""", label="addsymbol search")
rep("""      <button class="asf-btn" data-asf="index">Indices</button>
      <span class="asf-target" id="asf-target">""", """      <button class="asf-btn" data-asf="index">Indices</button>
      <button class="asf-btn" data-asf="data" title="Every data series: FRED, ECB, Eurostat, BoJ, StatCan, World Bank, NY Fed, OFR, BLS, Census…">Data series</button>
      <span class="asf-target" id="asf-target">""", label="addsymbol filter btn")
rep("""    const result = this.currentResults.find(r => r.symbol === symbol);
    if (result) {
      const type = (result.type || '').toLowerCase();""", """    const result = this.currentResults.find(r => r.symbol === symbol);
    if (result && result._row) SymDir.remember(result._row);   // ops 5117: directory series carry their metadata
    if (result && result.type !== 'data') {
      const type = (result.type || '').toLowerCase();""", label="addsymbol add")

# ───────────────────────── 8. loadTicker: case-preserving ids + series routing
rep("""  static async loadTicker(ticker, knownFull) {
    if (!ticker) return;
    ticker = ticker.toUpperCase().trim();
    """, """  static async loadTicker(ticker, knownFull) {
    if (!ticker) return;
    ticker = jhCanonId(ticker);
    """, label="loadTicker canon")
rep("""    // Macro series → always native line
    if (isFred || isDbn) { NativeChart.render(ticker); return; }""", """    // Macro series → always native line (ops 5117: any directory series id — fred:, eurostat:, ecb:, boj:, statcan:, …)
    if (isFred || isDbn || jhSeriesProvider(ticker)) { NativeChart.render(ticker); return; }""", label="loadTicker series route")

# ───────────────────────── 9. NativeChart: series renderer (full history from the warehouse)
rep("""    if (ticker.startsWith('FRED:')) return this.renderLine(ticker, 'FRED');
    if (ticker.startsWith('DBN:')) return this.renderLine(ticker, 'DBN');""", """    if (jhSeriesProvider(ticker)) return this.renderSeries(ticker);
    if (ticker.startsWith('DBN:')) return this.renderLine(ticker, 'DBN');""", label="nativechart dispatch")
rep("""  static async renderLine(symbol, source) {""", """  // ops 5117: any directory series id → full history via /series (warehouse-first, live-API fallback, mirror-extended)
  static async renderSeries(symbol) {
    const paneIdx = State.activeChartPane;
    const container = document.getElementById(`tv-container-${paneIdx}`);
    if (!container) return;
    const prov = jhSeriesProvider(symbol) || 'series';
    const known = SymDir.meta(symbol) || {};
    const shortSym = symbol.length > 60 ? symbol.slice(0, 58) + '…' : symbol;
    container.innerHTML = `
      <div class="native-chart-wrap">
        <div class="native-chart-head"><div class="nch-sym" title="${UI.esc(symbol)}">${UI.esc(shortSym)}</div><div class="nch-meta" id="nch-meta-${paneIdx}">${UI.esc(SymDir.provLabel(prov))} · loading full history…</div></div>
        <div class="native-chart-loading" id="nch-loading-${paneIdx}">Loading ${UI.esc(known.name || symbol)} — full history…</div>
        <div id="nch-chart-${paneIdx}" style="width:100%;height:100%"></div>
      </div>`;
    try {
      const d = await SymDir.series(symbol);
      const bars = (d.obs || []).map(o => ({ time: o[0], value: o[1] }));
      const loadingEl = document.getElementById(`nch-loading-${paneIdx}`);
      if (loadingEl) loadingEl.style.display = 'none';
      if (!bars.length) { if (loadingEl) { loadingEl.style.display = 'flex'; loadingEl.textContent = `No observations for ${symbol}${d.error ? ' — ' + d.error : ''}`; } return; }
      if (State.activeTicker !== symbol) return;
      SymDir.remember({ id: symbol, name: d.name, provider: d.provider, unit: d.unit, freq: d.freq, first: d.first, last: d.last, kind: 'series' });
      WatchlistManager.saveLocal();
      const nameEl = document.getElementById('active-name'); if (nameEl) nameEl.textContent = d.name || '';
      const seen = new Set(); const clean = [];
      for (const b of bars) { if (!seen.has(b.time) && b.value != null && isFinite(b.value)) { seen.add(b.time); clean.push({ time: b.time, value: b.value }); } }
      const cmL = State.changeMode;
      if (cmL && cmL !== 'price') {
        NativeChart.renderChangeHisto(paneIdx, clean, cmL, NativeChart.CHANGE_LABELS[cmL], symbol);
        NativeChart.renderStatBlock(paneIdx, clean, symbol);
        return;
      }
      const chartEl = document.getElementById(`nch-chart-${paneIdx}`);
      if (!chartEl || typeof LightweightCharts === 'undefined') return;
      const chart = LightweightCharts.createChart(chartEl, {
        autoSize: true,
        layout: { background: { color: '#0a0e14' }, textColor: '#a8b3c7', fontFamily: "'IBM Plex Mono',monospace" },
        grid: { vertLines: { color: '#1c2433' }, horzLines: { color: '#1c2433' } },
        rightPriceScale: { borderColor: '#1c2433' }, timeScale: { borderColor: '#1c2433' }, crosshair: { mode: 1 },
      });
      const area = chart.addAreaSeries({ lineColor: '#22d3ee', topColor: 'rgba(34,211,238,0.25)', bottomColor: 'rgba(34,211,238,0.02)', lineWidth: 2 });
      area.setData(clean);
      NativeChart.renderStatBlock(paneIdx, clean, symbol);
      chart.timeScale().fitContent();
      const tfDays = (State.tf && State.tf.days) || 9999;
      if (tfDays < 9999 && clean.length > 2) {
        const lastT = new Date(clean[clean.length - 1].time).getTime();
        const fromT = new Date(lastT - tfDays * 86400000).toISOString().slice(0, 10);
        if (fromT > clean[0].time) { try { chart.timeScale().setVisibleRange({ from: fromT, to: clean[clean.length - 1].time }); } catch (e) {} }
      }
      State.nativeChart = chart; ChartSync.register(paneIdx, chart);
      const last = clean[clean.length-1], prev = clean[clean.length-2]||last;
      const chg = prev.value ? ((last.value-prev.value)/Math.abs(prev.value)*100) : 0;
      const metaEl = document.getElementById(`nch-meta-${paneIdx}`);
      if (metaEl) metaEl.innerHTML = `${UI.esc(d.provider_name || SymDir.provLabel(prov))} · <b>${jhFmtVal(last.value)}</b>${d.unit ? ' ' + UI.esc(String(d.unit).slice(0, 22)) : ''} <span style="color:${chg>=0?'#26ffaf':'#ff5577'}">${chg>=0?'+':''}${chg.toFixed(2)}%</span> · ${clean.length.toLocaleString()} obs · ${UI.esc(d.first || '')}→${UI.esc(d.last || '')}${d.freq ? ' · ' + UI.esc(d.freq) : ''}${d.source ? ' · <span style="color:var(--fg-4)" title="' + UI.esc(d.source) + '">' + UI.esc(String(d.source).split(':')[0]) + '</span>' : ''}`;
    } catch (e) {
      const loadingEl = document.getElementById(`nch-loading-${paneIdx}`);
      if (loadingEl) { loadingEl.style.display='flex'; loadingEl.textContent = `${SymDir.provLabel(prov)} error: ${String(e).slice(0,80)}`; }
    }
  }
  
  static async renderLine(symbol, source) {""", label="renderSeries")

# ───────────────────────── 10. Watchlist: case-preserving ids, names + last/chg for series
rep("""  static addTicker(id, ticker) {
    ticker = (ticker || '').toUpperCase().trim();""", """  static addTicker(id, ticker) {
    ticker = jhCanonId(ticker);""", label="addTicker canon")
rep("""      return wl.tickers.map(t => ({ ticker: t, name: '', sub: '' }));""", """      return wl.tickers.map(t => { const m = SymDir.meta(t); return { ticker: t, name: (m && m.name) || '', sub: (m && m.name) || '' }; });""", label="getMembers names")
rep("""  static getFavorites() {
    return Object.keys(State.favorites).map(t => ({ ticker: t, name: '', sub: 'favorite' }));
  }""", """  static getFavorites() {
    return Object.keys(State.favorites).map(t => { const m = SymDir.meta(t); return { ticker: t, name: (m && m.name) || '', sub: (m && m.name) || 'favorite' }; });
  }""", label="getFavorites names")
rep("""class QuoteService {
  static async fetchBatch(tickers) {
    if (!tickers || tickers.length === 0) return {};
    const equities = tickers.filter(t => /^[A-Z]{1,5}$/.test(t));
    if (equities.length === 0) return {};
    try {
      const r = await fetch(`${PROXY}/quotes?tickers=${equities.join(',')}`);
      if (!r.ok) return {};
      const data = await r.json();
      return data.tickers || {};
    } catch (e) { console.warn('Quote fetch failed', e); return {}; }
  }""", """class QuoteService {
  static async fetchBatch(tickers) {
    if (!tickers || tickers.length === 0) return {};
    const equities = tickers.filter(t => /^[A-Z]{1,5}$/.test(t));
    const series = tickers.filter(t => jhSeriesProvider(t) || /^FRED:/i.test(t));
    const out = {};
    const jobs = [];
    if (equities.length) jobs.push(fetch(`${PROXY}/quotes?tickers=${equities.join(',')}`).then(r => r.ok ? r.json() : {}).then(d => Object.assign(out, d.tickers || {})).catch(e => console.warn('Quote fetch failed', e)));
    if (series.length) jobs.push(SymDir.quotes(series.slice(0, 60)).then(qs => {
      for (const [id, q] of Object.entries(qs)) {
        if (!q || !q.ok) continue;
        const key = series.find(t => t === id || ('fred:' + t.slice(5)) === id) || id;
        out[key] = { price: q.last, changePct: q.chg_pct, change: q.chg, series: true, unit: q.unit, name: q.name, lastDate: q.last_date, mom: q.mom_pct, qoq: q.qoq_pct, yoy: q.yoy_pct, spark: q.spark };
        SymDir.remember({ id: key, name: q.name, unit: q.unit, freq: q.freq, first: q.first, last: q.last_date, kind: 'series' });
      }
    }));
    await Promise.all(jobs);
    return out;
  }""", label="quoteservice")
rep("""    // Attach signals + quotes + flags
    members = members.map(m => {
      const q = State.quotes[m.ticker] || {};
      const signals = WatchlistManager.getSymbolSignals(m.ticker);
      return {
        ...m,
        last: q.price != null ? q.price : null,
        changePct: q.changePct != null ? q.changePct : m.fallbackChange,
        signals,
        flag: WatchlistManager.getFlag(m.ticker),
      };
    });""", """    // Attach signals + quotes + flags
    members = members.map(m => {
      const q = State.quotes[m.ticker] || {};
      const signals = WatchlistManager.getSymbolSignals(m.ticker);
      const isSeries = !!(jhSeriesProvider(m.ticker) || /^FRED:/i.test(m.ticker));
      const meta = isSeries ? (SymDir.meta(m.ticker) || {}) : {};
      return {
        ...m,
        last: q.price != null ? q.price : null,
        changePct: q.changePct != null ? q.changePct : m.fallbackChange,
        signals,
        flag: WatchlistManager.getFlag(m.ticker),
        isSeries,
        seriesName: q.name || meta.name || m.name || '',
        seriesUnit: q.unit || meta.unit || '',
        lastDate: q.lastDate || meta.last || '',
      };
    });""", label="members series")
rep("""      const lastStr = m.last != null ? '$' + m.last.toFixed(2) : '—';""", """      const lastStr = m.last != null ? (m.isSeries ? jhFmtVal(m.last) : '$' + m.last.toFixed(2)) : '—';
      const symDisp = m.isSeries ? m.ticker.split(':').slice(1).join(':') : m.ticker;""", label="lastStr")
rep("""              <span class="wl-symbol">${this.esc(m.ticker)}</span>""", """              <span class="wl-symbol ${m.isSeries ? 'series' : ''}" title="${this.esc(m.ticker)}${m.isSeries && m.lastDate ? ' · last obs ' + this.esc(m.lastDate) : ''}">${this.esc(symDisp)}</span>""", label="wl symbol")
rep("""            <span class="wl-sub">${this.esc(m.sub || m.name || '')}</span>
          </span>
          <span class="wl-last">${lastStr}</span>""", """            <span class="wl-sub" title="${this.esc(m.isSeries ? m.seriesName : (m.sub || m.name || ''))}">${this.esc(m.isSeries ? (m.seriesName || SymDir.provLabel(jhSeriesProvider(m.ticker))) : (m.sub || m.name || ''))}</span>
          </span>
          <span class="wl-last ${m.isSeries ? 'series' : ''}" title="${m.isSeries && m.seriesUnit ? this.esc(String(m.seriesUnit)) : ''}">${lastStr}</span>""", label="wl sub/last")

# ───────────────────────── 11. rememberSymbol: keep ids as-is; init hooks; escape closes browser
rep("""    if (e.key === 'Escape') {
      UniverseSearch.close(); Heatmap.close(); FlagPicker.close(); AddSymbol.close();
      MacroData.close();""", """    if (e.key === 'Escape') {
      UniverseSearch.close(); Heatmap.close(); FlagPicker.close(); AddSymbol.close();
      MacroData.close(); DatasetBrowser.close();""", label="escape")
# init: find HeaderSearch.init() call and add DatasetBrowser.init + instruments preload
m = re.search(r"\n(\s*)HeaderSearch\.init\(\);", s)
assert m, "HeaderSearch.init() call not found"
ind = m.group(1)
s = s.replace(m.group(0), "\n%sHeaderSearch.init();\n%sDatasetBrowser.init(); SymDir.loadInstruments();  // ops 5117: symbol directory (instant instruments + dataset browser)" % (ind, ind), 1)

P.write_text(s, encoding="utf-8")
print("chart-pro.html patched: %d -> %d bytes (+%d)" % (orig_len, len(s), len(s) - orig_len))
