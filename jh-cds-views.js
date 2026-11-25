/* JustHodl CDS desk, the 2.0 views (2026-11-25): the market at a glance, every product of the credit file, index options,
   the commercial-mortgage indices, quiet names, what the desk repaired, a sheet for any name and the whole document.
   Added beside the 1.6 page (jh-cds-desk.js); nothing there is renamed, moved or removed.  Data: data/cds-desk.json,
   data/cds-desk-tape.json (the prints behind a name, fetched when a name is opened).  Descriptive only: no calls, no sizing.
   jh-reskin-skip: the colours in this file are measured (see PAL and CAT) and must reach the page as written. */
(function (root) {
'use strict';
/* Diverging pair for a change (wider is trouble, tighter is safety) and the neutral between them; the credit board's own. */
const PAL = { bad: '#f23645', good: '#089981', mid: '#363a45', none: '#20242e', badText: '#f7525f', goodText: '#2bb89f', ink: '#e8e2d4', dim: '#8a836f', line: '#2a2618', panel: '#12110C' };
/* Categorical, fixed order, validated on the page's dark surface (lightness band, chroma, colour-blind separation, contrast). */
const CAT = { index: '#4C8DF6', option: '#B8801A', single: '#1F9E89', tranche: '#A56EE8', other: '#6b6f7b' };
const CAT_OF = { index: 'index', option: 'option', tranche: 'tranche', corp: 'single', sov: 'single', sub: 'single', single_other: 'other', trs: 'other', basket: 'other', other: 'other' };
const CAT_WORDS = { index: 'Index CDS', option: 'Index options', single: 'Single names', tranche: 'Index tranches', other: 'Other (loans, total return, baskets)' };
const WIN = { '1d': ['chg_1d_bp', '1D', 'on the day'], '1w': ['chg_1w_bp', '1W', 'in a week'], '1m': ['chg_1m_bp', '1M', 'in a month'], '3m': ['chg_3m_bp', '3M', 'in three months'] };
const GROUPS = { all: 'All corporates', us_corp: 'U.S.', global_corp: 'Global', sub: 'Subordinated', sovereign: 'Sovereigns' };
const MEASURES = { chg: 'Change, bp', pct: 'Change, %', rank: '1-year percentile', level: 'Level' };
const V = { win: '1m', grp: 'all', measure: 'chg', pick: null, prod: '5d', quiet: false, tape: null, tapePromise: null, open: {}, api: null, doc: null, bondSort: null };

const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
const fin = v => typeof v === 'number' && Number.isFinite(v);
const num = (v, dp) => fin(v) ? v.toLocaleString('en-US', { minimumFractionDigits: dp || 0, maximumFractionDigits: dp || 0 }) : '—';
const sgn = (v, dp) => fin(v) ? (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: dp == null ? 1 : dp, maximumFractionDigits: dp == null ? 1 : dp }) : '—';
const money = mm => !fin(mm) ? '—' : mm >= 1000 ? '$' + (mm / 1000).toFixed(mm >= 10000 ? 0 : 1) + 'B' : '$' + Math.round(mm) + 'M';
const euro = mm => !fin(mm) || !mm ? '' : mm >= 1000 ? '€' + (mm / 1000).toFixed(mm >= 10000 ? 0 : 1) + 'B' : '€' + Math.round(mm) + 'M';
const median = xs => { const s = xs.filter(fin).sort((a, b) => a - b); return s.length ? (s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2) : null; };
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
function mix(a, b, t) { const ch = (h, i) => parseInt(h.slice(1 + 2 * i, 3 + 2 * i), 16); let out = '#'; for (let i = 0; i < 3; i++) { const v = Math.round(ch(a, i) + (ch(b, i) - ch(a, i)) * t); out += (v < 16 ? '0' : '') + v.toString(16); } return out; }
const tone = v => !fin(v) || v === 0 ? PAL.dim : v > 0 ? PAL.badText : PAL.goodText;
const priced = r => r && (r.status === 'liquid' || r.status === 'thin' || r.status === 'aged' || r.status === 'bracketed');

/* ------------------------------------------------------------------------------------------------ rows and measures */
function rowsOf(d, grp) {
  const g = (d && d.groups) || {}, out = [];
  (grp === 'all' ? ['us_corp', 'global_corp', 'sub'] : [grp]).forEach(k => ((g[k] || {}).rows || []).forEach(r => out.push(Object.assign({ group: k }, r))));
  return out;
}
function measureOf(r, win, measure) {
  if (measure === 'level') return r.spread_bp;
  if (measure === 'rank') return r.pct_rank_1y;
  const c = r[WIN[win][0]];
  if (!fin(c)) return null;
  if (measure === 'pct') { const from = r.spread_bp - c; return from > 0 ? 100 * c / from : null; }
  return c;
}
/* One tile a sector (or a region for the sovereigns): the middle name's figure, sized by how many names carry a level. */
function tilesOf(d, grp, win, measure) {
  const by = new Map();
  rowsOf(d, grp).filter(priced).forEach(r => {
    const key = (grp === 'sovereign' ? r.region : r.sector) || 'Other';
    if (!by.has(key)) by.set(key, []);
    by.get(key).push(r);
  });
  return [...by.entries()].map(([key, rows]) => {
    const vals = rows.map(r => measureOf(r, win, measure)).filter(fin);
    return { key, n: rows.length, n_with: vals.length, v: median(vals), level: median(rows.map(r => r.spread_bp)), wider: vals.filter(x => x > 0).length, tighter: vals.filter(x => x < 0).length, rows };
  }).filter(t => t.n > 0).sort((a, b) => b.n - a.n);
}
function scaleOf(tiles, measure) {
  const vals = tiles.map(t => t.v).filter(fin).map(Math.abs);
  if (measure === 'rank') return { lo: 0, hi: 100, mid: 50 };
  if (measure === 'level') return { lo: Math.min.apply(null, vals.concat([Infinity])), hi: Math.max.apply(null, vals.concat([0])) };
  return { lo: 0, hi: Math.max(measure === 'pct' ? 5 : 5, vals.length ? Math.max.apply(null, vals) : 0) };
}
function colourOf(v, scale, measure) {
  if (!fin(v)) return PAL.none;
  if (measure === 'rank') { const far = clamp(Math.abs(v - 50) / 50, 0, 1); return mix(PAL.mid, v > 50 ? PAL.bad : PAL.good, v === 50 ? 0 : 0.18 + 0.82 * far); }
  if (measure === 'level') { const span = Math.log(Math.max(scale.hi, 1)) - Math.log(Math.max(scale.lo, 1)); return mix(PAL.mid, PAL.bad, span > 0 ? clamp((Math.log(Math.max(v, 1)) - Math.log(Math.max(scale.lo, 1))) / span, 0, 1) * 0.9 + 0.1 : 0.5); }
  return mix(PAL.mid, v > 0 ? PAL.bad : PAL.good, v === 0 ? 0 : 0.18 + 0.82 * clamp(Math.abs(v) / (scale.hi || 1), 0, 1));
}
const fmtMeasure = (v, measure) => !fin(v) ? '—' : measure === 'level' ? num(v, 0) + ' bp' : measure === 'rank' ? Math.round(v) + 'th pct' : measure === 'pct' ? sgn(v, 1) + '%' : sgn(v, 1) + ' bp';

function squarify(nodes, x, y, w, h) {
  const list = nodes.filter(n => n.v > 0).sort((a, b) => b.v - a.v), total = list.reduce((s, n) => s + n.v, 0), out = [];
  if (!list.length || !(w > 0) || !(h > 0)) return out;
  const k = w * h / total; let i = 0;
  while (i < list.length) {
    const side = Math.min(w, h), row = []; let sum = 0, best = Infinity;
    while (i < list.length) {
      const a = list[i].v * k, s2 = sum + a, mx = row.length ? row[0] : a, worst = Math.max(side * side * mx / (s2 * s2), s2 * s2 / (side * side * a));
      if (row.length && worst > best) break;
      row.push(a); sum = s2; best = worst; i++;
    }
    const thick = sum / side, start = i - row.length; let off = 0;
    row.forEach((a, j) => { const len = a / thick; out.push(w >= h ? { node: list[start + j], x, y: y + off, w: thick, h: len } : { node: list[start + j], x: x + off, y, w: len, h: thick }); off += len; });
    if (w >= h) { x += thick; w -= thick; } else { y += thick; h -= thick; }
  }
  return out;
}

/* ------------------------------------------------------------------------------------------------ small pieces */
const seg = (name, items, cur) => `<span class="cv-seg" role="group">${items.map(([v, l]) => `<button type="button" class="cv-btn${v === cur ? ' on' : ''}" data-cv="${name}" data-v="${esc(v)}" aria-pressed="${v === cur}">${esc(l)}</button>`).join('')}</span>`;
const nameBtn = (r, label) => `<span class="cds-clickable cv-name" data-key="${esc(r.key)}" data-name="${esc(r.name)}" data-group="${esc(r.group || '')}" data-tier="${esc(r.tier || '')}" data-iso3="${esc(r.iso3 || '')}" title="open everything on ${esc(r.name)}">${esc(label || r.name)}</span>`;
function sparkSvg(pts, w, h, colour) {
  const ys = (pts || []).map(p => p[1]).filter(fin);
  if (ys.length < 2) return '';
  const lo = Math.min.apply(null, ys), hi = Math.max.apply(null, ys), n = ys.length;
  const path = ys.map((y, i) => `${i ? 'L' : 'M'}${(i / (n - 1) * w).toFixed(1)},${(hi === lo ? h / 2 : h - 2 - (y - lo) / (hi - lo) * (h - 4)).toFixed(1)}`).join(' ');
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" style="max-width:100%;height:auto" aria-hidden="true"><path d="${path}" fill="none" stroke="${colour}" stroke-width="2" stroke-linejoin="round"/></svg>`;
}
/* Where today's level stands in the name's own year: low on the left, high on the right. */
function meter(lo, hi, v, rank) {
  if (!fin(lo) || !fin(hi) || !fin(v) || hi <= lo) return '';
  const x = clamp((v - lo) / (hi - lo), 0, 1) * 100;
  return `<div class="cv-meter" title="its own year: low ${num(lo, 0)}, high ${num(hi, 0)}; today at the ${fin(rank) ? Math.round(rank) + 'th percentile' : 'level shown'}"><i style="left:${x.toFixed(1)}%"></i></div><div class="cv-meter-ends"><span>${num(lo, 0)}</span><span>1-year range</span><span>${num(hi, 0)}</span></div>`;
}

/* ------------------------------------------------------------------------------------------------ 1. at a glance */
function indexCards(d) {
  const aged = i => i.status === 'aged' || (i.status == null && (i.stale_days || 0) > 10);
  const idx = (d.indices || []).slice().sort((a, b) => aged(a) - aged(b));     // an index that has stopped printing goes last, with its date
  if (!idx.length) return '';
  return `<div class="cv-cards">${idx.map(i => {
    const lvl = i.quote === 'price' && fin(i.price) ? num(i.price, 2) : num(i.spread_bp, 1), unit = i.quote === 'price' && fin(i.price) ? 'price' : 'bp', old = aged(i), c = old ? null : i.chg_1d_bp, m = old ? null : i.chg_1m_bp;
    return `<div class="cv-card cds-clickable${old ? ' cv-aged' : ''}" data-key="IDX:${esc(i.key)}" data-name="${esc(i.name)}" data-group="index" title="${esc(i.read || i.name)}">
      <div class="cv-card-top"><b>${esc(i.name)}</b><span class="cv-dim">${esc(i.group || '')}${i.since === '2.0' ? ' · new' : ''}</span></div>
      ${old ? `<div class="cv-aged-tag">last printed ${esc(i.last_date || '')} · ${num(i.stale_days, 0)} days ago</div>` : ''}
      <div class="cv-card-val">${lvl}<small> ${unit}</small>${unit === 'price' ? `<span class="cv-dim" style="font-size:11px;margin-left:8px">${num(i.spread_bp, 0)} bp</span>` : ''}</div>
      <div class="cv-card-chg"><span style="color:${tone(c)}">${sgn(c)} 1D</span><span style="color:${tone(m)}">${sgn(m)} 1M</span><span class="cv-dim">${fin(i.pct_rank_1y) ? Math.round(i.pct_rank_1y) + 'th pct' : ''}</span></div>
      ${sparkSvg(i.spark, 150, 26, fin(m) && m < 0 ? PAL.goodText : PAL.badText)}
      ${meter(i.lo_1y_bp, i.hi_1y_bp, i.spread_bp, i.pct_rank_1y)}
      <div class="cv-dim" style="font-size:10px;margin-top:4px">${num(i.n_last, 0)} prints · ${esc(i.last_date || '')}</div>
    </div>`;
  }).join('')}</div>`;
}
const isNarrow = () => !!(root && root.innerWidth && root.innerWidth < 720);
function heatHtml(d, st) {
  const narrow = isNarrow(), tiles = tilesOf(d, st.grp, st.win, st.measure), scale = scaleOf(tiles, st.measure), W = narrow ? 380 : 1000, H = narrow ? 540 : 380;   // a phone gets a tall map at true text size
  if (!tiles.length) return '<div class="cv-dim">no name with a level in this group</div>';
  const boxes = squarify(tiles.map(t => ({ v: t.n, t })), 0, 0, W, H);
  const svg = boxes.map(b => {
    const t = b.node.t, big = b.w > 96 && b.h > 46, mid = b.w > 62 && b.h > 28, label = t.key.length > Math.floor(b.w / 7.2) ? t.key.slice(0, Math.max(3, Math.floor(b.w / 7.2) - 1)) + '…' : t.key;
    return `<g class="cv-tile${st.pick === t.key ? ' on' : ''}" data-cv="pick" data-v="${esc(t.key)}" tabindex="0" role="button" aria-label="${esc(t.key)}: ${esc(fmtMeasure(t.v, st.measure))}, ${t.n} names">
      <rect x="${(b.x + 1).toFixed(1)}" y="${(b.y + 1).toFixed(1)}" width="${Math.max(0, b.w - 2).toFixed(1)}" height="${Math.max(0, b.h - 2).toFixed(1)}" rx="3" fill="${colourOf(t.v, scale, st.measure)}"/>
      <title>${esc(t.key)}: ${esc(fmtMeasure(t.v, st.measure))} (middle name) · ${t.n} names with a level · middle level ${num(t.level, 0)} bp · ${t.wider} wider, ${t.tighter} tighter</title>
      ${mid ? `<text x="${(b.x + 8).toFixed(1)}" y="${(b.y + 18).toFixed(1)}" class="cv-tile-name">${esc(label)}</text>` : ''}
      ${mid ? `<text x="${(b.x + 8).toFixed(1)}" y="${(b.y + 35).toFixed(1)}" class="cv-tile-val">${esc(fmtMeasure(t.v, st.measure))}</text>` : ''}
      ${big ? `<text x="${(b.x + 8).toFixed(1)}" y="${(b.y + 50).toFixed(1)}" class="cv-tile-sub">${num(t.level, 0)} bp · ${t.n} names</text>` : ''}
    </g>`;
  }).join('');
  const legend = st.measure === 'level'
    ? `<span>tight ${num(scale.lo, 0)} bp</span><i class="cv-ramp" style="background:linear-gradient(90deg,${mix(PAL.mid, PAL.bad, .1)},${PAL.bad})"></i><span>wide ${num(scale.hi, 0)} bp</span>`
    : st.measure === 'rank' ? `<span>at its year's lows</span><i class="cv-ramp" style="background:linear-gradient(90deg,${PAL.good},${PAL.mid},${PAL.bad})"></i><span>at its year's highs</span>`
      : `<span>tighter (safety) −${num(scale.hi, 1)}</span><i class="cv-ramp" style="background:linear-gradient(90deg,${PAL.good},${PAL.mid},${PAL.bad})"></i><span>+${num(scale.hi, 1)} wider (trouble)</span>`;
  return `<svg viewBox="0 0 ${W} ${H}" width="100%" class="cv-heat" role="img" aria-label="${esc(GROUPS[st.grp])} by ${st.grp === 'sovereign' ? 'region' : 'sector'}: ${esc(MEASURES[st.measure])}">${svg}</svg>
    <div class="cv-legend">${legend}<span class="cv-dim" style="margin-left:auto">one tile a ${st.grp === 'sovereign' ? 'region' : 'sector'} · size = names with a level · colour = the middle name's ${esc(MEASURES[st.measure].toLowerCase())}${st.measure === 'chg' || st.measure === 'pct' ? ' ' + WIN[st.win][2] : ''} · click a tile for its names</span></div>`;
}
const levelText = r => fin(r.points_upfront) ? `${num(r.points_upfront, 1)} pts` : num(r.spread_bp, 1);      // a distressed name is read in points
function pickHtml(d, st) {
  if (!st.pick) return '';
  const t = tilesOf(d, st.grp, st.win, st.measure).find(x => x.key === st.pick);
  if (!t) return '';
  const rows = t.rows.slice().sort((a, b) => (measureOf(b, st.win, st.measure) ?? -1e12) - (measureOf(a, st.win, st.measure) ?? -1e12));
  return `<div class="cv-pick"><div class="cv-pick-head"><b>${esc(t.key)}</b><span class="cv-dim">${t.n} names with a level · middle ${num(t.level, 0)} bp · ${esc(fmtMeasure(t.v, st.measure))}</span><button type="button" class="cv-btn" data-cv="pick" data-v="">close</button></div>
    <div class="cv-scroll"><table class="cv-table" data-jh-nosort><thead><tr><th>Name</th><th>Group</th><th class="r">5Y, bp</th><th class="r">1D</th><th class="r">1W</th><th class="r">1M</th><th class="r">3M</th><th class="r">1Y pct</th><th>Status</th><th>Stock</th><th class="r">Its bonds</th></tr></thead><tbody>
    ${rows.map(r => `<tr><td>${nameBtn(r)}</td><td class="cv-dim">${esc(GROUPS[r.group] || r.group)}</td><td class="r" data-sort="${fin(r.spread_bp) ? r.spread_bp : ''}"><b>${levelText(r)}</b></td>${['1d', '1w', '1m', '3m'].map(w => `<td class="r" style="color:${tone(r[WIN[w][0]])}">${sgn(r[WIN[w][0]])}</td>`).join('')}<td class="r">${fin(r.pct_rank_1y) ? Math.round(r.pct_rank_1y) : '—'}</td><td class="cv-dim">${esc(r.status)}</td><td class="cv-dim">${esc(r.ticker || (r.listing && r.listing.symbol) || '—')}</td><td class="r">${num(r.bond_bp, 0)}</td></tr>`).join('')}
    </tbody></table></div></div>`;
}
function moversHtml(d, st) {
  const col = WIN[st.win][0], rows = rowsOf(d, st.grp === 'all' ? 'all' : st.grp).filter(r => r.liquid && fin(r[col]) && !fin(r.points_upfront) && (r.spread_bp || 0) < 1000);
  const up = rows.filter(r => r[col] > 0).sort((a, b) => b[col] - a[col]).slice(0, 10), dn = rows.filter(r => r[col] < 0).sort((a, b) => a[col] - b[col]).slice(0, 10);
  const top = Math.max.apply(null, up.concat(dn).map(r => Math.abs(r[col])).concat([1]));
  const bars = (list, colour, text) => list.length ? list.map(r => `<div class="cv-bar-row"><span class="cv-bar-name">${nameBtn(r)}</span><span class="cv-bar-track"><i style="width:${(Math.abs(r[col]) / top * 100).toFixed(1)}%;background:${colour}"></i></span><span class="cv-bar-val" style="color:${text}">${sgn(r[col])}</span><span class="cv-bar-lvl cv-dim">${num(r.spread_bp, 0)}</span></div>`).join('') : '<div class="cv-dim">none</div>';
  return `<div class="cv-two"><div><h4>Widened most ${WIN[st.win][2]} <span class="cv-dim">bp · level beside it · liquid names under 1,000 bp</span></h4>${bars(up, PAL.bad, PAL.badText)}</div>
    <div><h4>Tightened most ${WIN[st.win][2]}</h4>${bars(dn, PAL.good, PAL.goodText)}</div></div>`;
}
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const monthYear = iso => { const m = /^(\d{4})-(\d{2})/.exec(iso || ''); return m ? MONTHS[+m[2] - 1] + ' ' + m[1] : String(iso || ''); };
const BANDS = [[0, 50, 'under 50'], [50, 100, '50–100'], [100, 200, '100–200'], [200, 400, '200–400'], [400, 1000, '400–1,000'], [1000, Infinity, '1,000 +']];
function bandsHtml(d) {
  const groups = ['sovereign', 'us_corp', 'global_corp', 'sub'].map(g => [g, rowsOf(d, g).filter(priced)]).filter(x => x[1].length);
  const shade = i => mix(PAL.mid, PAL.bad, 0.08 + 0.92 * i / (BANDS.length - 1));
  return `<h4>Where levels sit <span class="cv-dim">share of the names with a level, by 5Y spread</span></h4>${groups.map(([g, rows]) => {
    const counts = BANDS.map(b => rows.filter(r => r.spread_bp >= b[0] && r.spread_bp < b[1]).length), n = rows.length;
    return `<div class="cv-stack-row"><span class="cv-stack-name">${esc(GROUPS[g])} <span class="cv-dim">${n}</span></span><span class="cv-stack">${counts.map((c, i) => c ? `<i style="width:${(100 * c / n).toFixed(2)}%;background:${shade(i)}" title="${esc(GROUPS[g])}: ${c} names at ${BANDS[i][2]} bp (${Math.round(100 * c / n)}%)">${100 * c / n >= 7 ? c : ''}</i>` : '').join('')}</span></div>`;
  }).join('')}<div class="cv-legend">${BANDS.map((b, i) => `<span><i class="cv-swatch" style="background:${shade(i)}"></i>${b[2]}</span>`).join('')}<span class="cv-dim">bp</span></div>`;
}
function glanceHtml(d, st) {
  const g = d.groups || {}, tot = k => ['sovereign', 'us_corp', 'global_corp', 'sub'].reduce((s, x) => s + ((g[x] || {})[k] || 0), 0), is2 = !!d.products;
  const head = `<div class="cv-head"><h3 id="cds2-glance-h">The credit market at a glance</h3><span class="cv-dim">${num(tot('n_priced'), 0)} names with a level${is2 ? ` · ${num(tot('n_unpriced'), 0)} trade with no level · ${(d.indices || []).length} indices · ${num(tot('n_tied'), 0)} tied to a listed company` : ''} · as of ${esc(d.as_of || '')}</span></div>`;
  return `<div class="panel cv-panel" aria-labelledby="cds2-glance-h">${head}
    ${indexCards(d)}
    <div class="cv-ctl"><span class="cv-lab">Heat map of</span>${seg('grp', Object.entries(GROUPS).filter(([k]) => k === 'all' || ((g[k] || {}).rows || []).length), st.grp)}
      <span class="cv-lab">Since</span>${seg('win', Object.entries(WIN).map(([k, v]) => [k, v[1]]), st.win)}
      <span class="cv-lab">Colour by</span>${seg('measure', Object.entries(MEASURES), st.measure)}</div>
    <div id="cds2-heat">${heatHtml(d, st)}</div><div id="cds2-pick">${pickHtml(d, st)}</div>
    <div id="cds2-movers">${moversHtml(d, st)}</div>
    <div id="cds2-bands">${bandsHtml(d)}</div></div>`;
}

/* ------------------------------------------------------------------------------------------------ 2. products */
function dailyStack(pr) {
  const cols = pr.daily.columns.slice(1), rows = pr.daily.rows || [];
  if (rows.length < 2) return '';
  const cats = ['index', 'option', 'tranche', 'single', 'other'], data = rows.map(r => { const o = { d: r[0], index: 0, option: 0, tranche: 0, single: 0, other: 0 }; cols.forEach((c, i) => { o[CAT_OF[c] || 'other'] += r[i + 1] || 0; }); o.t = cats.reduce((s, k) => s + o[k], 0); return o; });
  const W = 1000, H = 190, L = 44, B = 20, top = Math.max.apply(null, data.map(o => o.t).concat([1])), bw = (W - L - 6) / data.length;
  const ticks = [0, .5, 1].map(f => Math.round(top * f / 100) * 100);
  let g = ticks.map(t => { const y = H - B - (t / top) * (H - B - 8); return `<line x1="${L}" x2="${W}" y1="${y.toFixed(1)}" y2="${y.toFixed(1)}" stroke="${PAL.line}" stroke-width="1"/><text x="${L - 6}" y="${(y + 3).toFixed(1)}" text-anchor="end" class="cv-axis">${num(t, 0)}</text>`; }).join('');
  data.forEach((o, i) => {
    let y = H - B; const x = L + i * bw;
    cats.forEach(k => { if (!o[k]) return; const h = (o[k] / top) * (H - B - 8); y -= h; g += `<rect x="${(x + 1).toFixed(1)}" y="${y.toFixed(1)}" width="${Math.max(1, bw - 2).toFixed(1)}" height="${Math.max(0, h - 1).toFixed(1)}" fill="${CAT[k]}"><title>${esc(o.d)}: ${esc(CAT_WORDS[k])} ${num(o[k], 0)} of ${num(o.t, 0)} new trades</title></rect>`; });
    if (i === 0 || i === data.length - 1 || (i % Math.ceil(data.length / 6) === 0 && i < data.length - 4)) g += `<text x="${(x + bw / 2).toFixed(1)}" y="${H - 5}" text-anchor="${i === 0 ? 'start' : i === data.length - 1 ? 'end' : 'middle'}" class="cv-axis">${esc(o.d.slice(5))}</text>`;
  });
  return `<h4>New trades a day, by product <span class="cv-dim">${rows.length} file days · counts as disseminated</span></h4><svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="new credit-derivative trades a day by product">${g}</svg>
    <div class="cv-legend">${cats.map(k => `<span><i class="cv-swatch" style="background:${CAT[k]}"></i>${esc(CAT_WORDS[k])}</span>`).join('')}</div>`;
}
function productsHtml(d, st) {
  const pr = d.products;
  if (!pr || !pr.available) return `<div class="panel cv-panel"><div class="cv-head"><h3 id="cds2-products-h">Every product of the credit file</h3></div><div class="cv-dim">${esc((pr && pr.why) || 'the product census is published by engine 2.0; this document is older')}</div></div>`;
  const w = pr.windows[st.prod] || pr.windows['5d'], top = Math.max.apply(null, w.classes.map(c => c.n).concat([1]));
  const bars = w.classes.map(c => `<div class="cv-prod-row"><span class="cv-prod-name"><i class="cv-swatch" style="background:${CAT[CAT_OF[c.cls] || 'other']}"></i>${esc(c.words)}</span>
      <span class="cv-bar-track"><i style="width:${Math.max(0.6, c.n / top * 100).toFixed(1)}%;background:${CAT[CAT_OF[c.cls] || 'other']}"></i></span>
      <span class="cv-prod-n"><b>${num(c.n, 0)}</b> <span class="cv-dim">${c.share_pct}%</span></span><span class="cv-prod-usd">${money(c.usd_mm)} <span class="cv-dim">${euro(c.eur_mm)}${c.other_ccy_n ? ` · ${c.other_ccy_n} other-currency prints` : ''}</span></span>
      <span class="cv-prod-how cv-dim">${esc(c.measured)}</span></div>`).join('');
  const fams = `<details class="cv-details"><summary>Every index family, option underlier, tranche and other product in the window, as the file names it (${w.families.length})</summary><div class="cv-scroll"><table class="cv-table sortable-table"><thead><tr><th>Product</th><th>Family</th><th class="r">Prints</th><th class="r">Dollars</th><th class="r">Euros</th><th>What the desk gives</th></tr></thead><tbody>
    ${w.families.map(f => `<tr><td>${esc(CAT_WORDS[CAT_OF[f.cls]] || f.cls)}</td><td><b>${esc(f.label)}</b>${f.family !== f.label ? ` <span class="cv-dim">${esc(f.family)}</span>` : ''}</td><td class="r" data-sort="${f.n}">${num(f.n, 0)}</td><td class="r" data-sort="${f.usd_mm}">${money(f.usd_mm)}</td><td class="r" data-sort="${f.eur_mm}">${euro(f.eur_mm) || '—'}</td><td class="cv-dim">${esc(f.measured)}</td></tr>`).join('')}</tbody></table></div></details>`;
  return `<div class="panel cv-panel" aria-labelledby="cds2-products-h"><div class="cv-head"><h3 id="cds2-products-h">Every product of the credit file</h3><span class="cv-dim">${num(w.n, 0)} new trades over ${w.days} file day${w.days === 1 ? '' : 's'} to ${esc(w.last || '')} · counted since ${esc(pr.first_day || '')}</span></div>
    <div class="cv-ctl"><span class="cv-lab">Window</span>${seg('prod', [['1d', 'Last day'], ['5d', '5 days'], ['30d', '30 days']], st.prod)}</div>
    <div class="cv-prod">${bars}</div>${fams}${dailyStack(pr)}<div class="cv-note">${esc(pr.note || '')}</div></div>`;
}

/* ------------------------------------------------------------------------------------------------ 3. options */
function ladderSvg(f) {
  const rows = f.ladder || [];
  if (!rows.length) return '<div class="cv-dim">no strike on file</div>';
  const W = 480, H = 170, L = 8, B = 30, top = Math.max.apply(null, rows.map(r => r.notional_mm).concat([1])), bw = (W - L * 2) / rows.length, lvl = f.index_level, spread = f.strike_kind === 'spread';
  let g = '', marker = '';
  rows.forEach((r, i) => {
    const h = (r.notional_mm / top) * (H - B - 14), x = L + i * bw, wider = fin(lvl) && (spread ? r.strike > lvl : r.strike < lvl);
    g += `<rect x="${(x + 1).toFixed(1)}" y="${(H - B - h).toFixed(1)}" width="${Math.max(1, bw - 2).toFixed(1)}" height="${Math.max(1, h).toFixed(1)}" rx="2" fill="${!fin(lvl) ? PAL.mid : wider ? PAL.bad : PAL.good}"><title>strike ${num(r.strike, spread ? 1 : 2)}${spread ? ' bp' : ''}: ${money(r.notional_mm)} over ${r.n} print${r.n === 1 ? '' : 's'}${fin(lvl) ? (wider ? ' · on the weaker-credit side of today\'s level' : ' · on the stronger-credit side') : ''}</title></rect>`;
    if (rows.length <= 16 || i % Math.ceil(rows.length / 12) === 0) g += `<text x="${(x + bw / 2).toFixed(1)}" y="${H - B + 12}" text-anchor="middle" class="cv-axis">${num(r.strike, r.strike % 1 ? 1 : 0)}</text>`;
  });
  if (fin(lvl)) {
    let pos = rows.findIndex(r => r.strike >= lvl); if (pos < 0) pos = rows.length;
    const lo = rows[pos - 1], hi = rows[pos], frac = lo && hi && hi.strike > lo.strike ? (lvl - lo.strike) / (hi.strike - lo.strike) : 0.5, x = L + (pos - 1 + 0.5 + frac) * bw;
    const xx = clamp(x, L, W - L);
    marker = `<line x1="${xx.toFixed(1)}" x2="${xx.toFixed(1)}" y1="6" y2="${H - B}" stroke="${PAL.ink}" stroke-width="1.5" stroke-dasharray="4 3"/><text x="${(xx + (xx > W * 0.7 ? -5 : 5)).toFixed(1)}" y="13" text-anchor="${xx > W * 0.7 ? 'end' : 'start'}" class="cv-axis" style="fill:${PAL.ink}">index ${num(lvl, spread ? 1 : 2)}</text>`;
  }
  return `<svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="${esc(f.label)} options: notional by strike">${g}${marker}<text x="${W / 2}" y="${H - 3}" text-anchor="middle" class="cv-axis">strike, ${spread ? 'spread in bp' : 'price'}</text></svg>`;
}
const dayMonth = iso => { const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso || ''); return m ? `${+m[3]} ${MONTHS[+m[2] - 1]} ${m[1].slice(2)}` : String(iso || ''); };
function expiriesHtml(f) {
  const ex = (f.expiries || []).filter(e => e.expiry), top = Math.max.apply(null, ex.map(e => e.notional_mm).concat([1])), cur = f.ccy === 'EUR' ? '€' : '$';
  const amt = mm => cur + (mm >= 1000 ? num(mm / 1000, 1) + 'B' : num(mm, 0) + 'M');
  const off = f.off_ladder || [], ng = f.expiry_not_given;
  return `<div class="cv-exp" role="img" aria-label="${esc(f.label)} options: notional by expiry">${ex.map(e => `<div class="cv-exp-row${e.expired ? ' past' : ''}" title="${esc(e.expiry)}: ${amt(e.notional_mm)} over ${e.n} prints${e.expired ? ' · this expiry has passed; the prints traded inside the window' : ''}"><span>${esc(dayMonth(e.expiry))}${e.expired ? ' · passed' : ''}</span><span class="cv-exp-bar"><i style="width:${(100 * e.notional_mm / top).toFixed(1)}%"></i></span><b>${amt(e.notional_mm)}</b><em>${num(e.n, 0)}</em></div>`).join('')}</div>
    ${ng && ng.n ? `<div class="cv-dim cv-exp-note">Expiry not given on ${num(ng.n, 0)} prints (${amt(ng.notional_mm)}): the file carried the index's own maturity there.</div>` : ''}
    ${off.length ? `<div class="cv-dim cv-exp-note">Strikes off this ladder, listed not drawn: ${off.map(x => `${num(x.strike, x.strike % 1 ? 1 : 0)}${x.kind ? ' (' + esc(x.kind) + ')' : ''} ×${x.n}`).join(' · ')}</div>` : ''}`;
}
function optionsHtml(d) {
  const o = d.options;
  if (!o || !o.available || !(o.families || []).length) return '';
  return `<div class="panel cv-panel" aria-labelledby="cds2-options-h"><div class="cv-head"><h3 id="cds2-options-h">Options on the indices</h3><span class="cv-dim">${num(o.n, 0)} prints · ${money(o.notional_mm)} · ${o.days} file days to ${esc(o.last || '')}</span></div>
    <div class="cv-opt">${o.families.map(f => `<div class="cv-opt-card"><div class="cv-card-top"><b>${esc(f.label)}</b><span class="cv-dim">${num(f.n, 0)} prints · ${f.ccy === 'EUR' ? '€' + num(f.notional_mm / 1000, 1) + 'B' : money(f.notional_mm)}${f.capped_n ? ` · ${f.capped_n} capped blocks` : ''}</span></div>
      ${fin(f.share_struck_wider_pct) ? `<div class="cv-opt-share"><span class="cv-stack"><i style="width:${(100 - f.share_struck_wider_pct).toFixed(1)}%;background:${PAL.good}"></i><i style="width:${f.share_struck_wider_pct.toFixed(1)}%;background:${PAL.bad}"></i></span><span><b style="color:${PAL.badText}">${num(f.share_struck_wider_pct, 0)}%</b> of the notional is struck on the weaker-credit side of today's level</span></div>` : ''}
      ${ladderSvg(f)}
      ${expiriesHtml(f)}</div>`).join('')}</div>
    ${fin(o.n_other_underlier) && o.n_other_underlier > 0 ? `<div class="cv-dim" style="font-size:11px;margin-top:8px">${num(o.n_other_underlier, 0)} more option prints in the window are on an underlier the file does not let the desk name (another currency or coupon): they are counted in the products table above, not drawn here.</div>` : ''}
    <div class="cv-legend"><span><i class="cv-swatch" style="background:${PAL.bad}"></i>strike on the weaker-credit side of today's index level</span><span><i class="cv-swatch" style="background:${PAL.good}"></i>on the stronger-credit side</span></div>
    <div class="cv-note">${esc(o.note || '')}</div></div>`;
}

/* ------------------------------------------------------------------------------------------------ 4. mortgage indices */
function structuredHtml(d) {
  const s = d.structured;
  if (!s || !(s.rows || []).length) return '';
  const tiers = [...new Set(s.rows.map(r => r.label))], finals = [...new Set(s.rows.map(r => r.final))].sort(), top = Math.max.apply(null, s.rows.map(r => r.points).concat([1]));
  const cell = (t, f) => { const r = s.rows.find(x => x.label === t && x.final === f); if (!r) return '<td class="cv-dim c">·</td>'; return `<td class="c" style="background:${mix(PAL.mid, PAL.bad, 0.1 + 0.9 * clamp(r.points / top, 0, 1))}" title="${esc(r.label)} to ${esc(r.final)}: ${r.points} points upfront on ${esc(r.last_date)} (coupon ${r.coupon_bp} bp), price ${r.price_if_paid_for_protection} if the side taking protection paid; ${r.n_30d} prints in 30 days${fin(r.chg_1m_points) ? `, ${sgn(r.chg_1m_points, 2)} points in a month` : ''}"><b>${num(r.points, 1)}</b><span class="cv-cell-sub">${r.n_30d}p</span></td>`; };
  return `<div class="panel cv-panel" aria-labelledby="cds2-structured-h"><div class="cv-head"><h3 id="cds2-structured-h">Commercial-mortgage indices (CMBX)</h3><span class="cv-dim">${s.n_series} series traded in 30 days · points upfront · darker = more protection paid for</span></div>
    <div class="cv-scroll"><table class="cv-table cv-grid" data-jh-nosort><tbody><tr><th>Rating tier</th>${finals.map(f => `<th class="c" title="the series whose contracts run to ${esc(f)}">to ${esc(monthYear(f))}</th>`).join('')}</tr>${tiers.map(t => `<tr><th>${esc(t)}</th>${finals.map(f => cell(t, f)).join('')}</tr>`).join('')}</tbody></table></div>
    <details class="cv-details"><summary>Every series as figures (${s.rows.length})</summary><div class="cv-scroll"><table class="cv-table sortable-table"><thead><tr><th>Tier</th><th>Runs to</th><th class="r">Coupon</th><th class="r">Points upfront</th><th class="r">Price if paid for protection</th><th class="r">1M, points</th><th class="r">Prints 30d</th><th class="r">Dollars 30d</th><th>Last print</th></tr></thead><tbody>
    ${s.rows.map(r => `<tr><td>${esc(r.label)}</td><td>${esc(r.final)}</td><td class="r">${num(r.coupon_bp, 0)}</td><td class="r" data-sort="${r.points}"><b>${num(r.points, 2)}</b></td><td class="r">${num(r.price_if_paid_for_protection, 2)}</td><td class="r" style="color:${tone(r.chg_1m_points)}">${sgn(r.chg_1m_points, 2)}</td><td class="r">${r.n_30d}</td><td class="r">${money(r.usd_mm_30d)}</td><td class="cv-dim">${esc(r.last_date)}</td></tr>`).join('')}</tbody></table></div></details>
    <div class="cv-note">${esc(s.note || '')}</div></div>`;
}

/* ------------------------------------------------------------------------------------------------ 5. quiet names, 6. quality */
function quietRows(d) { const out = []; Object.entries(d.groups || {}).forEach(([g, blk]) => (blk.quiet || []).forEach(q => out.push(Object.assign({ group: g }, q)))); return out; }
function quietHtml(d, st) {
  const rows = quietRows(d);
  if (!rows.length) return '';
  const older = Object.values(d.groups || {}).reduce((s, b) => s + (b.n_older || 0), 0), show = st.quiet ? rows : rows.slice(0, 12);
  return `<div class="panel cv-panel" aria-labelledby="cds2-quiet-h"><div class="cv-head"><h3 id="cds2-quiet-h">Quiet names</h3><span class="cv-dim">${num(rows.length, 0)} names printed in the last year but too rarely for a level · ${num(older, 0)} more have not printed for over a year</span>
      <button type="button" class="cv-btn" data-cv="quiet" data-v="${st.quiet ? '' : '1'}">${st.quiet ? 'Show the first 12' : 'Show all ' + num(rows.length, 0)}</button></div>
    <div class="cv-scroll"><table class="cv-table sortable-table"><thead><tr><th>Name</th><th>Group</th><th>Sector / region</th><th>Stock</th><th>Last print</th><th class="r">Prints in a year</th><th class="r">Days it printed</th></tr></thead><tbody>
    ${show.map(r => `<tr><td>${nameBtn(r)}</td><td class="cv-dim">${esc(GROUPS[r.group] || r.group)}</td><td class="cv-dim">${esc(r.sector || r.region || '—')}</td><td class="cv-dim">${esc(r.ticker || (r.listing && r.listing.symbol) || '—')}</td><td data-sort="${esc(r.last_date || '')}">${esc(r.last_date || '—')}</td><td class="r">${num(r.prints_1y, 0)}</td><td class="r">${num(r.days_1y, 0)}</td></tr>`).join('')}</tbody></table></div></div>`;
}
function qualityHtml(d) {
  const q = d.quality, b = d.beside;
  if (!q) return '';
  const how = q.coupon_written_as || {}, total = Object.values(how).reduce((s, x) => s + x, 0) || 1, m = d.method || {};
  const rule = (k, label) => m[k] ? `<li><b>${label}.</b> ${esc(m[k])}</li>` : '';
  return `<div class="panel cv-panel" aria-labelledby="cds2-quality-h"><div class="cv-head"><h3 id="cds2-quality-h">What the desk repaired, and what it holds back</h3><span class="cv-dim">last ${q.window_days} days · engine ${esc(d.version || '')}</span></div>
    <div class="cv-kpis">
      <div class="cv-kpi"><span>Coupons written in percent</span><b>${num(how.percent || 0, 0)}</b><small>of ${num(total, 0)} single-name prints; read as 100 or 500 bp, not 1 or 5${how.bespoke ? ` · ${how.bespoke} bespoke` : ''}${how.unreadable ? ` · ${how.unreadable} unreadable` : ''}</small></div>
      <div class="cv-kpi"><span>Unconfirmed prints now</span><b>${(q.unconfirmed_now || []).length}</b><small>a print far from the name's own level that nothing has repeated: shown, never the level</small></div>
      <div class="cv-kpi"><span>Levels at the coupon</span><b>${num(q.n_bracketed, 0)}</b><small>the two feasible levels are within a fifth of each other: the middle is shown with both</small></div>
      <div class="cv-kpi"><span>Aged levels</span><b>${num(q.n_aged, 0)}</b><small>11 to 45 days old, shown with their date, never counted as liquid</small></div>
      <div class="cv-kpi"><span>Quiet names listed</span><b>${num(q.n_quiet, 0)}</b><small>${num(q.n_older, 0)} more have not printed for over a year</small></div></div>
    ${(q.unconfirmed_now || []).length ? `<div class="cv-scroll"><table class="cv-table" data-jh-nosort><thead><tr><th>Name</th><th class="r">Level shown</th><th>Its date</th><th class="r">The unrepeated print</th><th>Its date</th><th>How it was read</th></tr></thead><tbody>${q.unconfirmed_now.map(u => `<tr><td>${nameBtn(u)}</td><td class="r"><b>${num(u.level_bp, 1)}</b></td><td class="cv-dim">${esc(u.level_date)}</td><td class="r" style="color:${PAL.badText}">${num(u.spread_bp, 1)}</td><td class="cv-dim">${esc(u.date)}</td><td class="cv-dim">${esc(u.basis || '')}${fin(u.n_priced) ? ` · ${u.n_priced} print${u.n_priced === 1 ? '' : 's'}` : ''}</td></tr>`).join('')}</tbody></table></div>` : ''}
    ${b ? `<h4>What stands beside a name, and where it comes from</h4><ul class="cv-list">${Object.entries(b.status || {}).map(([k, v]) => `<li><b>${esc({ ties: 'Listed company and its own bonds', reference: 'Sovereign reference quote', listings: 'Home listing, worldwide' }[k] || k)}.</b> ${esc(v)}</li>`).join('')}<li class="cv-dim">${esc(b.note || '')}</li></ul>` : ''}
    <h4>The rules added in 2.0</h4><ul class="cv-list">${rule('coupon_rule', 'Coupon')}${rule('confirm_rule', 'A jump')}${rule('bracket_rule', 'At the coupon')}${rule('aged_rule', 'An aged level')}${rule('bond_anchor_rule', 'Evidence from outside the tape')}${rule('subordinated_rule', 'Subordinated names')}${rule('ceiling_rule', 'Distressed names')}${rule('quote_rule', 'A quoted spread')}${rule('netted_rule', 'Points filed as a spread')}${rule('spike_rule', 'A spike that came back')}${rule('uniform_rule', 'A portfolio at one flat price')}${rule('listing_rule', 'Listed stock')}${rule('product_rule', 'Products')}</ul></div>`;
}

/* ------------------------------------------------------------------------------------------------ 7. the whole document */
const DOC_WORDS = { engine: 'The engine that wrote this', version: 'Its version', generated_at: 'When it was written', as_of: 'The newest execution day in it', source: 'The public files it reads', method: 'Every rule, in words', decision: 'Why it makes no call',
  breadth: 'How many liquid names moved wider or tighter', term: 'Names whose 1-year level is above their 5-year', wides_1y: 'Liquid names at their widest in a year', tights_1y: 'Liquid names at their tightest in a year', activity: 'Single-name prints a day, 120 days',
  groups: 'Every name: sovereign, U.S., global and subordinated, with or without a level', indices: 'Every index family', products: 'Every product of the file, counted', structured: 'Commercial-mortgage indices by series', options: 'Index options by strike and expiry',
  quality: 'What was repaired or held back', run: 'The run that wrote it', fundamentals: 'IMF figures beside the sovereigns', layers: 'The risk layers and their status', bank_sovereign: 'Euro-area banks\' holdings of each government', hedge_funds: 'Hedge-fund books (OFR)',
  us_credit: 'U.S. credit functioning and bank fragility', ecb_stress: 'ECB stress records', cross_reference: 'Cross-checks with sibling engines', history: 'The history document and the long records', beside: 'What stands beside a name, and its sources', tape: 'The document of prints behind the levels' };
const typeOf = v => v === null ? 'empty' : Array.isArray(v) ? `${v.length} row${v.length === 1 ? '' : 's'}` : typeof v === 'object' ? `${Object.keys(v).length} key${Object.keys(v).length === 1 ? '' : 's'}` : typeof v;
const scalar = v => v === null || v === undefined ? '<span class="cv-dim">empty</span>' : typeof v === 'boolean' ? (v ? 'yes' : 'no') : typeof v === 'number' ? esc(String(v)) : esc(String(v));
function atPath(doc, path) { let v = doc; for (const k of path) { if (v == null) return undefined; v = v[k]; } return v; }
function nodeHtml(v, path, limit) {
  if (v === null || typeof v !== 'object') return `<span class="cv-val">${scalar(v)}</span>`;
  const entries = Array.isArray(v) ? v.map((x, i) => [i, x]) : Object.entries(v), cap = limit || 60, shown = entries.slice(0, cap);
  if (Array.isArray(v) && v.length && v.every(x => x === null || typeof x !== 'object')) return `<span class="cv-val">${shown.map(([, x]) => scalar(x)).join(', ')}${entries.length > cap ? ` <button type="button" class="cv-btn" data-cv="more" data-v="${esc(JSON.stringify(path))}">… ${entries.length - cap} more</button>` : ''}</span>`;
  return `<div class="cv-tree">${shown.map(([k, x]) => {
    const p = path.concat([k]), complex = x !== null && typeof x === 'object';
    return complex ? `<details class="cv-node" data-path="${esc(JSON.stringify(p))}"><summary><b>${esc(String(k))}</b> <span class="cv-dim">${esc(typeOf(x))}${x && !Array.isArray(x) && x.name ? ' · ' + esc(String(x.name)) : ''}</span></summary><div class="cv-node-body"></div></details>`
      : `<div class="cv-leaf"><span class="cv-key">${esc(String(k))}</span><span class="cv-val">${scalar(x)}</span></div>`;
  }).join('')}${entries.length > cap ? `<button type="button" class="cv-btn" data-cv="more" data-v="${esc(JSON.stringify(path))}">Show all ${entries.length}</button>` : ''}</div>`;
}
function dataHtml(d) {
  const keys = Object.keys(d || {});
  return `<div class="panel cv-panel" aria-labelledby="cds2-data-h"><div class="cv-head"><h3 id="cds2-data-h">Everything the engine published</h3><span class="cv-dim">${keys.length} keys of data/cds-desk.json · open any of them · nothing here is left out</span>
      <button type="button" class="cv-btn" data-cv="csv" data-v="names">Download every name as a file</button></div>
    <div class="cv-tree">${keys.map(k => { const x = d[k], complex = x !== null && typeof x === 'object'; return complex ? `<details class="cv-node" data-path="${esc(JSON.stringify([k]))}"><summary><b>${esc(k)}</b> <span class="cv-dim">— ${esc(DOC_WORDS[k] || 'published by the engine')} · ${esc(typeOf(x))}</span></summary><div class="cv-node-body"></div></details>` : `<div class="cv-leaf"><span class="cv-key">${esc(k)}</span><span class="cv-val">${scalar(x)}</span><span class="cv-dim">${esc(DOC_WORDS[k] || '')}</span></div>`; }).join('')}</div>
    <div class="cv-note">Two more documents are written beside this one: <b>data/cds-desk-history.json</b> (every name's level on every priced day, drawn in a name's window) and <b>data/cds-desk-tape.json</b> (the prints behind each level for the last 30 days, listed in a name's window).</div></div>`;
}
function csvOf(d) {
  const rows = []; Object.entries(d.groups || {}).forEach(([g, blk]) => ['rows', 'unpriced', 'dormant', 'quiet'].forEach(kind => (blk[kind] || []).forEach(r => rows.push(Object.assign({ group: g, list: kind }, r)))));
  (((d.groups || {}).sovereign || {}).universe || []).filter(u => u.status === 'not_in_tape').forEach(u => rows.push(Object.assign({ group: 'sovereign', list: 'not_in_tape' }, u)));   // a known issuer with no print is still a name the desk knows
  const cols = []; rows.forEach(r => Object.keys(r).forEach(k => { if (cols.indexOf(k) < 0 && (r[k] === null || typeof r[k] !== 'object')) cols.push(k); }));
  cols.push('listing_symbol', 'listing_exchange', 'listing_country');
  const cell = v => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
  return [cols.join(',')].concat(rows.map(r => cols.map(c => cell(c === 'listing_symbol' ? (r.listing || {}).symbol : c === 'listing_exchange' ? (r.listing || {}).exchange : c === 'listing_country' ? (r.listing || {}).country : r[c])).join(','))).join('\n');
}

/* ------------------------------------------------------------------------------------------------ 8. a name's sheet */
const FIELDS = { key: ['Key', 'the name as the desk files it'], name: ['Name', ''], group: ['Group', 'sovereign, U.S. corporate, global corporate or subordinated'], status: ['Status', 'liquid, thin, at coupon, aged, either / or, dormant or quiet'],
  sector: ['Sector', ''], region: ['Region', ''], tier: ['Tier', 'developed, emerging or frontier market'], iso3: ['Country code', ''], ccy: ['Currency', 'the currency most of its prints are in'],
  spread_bp: ['5Y level, bp', 'the middle of the day\'s priced 5-year prints'], spread_basis: ['How the level was read', 'quoted, derived from the upfront, mixed, or bracketed by two close branches'], bracket_bp: ['The two branches', 'the two levels the unsigned upfront could mean'],
  range_bp: ['Day\'s range, bp', 'lowest and highest priced print of the day'], last_date: ['Date of the level', ''], stale_days: ['Days old', ''], n_last: ['Prints that day', ''], n_priced_last: ['Priced prints that day', ''],
  quoted_last: ['Quoted prints that day', 'prints that carry a spread, not only an upfront'], ambiguous_last: ['Unresolved prints that day', 'prints whose upfront sign nothing fixed'], capped_share: ['Capped blocks, share', 'prints whose size the public file caps'],
  cleared_share: ['Cleared, share', ''], notional_mm_last: ['5Y notional that day, $M', 'capped blocks counted at the cap'], anchor_source: ['What signed the upfront', 'the evidence that chose the side of the coupon'], sign_evidence: ['How firm', 'firm, inferred or bracketed'],
  points_upfront: ['Points upfront', 'for a distressed name quoted in points'], quote_style: ['Quote style', ''], at_ceiling: ['Past the ceiling', 'its running equivalent is beyond 10,000 bp: carried at 10,000 and read in points'], chg_1d_bp: ['1 day, bp', ''], chg_1w_bp: ['1 week, bp', ''], chg_1m_bp: ['1 month, bp', ''], chg_3m_bp: ['3 months, bp', ''],
  z_90d: ['Against its 90 days', 'standard deviations from its own 90-day mean'], pct_rank_1y: ['1-year percentile', 'share of its own priced days at or below today'], hi_1y_bp: ['1-year high, bp', ''], lo_1y_bp: ['1-year low, bp', ''],
  n_days_priced_1y: ['Priced days in a year', ''], trades_30d: ['Prints in 30 days', ''], days_active_30d: ['Days active in 30', ''], liquid: ['Liquid', '10 prints on 4 days in 30'], slope_1y5y_bp: ['5Y less 1Y, bp', 'negative is an inverted curve'],
  n_unconfirmed_1y: ['Unrepeated prints in a year', 'days set aside because nothing confirmed them'], ticker: ['Listed stock (U.S.)', 'the credit board\'s tie'], company: ['Company', ''], bond_bp: ['Its bonds, bp', 'the company\'s bond spread over governments, from the credit board'],
  bond_book: ['Bond book', 'investment grade, high yield, emerging or a non-dollar book'], bond_as_of: ['Bonds as of', ''], basis_bp: ['CDS less bonds, bp', ''], stock_1m_pct: ['Stock, 1 month %', ''], read: ['In words', ''], why: ['Why no level', ''],
  last_spread_bp: ['Last level, bp', ''], last_priced_date: ['Date of the last level', ''], last_points_upfront: ['Last points upfront', ''], ambiguous_30d: ['Unresolved prints in 30 days', ''], capped_share_30d: ['Capped share, 30 days', ''],
  prints_1y: ['Prints in a year', ''], days_1y: ['Days it printed in a year', ''], coupon_bp: ['Coupon, bp', ''], price: ['Price', ''], expiry: ['On-the-run expiry', ''], quote: ['Quoted in', ''], since: ['On the desk since', ''] };
function findRow(d, key) {
  if (key.indexOf('IDX:') === 0) { const i = (d.indices || []).find(x => 'IDX:' + x.key === key); return i ? Object.assign({ group: 'index' }, i) : null; }
  for (const [g, blk] of Object.entries(d.groups || {})) for (const kind of ['rows', 'unpriced', 'dormant', 'quiet']) { const r = (blk[kind] || []).find(x => x.key === key); if (r) return Object.assign({ group: g, list: kind }, r); }
  const u = (((d.groups || {}).sovereign || {}).universe || []).find(x => x.key === key || 'NA:' + x.iso3 === key);
  return u ? Object.assign({ group: 'sovereign', list: 'universe' }, u) : null;
}
function curveSvg(cv) {
  const order = ['1Y', '3Y', '5Y', '7Y', '10Y'], pts = order.filter(k => fin((cv || {})[k])).map(k => [k, cv[k]]);
  if (pts.length < 2) return '';
  const W = 340, H = 140, L = 34, B = 22, T = 22, lo = Math.min.apply(null, pts.map(p => p[1])), hi = Math.max.apply(null, pts.map(p => p[1])), span = hi - lo || 1;      // T: room for the label above the highest point
  const X = k => L + order.indexOf(k) / (order.length - 1) * (W - L - 22), Y = v => H - B - 4 - (v - lo) / span * (H - B - T - 4);
  return `<div><h4>Its curve on the day of the level <span class="cv-dim">bp by maturity, from prints whose sign their own data fixes</span></h4><svg viewBox="0 0 ${W} ${H}" width="${W}" role="img" aria-label="term structure">
    <line x1="${L}" x2="${W - 8}" y1="${H - B}" y2="${H - B}" stroke="${PAL.line}"/>${order.map(k => `<text x="${X(k).toFixed(1)}" y="${H - 6}" text-anchor="middle" class="cv-axis">${k}</text>`).join('')}
    <path d="${pts.map((p, i) => (i ? 'L' : 'M') + X(p[0]).toFixed(1) + ',' + Y(p[1]).toFixed(1)).join(' ')}" fill="none" stroke="${CAT.index}" stroke-width="2"/>
    ${pts.map(p => `<circle cx="${X(p[0]).toFixed(1)}" cy="${Y(p[1]).toFixed(1)}" r="4" fill="${CAT.index}" stroke="${PAL.panel}" stroke-width="2"><title>${p[0]}: ${num(p[1], 1)} bp</title></circle><text x="${X(p[0]).toFixed(1)}" y="${(Y(p[1]) - 8).toFixed(1)}" text-anchor="middle" class="cv-axis" style="fill:${PAL.ink}">${num(p[1], 0)}</text>`).join('')}</svg></div>`;
}
function pairBars(r) {
  if (!fin(r.bond_bp) || !fin(r.spread_bp)) return '';
  const top = Math.max(r.bond_bp, r.spread_bp, 1), bar = (label, v, colour) => `<div class="cv-bar-row"><span class="cv-bar-name">${label}</span><span class="cv-bar-track"><i style="width:${(v / top * 100).toFixed(1)}%;background:${colour}"></i></span><span class="cv-bar-val">${num(v, 1)}</span></div>`;
  return `<div><h4>The swap beside the company's own bonds <span class="cv-dim">bp · bonds are the spread over governments, from the credit board (${esc(r.bond_as_of || '')})</span></h4>${bar('5Y CDS', r.spread_bp, CAT.index)}${bar('Its bonds', r.bond_bp, CAT.option)}
    <div class="cv-dim" style="font-size:11px">CDS less bonds: <b style="color:${PAL.ink}">${sgn(r.basis_bp, 1)} bp</b>. A swap usually trades inside the bond spread over governments; the gap is the financing and delivery terms, not a signal by itself.</div></div>`;
}
function figuresHtml(r) {
  const skip = { spark: 1, curve_last: 1, sign_sources_20d: 1, candidates: 1, unconfirmed: 1, listing: 1, fund: 1, rating: 1, ara: 1, pdebt: 1, banks: 1, eba: 1, stress: 1, list: 1 };
  const LONG = { read: 1, why: 1 };           // sentences get the full width and a reading weight, after the figures
  const keys = Object.keys(r).filter(k => !skip[k]).sort((a, b) => (LONG[a] || 0) - (LONG[b] || 0));
  const val = (k, v) => v === null || v === undefined || v === '' ? '<span class="cv-dim">—</span>' : Array.isArray(v) ? esc(v.map(x => x == null ? '—' : x).join(' – ')) : typeof v === 'boolean' ? (v ? 'yes' : 'no') : typeof v === 'object' ? esc(JSON.stringify(v)) : esc(String(v));
  return `<div class="cv-figs">${keys.map(k => `<div class="cv-fig${LONG[k] ? ' cv-fig-long' : ''}" title="${esc((FIELDS[k] || [])[1] || '')}"><span>${esc((FIELDS[k] || [k])[0])}</span><b>${val(k, r[k])}</b>${(FIELDS[k] || [])[1] ? `<small>${esc(FIELDS[k][1])}</small>` : ''}</div>`).join('')}</div>`;
}
const QUOTE_AS = { points: ['points', 'the file put points upfront in the spread field: the quote less the cash is exactly the accrued coupon'], bp_under_decimal: ['unit repaired', 'written in basis points under the decimal notation; believed only next to the name\'s own level'],
  percent_under_bp: ['unit repaired', 'written in percent under the basis-point notation; believed only next to the name\'s own level'] };
function tapeHtml(tape, key) {
  if (!tape) return '<div class="cv-dim">loading the prints…</div>';
  const rows = (tape.names || {})[key], c = tape.cols || [], ix = k => c.indexOf(k);
  if (!rows || !rows.length) return `<div class="cv-dim">no single-name print between ${esc(tape.first || '')} and ${esc(tape.as_of || '')}, the days the tape document keeps</div>`;
  const g = (r, k) => r[ix(k)];
  return `<div class="cv-scroll" style="max-height:340px"><table class="cv-table sortable-table"><thead><tr><th>Day</th><th class="r">Years</th><th>Currency</th><th class="r">Size, M</th><th class="r">Coupon, bp</th><th class="r">Upfront, % of size</th><th class="r">Quoted, bp</th><th class="r">If paid below the coupon</th><th class="r">If above</th><th>Cleared</th><th>Venue</th><th>File</th></tr></thead><tbody>
    ${rows.slice().reverse().map(r => `<tr><td>${esc(g(r, 'date'))}</td><td class="r">${num(g(r, 'tenor'), 2)}</td><td>${esc(g(r, 'ccy') || '')}</td><td class="r">${num(g(r, 'notional_mm'), 1)}${g(r, 'capped') ? '+' : ''}</td><td class="r">${num(g(r, 'coupon_bp'), 0)}${g(r, 'coupon_how') ? ` <span class="cv-dim" title="the file wrote the coupon as ${esc(g(r, 'coupon_how'))}">(${esc(g(r, 'coupon_how'))})</span>` : ''}</td><td class="r">${num(g(r, 'upfront_pct'), 3)}</td><td class="r" data-sort="${fin(g(r, 'quoted_bp')) ? g(r, 'quoted_bp') : ''}">${fin(g(r, 'quoted_bp')) ? '<b>' + num(g(r, 'quoted_bp'), 1) + '</b>' + (QUOTE_AS[g(r, 'quote_as')] ? ` <span class="cv-dim" title="${esc(QUOTE_AS[g(r, 'quote_as')][1])}">${esc(QUOTE_AS[g(r, 'quote_as')][0])}</span>` : '') : '—'}</td><td class="r">${num(g(r, 'below_bp'), 1)}</td><td class="r">${num(g(r, 'above_bp'), 1)}</td><td class="cv-dim">${g(r, 'cleared') ? 'yes' : 'no'}</td><td class="cv-dim">${esc(g(r, 'platform') || '—')}${g(r, 'package') ? ' · package' : ''}</td><td class="cv-dim">${esc(g(r, 'source') || '')}</td></tr>`).join('')}</tbody></table></div>
    <div class="cv-dim" style="font-size:10.5px;margin-top:4px">${rows.length} print${rows.length === 1 ? '' : 's'}, newest first${(tape.cut || {})[key] ? ` (the newest ${rows.length} of the ${num(tape.cut[key], 0)} it printed in the window)` : ''}. A print with no quoted spread gives two levels, one on each side of the coupon; the page above says which one the evidence chose. A size with + is a block the public file caps.</div>`;
}
function sheetHtml(r, d, tape) {
  if (!r) return '';
  const l = r.listing, isIdx = r.group === 'index', hasLevel = fin(r.spread_bp);
  const chips = [r.status && `<span class="cv-chip">${esc(r.status === 'unpriced' ? 'either / or' : r.status === 'bracketed' ? 'at coupon' : r.status)}</span>`, `<span class="cv-chip">${esc(GROUPS[r.group] || (isIdx ? 'Index' : r.group))}</span>`, (r.sector || r.region) && `<span class="cv-chip">${esc(r.sector || r.region)}</span>`,
    r.ticker && `<span class="cv-chip on">${esc(r.ticker)}${r.company ? ' · ' + esc(r.company) : ''}</span>`, l && `<span class="cv-chip${r.ticker ? '' : ' on'}" title="matched by name against the worldwide company list">${esc(l.symbol)} · ${esc(l.exchange || '')}${l.country ? ' · ' + esc(l.country) : ''}</span>`].filter(Boolean).join('');
  const tiles = hasLevel ? `<div class="cv-kpis">
      <div class="cv-kpi"><span>5Y level</span><b>${fin(r.points_upfront) ? `${num(r.points_upfront, 1)} <small style="display:inline">points upfront</small>` : `${num(r.spread_bp, 1)} <small style="display:inline">bp</small>`}</b><small>${fin(r.points_upfront) ? (r.at_ceiling ? 'past the 10,000 bp ceiling: read the points · ' : `${num(r.spread_bp, 0)} bp running equivalent · `) : ''}${r.bracket_bp ? `between ${num(r.bracket_bp[0], 1)} and ${num(r.bracket_bp[1], 1)}: it trades at its coupon` : r.status === 'aged' ? `${r.stale_days} days old (${esc(r.last_date)})` : esc(r.last_date || '')}</small></div>
      ${r.at_ceiling ? `<div class="cv-kpi"><span>Change</span><b class="cv-dim">—</b><small>at the ceiling a change in basis points says nothing: read the points in the prints below</small></div>`
    : `<div class="cv-kpi"><span>Change</span><b style="color:${tone(r.chg_1m_bp)}">${sgn(r.chg_1m_bp)}</b><small>in a month · 1D ${sgn(r.chg_1d_bp)} · 1W ${sgn(r.chg_1w_bp)} · 3M ${sgn(r.chg_3m_bp)}</small></div>`}
      <div class="cv-kpi"><span>In its own year</span><b>${fin(r.pct_rank_1y) ? Math.round(r.pct_rank_1y) + 'th' : '—'}</b>${meter(r.lo_1y_bp, r.hi_1y_bp, r.spread_bp, r.pct_rank_1y)}</div>
      <div class="cv-kpi"><span>How it trades</span><b>${num(r.trades_30d, 0)}</b><small>prints in 30 days on ${num(r.days_active_30d, 0)} days · sign from ${esc(((V.api || {}).SRC || {})[r.anchor_source] || r.anchor_source || '—')} (${esc(r.sign_evidence || '—')})</small></div>
      ${fin(r.bond_bp) ? `<div class="cv-kpi"><span>Its bonds</span><b>${num(r.bond_bp, 0)} <small style="display:inline">bp</small></b><small>CDS less bonds ${sgn(r.basis_bp, 0)} bp${fin(r.stock_1m_pct) ? ` · stock ${sgn(r.stock_1m_pct, 1)}% in a month` : ''}</small></div>` : ''}</div>` : '';
  const odd = r.unconfirmed ? `<div class="cv-flag">One print on <b>${esc(r.unconfirmed.date)}</b> gives <b>${num(r.unconfirmed.spread_bp, 1)} bp</b>, far from this name's own recent level. Nothing has repeated it, so it is listed here and is not the level. ${fin(r.n_unconfirmed_1y) ? `${r.n_unconfirmed_1y} such print${r.n_unconfirmed_1y === 1 ? '' : 's'} in a year.` : ''}</div>` : '';
  const cands = (r.candidates || []).length ? `<div class="cv-flag">No level: the upfront fits <b>${r.candidates.map(c => `${num(c.below_coupon_bp, 0)} or ${num(c.above_coupon_bp, 0)} bp (coupon ${c.coupon_bp})`).join('; ')}</b>, and nothing on the tape or beside it picks a side.${fin(r.bond_bp) ? ` Its bonds trade at ${num(r.bond_bp, 0)} bp over governments, which is not far enough from one branch to choose.` : ''}</div>` : '';
  const ref = fin(r.ref_bp) ? `<div class="cv-flag ok">Reference quote: about <b>${num(r.ref_bp, 1)} bp</b> (${esc(r.ref_source || '')}${r.ref_date ? ', ' + esc(r.ref_date) : ''}). It is a quote from another desk of this site, not a print of the public tape.</div>` : '';
  return `<div class="cv-sheet" id="cds2-sheet" data-key="${esc(r.key || '')}"><h4 class="cv-sheet-h">Everything the desk has on ${esc(r.name)}</h4><div class="cv-chips">${chips}</div>${tiles}${odd}${cands}${ref}
    <div class="cv-two">${curveSvg(r.curve_last)}${pairBars(r)}</div>
    <h4>Every figure <span class="cv-dim">hover a figure for what it means</span></h4>${figuresHtml(r)}
    ${isIdx ? '' : `<h4>The prints behind it <span class="cv-dim">as disseminated by DTCC, last 30 days · the reference obligation's identifier is not kept</span> <button type="button" class="cv-btn" data-cv="csv" data-v="tape">Download as a file</button></h4><div id="cds2-tape">${tapeHtml(tape, r.key)}</div>`}</div>`;
}

/* ------------------------------------------------------------------------------------------------ mounting */
const CSS = `.cv-fig.cv-fig-long{grid-column:1/-1}.cv-fig.cv-fig-long b{font-family:inherit;font-weight:400;font-size:12.5px;line-height:1.5}.cv-card.cv-aged{opacity:.72;border-style:dashed}.cv-aged-tag{font-size:10.5px;color:#d9a441;margin:2px 0 0}.cv-exp{display:grid;gap:3px;margin-top:8px}.cv-exp-row{display:grid;grid-template-columns:104px 1fr 62px 34px;gap:8px;align-items:center;font-size:10.5px;color:var(--text-muted,#8a8371)}.cv-exp-row b{color:#e8e2d4;font-weight:600;text-align:right;font-variant-numeric:tabular-nums}.cv-exp-row em{font-style:normal;text-align:right;font-variant-numeric:tabular-nums}.cv-exp-bar{height:6px;border-radius:3px;background:rgba(255,255,255,.05);overflow:hidden}.cv-exp-bar i{display:block;height:100%;border-radius:3px;background:#B8801A}.cv-exp-row.past{opacity:.55}.cv-exp-note{font-size:10.5px;margin-top:6px;line-height:1.45}.cv-panel h3{margin:0;font-size:15px}.cv-head{display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 14px;margin-bottom:12px}.cv-dim{color:var(--text-dim,#8a836f);font-weight:400}.cv-panel h4{font-size:12.5px;margin:18px 0 8px;font-weight:600}
.cv-cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(196px,1fr));gap:10px;margin-bottom:14px}.cv-card{border:1px solid var(--border-hi,#2a2618);border-radius:8px;padding:10px 12px;background:var(--panel-2,#12110C);text-decoration:none!important}.cv-card:hover{border-color:${PAL.dim}}
.cv-card-top{display:flex;justify-content:space-between;gap:8px;font-size:12px;align-items:baseline}.cv-card-top .cv-dim{font-size:10px;white-space:nowrap}.cv-card-val{font-family:var(--font-mono);font-size:22px;font-weight:600;margin:4px 0 2px;color:${PAL.ink}}.cv-card-val small{font-size:11px;color:${PAL.dim};font-weight:400}
.cv-card-chg{display:flex;gap:9px;font-family:var(--font-mono);font-size:10.5px;margin-bottom:4px;white-space:nowrap;overflow:hidden}.cv-meter{position:relative;height:6px;border-radius:3px;background:linear-gradient(90deg,${PAL.good},${PAL.mid},${PAL.bad});margin-top:6px}.cv-meter i{position:absolute;top:-3px;width:3px;height:12px;margin-left:-1.5px;background:${PAL.ink};border-radius:1px;box-shadow:0 0 0 2px ${PAL.panel}}
.cv-meter-ends{display:flex;justify-content:space-between;font-family:var(--font-mono);font-size:9.5px;color:${PAL.dim};margin-top:3px}
.cv-ctl{display:flex;flex-wrap:wrap;align-items:center;gap:8px 10px;margin:6px 0 10px}.cv-lab{font-family:var(--font-mono);font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:${PAL.dim}}.cv-seg{display:inline-flex;flex-wrap:wrap;gap:0}
.cv-btn{background:var(--panel-2,#12110C);border:1px solid var(--border-hi,#2a2618);color:var(--text-dim,#8a836f);font-family:var(--font-mono);font-size:11px;padding:5px 10px;cursor:pointer;border-radius:4px}.cv-seg .cv-btn{border-radius:0;margin-left:-1px}.cv-seg .cv-btn:first-child{border-radius:4px 0 0 4px;margin-left:0}.cv-seg .cv-btn:last-child{border-radius:0 4px 4px 0}
.cv-btn:hover{color:${PAL.ink}}.cv-btn.on{background:#F0B429;border-color:#F0B429;color:#12110C;font-weight:600}.cv-btn:focus-visible,.cv-tile:focus-visible{outline:2px solid ${PAL.ink};outline-offset:1px}
.cv-heat{display:block;border-radius:6px}.cv-tile{cursor:pointer}.cv-tile rect{stroke:${PAL.panel};stroke-width:2}.cv-tile.on rect{stroke:${PAL.ink}}.cv-tile-name{font:600 12px var(--font-ui);fill:#fff}.cv-tile-val{font:600 14px var(--font-mono);fill:#fff}.cv-tile-sub{font:400 10.5px var(--font-mono);fill:rgba(255,255,255,.78)}
.cv-legend{display:flex;flex-wrap:wrap;align-items:center;gap:8px 14px;font-family:var(--font-mono);font-size:10.5px;color:${PAL.dim};margin-top:8px}.cv-ramp{display:inline-block;width:150px;height:8px;border-radius:4px}.cv-swatch{display:inline-block;width:11px;height:11px;border-radius:2px;margin-right:5px;vertical-align:-1px}
.cv-pick{border:1px solid var(--border-hi,#2a2618);border-radius:8px;padding:10px 12px;margin-top:10px}.cv-pick-head{display:flex;flex-wrap:wrap;gap:10px;align-items:baseline;margin-bottom:8px}.cv-pick-head .cv-btn{margin-left:auto}
.cv-scroll{overflow:auto;max-height:520px}.cv-table{width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap}.cv-table th{position:sticky;top:0;background:var(--panel,#0C0B09);z-index:1}.cv-table .r{text-align:right}.cv-table .c{text-align:center}
.cv-name{cursor:pointer;text-decoration:underline dotted;text-underline-offset:3px;color:${PAL.ink}}.cv-two{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:6px 28px}
.cv-bar-row{display:grid;grid-template-columns:minmax(120px,1.2fr) 2fr 62px 48px;gap:8px;align-items:center;font-size:12px;padding:3px 0}.cv-bar-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.cv-bar-track{height:10px;background:${PAL.none};border-radius:5px;overflow:hidden}.cv-bar-track i{display:block;height:100%;border-radius:0 4px 4px 0;min-width:2px}
.cv-bar-val,.cv-bar-lvl{font-family:var(--font-mono);text-align:right;font-size:11.5px}.cv-stack-row{display:grid;grid-template-columns:150px 1fr;gap:10px;align-items:center;font-size:12px;padding:3px 0}.cv-stack{display:flex;height:18px;border-radius:4px;overflow:hidden;gap:2px}.cv-stack i{display:block;font:600 10px var(--font-mono);color:#fff;text-align:center;line-height:18px;font-style:normal;overflow:hidden}
.cv-prod-row{display:grid;grid-template-columns:minmax(210px,1.3fr) 1.4fr 96px 250px minmax(160px,1.6fr);gap:10px;align-items:center;font-size:12px;padding:4px 0;border-bottom:1px solid var(--border,#12110C)}.cv-prod-n,.cv-prod-usd{font-family:var(--font-mono);text-align:right}.cv-prod-how{font-size:11px}
.cv-details{margin-top:10px}.cv-details summary,.cv-node summary{cursor:pointer;font-size:12px;padding:4px 0}.cv-note{font-size:11px;color:${PAL.dim};margin-top:10px;line-height:1.5}.cv-axis{font:400 10px var(--font-mono);fill:${PAL.dim}}
.cv-opt{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:12px}.cv-opt-card{border:1px solid var(--border-hi,#2a2618);border-radius:8px;padding:10px 12px}.cv-opt-share{display:grid;grid-template-columns:110px 1fr;gap:10px;align-items:center;font-size:11.5px;margin:6px 0}.cv-opt-share .cv-stack{height:8px}
.cv-grid th{font-size:11px;color:#e8e2d4;text-transform:none;letter-spacing:0;white-space:nowrap}.cv-grid td{font-family:var(--font-mono);padding:8px 10px;border:2px solid var(--panel,#0C0B09);color:#fff}.cv-cell-sub{display:block;font-size:9.5px;font-weight:400;opacity:.8}
.cv-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;margin:10px 0}.cv-kpi{border:1px solid var(--border-hi,#2a2618);border-radius:8px;padding:10px 12px}.cv-kpi span{font-size:11px;color:${PAL.dim}}.cv-kpi b{display:block;font-family:var(--font-mono);font-size:20px;margin:3px 0;color:${PAL.ink}}.cv-kpi small{display:block;font-size:10.5px;color:${PAL.dim};line-height:1.4}
.cv-list{margin:6px 0 0 18px;padding:0;font-size:12px;line-height:1.6}.cv-tree{font-size:12px}.cv-node{border-top:1px solid var(--border,#12110C)}.cv-node-body{padding:2px 0 6px 16px}.cv-leaf{display:grid;grid-template-columns:minmax(150px,240px) 1fr auto;gap:10px;padding:2px 0}.cv-key{font-family:var(--font-mono);color:${PAL.dim}}.cv-val{font-family:var(--font-mono);overflow-wrap:anywhere}
.cv-sheet{margin-top:18px;border-top:1px solid var(--border-hi,#2a2618);padding-top:4px}.cv-sheet-h{font-size:14px!important}.cv-chips{display:flex;flex-wrap:wrap;gap:6px}.cv-chip{font-family:var(--font-mono);font-size:10.5px;border:1px solid var(--border-hi,#2a2618);border-radius:10px;padding:2px 9px;color:${PAL.dim}}.cv-chip.on{color:#F0B429;border-color:#F0B429}
.cv-flag{border-left:3px solid ${PAL.bad};background:rgba(242,54,69,.08);padding:8px 12px;border-radius:0 6px 6px 0;font-size:12px;margin:8px 0;line-height:1.5}.cv-flag.ok{border-left-color:${CAT.index};background:rgba(76,141,246,.08)}
.cv-figs{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:6px}.cv-fig{border:1px solid var(--border,#12110C);border-radius:6px;padding:6px 9px;background:var(--panel-2,#12110C)}.cv-fig span{display:block;font-size:10.5px;color:${PAL.dim}}.cv-fig b{font-family:var(--font-mono);font-size:12.5px;font-weight:600;overflow-wrap:anywhere}.cv-fig small{display:block;font-size:9.5px;color:${PAL.dim};line-height:1.3;margin-top:2px}
@media(max-width:720px){.cv-prod-row{grid-template-columns:1fr 70px}.cv-prod-row .cv-bar-track,.cv-prod-how,.cv-prod-usd{display:none}.cv-bar-row{grid-template-columns:1fr 1fr 56px}.cv-bar-lvl{display:none}.cv-stack-row{grid-template-columns:1fr}.cv-leaf{grid-template-columns:1fr}.cv-cards{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.cv-card{padding:9px 10px;min-width:0}.cv-card-val{font-size:19px}.cv-card-top{flex-direction:column;gap:0}.cv-card-chg{flex-wrap:wrap;white-space:normal;gap:2px 8px}.cv-exp-row{grid-template-columns:86px 1fr 58px 28px}}`;
function ensureCss() { const D = root.document; if (D.getElementById('cv-css')) return; const st = D.createElement('style'); st.id = 'cv-css'; st.textContent = CSS; D.head.appendChild(st); }
const slot = id => root.document.getElementById(id);
function draw(part) {
  const d = V.doc; if (!d) return;
  const put = (id, html) => { const el = slot(id); if (el) el.innerHTML = html; };
  if (!part || part === 'glance') put('cds2-glance', glanceHtml(d, V));
  if (part === 'heat') { put('cds2-heat', heatHtml(d, V)); put('cds2-pick', pickHtml(d, V)); put('cds2-movers', moversHtml(d, V)); syncSeg(); }
  if (!part || part === 'products') put('cds2-products', productsHtml(d, V));
  if (!part) { put('cds2-options', optionsHtml(d)); put('cds2-structured', structuredHtml(d)); put('cds2-quality', qualityHtml(d)); put('cds2-data', dataHtml(d)); }
  if (!part || part === 'quiet') put('cds2-quiet', quietHtml(d, V));
  if (V.api && V.api.enhanceTables) ['cds2-glance', 'cds2-products', 'cds2-structured', 'cds2-quiet', 'cds2-quality'].forEach(id => { const el = slot(id); if (el) V.api.enhanceTables(el); });
}
function syncSeg() { root.document.querySelectorAll('#cds2-glance [data-cv]').forEach(b => { const k = b.dataset.cv; if (k === 'grp' || k === 'win' || k === 'measure') { const on = V[k] === b.dataset.v; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on); } }); }
function download(name, text) { const blob = new Blob([text], { type: 'text/csv' }), a = root.document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = name; root.document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(a.href), 2000); }
function loadTape() { if (V.tape) return Promise.resolve(V.tape); if (!V.tapePromise) V.tapePromise = V.api.fetchJson(V.api.KEYS.tape || 'data/cds-desk-tape.json').then(t => { V.tape = t; return t; }).catch(() => { V.tapePromise = null; return null; }); return V.tapePromise; }
let wired = false;
function wire() {
  if (wired) return; wired = true;
  const D = root.document;
  D.addEventListener('click', e => {
    const b = e.target.closest('[data-cv]'); if (!b) return;
    const k = b.dataset.cv, v = b.dataset.v;
    if (k === 'grp' || k === 'win' || k === 'measure') { V[k] = v; if (k === 'grp') V.pick = null; draw('heat'); }
    else if (k === 'pick') { V.pick = v && V.pick !== v ? v : null; draw('heat'); }
    else if (k === 'prod') { V.prod = v; draw('products'); }
    else if (k === 'quiet') { V.quiet = !!v; draw('quiet'); }
    else if (k === 'more') { const path = JSON.parse(v), host = b.closest('.cv-node-body') || b.parentElement; host.innerHTML = nodeHtml(atPath(V.doc, path), path, 1e9); }
    else if (k === 'csv') {
      if (v === 'names') download('justhodl-cds-desk-every-name-' + (V.doc.as_of || '') + '.csv', csvOf(V.doc));
      else if (v === 'tape' && V.tape) { const sh = D.getElementById('cds2-sheet'), key = sh && sh.dataset.key, rows = (V.tape.names || {})[key] || []; download('justhodl-cds-prints-' + String(key).replace(/[^A-Za-z0-9]+/g, '-') + '.csv', [V.tape.cols.join(',')].concat(rows.map(r => r.map(x => x == null ? '' : x).join(','))).join('\n')); }
    }
  });
  let wasNarrow = isNarrow();
  root.addEventListener('resize', () => { const n = isNarrow(); if (n !== wasNarrow) { wasNarrow = n; draw('heat'); } });      // a turned phone gets the map for its new shape
  D.addEventListener('keydown', e => { if ((e.key === 'Enter' || e.key === ' ') && e.target.classList && e.target.classList.contains('cv-tile')) { e.preventDefault(); e.target.dispatchEvent(new MouseEvent('click', { bubbles: true })); } });
  D.addEventListener('toggle', e => { const n = e.target; if (!n.classList || !n.classList.contains('cv-node') || !n.open) return; const body = n.querySelector(':scope > .cv-node-body'); if (body && !body.dataset.done) { const path = JSON.parse(n.dataset.path); body.innerHTML = nodeHtml(atPath(V.doc, path), path); body.dataset.done = '1'; } }, true);
}
function render(d, api) { V.doc = d; V.api = api; ensureCss(); wire(); draw(); }
function sheet(key, name, group, body, api) {
  V.api = api || V.api; const d = V.doc; if (!d || !body) return;
  const r = findRow(d, key); if (!r) return;
  body.insertAdjacentHTML('beforeend', sheetHtml(r, d, V.tape));
  if (r.group !== 'index') loadTape().then(t => { const el = slot('cds2-tape'), sh = slot('cds2-sheet'); if (el && sh && sh.dataset.key === key) { el.innerHTML = tapeHtml(t || { names: {}, cols: [], note: '' }, key); if (V.api.enhanceTables) V.api.enhanceTables(el); } });
}
const api = { render, sheet, state: V, PAL, CAT, CAT_OF, CAT_WORDS, WIN, GROUPS, MEASURES, FIELDS, DOC_WORDS, BANDS, rowsOf, measureOf, tilesOf, scaleOf, colourOf, fmtMeasure, squarify, indexCards, heatHtml, pickHtml, moversHtml, bandsHtml, glanceHtml,
  productsHtml, dailyStack, ladderSvg, optionsHtml, structuredHtml, quietRows, quietHtml, qualityHtml, dataHtml, nodeHtml, atPath, csvOf, findRow, curveSvg, pairBars, figuresHtml, tapeHtml, sheetHtml, median, mix };
if (typeof module !== 'undefined' && module.exports) module.exports = api;
if (root && typeof root.document !== 'undefined') root.JHCdsViews = api;
})(typeof window !== 'undefined' ? window : null);
