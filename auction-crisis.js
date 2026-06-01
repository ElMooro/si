/* ─── Auction Crisis Detector — AI Edition · v2 page renderer ───
 * Fetches both data/auction-crisis.json (rich v2 data) and
 * data/auction-crisis-ai.json (Claude narrative + predictions).
 * Renders progressively — data-only sections appear first, AI overlays
 * arrive when the AI fetch resolves.
 */
const DATA_URL = 'https://justhodl-dashboard-live.s3.amazonaws.com/data/auction-crisis.json?t=' + Date.now();
const AI_URL   = 'https://justhodl-dashboard-live.s3.amazonaws.com/data/auction-crisis-ai.json?t=' + Date.now();

let DATA = null;
let AI   = null;

// ── helpers ──
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmt = (v, d=1) => v == null ? '—' : (typeof v === 'number' ? v.toFixed(d) : v);
const stressClass = (s) => s == null ? 'stress-no-data' : s >= 50 ? 'stress-2' : s >= 25 ? 'stress-1' : 'stress-0';
const ageFromIso = (iso) => {
  if (!iso) return '—';
  const t = new Date(iso).getTime();
  const mins = Math.round((Date.now() - t) / 60000);
  if (mins < 60) return mins + 'm ago';
  if (mins < 1440) return Math.round(mins / 60) + 'h ago';
  return Math.round(mins / 1440) + 'd ago';
};

// ─────────────────────────────────────────────────────────────────────
// MAIN — fetch both files in parallel, render data sections, then AI
// ─────────────────────────────────────────────────────────────────────
async function load() {
  // Fetch data first (mandatory)
  try {
    const dRes = await fetch(DATA_URL);
    if (!dRes.ok) throw new Error('data fetch HTTP ' + dRes.status);
    DATA = await dRes.json();
  } catch (e) {
    showError('Failed to load auction crisis data: ' + e.message);
    return;
  }

  renderAllDataSections();

  // Fetch AI separately — if it fails, show degraded UI
  try {
    const aRes = await fetch(AI_URL);
    if (aRes.ok) {
      AI = await aRes.json();
      renderAISections();
    } else {
      showAIDegraded('AI commentary unavailable (HTTP ' + aRes.status + ')');
    }
  } catch (e) {
    showAIDegraded('AI fetch failed: ' + e.message);
  }
}

function showError(msg) {
  const b = $('errorBanner');
  b.style.display = 'block';
  b.textContent = msg;
}

function showAIDegraded(msg) {
  console.warn('[ai]', msg);
  $('ai-exec-title').textContent = 'AI commentary unavailable';
  $('ai-exec-body').textContent = msg + '. Numerical layers still rendered below.';
  $('decisive-text').textContent = 'AI decisive call unavailable — see triggers section for static action prescriptions.';
  $('what-changed').textContent = 'AI narrative unavailable. See indicator cards + tenor decomposition for current state.';
  $('analog-ai').innerHTML = '<div class="lbl">AI Analog Discussion</div><div>Unavailable — see top match metrics above.</div>';
  $('tail-ai-text').textContent = 'AI tail risk assessment unavailable — see the 3 probability cards above for drivers.';
  $('triggers-ai-text').textContent = 'AI narrative unavailable — see individual trigger cards above.';
  $('ai-forward-grid').innerHTML = '<div style="color:var(--fg-3);font-size:12px;padding:14px">AI forward predictions unavailable — see the calendar table above for numerical forecasts.</div>';
}

// ─────────────────────────────────────────────────────────────────────
// DATA-ONLY SECTIONS (immediately on first fetch)
// ─────────────────────────────────────────────────────────────────────
function renderAllDataSections() {
  renderTopMeta();
  renderHero();
  renderTenorDecomposition();
  renderCrossSignals();
  renderCompositeChart();
  renderAnalogDataOnly();
  renderForwardTable();
  renderPostIssuePerformance();
  renderTailRiskDataOnly();
  renderTriggers();
  renderIndicators();
  renderAuctionsTable();
  renderHistoricalReference();
}

function renderTopMeta() {
  const ts = DATA.generated_at ? new Date(DATA.generated_at).toLocaleString() : '—';
  $('updated').textContent = ts;
  $('footer-ts').textContent = ts;
}

function renderHero() {
  const score = DATA.composite_score ?? 0;
  const regime = DATA.regime || 'CALM';
  $('composite-score').textContent = fmt(score);
  $('regime-text').textContent = regime.replace(/_/g, ' ');
  $('regime-desc').textContent = DATA.interpretation || '';
  $('n-recent').textContent = DATA.n_recent_auctions_14d ?? '—';
  $('fed-rate').textContent = DATA.fed_funds_rate != null ? DATA.fed_funds_rate.toFixed(2) : '—';

  ['regime-banner', 'score-circle', 'regime-meta', 'decisive-call'].forEach(id => {
    const el = $(id);
    if (el) el.className = el.className.replace(/\b(ACUTE_STRESS|ELEVATED|WATCH|CALM)\b/g, '').trim() + ' ' + regime;
  });
}

function renderTenorDecomposition() {
  const td = DATA.tenor_decomposition || {};
  let html = '';
  // Sort by composite desc, putting "no data" last
  const entries = Object.entries(td).sort((a, b) => {
    const ax = a[1].composite, bx = b[1].composite;
    if (ax == null && bx == null) return 0;
    if (ax == null) return 1;
    if (bx == null) return -1;
    return bx - ax;
  });
  for (const [key, v] of entries) {
    const cls = v.composite == null ? 'stress-no-data' :
                  v.composite >= 50 ? 'stress-2' :
                  v.composite >= 25 ? 'stress-1' : 'stress-0';
    const dominantStr = v.dominant_signal ? `<b>${esc(v.dominant_signal)}</b> dominant` : 'no signal firing';
    const rankStr = v.rank ? `<span class="rank-pill">rank #${v.rank}</span>` : '';
    html += `<div class="tenor-card ${cls}">
      <div class="name">${esc(v.label || key)} ${rankStr}</div>
      <div class="score-row">
        <div class="score">${fmt(v.composite)}</div>
        <div class="max">${v.max_composite != null ? 'max ' + v.max_composite : ''}</div>
      </div>
      <div class="meta">${v.n_auctions} auctions · ${dominantStr}${v.latest_date ? ' · latest ' + v.latest_date.slice(0,10) : ''}</div>
      <div class="risk">${esc(v.risk_profile || '')}</div>
    </div>`;
  }
  $('tenor-grid').innerHTML = html;
}

function renderCrossSignals() {
  const cs = DATA.cross_signals || {};
  let html = '';

  if (cs.repo_stress && !cs.repo_stress.err) {
    const r = cs.repo_stress;
    html += `<div class="cross-card">
      <div class="nm">Repo Stress (SOFR-IORB)<span class="regime-pill ${esc(r.regime)}">${esc(r.regime)}</span></div>
      <div class="val">${r.spread_bp != null ? (r.spread_bp >= 0 ? '+' : '') + r.spread_bp.toFixed(1) + 'bp' : '—'}</div>
      <div class="interp">SOFR ${fmt(r.sofr_pct, 3)}% vs IORB ${fmt(r.iorb_pct, 3)}%. ${esc(r.interpretation || '')}</div>
    </div>`;
  }
  if (cs.dollar_strength && !cs.dollar_strength.err) {
    const d = cs.dollar_strength;
    html += `<div class="cross-card">
      <div class="nm">USD Strength (DXY-equiv)<span class="regime-pill ${esc(d.regime)}">${esc(d.regime)}</span></div>
      <div class="val">${fmt(d.level, 1)} <span style="font-size:12px;color:var(--fg-3)">(${d.change_30d_pct >= 0 ? '+' : ''}${fmt(d.change_30d_pct, 2)}% / 30d)</span></div>
      <div class="interp">${esc(d.interpretation || '')}</div>
    </div>`;
  }
  if (cs.curve_slope) {
    const c = cs.curve_slope;
    html += `<div class="cross-card">
      <div class="nm">Yield Curve (10y-2y)<span class="regime-pill ${esc(c.regime)}">${esc(c.regime)}</span></div>
      <div class="val">${c.spread_bp >= 0 ? '+' : ''}${fmt(c.spread_bp, 0)}bp</div>
      <div class="interp">${esc(c.interpretation || '')}</div>
    </div>`;
  }
  if (cs.inflation_expectations) {
    const i = cs.inflation_expectations;
    html += `<div class="cross-card">
      <div class="nm">5y5y Inflation BE<span class="regime-pill ${esc(i.regime)}">${esc(i.regime)}</span></div>
      <div class="val">${fmt(i.rate_pct, 2)}%</div>
      <div class="interp">${esc(i.interpretation || '')}</div>
    </div>`;
  }
  $('cross-strip').innerHTML = html || '<div style="color:var(--fg-3);font-size:11px;padding:10px">Cross-signal data unavailable</div>';
}

function renderCompositeChart() {
  const hist = DATA.composite_history || {};
  const series = hist.series || [];
  if (series.length === 0) return;

  const W = 1000, H = 260, M = {top: 20, right: 20, bottom: 30, left: 40};
  const innerW = W - M.left - M.right;
  const innerH = H - M.top - M.bottom;

  const xs = series.length - 1;
  const xOf = (i) => M.left + (i / xs) * innerW;
  const yOf = (v) => v == null ? null : M.top + innerH - (v / 100) * innerH;

  let svg = '';
  // Regime band backgrounds
  const bands = [
    {top: 0,  bot: 25, fill: 'rgba(27,138,92,0.06)'},
    {top: 25, bot: 50, fill: 'rgba(138,94,16,0.05)'},
    {top: 50, bot: 75, fill: 'rgba(255,177,61,0.06)'},
    {top: 75, bot:100, fill: 'rgba(138,44,68,0.08)'}
  ];
  for (const b of bands) {
    const y1 = yOf(b.bot), y2 = yOf(b.top);
    svg += `<rect x="${M.left}" y="${y1}" width="${innerW}" height="${y2-y1}" fill="${b.fill}"/>`;
  }
  // Threshold lines
  for (const thr of [25, 50, 75]) {
    const y = yOf(thr);
    svg += `<line x1="${M.left}" x2="${M.left + innerW}" y1="${y}" y2="${y}" stroke="rgba(120,145,180,0.25)" stroke-dasharray="2 4" stroke-width="1"/>`;
    svg += `<text x="${M.left - 6}" y="${y + 3}" text-anchor="end" fill="var(--fg-4)" font-family="var(--font-mono)" font-size="9">${thr}</text>`;
  }
  // Composite line + area
  const points = series.map((s, i) => s.composite != null ? `${xOf(i)},${yOf(s.composite)}` : null).filter(Boolean);
  if (points.length > 1) {
    svg += `<polyline points="${points.join(' ')}" fill="none" stroke="var(--cyan)" stroke-width="1.8"/>`;
    // Area fill
    const areaPath = points.join(' L ');
    const lastY = M.top + innerH;
    svg += `<path d="M ${points[0]} L ${areaPath} L ${xOf(series.length-1)},${lastY} L ${xOf(0)},${lastY} Z" fill="rgba(0,212,255,0.08)"/>`;
  }
  // Change-point markers
  const cps = hist.change_points || [];
  for (const cp of cps) {
    const idx = series.findIndex(s => s.date === cp.date);
    if (idx < 0 || series[idx].composite == null) continue;
    const x = xOf(idx), y = yOf(series[idx].composite);
    svg += `<circle cx="${x}" cy="${y}" r="4.5" fill="var(--violet)" stroke="var(--bg-base)" stroke-width="1.5"/>`;
    svg += `<text x="${x}" y="${y - 9}" text-anchor="middle" fill="var(--violet)" font-family="var(--font-mono)" font-size="9">${cp.from.charAt(0)}→${cp.to.charAt(0)}</text>`;
  }
  // X axis dates (every 5 days)
  for (let i = 0; i < series.length; i += 5) {
    if (!series[i] || !series[i].date) continue;
    const x = xOf(i);
    const d = series[i].date.slice(5);  // MM-DD
    svg += `<text x="${x}" y="${M.top + innerH + 18}" text-anchor="middle" fill="var(--fg-3)" font-family="var(--font-mono)" font-size="9.5">${d}</text>`;
  }
  // Current value annotation
  const curIdx = series.length - 1;
  const cur = series[curIdx];
  if (cur && cur.composite != null) {
    const x = xOf(curIdx), y = yOf(cur.composite);
    svg += `<circle cx="${x}" cy="${y}" r="4" fill="var(--cyan)" stroke="var(--bg-base)" stroke-width="2"/>`;
    svg += `<text x="${x - 8}" y="${y - 8}" text-anchor="end" fill="var(--cyan)" font-family="var(--font-mono)" font-size="11" font-weight="600">${cur.composite.toFixed(1)}</text>`;
  }

  $('composite-chart').innerHTML = svg;
}

function renderAnalogDataOnly() {
  const ha = DATA.historical_analog || {};
  const top = (ha.top_matches || [])[0];
  if (!top) {
    $('analog-top').innerHTML = '<div style="color:var(--fg-3);padding:10px">No historical analog match available</div>';
    return;
  }
  const simPct = Math.round(top.similarity * 100);
  $('analog-top').innerHTML = `
    <div class="similarity-circle"><div class="big">${simPct}%</div><div class="sub">SIMILARITY</div></div>
    <div class="info">
      <div>
        <span class="anchor-date">${esc(top.date)}</span>
        <span class="anchor-regime">${esc(top.regime || '?')}</span>
      </div>
      <div class="anchor-context">${esc(top.context || '')}</div>
      <div class="anchor-next"><strong>What happened next:</strong> ${esc(top.what_happened_next || '')} <em style="color:var(--fg-3)">(${esc(top.duration || '')})</em></div>
    </div>
    <div style="text-align:right;font-family:var(--font-mono);font-size:10px;color:var(--fg-3);max-width:160px">
      <div style="margin-bottom:4px">Anchor metrics:</div>
      <div>BTC: ${top.anchor_metrics?.btc ?? '—'}</div>
      <div>AAH: ${top.anchor_metrics?.aah ?? '—'}%</div>
      <div>PD: ${top.anchor_metrics?.pd_share_pct ?? '—'}%</div>
      <div>Ind: ${top.anchor_metrics?.indirect_share_pct ?? '—'}%</div>
    </div>
  `;

  // Other matches (top 2-3)
  const others = (ha.top_matches || []).slice(1, 4);
  let oh = '';
  for (const m of others) {
    oh += `<div class="om">
      <div class="d">${esc(m.date)}</div>
      <div class="r">${esc(m.regime || '?')}</div>
      <div class="s">${Math.round((m.similarity || 0) * 100)}% similar</div>
      <div class="nxt">${esc(((m.what_happened_next || '').slice(0, 140)) + (m.what_happened_next && m.what_happened_next.length > 140 ? '…' : ''))}</div>
    </div>`;
  }
  $('analog-others').innerHTML = oh;
}

function renderForwardTable() {
  const cal = DATA.forward_calendar || [];
  if (cal.length === 0) {
    $('forward-tbody').innerHTML = '<tr><td colspan="10" style="color:var(--fg-3);text-align:center;padding:16px">No upcoming auctions in window</td></tr>';
    return;
  }
  // Already sorted by auction_date asc
  let html = '';
  for (const f of cal) {
    const fc = f.forecast || {};
    const drv = (fc.narrative || '').slice(0, 70) + (fc.narrative && fc.narrative.length > 70 ? '…' : '');

    // v2.1 concession fields
    const c5 = f.concession_5d_bp;
    const cRegime = f.concession_regime || '';
    let cClass = 'concession-na', cDisplay = '—';
    if (c5 != null) {
      cDisplay = (c5 >= 0 ? '+' : '') + c5.toFixed(1) + 'bp';
      if (cRegime === 'HEAVY_CONCESSION') cClass = 'concession-heavy';
      else if (cRegime === 'CONCESSION')   cClass = 'concession-mild';
      else if (cRegime === 'STRONG_RALLY') cClass = 'concession-strong-rally';
      else if (cRegime === 'RALLY')        cClass = 'concession-rally';
      else                                  cClass = 'concession-flat';
    }
    const cTooltip = esc(f.concession_interpretation || '');

    html += `<tr>
      <td class="date-cell">${esc(f.auction_date)}</td>
      <td>${f.days_ahead}d</td>
      <td class="term-cell">${esc(f.security_type)}</td>
      <td>${esc(f.security_term)}</td>
      <td class="size-cell">${f.offering_amount_billions != null ? '$' + f.offering_amount_billions.toFixed(0) + 'B' : '—'}</td>
      <td class="score-cell ${esc(fc.forecast_label || '')}">${fc.forecast_score != null ? fc.forecast_score.toFixed(1) : '—'} <span style="font-size:9.5px;color:var(--fg-3);font-weight:400">[${esc(fc.forecast_label || '?')}]</span></td>
      <td class="conf-cell ${esc(fc.confidence || '')}">${esc(fc.confidence || '?')}</td>
      <td class="${cClass}" title="${cTooltip}">${cDisplay}</td>
      <td style="font-size:9.5px;color:var(--fg-3)">${esc(cRegime.replace(/_/g,' '))}</td>
      <td style="font-size:10.5px;color:var(--fg-3)">${esc(drv)}</td>
    </tr>`;
  }
  $('forward-tbody').innerHTML = html;
}

function renderPostIssuePerformance() {
  const auctions = (DATA.recent_auctions || []).filter(a => a.postissue_classification);
  const el = $('postissue-section');
  if (!el) return;
  if (!auctions.length) {
    el.innerHTML = '<div style="color:var(--fg-3);font-size:11.5px;padding:14px">No post-issue performance data yet (settled auctions appear here once 1+ day past issuance)</div>';
    return;
  }
  let html = `<table class="postissue-table">
    <thead><tr>
      <th>Issue Date</th><th>Tenor</th><th>Term</th>
      <th class="num">High%</th>
      <th class="num">1d Δbp</th><th class="num">5d Δbp</th><th class="num">30d Δbp</th>
      <th>Classification</th>
    </tr></thead><tbody>`;
  for (const a of auctions) {
    const cls = a.postissue_classification || 'PENDING';
    const fmt = (v) => v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(1);
    const cellCls = (v) => v == null ? '' : (v >= 0 ? 'pi-positive' : 'pi-negative');
    html += `<tr>
      <td>${esc((a.issue_date || '').slice(0,10))}</td>
      <td>${esc(a.security_type)}</td>
      <td>${esc(a.security_term)}</td>
      <td class="num">${a.high_rate != null ? a.high_rate.toFixed(3) : '—'}</td>
      <td class="num ${cellCls(a.postissue_1d_bp)}">${fmt(a.postissue_1d_bp)}</td>
      <td class="num ${cellCls(a.postissue_5d_bp)}">${fmt(a.postissue_5d_bp)}</td>
      <td class="num ${cellCls(a.postissue_30d_bp)}">${fmt(a.postissue_30d_bp)}</td>
      <td><span class="pi-class pi-${cls.toLowerCase()}">${esc(cls)}</span></td>
    </tr>`;
  }
  html += '</tbody></table>';
  el.innerHTML = html;
}

function renderTailRiskDataOnly() {
  const tr = DATA.tail_risk || {};
  const cards = [
    {key: 'p_failed_auction_30d',     label: 'Failed Auction · 30d'},
    {key: 'p_regime_escalation_14d',  label: 'Regime Escalation · 14d'},
    {key: 'p_supply_volatility_30d',  label: 'Supply Vol Spike · 30d'},
  ];
  let html = '';
  for (const c of cards) {
    const v = tr[c.key];
    if (!v) continue;
    const drivers = Object.entries(v.drivers || {})
      .map(([k, vv]) => `<span>${esc(k)}=<b>${typeof vv === 'number' ? vv.toFixed(1) : esc(String(vv))}</b></span>`)
      .join('');
    html += `<div class="tail-card">
      <div class="nm">${esc(c.label)}</div>
      <div class="bar-wrap"><div class="bar" style="width:${v.probability || 0}%"></div></div>
      <div class="pct">${(v.probability ?? 0).toFixed(0)}<span class="unit">%</span></div>
      <div class="interp">${esc(v.interpretation || '')}</div>
      <div class="drivers">${drivers}</div>
    </div>`;
  }
  $('tail-grid').innerHTML = html;
}

function renderTriggers() {
  const trs = DATA.triggers || [];
  let html = '';
  for (const t of trs) {
    const urg = t.urgency || 'monitoring';
    const distStr = t.distance != null ? `<span class="dist">${t.distance > 0 ? '+' : ''}${t.distance}</span>` : '—';
    html += `<div class="trigger-card ${esc(urg)}">
      <div class="name">${esc(t.name)} <span class="urgency-pill ${esc(urg)}">${esc(urg)}</span></div>
      <div class="condition">${esc(t.condition)}</div>
      <div class="vals">
        <span>current=<b>${t.current ?? '—'}</b></span>
        <span>threshold=<b>${t.threshold ?? '—'}</b></span>
        <span>distance=${distStr}</span>
        ${t.auctions_already_fired_14d != null ? `<span>fired 14d=<b>${t.auctions_already_fired_14d}</b></span>` : ''}
      </div>
      <div class="action"><strong>Action:</strong> ${esc(t.action || '')}</div>
    </div>`;
  }
  $('trigger-grid').innerHTML = html;
}

const INDICATOR_DEFS = {
  zero_rate_floor:   {name: 'Zero-Rate Bill Floor',     short: 'Money parking at any cost (≤0.001%) when Fed > 0', historical: '2008 Sep 17/18/23 + 2020 Mar 19/26'},
  btc_extreme:       {name: 'Bid-to-Cover Extremes',    short: 'Extreme high BTC (stampede) or extreme low (failure)', historical: 'High: 2020-Mar-26 BTC 4.74 · Low: 2008-Sep BTC 2.16'},
  tail_stress:       {name: 'Allotted-At-High Tail',    short: 'AAH > 95% (dealers absorbed) or < 15% (panic clustering)', historical: '2024-10-09 AAH 99.31% on 10y'},
  pd_absorption:     {name: 'Primary Dealer Share',     short: 'PD > 35% coupons = dealers stuck with paper', historical: '2008-09-18 PD 69% (extreme)'},
  indirect_collapse: {name: 'Indirect (Foreign) Bid',   short: 'Foreign share < 50% on coupons = exodus', historical: '2008-09-18 ind 29% (vs 60-78% normal)'},
  issuance_anomaly:  {name: 'Bill Issuance Explosion',  short: '4w vs 1y avg > +30% = liquidity injection / panic', historical: '2020-Mar bills surge'},
};

function renderIndicators() {
  const agg = DATA.indicator_aggregate_14d || {};
  const issuance = DATA.issuance_anomaly || {};
  let html = '';
  for (const [key, def] of Object.entries(INDICATOR_DEFS)) {
    let v = agg[key];
    let statValue;
    let pillCls = 'calm';
    let pillText = 'CALM';
    let cardCls = '';
    let nFired = 0, maxScore = 0;

    if (key === 'issuance_anomaly') {
      const pct = issuance.pct_above_baseline;
      const score = issuance.score || 0;
      statValue = pct != null ? `<div>${pct > 0 ? '+' : ''}${pct.toFixed(1)}%</div>` : '<div>—</div>';
      if (score >= 60) { pillCls = 'acute'; pillText = 'ACUTE'; cardCls = 'fired-acute'; maxScore = score; }
      else if (score >= 30) { pillCls = 'fired'; pillText = 'FIRED'; cardCls = 'fired'; maxScore = score; }
      html += `<div class="indicator-card ${cardCls}" data-signal="${key}">
        <div class="name">${esc(def.name)}<span class="pill ${pillCls}">${pillText}</span></div>
        <div class="stats"><div class="label">4w vs 1y baseline</div>${statValue}</div>
        <div class="desc">${esc(def.short)}<br/><em>${esc(def.historical)}</em></div>
        <div class="ai-take" id="ai-take-${key}" style="display:none"></div>
      </div>`;
      continue;
    }

    if (v) {
      nFired = v.n_fired || 0;
      maxScore = v.max_score || 0;
      if (maxScore >= 70 || nFired >= 3) { pillCls = 'acute'; pillText = 'ACUTE'; cardCls = 'fired-acute'; }
      else if (nFired > 0) { pillCls = 'fired'; pillText = 'FIRED'; cardCls = 'fired'; }
    }
    html += `<div class="indicator-card ${cardCls}" data-signal="${key}">
      <div class="name">${esc(def.name)}<span class="pill ${pillCls}">${pillText}</span></div>
      <div class="stats">
        <div class="label">14d auctions firing</div>
        <div>${nFired} / max ${maxScore}</div>
      </div>
      <div class="desc">${esc(def.short)}<br/><em>${esc(def.historical)}</em></div>
      <div class="ai-take" id="ai-take-${key}" style="display:none"></div>
    </div>`;
  }
  $('indicator-grid').innerHTML = html;
}

function renderAuctionsTable() {
  const tbl = $('auctions-tbl');
  const auctions = DATA.recent_auctions || [];
  let html = `<thead><tr>
    <th>Date</th><th>Type</th><th>Term</th>
    <th class="num">BTC</th><th class="num">High%</th>
    <th class="num">AAH%</th><th class="num">PD%</th>
    <th class="num">Ind%</th><th class="num">Score</th>
  </tr></thead><tbody>`;
  for (const a of auctions) {
    const sc = a.composite_score ?? 0;
    const sCls = sc >= 50 ? 'stress-2' : sc >= 25 ? 'stress-1' : 'stress-0';
    html += `<tr>
      <td>${esc((a.auction_date || '').slice(0,10))}</td>
      <td>${esc(a.security_type)}</td>
      <td>${esc(a.security_term)}</td>
      <td class="num">${fmt(a.btc, 2)}</td>
      <td class="num">${fmt(a.high_rate, 3)}</td>
      <td class="num">${fmt(a.allocated_at_high_pct, 1)}</td>
      <td class="num">${a.primary_dealer_pct != null ? a.primary_dealer_pct.toFixed(1) : '—'}</td>
      <td class="num">${a.indirect_pct != null ? a.indirect_pct.toFixed(1) : '—'}</td>
      <td class="num ${sCls}">${sc.toFixed(1)}</td>
    </tr>`;
  }
  html += '</tbody>';
  tbl.innerHTML = html;
}

function renderHistoricalReference() {
  const refs = DATA.historical_reference || [];
  let html = '';
  for (const h of refs) {
    html += `<div class="hist-card">
      <div class="date">${esc(h.date)}</div>
      <div class="regime ${esc(h.regime)}">${esc((h.regime || '').replace(/_/g, ' '))}</div>
      <div class="metrics">
        BTC: <b>${fmt(h.btc, 2)}</b><br>
        Low rate: <b>${fmt(h.low_rate, 3)}%</b><br>
        AAH: <b>${fmt(h.aah, 2)}%</b><br>
        PD: <b>${fmt(h.pd_share_pct, 1)}%</b><br>
        Indirect: <b>${fmt(h.indirect_share_pct, 1)}%</b>
      </div>
    </div>`;
  }
  $('hist-grid').innerHTML = html;
}

// ─────────────────────────────────────────────────────────────────────
// AI SECTIONS (after AI fetch resolves)
// ─────────────────────────────────────────────────────────────────────
function renderAISections() {
  if (!AI || AI.status === 'error') {
    showAIDegraded(AI?.error || 'AI commentary returned error');
    return;
  }
  const ai = AI.ai_commentary || {};

  // Meta
  if (AI.generated_at) {
    const aiAgeMin = AI.data_age_minutes != null ? AI.data_age_minutes.toFixed(0) + 'min source' : '';
    $('ai-updated').textContent = ageFromIso(AI.generated_at);
    $('ai-stale').textContent = aiAgeMin;
  }
  $('ai-regime').textContent = AI.regime || '—';

  // Executive summary
  $('ai-exec-title').textContent = 'Senior fixed-income strategist read';
  $('ai-exec-body').innerHTML = boldNumbers(esc(ai.executive_summary || 'No commentary available'));

  // Decisive call
  $('decisive-text').innerHTML = boldNumbers(esc(ai.decisive_call || 'No call available'));

  // What changed
  $('what-changed').innerHTML = boldNumbers(esc(ai.what_changed || ''));

  // Analog discussion
  $('analog-ai').innerHTML = `
    <div class="lbl">AI Analog Discussion</div>
    <div>${boldNumbers(esc(ai.historical_analog_discussion || ''))}</div>
  `;

  // Forward predictions cards
  renderAIForwardPredictions(ai.forward_predictions || []);

  // Tail risk narrative
  $('tail-ai-text').innerHTML = boldNumbers(esc(ai.tail_risk_assessment || ''));

  // Triggers narrative
  $('triggers-ai-text').innerHTML = boldMd(esc(ai.actionable_triggers || ''));

  // Indicator interpretations — attach AI takes to fired indicators
  for (const ii of (ai.indicator_interpretation || [])) {
    if (!ii.signal) continue;
    // Match by partial signal name (AI sometimes adds context)
    const sigKey = Object.keys(INDICATOR_DEFS).find(k =>
      ii.signal.toLowerCase().includes(k.replace(/_/g, ' ')) ||
      ii.signal.toLowerCase().includes(k)
    );
    if (sigKey) {
      const el = $('ai-take-' + sigKey);
      if (el) {
        el.style.display = 'block';
        el.innerHTML = `
          <div class="lbl">AI Read</div>
          <div>${boldNumbers(esc(ii.narrative || ''))}</div>
          <div style="margin-top:6px;color:var(--fg-3);font-size:10.5px"><strong style="color:var(--violet)">Implication:</strong> ${boldNumbers(esc(ii.implication || ''))}</div>
        `;
      }
    }
  }
}

function renderAIForwardPredictions(preds) {
  if (!preds.length) {
    $('ai-forward-grid').innerHTML = '<div style="color:var(--fg-3);padding:14px">No forward predictions available</div>';
    return;
  }
  let html = '';
  for (const p of preds) {
    const label = (p.predicted_score ?? 0) >= 70 ? 'ACUTE' :
                    (p.predicted_score ?? 0) >= 45 ? 'ELEVATED' :
                    (p.predicted_score ?? 0) >= 22 ? 'WATCH' : 'CALM';
    html += `<div class="forward-pred-card ${label}">
      <div class="head">
        <div class="when">${esc(p.auction_date || '?')} <span class="term">${esc(p.tenor || '')} ${esc(p.term || '')}</span></div>
        <div class="score-tag ${label}">${p.predicted_score ?? '—'} <span style="font-size:9.5px;color:var(--fg-3);font-weight:400;letter-spacing:1px">${label}</span></div>
      </div>
      <div class="outcome"><strong style="color:var(--cyan)">Expected:</strong> ${boldNumbers(esc(p.expected_outcome || ''))}</div>
      <div class="watch"><strong>Watch:</strong> ${boldNumbers(esc(p.what_to_watch || ''))}</div>
    </div>`;
  }
  $('ai-forward-grid').innerHTML = html;
}

// ─────────────────────────────────────────────────────────────────────
// Inline formatters — make numbers/percentages pop in AI prose
// ─────────────────────────────────────────────────────────────────────
function boldNumbers(s) {
  // Highlight: $XB, NNbp, N.NN%, NN%, N.N/N.N, scores ≥ XX
  return s
    .replace(/(\$\d+(?:\.\d+)?B)/g, '<strong>$1</strong>')
    .replace(/(\b\d+(?:\.\d+)?\s?bp)/g, '<strong>$1</strong>')
    .replace(/(\b\d+(?:\.\d+)?%)/g, '<strong>$1</strong>')
    .replace(/(\b\d{4}-\d{2}-\d{2}\b)/g, '<strong>$1</strong>')
    .replace(/\b(GFC|Lehman|COVID|2008|2020|2021|2024)\b/g, '<strong>$1</strong>');
}

function boldMd(s) {
  // Convert **markdown bold** to <strong> + boldNumbers
  return boldNumbers(s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>'));
}

// ─── Boot ───
load();
