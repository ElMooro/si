(function (root) {
  'use strict';
  const esc = x => String(x == null ? '' : x).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = x => typeof x === 'number' && Number.isFinite(x) ? x : null;
  const fmt = (x, digits = 2) => number(x) === null ? 'Unavailable' : x.toLocaleString('en-US', {maximumFractionDigits: digits});
  const date = x => typeof x === 'string' && /^\d{4}-\d{2}-\d{2}/.test(x) ? esc(x.slice(0,10)) : 'Not supplied';
  function link(key, label) {
    return typeof key === 'string' && /^data\/[a-zA-Z0-9_.\/-]+$/.test(key) && !key.includes('..')
      ? '<a href="/'+esc(key)+'" target="_blank" rel="noopener">'+esc(label)+'</a>' : esc(label+' unavailable');
  }
  function original(ref, label) { return ref ? link(ref.key, label) : 'Original unavailable'; }
  function history(rows, unit) {
    const finite = (rows || []).filter(r => number(r.value) !== null);
    if (finite.length < 2) return '<p>No comparable history.</p>';
    const data = rows.slice(-252), values = data.filter(r => number(r.value) !== null).map(r => r.value);
    if (values.length < 2) return '<p>No comparable history.</p>';
    const low = Math.min(...values), high = Math.max(...values), span = high-low || 1;
    let drawing = '', connected = false;
    data.forEach((r, i) => {
      if (number(r.value) === null) { connected = false; return; }
      const x = 8 + 584*i/Math.max(1, data.length-1), y = 78 - (r.value-low)/span*64;
      drawing += (connected ? ' L ' : ' M ')+x.toFixed(2)+' '+y.toFixed(2); connected = true;
    });
    const label = 'Provider history from '+data[0].date+' to '+data[data.length-1].date+', '+unit+'. Missing rows break the line.';
    return '<figure><svg viewBox="0 0 600 92" role="img" aria-label="'+esc(label)+'"><path d="'+drawing+'" fill="none" stroke="currentColor" stroke-width="2"/></svg>'+
      '<figcaption>'+date(data[0].date)+' — '+date(data[data.length-1].date)+' · '+fmt(low)+'–'+fmt(high)+' '+esc(unit)+'</figcaption></figure>';
  }
  function measurement(row) {
    const q = row.quality || {}, c = row.change_5_provider_rows || {}, p = row.percentile_2y || {};
    return '<article class="measure"><header><h3>'+esc(row.series_id)+'</h3><span class="badge">'+esc(q.status || 'unavailable')+'</span></header>'+
      '<p class="muted">'+esc(row.title || 'Provider measurement unavailable')+'</p><div class="value">'+fmt(row.value)+' <small>'+esc(row.unit)+'</small></div>'+
      '<p>Observation '+date(row.observation_date)+' · source age '+fmt(q.age_days,0)+' days</p>'+
      history(row.history, row.unit)+'<dl><dt>Change across 5 provider rows</dt><dd>'+fmt(c.value)+' '+esc(c.unit || '')+'</dd>'+
      '<dt>Compared dates</dt><dd>'+date(c.baseline_date)+' → '+date(c.current_date)+'</dd><dt>Two-year empirical percentile</dt><dd>'+fmt(p.value,1)+'/100</dd>'+
      '<dt>Percentile coverage</dt><dd>'+fmt(p.n_finite,0)+' finite · '+fmt(p.n_missing,0)+' missing</dd></dl>'+
      '<details><summary>Definition, dates and original records</summary><p>Current-vintage history, not point-in-time backtest data. Percentile is a historical rank, not a probability.</p>'+
      '<p>'+esc(p.formula || '')+'</p><p>Provider updated: '+esc(row.provider_updated_at || 'Not supplied')+'<br>Acquired: '+esc(row.acquired_at || 'Unavailable')+'<br>First publication: not supplied</p>'+
      '<p>'+original((row.originals || {}).definition,'Original definition')+' · '+original((row.originals || {}).observations,'Original observations')+'</p>'+
      '<div class="table-wrap"><table><caption>Most recent 12 provider rows · '+esc(row.unit)+'</caption><thead><tr><th>Date</th><th>Value</th><th>Source row</th></tr></thead><tbody>'+
      (row.history || []).slice(-12).reverse().map(r => '<tr><td>'+date(r.date)+'</td><td>'+fmt(r.value)+'</td><td>'+esc(r.row_index)+'</td></tr>').join('')+'</tbody></table></div></details></article>';
  }
  function options(row) {
    const u = row.universe || {};
    return '<article class="options"><header><h3>'+esc(row.symbol)+' options</h3><span class="badge">'+(row.pagination_complete ? 'Complete bounded universe' : 'Incomplete / unavailable')+'</span></header>'+
      '<p>'+fmt(row.contracts,0)+' contracts · '+fmt(row.pages,0)+' pages · expiry '+date(u.expiration_min)+'–'+date(u.expiration_max)+'. Strike bounds '+esc(u.strike_min || '—')+'–'+esc(u.strike_max || '—')+'.</p>'+
      '<p class="muted">'+esc(u.note || 'No verified universe available.')+' Volume and open interest are separate. Undated IV snapshots cannot establish synchronized market skew.</p>'+
      '<div class="table-wrap"><table><caption>Same-expiry measurements</caption><thead><tr><th>Expiry</th><th>Volume date</th><th>Volume coverage</th><th>Put/call volume</th><th>Put/call OI</th><th>25Δ skew (vol points)</th></tr></thead><tbody>'+
      (row.expiries || []).map(r => '<tr><td>'+date(r.expiry)+'</td><td>'+date(r.daily_volume_session)+'</td><td>'+fmt(100*r.volume_coverage,1)+'% ('+fmt(r.volume_rows_in_session,0)+'/'+fmt(r.contracts,0)+')</td><td>'+fmt(r.put_call_volume_ratio)+'</td><td>'+fmt(r.put_call_open_interest_ratio)+'</td><td>'+fmt(r.skew_25delta_vol_points)+(number(r.skew_25delta_vol_points) === null ? '' : ' · undated')+'</td></tr>').join('')+
      '</tbody></table></div><details><summary>Universe, source pages and calculation limits</summary><p>Ratios require the complete query and every contract’s required field. Missing volume remains missing; it is never replaced by open interest. Daily volume uses the underlying reference session. OI has no dated timestamp in this response.</p>'+
      '<p>Skew = put IV minus call IV at absolute delta 0.25 in the same expiry. Linear interpolation requires brackets within 0.15–0.35 and no more than 0.15 apart. No extrapolation. Contract rows and weights are in the '+link('data/risk-regime.json','public packet')+'.</p>'+
      '<p>Underlying reference '+fmt(u.underlying_reference_close)+' on '+date(u.underlying_reference_date)+' · '+original(u.underlying_reference,'original aggregate')+'</p><ul>'+
      (row.originals || []).map((r,i) => '<li>'+original(r,'Original option page '+(i+1))+' · acquired '+esc(r.acquired_at)+'</li>').join('')+'</ul></details></article>';
  }
  function render(packet, now = Date.now()) {
    if (!packet || packet.contract !== 'risk-regime-research.v1') return '<section class="notice" role="alert"><h2>Verified research packet unavailable</h2><p>The source-backed Risk Regime contract has not loaded. No score or sizing advice can be shown.</p></section>';
    const age = (now-Date.parse(packet.generated_at))/3600000;
    const stale = !Number.isFinite(age) || age < 0 || age > 36;
    const term = packet.term_structure || {}, fx = packet.fx_measurement || {}, fails = packet.pd_settlement_fails || {}, head = fails.ust_ex_tips || {};
    return '<section class="notice"><div class="eyebrow">DECISION AUTHORITY</div><h2>WAIT <span>— research abstains</span></h2><p>No validated trade or sizing score. These observations authorize no position or hedge-budget change.</p>'+
      '<p class="muted">Compiled '+esc(packet.generated_at)+' · '+(stale ? '<strong class="warn">Packet stale or clock invalid; refresh required.</strong>' : 'Each measurement carries its own observation date.')+'</p>'+
      '<p>'+link((packet.replay || {}).manifest_key,'Open reproducible run')+' · '+link('data/risk-regime.json','Download public packet')+' · <a href="/position-sizer.html">Explore explicit portfolio scenarios</a></p></section>'+
      '<section aria-labelledby="native-heading"><h2 id="native-heading">Volatility and credit</h2><p class="muted">Independent source measurements are visible together; correlated readings do not become additional votes.</p><div class="measure-grid">'+Object.values(packet.measurements || {}).map(measurement).join('')+'</div>'+
      '<article><h3>Matched-date volatility spread</h3><p><b>'+fmt(term.value)+' '+esc(term.unit || '')+'</b> · '+date(term.observation_date)+'</p><p>'+esc(term.formula || '')+'. '+esc(term.interpretation || '')+'.</p></article></section>'+
      '<section aria-labelledby="options-heading"><h2 id="options-heading">Options by expiry</h2>'+Object.values(packet.option_cohorts || {}).map(options).join('')+'</section>'+
      '<section class="two-columns"><article><h2>AUD/JPY</h2><div class="value">'+fmt(fx.value)+' <small>'+esc(fx.unit || '')+'</small></div><p>Observation '+date(fx.observation_date)+' · '+esc((fx.quality || {}).status || 'unavailable')+'</p>'+history(fx.history,fx.unit)+
      '<p>7 calendar-day change: '+fmt(fx.change_7_calendar_days_pct)+'%<br>Baseline: '+date(fx.baseline_date)+'</p><p class="muted">'+esc(fx.note || '')+'</p><p>'+original(fx.original,'Original daily bars')+'</p></article>'+
      '<article><h2>Settlement context</h2><p>Weekly FR2004 · two-sided gross reported fails, not defaults.</p><dl><dt>Treasury including TIPS</dt><dd>'+fmt(fails.combined_bn)+' USD bn</dd><dt>Observation</dt><dd>'+date(fails.as_of)+'</dd><dt>UST excluding TIPS</dt><dd>'+fmt(head.combined_bn)+' USD bn</dd><dt>Observation</dt><dd>'+date(head.as_of)+'</dd></dl><p class="muted">Scopes overlap. Never add these totals together. Repeated failed settlements may appear on both sides and successive days.</p><p>'+link(fails.source,'Source packet')+' · <a href="/fails.html">Settlement research</a></p></article></section>'+
      '<section><h2>Related engines and coverage</h2><p class="muted">Context links retain coverage visibility. Their scores do not vote in this research packet, and their original provider evidence has not been verified here.</p><details><summary>'+Object.keys(packet.context || {}).length+' linked public inputs</summary><div class="table-wrap"><table><thead><tr><th>Engine</th><th>Source compiled</th><th>Declared quality</th><th>Role</th></tr></thead><tbody>'+
      Object.entries(packet.context || {}).map(([name,r]) => '<tr><td>'+link(r.source,name)+'</td><td>'+esc(r.source_generated_at || 'Unavailable')+'</td><td>'+esc(r.declared_quality)+'</td><td>Context only</td></tr>').join('')+'</tbody></table></div></details></section>'+
      '<section><h2>Method and portfolio use</h2><p>Historical ranks describe current-vintage observations. They do not establish future returns, crisis probabilities or profitable trading rules. Options pagination can complete while IV timing remains unverified.</p><p>A qualified decision model still needs point-in-time inputs, costs, out-of-sample validation and a defined risk budget. Until then, use the scenario tool to state your own shocks and inspect consequences; Risk Regime supplies no suggested position size.</p>'+
      '<details><summary>Source failures ('+Object.keys(packet.source_failures || {}).length+')</summary><ul>'+Object.entries(packet.source_failures || {}).map(([name,reason]) => '<li>'+esc(name)+': '+esc(reason)+'</li>').join('')+'</ul></details></section>';
  }
  async function start() {
    const target = document.getElementById('risk-research'); if (!target) return;
    const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 20000);
    try {
      const response = await fetch('/data/risk-regime.json', {cache:'no-store', signal:controller.signal});
      if (!response.ok) throw new Error('HTTP '+response.status);
      target.innerHTML = render(await response.json());
    } catch (_) { target.innerHTML = '<section class="notice" role="alert"><h2>Research temporarily unavailable</h2><p>The public packet could not be loaded. No score or position advice is shown.</p><button type="button" id="risk-retry">Retry</button></section>'; document.getElementById('risk-retry').addEventListener('click',start); }
    finally { clearTimeout(timer); }
  }
  const api = {render, fmt, link, history};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof document !== 'undefined') start();
  root.JHRiskResearch = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
