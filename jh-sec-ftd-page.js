(function () {
  'use strict';
  const api = window.JHSecFtdResearch, desk = document.querySelector('[data-sec-ftd]');
  if (!desk || !api) return;
  const el = name => desk.querySelector('[data-ftd-' + name + ']'), query = new URLSearchParams(location.search);
  let pinned = query.has('run') ? query.get('run') : null, state = null, selected = null, scenario = null;
  let stamp = query.get('date'), loadToken = 0, selectToken = 0;
  const meanings = {
    no_preceding_date_in_selected_archives: 'First reported date in selected archives',
    prior_cusip_record_not_reported: 'Prior CUSIP record not reported; comparison unavailable',
    reported_label_changed_comparison_unqualified: 'Reported label changed; comparison withheld',
    reported_previous_balance_zero: 'Prior balance is zero; percentage unavailable',
    adjacent_reported_balances_only: 'Adjacent reported balances; no flow or trade inference'
  };
  const fmt = value => value === null || value === undefined || value === '.' ? 'Unavailable' :
    typeof value === 'boolean' ? (value ? 'Yes' : 'No') : meanings[value] || String(value).replace(/^([-+]?\d+)(\.\d+)?$/,
      (_, whole, fraction) => whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',') + (fraction || ''));
  function node(tag, text) { const item = document.createElement(tag); if (text !== undefined) item.textContent = text; return item; }
  function paragraph(host, text) { host.append(node('p', text)); }
  function link(host, key, label) { const a = node('a', label); a.href = '/' + key; a.target = '_blank'; a.rel = 'noopener'; host.append(a); }
  function table(target, headers, rows) {
    const host = el(target); host.replaceChildren();
    const wrap = node('div'), t = node('table'), head = node('thead'), h = node('tr'), body = node('tbody');
    wrap.className = 'ftd-table-wrap';
    headers.forEach(text => { const th = node('th', text); th.scope = 'col'; h.append(th); }); head.append(h); t.append(head);
    rows.forEach(row => { const tr = node('tr'); row.forEach(value => tr.append(node('td', fmt(value)))); body.append(tr); });
    t.append(body); wrap.append(t); host.append(wrap);
  }
  function clearScenario() { scenario = null; el('scenario').replaceChildren(); }
  function reset() {
    selected = null; clearScenario();
    ['export', 'calculate', 'date'].forEach(name => el(name).disabled = true);
    ['identity', 'observation-status', 'observation', 'source', 'history', 'chart', 'audit', 'date'].forEach(name => el(name).replaceChildren());
  }
  function permalink() {
    const url = new URL(location.href); url.searchParams.set('run', state.runId);
    url.searchParams.delete('symbol');
    if (selected) url.searchParams.set('cusip', selected.issue.cusip);
    if (stamp) url.searchParams.set('date', stamp);
    history.replaceState(null, '', url);
  }
  function observe() {
    clearScenario(); if (!state || !selected) return;
    const value = api.observation(state, selected, stamp), p = value.point, source = value.source;
    el('export').disabled = false; el('calculate').disabled = !p;
    el('observation-status').textContent = selected.issue.cusip + ' · ' + stamp + ' · ' + (value.missing_reason || 'Exact reported source row available');
    table('observation', ['Measurement', 'Recorded value'], [
      ['Outstanding fail balance, shares', p?.fail_balance_shares], ['Reported label', p ? p.reported_symbol || 'Not reported' : null],
      ['Reported description', p?.reported_description], ['Prior date in selected population', p?.previous_reported_date],
      ['Prior CUSIP record present', p?.previous_record_present], ['Prior reported balance, shares', p?.previous_fail_balance_shares],
      ['Adjacent balance difference, shares', p?.adjacent_balance_change_shares], ['Adjacent balance difference, %', p?.adjacent_balance_change_pct],
      ['Comparison meaning', p?.comparison_status], ['Reported prior-day price (currency unspecified)', p?.reported_previous_day_price]
    ]);
    const host = el('source'); host.replaceChildren();
    if (source) {
      paragraph(host, 'Source member ' + source.member.name + ' · line ' + p.source_line + ' (one-based) · archive ' + (p.source_index + 1) + ' of ' + state.packet.sources.length + '.');
      paragraph(host, 'Requested ' + source.source_requested_at + ' · acquired ' + source.source_received_at + '. These are acquisition times, not original publication times.');
      paragraph(host, 'Retained ZIP SHA-256: ' + source.original.sha256 + ' · ' + fmt(source.original.bytes) + ' bytes.');
      paragraph(host, 'Decoded member SHA-256: ' + source.member.sha256 + '.');
      if (/^https:\/\/www\.sec\.gov\/files\/data\/(?:other\/)?fails-deliver-data\/cnsfails20\d{4}[ab]\.zip$/.test(source.url)) {
        const a = node('a', 'Open provider archive'); a.href = source.url; a.target = '_blank'; a.rel = 'noopener'; host.append(a);
      }
      link(host, selected.artifact.key, 'Recorded CUSIP bucket');
    } else paragraph(host, 'No source row exists for this selection. A last-known balance is not substituted.');
    permalink();
  }
  function chart() {
    const host = el('chart'); host.replaceChildren(); if (!selected) return;
    const dates = state.packet.dates, points = new Map(selected.row.observations.map(point => [point[2], point]));
    const maximum = selected.row.observations.reduce((a, point) => BigInt(point[5]) > a ? BigInt(point[5]) : a, 0n);
    const width = Math.max(320, Math.min(1000, host.clientWidth)), height = 190, ns = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(ns, 'svg'); svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
    svg.setAttribute('role', 'img'); svg.setAttribute('aria-label', 'Outstanding fail balances in shares for ' + selected.issue.cusip + '; missing records and changed labels break the line.'); svg.classList.add('ftd-chart');
    function shape(tag, attributes, text) { const item = document.createElementNS(ns, tag); Object.entries(attributes).forEach(([k,v]) => item.setAttribute(k, v)); if (text) item.textContent = text; svg.append(item); return item; }
    shape('line', {x1: 100, y1: 145, x2: width - 20, y2: 145});
    shape('text', {x: 8, y: 22}, 'Shares'); shape('text', {x: 8, y: 39}, fmt(maximum.toString())); shape('text', {x: 8, y: 145}, '0');
    const first = Date.parse(dates[0]), last = Date.parse(dates.at(-1)); let path = '', pen = false;
    dates.forEach(day => {
      const point = points.get(day); if (!point) { pen = false; return; }
      const x = 100 + (Date.parse(day) - first) / (last - first || 1) * (width - 120);
      const y = 140 - Number(BigInt(point[5]) * 11000n / (maximum || 1n)) / 100;
      if (point[12] === 'reported_label_changed_comparison_unqualified') pen = false;
      path += (pen ? ' L ' : ' M ') + x + ' ' + y; pen = true;
      const dot = shape('circle', {cx: x, cy: y, r: 2}); const title = document.createElementNS(ns, 'title');
      title.textContent = day + ': ' + fmt(point[5]) + ' shares'; dot.append(title);
    });
    shape('path', {d: path}); shape('text', {x: 100, y: 172}, dates[0]); shape('text', {x: width - 20, y: 172, 'text-anchor': 'end'}, dates.at(-1)); host.append(svg);
  }
  function historyTable() {
    table('history', ['Date', 'Balance, shares', 'Reported label', 'Difference, shares', 'Difference, %', 'Interpretation', 'Archive / line'],
      state.packet.dates.map(day => { const value = api.observation(state, selected, day), p = value.point; return [day,
        p?.fail_balance_shares, p ? p.reported_symbol || 'Not reported' : null, p?.adjacent_balance_change_shares,
        p?.adjacent_balance_change_pct, p?.comparison_status || 'Not reported; no zero imputed', p ? (p.source_index + 1) + ' / ' + p.source_line : null]; }));
  }
  async function select(cusip) {
    const token = ++selectToken; reset(); el('status').textContent = 'Verifying the exact CUSIP history…';
    try {
      const value = await api.record(fetch, state, cusip); if (token !== selectToken) return;
      selected = value; el('identity').textContent = 'CUSIP ' + cusip + ' · ' + selected.row.observations.length + ' reported observations · ' + selected.row.missing_reported_dates.length + ' unreported dates in this selection.';
      el('date').replaceChildren(...state.packet.dates.map(day => { const option = node('option', day); option.value = day; return option; })); el('date').disabled = false;
      const audit = el('audit'); link(audit, state.reference, 'Recorded run'); link(audit, state.run.input.key, 'Input inventory'); link(audit, state.run.output.key, 'Recorded output'); link(audit, selected.artifact.key, 'CUSIP evidence');
      paragraph(audit, 'Output SHA-256: ' + state.run.output_sha256 + '.');
      Object.entries(state.run.compilers).forEach(([name, ref]) => link(audit, ref.key, name + '.py'));
      historyTable(); chart();
      if (!stamp) stamp = state.packet.dates.at(-1);
      if (!state.packet.dates.includes(stamp)) {
        el('date').selectedIndex = -1; el('status').textContent = 'The requested date is absent from this recorded population. Choose a reported settlement; no date was substituted.'; return;
      }
      el('date').value = stamp; observe(); el('status').textContent = 'Recorded output and selected CUSIP verified. Dated observations; no qualified trade signal.';
    } catch (error) { if (token === selectToken) { reset(); el('status').textContent = error.message; } }
  }
  function choices() {
    const matches = api.findIssues(state, el('query').value); el('choice').replaceChildren();
    if (matches.length > 1) { const blank = node('option', 'Choose an exact reported CUSIP'); blank.value = ''; el('choice').append(blank); }
    matches.forEach(item => { const option = node('option', item.cusip + ' · ' + item.reported_labels.map(v => v.symbol || '(no ticker)').join(' / ')); option.value = item.cusip; el('choice').append(option); });
    el('choice').disabled = !matches.length;
    return matches;
  }
  function suggest() {
    const q = el('query').value.trim().toUpperCase(), list = document.getElementById('ftd-identities'); list.replaceChildren(); if (!state || !q) return;
    const matches = state.packet.issues.filter(item => item.cusip.startsWith(q) || item.reported_labels.some(label => label.symbol.startsWith(q) || label.description.toUpperCase().includes(q))).slice(0,20);
    matches.forEach(item => { const option = node('option'); option.value = item.cusip; option.label = item.reported_labels.map(v => (v.symbol || 'No ticker') + ' · ' + v.description).join(' / '); list.append(option); });
  }
  async function load() {
    const token = ++loadToken; ++selectToken; reset(); state = null; el('load').disabled = true; el('choice').disabled = true;
    el('choice').replaceChildren(); el('metrics').replaceChildren(); el('record').replaceChildren(); el('coverage').replaceChildren(); el('status').textContent = 'Verifying the recorded publication…';
    try {
      const value = await api.load(fetch, pinned); if (token !== loadToken) return; state = value; pinned = value.runId;
      const p = state.packet, count = p.counts;
      [['Original rows',count.original_rows],['Reported CUSIPs',count.cusips],['Reported dates',count.reported_dates],['Ambiguous labels',count.symbols_with_multiple_reported_cusips]].forEach(([label, value]) => {
        const card = node('div'); card.className = 'ftd-metric'; card.append(node('span',label),node('strong',fmt(value))); el('metrics').append(card);
      });
      const lag = Math.max(0, Math.floor((Date.now() - Date.parse(p.settlement_date + 'T00:00:00Z')) / 86400000));
      el('record').textContent = 'Recorded ' + p.generated_at + ' · observations ' + p.dates[0] + ' through ' + p.settlement_date + ' · latest observation ' + lag + ' calendar days ago. Compilation time is not observation freshness.';
      table('coverage', ['Provider file', 'First / last reported date', 'Rows', 'Count control', 'Quantity checksum (integrity only)', 'Acquired'], p.sources.map(source => [source.member.name,
        Object.keys(source.reported_dates)[0] + ' / ' + Object.keys(source.reported_dates).at(-1), source.rows,
        source.integrity_controls.reported_record_count, source.integrity_controls.reported_quantity_sum, source.source_received_at]));
      el('load').disabled = false; permalink();
      el('status').textContent = 'Recorded output verified. Select a reported label or CUSIP to inspect its history.';
      const initial = el('query').value || query.get('cusip') || query.get('symbol');
      if (initial) { el('query').value = initial; const matches = choices(); if (matches.length === 1) await select(matches[0].cusip); else el('status').textContent = matches.length ? 'This label maps to several CUSIPs. Choose the exact reported identity.' : 'The requested identity is absent from this recorded population.'; }
    } catch (error) { if (token === loadToken) { state = null; reset(); ['metrics', 'record', 'coverage'].forEach(name => el(name).replaceChildren()); el('load').disabled = true; el('status').textContent = error.message; } }
  }
  el('search').addEventListener('submit', event => { event.preventDefault(); if (!state) return;
    const chosen = el('choice').value, matches = choices();
    if (!matches.length) { el('status').textContent = 'No exact label or CUSIP is reported in this record. Choose a suggested CUSIP.'; return; }
    if (matches.length > 1 && !matches.some(item => item.cusip === chosen)) { el('status').textContent = 'Choose a CUSIP; shared labels do not establish the same security.'; return; }
    const cusip = matches.length === 1 ? matches[0].cusip : chosen; el('choice').value = cusip; select(cusip);
  });
  el('query').addEventListener('input', () => { ++selectToken; reset(); el('choice').replaceChildren(); el('choice').disabled = true; suggest(); if (state) el('status').textContent = 'Choose an exact reported identity to inspect.'; });
  el('choice').addEventListener('change', () => { if (el('choice').value) select(el('choice').value); else { ++selectToken; reset(); el('status').textContent = 'Choose an exact reported CUSIP.'; } });
  el('date').addEventListener('change', () => { stamp = el('date').value; observe(); el('status').textContent = 'Recorded output and selected CUSIP verified. Dated observations; no qualified trade signal.'; });
  el('refresh').addEventListener('click', load); el('form').addEventListener('input', clearScenario);
  el('form').addEventListener('submit', event => { event.preventDefault(); clearScenario(); if (!selected || !api.observation(state, selected, stamp).point) return;
    try { scenario = api.scenario(Object.fromEntries(new FormData(el('form')).entries())); el('scenario').textContent = selected.issue.cusip + ' · assumed P&L ' + scenario.pnl_usd + ' USD · signed reference exposure ' + scenario.signed_notional_usd + ' USD. User assumptions only; no forecast or borrow assurance.'; }
    catch (error) { el('scenario').textContent = error.message; }
  });
  el('export').addEventListener('click', () => { if (!state || !selected) return;
    const data = {contract: 'sec-ftd-selected-evidence-export.v1', run: state.reference, output_sha256: state.run.output_sha256,
      artifact: selected.artifact, point_fields: state.packet.point_fields, record: selected.row, sources: state.packet.sources,
      selected_observation: api.observation(state, selected, stamp), scenario};
    const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], {type: 'application/json'})), a = node('a');
    a.href = url; a.download = 'sec-cns-' + selected.issue.cusip + '-' + state.runId.slice(0,12) + '.json'; document.body.append(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 60000);
  });
  window.addEventListener('resize', () => { if (selected) chart(); }); load();
})();
