/* jh-reskin-skip */
/* Native source calculation. Source text never becomes HTML or a trade signal. */
(function () {
  'use strict';
  const host = document.getElementById('jh-liquidity-research');
  if (!host) return;
  const ids = ['WALCL', 'WTREGEN', 'RRPONTSYD'];
  const numeric = value => typeof value === 'number' && Number.isFinite(value);
  const amount = value => numeric(value) ? value.toLocaleString('en-US', {minimumFractionDigits: 3, maximumFractionDigits: 3}) : 'Unavailable';
  const day = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value+'T00:00:00Z')) && new Date(value+'T00:00:00Z').toISOString().slice(0,10) === value ? value : 'Unavailable';
  function node(tag, text, className) {
    const item = document.createElement(tag); if (text !== undefined) item.textContent = text;
    if (className) item.className = className; return item;
  }
  function link(parent, text, url) { const a = node('a', text); a.href = url; parent.appendChild(a); }
  function table(headers, rows) {
    const wrap = node('div', undefined, 'lr-scroll'); wrap.tabIndex = 0; wrap.setAttribute('aria-label', headers[0]+' data table');
    const t = node('table'), head = node('thead'), tr = node('tr'), body = node('tbody');
    headers.forEach(text => { const th = node('th', text); th.scope = 'col'; tr.appendChild(th); }); head.appendChild(tr); t.appendChild(head);
    rows.forEach(values => { const row = node('tr'); values.forEach(value => { const td = node('td'); if (typeof value === 'string') td.textContent = value; else td.appendChild(value); row.appendChild(td); }); body.appendChild(row); });
    t.appendChild(body); wrap.appendChild(t); return wrap;
  }
  function render(packet) {
    host.replaceChildren(node('h2', 'US net-liquidity source calculation'));
    if (!packet || packet.contract !== 'liquidity-flow-research.v1' || packet.calls_eligible !== false || packet.sizing_eligible !== false) throw Error('Qualified native packet unavailable');
    const snapshot = packet.last_reconstructed_snapshot, series = packet.series || {};
    if (!snapshot || ids.some(id => !series[id] || !snapshot.legs || !snapshot.legs[id])) throw Error('Incomplete source coordinates');
    const fresh = ids.every(id => {
      const received = Date.parse(series[id].acquired_at), age = (Date.now()-received)/3600000;
      return Number.isFinite(received) && age >= 0 && age <= 26;
    });
    const generated = Date.parse(packet.generated_at), source = Date.parse(packet.source_generated_at);
    const current = fresh && Number.isFinite(generated) && generated <= Date.now() && Number.isFinite(source) && source <= generated && (Date.now()-source)/3600000 <= 26 && packet.quality && packet.quality.status === 'fresh';
    host.appendChild(node('p', current ? 'Descriptive proxy · acquisition age within limit · release calendar unverified' : 'Current value withheld · source stale, missing or not yet eligible', 'lr-status'));
    const net = current && packet.current ? packet.current.net_liquidity_b : null;
    host.appendChild(node('div', numeric(net) ? '$'+amount(net)+' bn' : 'Current value unavailable', 'lr-number'));
    host.appendChild(node('p', 'WALCL − WTREGEN − RRPONTSYD. Wednesday balance-sheet level, weekly-average Treasury balance, and daily reverse-repo operation amount have different measurement bases. This is not investable cash or an allocation instruction.'));
    host.appendChild(node('p', 'Retained snapshot date: '+day(snapshot.valuation_date)+'. Calculation produced: '+new Date(generated).toISOString()+'. Historical rows use the retrieved vintage; they do not establish what was known at the time.'));
    const rows = ids.map(id => {
      const s = series[id], leg = snapshot.legs[id], cell = node('span');
      link(cell, id, 'https://fred.stlouisfed.org/series/'+id);
      return [cell, s.measurement_basis || 'Unavailable', day(leg.selected && leg.selected.observation_date),
        amount(leg.value && leg.value.value), numeric(leg.carry_days) ? String(leg.carry_days)+' days' : 'Unavailable'];
    });
    host.appendChild(node('h3', 'Selected inputs in the retained snapshot'));
    host.appendChild(table(['Series', 'Measurement basis', 'Observation date', 'USD billions', 'Carry to snapshot'], rows));
    host.appendChild(node('h3', 'Signed contributions to the proxy change'));
    const changes = ['1d','1w','1m','3m'].map(label => {
      const row = (packet.comparisons || {})[label]; if (!row) return [label, 'Unavailable', 'Unavailable', 'Unavailable', 'Unavailable', 'Unavailable'];
      const values = ids.map(id => amount(row.legs && row.legs[id] && row.legs[id].signed_formula_contribution && row.legs[id].signed_formula_contribution.value));
      return [label, day(row.baseline_valuation_date)+' → '+day(row.current_valuation_date), ...values, amount(row.change && row.change.value)];
    });
    host.appendChild(table(['Window', 'Calendar endpoints', '+ WALCL', '− WTREGEN', '− RRP', 'Net change, USD bn'], changes));
    host.appendChild(node('p', '1m and 3m use calendar months. These are reported level differences, not a causal estimate of money entering markets. Repeated weekly observations can contribute zero to a daily change.'));
    host.appendChild(node('p', 'Research status: WAIT / abstain. No validated return forecast, position size or portfolio weight follows from this measurement. WAIT does not instruct liquidation.'));
    const links = node('div', undefined, 'lr-links');
    link(links, 'Complete observations and source receipts', '/data/liquidity-flow.json?exact=1&nogen=1');
    const key = packet.replay && packet.replay.manifest_key;
    if (typeof key === 'string' && /^data\/liquidity-flow-research\/runs\/[a-f0-9]{64}\.json$/.test(key)) link(links, 'Reproduction record', '/'+key+'?exact=1&nogen=1');
    link(links, 'Settlement-fails definitions', '/fails.html'); host.appendChild(links);
  }
  function unavailable() {
      host.replaceChildren(node('h2','US net-liquidity source calculation'),node('p','The source calculation is not available. No current liquidity amount or directional conclusion is inferred.','lr-status'));
  }
  function display(packet) {
    try { render(packet); } catch (_) { unavailable(); return; }
    // A page left open must not keep a fresh label after its source expires.
    // Re-evaluate the same retained snapshot without triggering acquisition.
    setTimeout(() => display(packet), 60000);
  }
  fetch('/data/liquidity-flow.json?exact=1&nogen=1', {cache:'no-store',credentials:'omit'})
    .then(response => { if (!response.ok) throw Error('Source unavailable'); return response.json(); })
    .then(display).catch(unavailable);
})();
