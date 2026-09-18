(function (root) {
  'use strict';
  const names = {debt_to_penny: 'Federal debt · daily', tga_operating_cash: 'Treasury cash · daily',
    avg_interest_rates: 'Average borrowing rates · monthly', interest_expense: 'Interest expense · monthly',
    debt_outstanding: 'Federal debt · fiscal year end', rates_of_exchange: 'Treasury reporting FX · quarterly'};
  const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function link(key, label) {
    if (typeof key !== 'string' || !/^data\/[a-zA-Z0-9_./-]+$/.test(key) || key.includes('..')) return 'Evidence unavailable';
    return '<a href="/' + esc(key) + '" target="_blank" rel="noopener">' + esc(label) + '</a>';
  }
  function freshness(row, now) {
    const f = row.freshness || {}, measure = (row.measurements || [])[0] || {};
    const date = measure.as_of;
    const time = /^\d{4}-\d{2}-\d{2}$/.test(date || '') ? Date.parse(date + 'T00:00:00Z') : NaN;
    const age = Math.floor(now / 86400000) - Math.floor(time / 86400000);
    if (!Number.isFinite(age) || age < 0 || !Number.isFinite(f.max_age_days)) return 'unavailable';
    if (age > f.max_age_days) return 'stale · ' + age + ' days';
    return esc(f.status || 'unavailable') + ' · observed ' + esc(date) + ' · ' + age + ' days ago';
  }
  function render(packet, now = Date.now()) {
    let sections = '', found = 0;
    for (const [id, title] of Object.entries(names)) {
      const dataset = packet[id] || {};
      if (dataset.dimension_contract !== 'treasury-fiscal-warehouse.v2') {
        sections += '<details><summary>' + title + ' · unavailable</summary><p>Verified dimensional data is unavailable.</p></details>';
        continue;
      }
      found++;
      const rows = (dataset.measurements || []).map(m => {
        const dim = Object.values(m.dimensions || {}).join(' / ') || 'All federal debt';
        const value = m.value_decimal == null ? '—' : m.value_decimal;
        const body = '<tr><td>' + esc(m.field.replace(/_/g, ' ')) + '<br><small>' + esc(dim) + '</small></td>' +
          '<td class="r">' + esc(value) + '</td><td>' + esc(m.unit) + '</td><td>' + esc(m.as_of) + '</td><td><details><summary>Inspect</summary>' +
          '<p>Series: <code>' + esc(m.series_id) + '</code></p><p>Original response row ' + esc(m.row_index) + ' (zero based). ' +
          link(m.source_key, 'Retained provider response') + '</p></details></td></tr>';
        return body;
      }).join('');
      const check = dataset.reconciliation || {};
      sections += '<details' + (id === 'debt_to_penny' ? ' open' : '') + '><summary>' + title + ' · ' + freshness(dataset, now) + '</summary>' +
        '<p>' + esc(dataset.usage) + '</p><p>Reconciliation: ' + esc(check.status || 'unavailable') +
        (check.difference_decimal != null ? ' · difference ' + esc(check.difference_decimal) + ' ' + esc(check.unit) : '') +
        '</p><p>' + link((dataset.replay || {}).manifest_key, 'Replay inputs and calculation version') + '</p>' +
        '<div class="tf-table"><table class="tbl"><thead><tr><th>Measurement / dimension</th><th>Exact reported value</th><th>Unit</th><th>Observation date</th><th>Evidence</th></tr></thead><tbody>' + rows + '</tbody></table></div></details>';
    }
    return '<p>' + found + '/6 datasets available · packet generated ' + esc(packet.generated_at || 'unknown') +
      '</p><p>Current-vintage research measurements. Original publication times and historical investment performance are not established. Reporting FX is not a spot valuation quote.</p>' + sections;
  }
  async function refresh(host, fetcher = root.fetch.bind(root)) {
    try {
      const response = await fetcher('/data/treasury-fiscal.json', {cache: 'no-store'});
      if (!response.ok) throw new Error('feed unavailable');
      host.innerHTML = render(await response.json());
    } catch (_) {
      host.innerHTML = '<p role="status">Treasury data is unavailable. Earlier values have been cleared.</p>';
    }
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = {render, freshness, link, refresh};
  if (root.document) {
    const host = root.document.getElementById('treasury-fiscal-research');
    if (host) {refresh(host); root.setInterval(() => refresh(host), 300000);}
  }
})(typeof window !== 'undefined' ? window : globalThis);
