(function () {
  'use strict';
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const display = value => value === null || value === undefined ? 'Unavailable' : typeof value === 'object' ? JSON.stringify(value) : String(value);
  const categories = {market_indices:'Market indices',economic_indicators:'Economic indicators',housing:'Housing',labor:'Labor'};
  const object = value => value && typeof value === 'object' && !Array.isArray(value);
  function render(data) {
    if (!object(data) || data.agent !== 'nasdaq-datalink-agent' || !object(data.categories) || data.error) throw new Error('invalid snapshot');
    let html = '';
    for (const [category, datasets] of Object.entries(data.categories)) {
      if (!object(datasets)) throw new Error('invalid datasets');
      html += '<h2 class="cat">' + escape(categories[category] || category) + '</h2><div class="gr">';
      for (const [code, item] of Object.entries(datasets)) {
        if (!object(item)) throw new Error('invalid dataset');
        html += '<article class="cd"><h3 class="cn">' + escape(item.name || code) + '</h3><p class="cc">' + escape(code) + '</p>';
        html += '<p>Provider: ' + escape(item.provider || 'Unverified') + (item.fallback_used ? ' · fallback active' : '') + '</p>';
        if (item.primary_provider_status) html += '<p>NASDAQ availability: ' + escape(item.primary_provider_status.error) + ' · HTTP ' + escape(item.primary_provider_status.http_status ?? 'Unavailable') + '</p>';
        if (item.error) html += '<p>Unavailable: ' + escape(item.error) + (item.http_status ? ' · HTTP ' + escape(item.http_status) : '') + '</p>';
        html += '<p>Observation change: ' + (typeof item.change_pct === 'number' && Number.isFinite(item.change_pct) ? escape(item.change_pct) + '%' : 'Unavailable') + '</p><p>' + escape(item.change_scope || 'Observation interval unavailable') + '</p>';
        for (const [title, row] of [['Latest observation',item.latest],['Previous observation',item.previous]]) {
          html += '<h4>' + title + '</h4>';
          if (object(row) && Object.keys(row).length) for (const [key,value] of Object.entries(row)) html += '<div class="dr"><span class="dl">' + escape(key) + '</span><span class="dv">' + escape(display(value)) + '</span></div>';
          else html += '<p>Unavailable</p>';
        }
        const history = item.history || [];
        if (!Array.isArray(history)) throw new Error('invalid history');
        html += '<details><summary>All returned history · ' + history.length + ' rows</summary><pre>' + escape(JSON.stringify(history,null,2)) + '</pre></details></article>';
      }
      html += '</div>';
    }
    const status = ['READY','PARTIAL','UNAVAILABLE'].includes(data.status) ? data.status : 'UNVERIFIED';
    return {html,status,summary:display(data.metrics_ok) + ' available · ' + display(data.metrics_err) + ' unavailable · Collected ' + display(data.generated_at || data.ts)};
  }
  async function load() {
    let delay = 300000;
    const status = document.getElementById('sb'), output = document.getElementById('out'), payload = document.getElementById('nasdaq-payload');
    try {
      const response = await fetch(NASDAQ_API_URL,{cache:'no-store',signal:AbortSignal.timeout(110000)});
      if (!response.ok) throw new Error('unavailable response');
      const data = await response.json(), view = render(data);
      status.textContent = view.status + ' · ' + view.summary;
      output.innerHTML = view.html;
      payload.textContent = JSON.stringify(data,null,2);
    } catch (_) {
      status.textContent = 'Unavailable';output.textContent = 'Unable to load the NASDAQ engine. Retrying in one minute.';payload.textContent = '';delay = 60000;
    } finally { setTimeout(load,delay); }
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = {render,display};
  if (typeof document !== 'undefined') document.addEventListener('DOMContentLoaded',load,{once:true});
})();
