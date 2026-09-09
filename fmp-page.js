(function () {
  'use strict';
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = value => value !== null && value !== undefined && value !== '' && typeof value !== 'boolean' && Number.isFinite(Number(value)) ? Number(value) : null;
  const percent = value => { const n = number(value); return n === null ? 'Unavailable' : (n > 0 ? '+' : '') + n.toFixed(2) + '%'; };
  const price = value => { const n = number(value); return n === null ? 'Unavailable' : n.toLocaleString(undefined, {minimumFractionDigits:2,maximumFractionDigits:4}); };
  function table(title, rows, columns) {
    if (!Array.isArray(rows) || !rows.length) return '<h2 class="sec">' + escape(title) + '</h2><p>No provider rows available.</p>';
    return '<h2 class="sec">' + escape(title) + ' · ' + rows.length + '</h2><div style="overflow-x:auto"><table><thead><tr>' +
      columns.map(([name]) => '<th>' + escape(name) + '</th>').join('') + '</tr></thead><tbody>' +
      rows.map(row => '<tr>' + columns.map(([,get]) => '<td>' + escape(get(row)) + '</td>').join('') + '</tr>').join('') + '</tbody></table></div>';
  }
  function render(data) {
    if (!data || data.agent !== 'fmp-fundamentals-agent' || !data.watchlist_quotes || Array.isArray(data.watchlist_quotes) || !data.movers || data.error) throw new Error('invalid snapshot');
    const quotes = Object.values(data.watchlist_quotes), sectors = data.sector_performance || [];
    const observed = row => { const t = number(row.timestamp), date = new Date(t === null ? NaN : t * 1000); return Number.isFinite(date.getTime()) ? date.toISOString() : 'Unavailable'; };
    const columns = [['Symbol',r=>r.symbol],['Name',r=>r.name],['Price',r=>price(r.price)],['Change',r=>percent(r.changePercentage ?? r.changesPercentage)],['Volume',r=>number(r.volume) === null ? 'Unavailable' : Number(r.volume).toLocaleString()],['Observation UTC',observed]];
    let html = table('Watchlist quotes', quotes, columns) + table('Index quotes', data.index_quotes || [], columns);
    html += table('Sector performance', sectors, [['Sector',r=>r.sector],['Exchange',r=>r.exchange],['Date',r=>r.date],['Change',r=>percent(r.averageChange ?? r.changesPercentage)]]);
    for (const [name,title] of [['gainers','Gainers'],['losers','Losers'],['actives','Most active']]) html += table(title,data.movers[name] || [],columns);
    const sources = Object.entries(data.source_health || {}).map(([name,value])=>({name,...value}));
    html += table('Source coverage',sources,[['Feed',r=>r.name],['Status',r=>r.status],['Rows',r=>r.row_count],['Reason',r=>r.reason || ''],['HTTP',r=>r.http_status ?? '']]);
    const status = ['READY','PARTIAL','UNAVAILABLE'].includes(data.status) ? data.status : 'UNVERIFIED';
    return {status,html,summary:(data.quotes_ok ?? 'Unverified') + ' valid / ' + (data.watchlist || []).length + ' expected quotes · ' + quotes.length + ' rows · Collected ' + (data.generated_at || data.ts || 'Unknown')};
  }
  async function load() {
    const status = document.getElementById('sb'), output = document.getElementById('out');
    let delay = 300000;
    try {
      const response = await fetch(FMP_API_URL,{signal:AbortSignal.timeout(25000),cache:'no-store'});
      if (!response.ok) throw new Error('unavailable response');
      const data = await response.json(), view = render(data);
      status.textContent = view.status + ' · ' + view.summary;
      output.innerHTML = view.html;
      document.getElementById('fmp-payload').textContent = JSON.stringify(data,null,2);
    } catch (_) {
      status.textContent = 'Unavailable';
      output.textContent = 'Unable to load the FMP engine. Retrying in one minute.';
      document.getElementById('fmp-payload').textContent = '';
      delay = 60000;
    } finally { setTimeout(load,delay); }
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = {render,percent,price};
  if (typeof document !== 'undefined') document.addEventListener('DOMContentLoaded',load,{once:true});
})();
