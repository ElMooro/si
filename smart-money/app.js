// Smart Money Tracker — JustHodl.AI
const API_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/smart-money.json";

let DATA = null;
let FILTERED = [];
let SORT = { col: 'market_value', dir: 'desc' };

async function load() {
  try {
    const r = await fetch(API_URL, { cache: 'default' });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    DATA = await r.json();
    document.getElementById('status').textContent = 'live';
    document.getElementById('status').style.color = 'var(--pos)';
    render();
  } catch (e) {
    document.getElementById('status').textContent = 'offline';
    document.getElementById('status').style.color = 'var(--neg)';
    document.getElementById('all-body').innerHTML =
      '<tr><td colspan="9" style="text-align:center;padding:40px;color:var(--text-mute)">Failed to load: ' + e.message + '</td></tr>';
  }
}

function fmtMv(v) {
  if (v == null || v === 0) return '-';
  const ab = Math.abs(v);
  if (ab >= 1e12) return '$' + (v/1e12).toFixed(2) + 'T';
  if (ab >= 1e9)  return '$' + (v/1e9).toFixed(1) + 'B';
  if (ab >= 1e6)  return '$' + (v/1e6).toFixed(0) + 'M';
  return '$' + v.toFixed(0);
}

function fmtPct(v) {
  if (v == null) return '-';
  return (v > 0 ? '+' : '') + v.toFixed(1) + '%';
}

function shortName(name) {
  if (!name) return '-';
  return name.replace(/\s+(INC|LLC|CORP|LP|LTD|GROUP|TRUST|CORPORATION|HOLDINGS|MANAGEMENT|ASSET|CAPITAL|PARTNERS)\b/gi, '').trim();
}

function rowHtml(f, rank, valCol) {
  // valCol: 'qoq_pct' | 'added' | 'removed' | 'activity' | 'mv'
  let valText = '', valClass = '';
  if (valCol === 'qoq_pct') {
    valText = fmtPct(f.qoq_pct);
    valClass = (f.qoq_pct || 0) > 0 ? 'pos' : ((f.qoq_pct || 0) < 0 ? 'neg' : '');
  } else if (valCol === 'added') {
    valText = '+' + (f.added || 0);
    valClass = (f.added || 0) > 0 ? 'pos' : '';
  } else if (valCol === 'removed') {
    valText = '-' + (f.removed || 0);
    valClass = (f.removed || 0) > 0 ? 'neg' : '';
  } else {
    valText = (f.added || 0) + ' / ' + (f.removed || 0);
    valClass = '';
  }
  return '<div class="filer-row">' +
    '<span class="filer-rank">#' + rank + '</span>' +
    '<span class="filer-name">' + shortName(f.name) + '</span>' +
    '<span class="filer-meta">' + fmtMv(f.mv) + '</span>' +
    '<span class="filer-num ' + valClass + '">' + valText + '</span>' +
    '</div>';
}

function render() {
  const filers = DATA.filers || [];
  const summary = DATA.summary || {};

  document.getElementById('stat-total').textContent = filers.length;
  document.getElementById('stat-quarter').textContent = DATA.as_of_quarter || 'latest 13F';

  const topGainer = (summary.biggest_gainers || [])[0];
  document.getElementById('stat-gainer').textContent = topGainer ? fmtPct(topGainer.qoq_pct) : '-';
  document.getElementById('stat-gainer-sub').textContent = topGainer ? shortName(topGainer.name) : '';

  const topDecliner = (summary.biggest_decliners || [])[0];
  document.getElementById('stat-decliner').textContent = topDecliner ? fmtPct(topDecliner.qoq_pct) : '-';
  document.getElementById('stat-decliner-sub').textContent = topDecliner ? shortName(topDecliner.name) : '';

  const topActive = (summary.most_active || [])[0];
  document.getElementById('stat-active').textContent = topActive
    ? ((topActive.added || 0) + (topActive.removed || 0)) : '-';
  document.getElementById('stat-active-sub').textContent = topActive ? shortName(topActive.name) : '';

  const gen = new Date(DATA.generated_at);
  document.getElementById('gen-time').textContent = gen.toUTCString().substring(5, 22);

  // 4 panels
  document.getElementById('gainers').innerHTML =
    (summary.biggest_gainers || []).map(function(f, i){ return rowHtml(f, i+1, 'qoq_pct'); }).join('') ||
    '<div style="padding:20px;color:var(--text-mute);text-align:center">No data</div>';
  document.getElementById('decliners').innerHTML =
    (summary.biggest_decliners || []).map(function(f, i){ return rowHtml(f, i+1, 'qoq_pct'); }).join('') ||
    '<div style="padding:20px;color:var(--text-mute);text-align:center">No data</div>';
  document.getElementById('increasers').innerHTML =
    (summary.biggest_increasers || []).map(function(f, i){ return rowHtml(f, i+1, 'added'); }).join('') ||
    '<div style="padding:20px;color:var(--text-mute);text-align:center">No data</div>';
  document.getElementById('reducers').innerHTML =
    (summary.biggest_reducers || []).map(function(f, i){ return rowHtml(f, i+1, 'removed'); }).join('') ||
    '<div style="padding:20px;color:var(--text-mute);text-align:center">No data</div>';

  applyFilters();
}

function applyFilters() {
  const q = (document.getElementById('search').value || '').toLowerCase().trim();
  const sort = document.getElementById('filter-sort').value;

  FILTERED = (DATA.filers || []).filter(function(f){
    if (q && (f.investor_name || '').toLowerCase().indexOf(q) === -1) return false;
    return true;
  });

  if (sort === 'qoq_pct_desc')   FILTERED.sort(function(a,b){ return (b.qoq_change_pct||-Infinity) - (a.qoq_change_pct||-Infinity); });
  else if (sort === 'qoq_pct_asc') FILTERED.sort(function(a,b){ return (a.qoq_change_pct||Infinity) - (b.qoq_change_pct||Infinity); });
  else if (sort === 'activity_desc') FILTERED.sort(function(a,b){ return (b.activity_score||0) - (a.activity_score||0); });
  else if (sort === 'added_desc')    FILTERED.sort(function(a,b){ return (b.securities_added||0) - (a.securities_added||0); });
  else if (sort === 'removed_desc')  FILTERED.sort(function(a,b){ return (b.securities_removed||0) - (a.securities_removed||0); });
  else                                FILTERED.sort(function(a,b){ return (b.market_value||0) - (a.market_value||0); });

  document.getElementById('filter-count').textContent =
    FILTERED.length + ' filer' + (FILTERED.length === 1 ? '' : 's');

  document.getElementById('all-body').innerHTML = FILTERED.map(function(f, i){
    const qoqClass = (f.qoq_change_pct || 0) > 0 ? 'pos' : ((f.qoq_change_pct || 0) < 0 ? 'neg' : '');
    const naClass = (f.net_activity || 0) > 0 ? 'pos' : ((f.net_activity || 0) < 0 ? 'neg' : '');
    const naStr = ((f.net_activity || 0) > 0 ? '+' : '') + (f.net_activity || 0);
    return '<tr>' +
      '<td><span style="font-family:var(--font-mono);color:var(--text-mute)">#' + (i+1) + '</span></td>' +
      '<td><span class="f-name">' + shortName(f.investor_name) + '</span></td>' +
      '<td class="f-mv">' + fmtMv(f.market_value) + '</td>' +
      '<td class="f-num ' + qoqClass + '">' + fmtPct(f.qoq_change_pct) + '</td>' +
      '<td class="f-num">' + (f.portfolio_size || '-') + '</td>' +
      '<td class="f-num pos">+' + (f.securities_added || 0) + '</td>' +
      '<td class="f-num neg">-' + (f.securities_removed || 0) + '</td>' +
      '<td class="f-num ' + naClass + '">' + naStr + '</td>' +
      '<td><span style="font-family:var(--font-mono);color:var(--text-mute);font-size:10px">' + (f.cik || '-') + '</span></td>' +
      '</tr>';
  }).join('') || '<tr><td colspan="9" style="text-align:center;padding:40px;color:var(--text-mute)">No filers match</td></tr>';
}

document.getElementById('search').addEventListener('input', applyFilters);
document.getElementById('filter-sort').addEventListener('change', applyFilters);

load();
