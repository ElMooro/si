// COT Futures Positioning — JustHodl.AI
const API_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/cot-latest.json";

let DATA = null;
let FILTERED = [];
let SORT = { col: 'z_score_3y', dir: 'desc' };

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
    document.getElementById('contracts-body').innerHTML =
      '<tr><td colspan="10" style="text-align:center;padding:40px;color:var(--text-mute)">Failed to load: ' + e.message + '</td></tr>';
  }
}

function fmtPct(v) {
  if (v === null || v === undefined) return '-';
  return Number(v).toFixed(1) + '%';
}

function fmtZ(z) {
  if (z === null || z === undefined) return '-';
  const sign = z > 0 ? '+' : '';
  return sign + Number(z).toFixed(2) + 'σ';
}

function fmtDate(s) {
  if (!s) return '-';
  const d = s.substring(0, 10);
  const date = new Date(d + 'T00:00:00Z');
  const now = new Date();
  const days = Math.floor((now - date) / (24 * 3600 * 1000));
  if (days < 7) return days + 'd ago';
  if (days < 30) return Math.floor(days/7) + 'w ago';
  return d.substring(5);
}

function render() {
  const contracts = DATA.contracts || [];
  const summary = DATA.summary || {};

  document.getElementById('stat-total').textContent = contracts.length;
  document.getElementById('stat-week').textContent =
    (contracts[0] && contracts[0].date) ? 'as of ' + contracts[0].date : 'latest report';
  document.getElementById('stat-long').textContent = summary.extreme_long_count || 0;
  document.getElementById('stat-long-sub').textContent =
    (summary.extreme_long_top || [])[0] ? 'top: ' + summary.extreme_long_top[0].symbol : 'z-score ≥ +1.5';
  document.getElementById('stat-short').textContent = summary.extreme_short_count || 0;
  document.getElementById('stat-short-sub').textContent =
    (summary.extreme_short_top || [])[0] ? 'top: ' + summary.extreme_short_top[0].symbol : 'z-score ≤ -1.5';

  const gen = new Date(DATA.generated_at);
  const ageMin = Math.round((Date.now() - gen.getTime()) / 60000);
  document.getElementById('stat-fresh').textContent =
    ageMin < 60 ? (ageMin + 'm') : (ageMin < 1440 ? Math.round(ageMin/60) + 'h' : Math.round(ageMin/1440) + 'd');
  document.getElementById('stat-elapsed').textContent =
    'fetched in ' + (DATA.elapsed_seconds || 0) + 's';
  document.getElementById('gen-time').textContent = gen.toUTCString().substring(5, 22);

  // Extreme panels
  function rowHtml(c, cls) {
    return '<div class="extreme-row"><span class="extreme-sym">' + c.symbol +
      '</span><span class="extreme-name">' + c.name +
      '</span><span class="extreme-sec">' + c.sector +
      '</span><span class="extreme-z ' + cls + '">' + fmtZ(c.z) + '</span></div>';
  }
  document.getElementById('extreme-long').innerHTML =
    (summary.extreme_long_top || []).map(function(c){ return rowHtml(c, 'long'); }).join('') ||
    '<div style="padding:20px;color:var(--text-mute);text-align:center">No contracts above +1.5σ</div>';
  document.getElementById('extreme-short').innerHTML =
    (summary.extreme_short_top || []).map(function(c){ return rowHtml(c, 'short'); }).join('') ||
    '<div style="padding:20px;color:var(--text-mute);text-align:center">No contracts below -1.5σ</div>';

  // Sector dropdown
  const sectors = Array.from(new Set(contracts.map(function(c){ return c.sector; }))).filter(Boolean).sort();
  document.getElementById('filter-sector').innerHTML =
    '<option value="">All sectors</option>' +
    sectors.map(function(s){ return '<option value="' + s + '">' + s + '</option>'; }).join('');

  applyFilters();
}

function applyFilters() {
  const q = (document.getElementById('search').value || '').toLowerCase().trim();
  const sec = document.getElementById('filter-sector').value;
  const sig = document.getElementById('filter-signal').value;

  FILTERED = (DATA.contracts || []).filter(function(c){
    if (sec && c.sector !== sec) return false;
    if (sig) {
      const csig = c.extreme_signal || 'flat';
      if (csig !== sig) return false;
    }
    if (q) {
      const blob = (c.symbol + ' ' + c.name + ' ' + c.sector).toLowerCase();
      if (blob.indexOf(q) === -1) return false;
    }
    return true;
  });
  sortContracts();
  renderTable();
}

function sortContracts() {
  const col = SORT.col;
  const dir = SORT.dir === 'desc' ? -1 : 1;
  FILTERED.sort(function(a, b){
    const va = a[col] == null ? -Infinity : a[col];
    const vb = b[col] == null ? -Infinity : b[col];
    if (typeof va === 'string') {
      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
      return 0;
    }
    return (vb - va) * (dir === -1 ? 1 : -1);
  });
}

function renderTable() {
  document.getElementById('filter-count').textContent =
    FILTERED.length + ' contract' + (FILTERED.length === 1 ? '' : 's');

  if (FILTERED.length === 0) {
    document.getElementById('contracts-body').innerHTML =
      '<tr><td colspan="10" style="text-align:center;padding:40px;color:var(--text-mute)">No contracts match the filters</td></tr>';
    return;
  }

  document.getElementById('contracts-body').innerHTML = FILTERED.map(function(c){
    const sigClass = c.extreme_signal || 'flat';
    const sigText = c.extreme_signal ? c.extreme_signal.toUpperCase() : '·';
    const zClass = c.z_score_3y == null ? '' : (c.z_score_3y > 0 ? 'pos' : 'neg');
    const netClass = c.net_position_pct > 0 ? 'pos' : (c.net_position_pct < 0 ? 'neg' : '');
    // Bar: net pct visualized symmetrically around midpoint
    const netAbs = Math.min(Math.abs(c.net_position_pct), 100);
    const barHtml = c.net_position_pct > 0
      ? '<div class="c-bar"><div class="c-bar-pos" style="width:' + (netAbs/2) + '%"></div></div>'
      : '<div class="c-bar"><div class="c-bar-neg" style="width:' + (netAbs/2) + '%"></div></div>';
    return '<tr>' +
      '<td><span class="c-sym">' + c.symbol + '</span></td>' +
      '<td>' + c.name + '</td>' +
      '<td><span class="sector-chip">' + c.sector + '</span></td>' +
      '<td class="c-z ' + zClass + '">' + fmtZ(c.z_score_3y) + '</td>' +
      '<td><span class="signal-chip ' + sigClass + '">' + sigText + '</span></td>' +
      '<td class="c-net ' + netClass + '">' + (c.net_position_pct > 0 ? '+' : '') + c.net_position_pct.toFixed(1) + '%</td>' +
      '<td>' + barHtml + '</td>' +
      '<td>' + fmtPct(c.current_long_pct) + '</td>' +
      '<td>' + fmtPct(c.current_short_pct) + '</td>' +
      '<td><span style="font-family:var(--font-mono);color:var(--text-dim);font-size:11px">' + fmtDate(c.date) + '</span></td>' +
      '</tr>';
  }).join('');

  document.querySelectorAll('table.contracts th[data-sort]').forEach(function(th){
    th.classList.remove('sorted-asc', 'sorted-desc');
    if (th.dataset.sort === SORT.col) th.classList.add('sorted-' + SORT.dir);
  });
}

document.getElementById('search').addEventListener('input', applyFilters);
document.getElementById('filter-sector').addEventListener('change', applyFilters);
document.getElementById('filter-signal').addEventListener('change', applyFilters);
document.querySelectorAll('table.contracts th[data-sort]').forEach(function(th){
  th.classList.add('sortable');
  th.addEventListener('click', function(){
    const col = th.dataset.sort;
    if (SORT.col === col) {
      SORT.dir = SORT.dir === 'desc' ? 'asc' : 'desc';
    } else {
      SORT.col = col; SORT.dir = 'desc';
    }
    sortContracts();
    renderTable();
  });
});

load();
