// M&A Activity Tracker — JustHodl.AI
// Fetches from the justhodl-ma-tracker Lambda Function URL.
// The API_URL is replaced at deploy time by ops step 435.

const API_URL = "__FUNCTION_URL_PLACEHOLDER__";

let DATA = null;
let FILTERED_DEALS = [];
let SORT = { col: 'acceptedDate', dir: 'desc' };
let PAGE = 0;
const PAGE_SIZE = 30;

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
    document.getElementById('deals-body').innerHTML =
      '<tr><td colspan="7" style="text-align:center;padding:40px;color:var(--text-mute)">Failed to load M&A data: ' + e.message + '</td></tr>';
  }
}

function fmtDate(iso) {
  if (!iso) return '-';
  const d = iso.substring(0, 10);
  const date = new Date(d + 'T00:00:00Z');
  const now = new Date();
  const days = Math.floor((now - date) / (24*3600*1000));
  if (days === 0) return 'Today';
  if (days === 1) return 'Yesterday';
  if (days <= 7) return days + 'd ago';
  return d.substring(5);
}

function render() {
  const deals = DATA.deals || [];
  const profiles = DATA.profiles || {};
  const summary = DATA.summary || {};

  document.getElementById('stat-total').textContent = deals.length;
  document.getElementById('stat-range').textContent =
    summary.date_range ? (summary.date_range.start || '-') + ' → ' + (summary.date_range.end || '-') : '-';
  const topSec = (summary.by_sector || [])[0];
  document.getElementById('stat-sector').textContent = topSec ? topSec.sector : '-';
  document.getElementById('stat-sector-sub').textContent = topSec ? topSec.count + ' deals' : '';
  const topAcq = (summary.top_acquirers || [])[0];
  document.getElementById('stat-acquirer').textContent = topAcq ? topAcq.symbol : '-';
  document.getElementById('stat-acquirer-sub').textContent = topAcq ? (topAcq.count + ' deals · ' + (topAcq.name || '')) : '';
  const gen = new Date(DATA.generated_at);
  const ageMin = Math.round((Date.now() - gen.getTime()) / 60000);
  document.getElementById('stat-fresh').textContent = ageMin < 60 ? (ageMin + 'm') : (Math.round(ageMin/60) + 'h');
  document.getElementById('stat-elapsed').textContent = 'fetched in ' + (DATA.elapsed_seconds || 0) + 's';
  document.getElementById('gen-time').textContent = gen.toUTCString().substring(5, 22);

  const maxSec = Math.max.apply(null, (summary.by_sector || []).map(function(s){return s.count;}).concat([1]));
  document.getElementById('by-sector').innerHTML =
    (summary.by_sector || []).slice(0, 12).map(function(s){
      return '<div class="sector-row"><span class="sector-name">' + s.sector +
        '</span><span class="sector-bar"><span class="sector-bar-fill" style="width:' +
        ((s.count/maxSec)*100) + '%"></span></span><span class="sector-count">' + s.count + '</span></div>';
    }).join('') || '<div style="color:var(--text-mute)">No data</div>';

  document.getElementById('top-acquirers').innerHTML =
    (summary.top_acquirers || []).map(function(a){
      return '<div class="acquirer-row"><span class="acquirer-sym">' + (a.symbol || '-') +
        '</span><span class="acquirer-name">' + (a.name || '-') +
        '</span><span class="acquirer-sec">' + (a.sector || '') +
        '</span><span class="acquirer-count">' + a.count + '</span></div>';
    }).join('') || '<div style="color:var(--text-mute)">No data</div>';

  const sectors = Array.from(new Set((summary.by_sector || []).map(function(s){return s.sector;}))).sort();
  document.getElementById('filter-sector').innerHTML =
    '<option value="">All sectors</option>' +
    sectors.map(function(s){ return '<option value="' + s + '">' + s + '</option>'; }).join('');

  applyFilters();
}

function applyFilters() {
  const q = (document.getElementById('search').value || '').toLowerCase().trim();
  const secFilter = document.getElementById('filter-sector').value;
  const profiles = DATA.profiles || {};
  FILTERED_DEALS = (DATA.deals || []).filter(function(d){
    if (secFilter) {
      const p = profiles[d.symbol] || {};
      if ((p.sector || '') !== secFilter) return false;
    }
    if (q) {
      const blob = [d.symbol, d.companyName, d.targetedSymbol, d.targetedCompanyName,
                     (profiles[d.symbol] || {}).sector].filter(Boolean).join(' ').toLowerCase();
      if (blob.indexOf(q) === -1) return false;
    }
    return true;
  });
  sortDeals();
  PAGE = 0;
  renderTable();
}

function sortDeals() {
  const col = SORT.col;
  const dir = SORT.dir === 'desc' ? -1 : 1;
  FILTERED_DEALS.sort(function(a, b){
    const va = a[col] || '';
    const vb = b[col] || '';
    if (va < vb) return -1 * dir;
    if (va > vb) return 1 * dir;
    return 0;
  });
}

function renderTable() {
  const profiles = DATA.profiles || {};
  const start = PAGE * PAGE_SIZE;
  const end = start + PAGE_SIZE;
  const slice = FILTERED_DEALS.slice(start, end);
  document.getElementById('filter-count').textContent =
    FILTERED_DEALS.length + ' deal' + (FILTERED_DEALS.length === 1 ? '' : 's');
  document.getElementById('page-info').textContent =
    (start + 1) + '–' + Math.min(end, FILTERED_DEALS.length) + ' of ' + FILTERED_DEALS.length;
  document.getElementById('prev-btn').disabled = PAGE === 0;
  document.getElementById('next-btn').disabled = end >= FILTERED_DEALS.length;

  if (slice.length === 0) {
    document.getElementById('deals-body').innerHTML =
      '<tr><td colspan="7" style="text-align:center;padding:40px;color:var(--text-mute)">No deals match the filters</td></tr>';
    return;
  }

  document.getElementById('deals-body').innerHTML = slice.map(function(d){
    const p = profiles[d.symbol] || {};
    const sector = p.sector || '';
    const link = d.link ? '<a class="deal-link" href="' + d.link + '" target="_blank">SEC ↗</a>' : '';
    return '<tr><td><span class="deal-date">' + fmtDate(d.acceptedDate || d.transactionDate) +
      '</span></td><td><span class="deal-sym">' + (d.symbol || '-') +
      '</span></td><td>' + (d.companyName || '-') +
      '</td><td>' + (d.targetedSymbol ? '<span class="deal-tgt-sym">' + d.targetedSymbol + '</span>' : '<span class="deal-cik">-</span>') +
      '</td><td>' + (d.targetedCompanyName || '-') +
      '</td><td>' + (sector ? '<span class="deal-sector-chip">' + sector + '</span>' : '-') +
      '</td><td>' + link + '</td></tr>';
  }).join('');

  document.querySelectorAll('table.deals th[data-sort]').forEach(function(th){
    th.classList.remove('sorted-asc', 'sorted-desc');
    if (th.dataset.sort === SORT.col) th.classList.add('sorted-' + SORT.dir);
  });
}

document.getElementById('search').addEventListener('input', applyFilters);
document.getElementById('filter-sector').addEventListener('change', applyFilters);
document.getElementById('prev-btn').addEventListener('click', function(){ if (PAGE > 0) { PAGE--; renderTable(); } });
document.getElementById('next-btn').addEventListener('click', function(){ PAGE++; renderTable(); });
document.querySelectorAll('table.deals th[data-sort]').forEach(function(th){
  th.classList.add('sortable');
  th.addEventListener('click', function(){
    const col = th.dataset.sort;
    if (SORT.col === col) {
      SORT.dir = SORT.dir === 'desc' ? 'asc' : 'desc';
    } else {
      SORT.col = col; SORT.dir = 'desc';
    }
    sortDeals();
    renderTable();
  });
});

load();
