// Earnings Call Pulse — JustHodl.AI
const API_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/earnings-sentiment.json";

let DATA = null;
let FILTERED = [];

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
    document.getElementById('all-cards').innerHTML =
      '<div style="text-align:center;padding:40px;color:var(--text-mute)">Failed to load: ' + e.message + '</div>';
  }
}

function fmtDate(s) {
  if (!s) return '-';
  const d = s.substring(0, 10);
  const date = new Date(d + 'T00:00:00Z');
  const now = new Date();
  const days = Math.floor((now - date) / (24 * 3600 * 1000));
  if (days === 0) return 'Today';
  if (days === 1) return 'Yesterday';
  if (days <= 7) return days + 'd ago';
  if (days <= 30) return Math.floor(days/7) + 'w ago';
  return d.substring(5);
}

function makeCard(r, withSummary) {
  const sent = r.overall_sentiment;
  const sentClass = sent > 0 ? 'pos' : 'neg';
  const sentStr = (sent > 0 ? '+' : '') + sent;
  const conf = r.confidence_score;
  const guide = r.forward_guidance || 'none';
  const themes = (r.themes || []).slice(0, 4)
    .map(function(t){ return '<span class="theme-chip">' + t + '</span>'; }).join('');
  return '<div class="tcard" data-sym="' + r.symbol + '" data-key="' + (r._key || '') + '">' +
    '<div class="tcard-row1">' +
      '<span class="tcard-sym">' + r.symbol + '</span>' +
      '<span class="tcard-name">' + (r.name || r.symbol) + '</span>' +
      '<span class="tcard-sent ' + sentClass + '">' + sentStr + '</span>' +
    '</div>' +
    '<div class="tcard-row2">' +
      '<span class="guide-chip ' + guide + '">' + guide.toUpperCase() + '</span>' +
      (conf != null ? '<span class="conf-bar">conf ' + conf + '</span>' : '') +
      '<span class="tcard-date">' + fmtDate(r.transcript_date) + '</span>' +
    '</div>' +
    (withSummary ? '<div class="tcard-summary">' + (r.one_line_summary || '') + '</div>' : '') +
    (themes ? '<div class="themes-row">' + themes + '</div>' : '') +
    '</div>';
}

function render() {
  const txs = DATA.transcripts || [];
  const summary = DATA.summary || {};

  document.getElementById('stat-total').textContent = txs.length;
  document.getElementById('stat-new').textContent =
    (DATA.n_new_this_run || 0) + ' new this run';
  const g = summary.guidance_changes || {};
  document.getElementById('stat-raised').textContent = g.raised || 0;
  document.getElementById('stat-lowered').textContent = g.lowered || 0;
  document.getElementById('stat-maintained').textContent = g.maintained || 0;
  const gen = new Date(DATA.generated_at);
  const ageMin = Math.round((Date.now() - gen.getTime()) / 60000);
  document.getElementById('stat-fresh').textContent =
    ageMin < 60 ? (ageMin + 'm') : (ageMin < 1440 ? Math.round(ageMin/60) + 'h' : Math.round(ageMin/1440) + 'd');
  document.getElementById('gen-time').textContent = gen.toUTCString().substring(5, 22);

  document.getElementById('bullish').innerHTML =
    (summary.most_bullish || []).slice(0, 8).map(function(r){
      // Build a minimal pseudo-record for the card
      const full = txs.find(function(t){ return t.symbol === r.symbol; }) || r;
      return makeCard(full, true);
    }).join('') || '<div style="padding:20px;color:var(--text-mute);text-align:center">No bullish calls yet</div>';

  document.getElementById('bearish').innerHTML =
    (summary.most_bearish || []).slice(0, 8).map(function(r){
      const full = txs.find(function(t){ return t.symbol === r.symbol; }) || r;
      return makeCard(full, true);
    }).join('') || '<div style="padding:20px;color:var(--text-mute);text-align:center">No bearish calls yet</div>';

  attachCardListeners();
  applyFilters();
}

function applyFilters() {
  const q = (document.getElementById('search').value || '').toLowerCase().trim();
  const guideFilter = document.getElementById('filter-guidance').value;
  const sort = document.getElementById('filter-sort').value;
  const txs = DATA.transcripts || [];

  FILTERED = txs.filter(function(r){
    if (guideFilter && (r.forward_guidance || 'none') !== guideFilter) return false;
    if (q) {
      const blob = [r.symbol, r.name, r.one_line_summary, (r.themes || []).join(' ')].filter(Boolean).join(' ').toLowerCase();
      if (blob.indexOf(q) === -1) return false;
    }
    return true;
  });

  if (sort === 'sent_desc')      FILTERED.sort(function(a,b){ return (b.overall_sentiment||0) - (a.overall_sentiment||0); });
  else if (sort === 'sent_asc')  FILTERED.sort(function(a,b){ return (a.overall_sentiment||0) - (b.overall_sentiment||0); });
  else if (sort === 'conf_desc') FILTERED.sort(function(a,b){ return (b.confidence_score||0) - (a.confidence_score||0); });
  else                            FILTERED.sort(function(a,b){ return (b.transcript_date||'').localeCompare(a.transcript_date||''); });

  document.getElementById('filter-count').textContent =
    FILTERED.length + ' transcript' + (FILTERED.length === 1 ? '' : 's');
  document.getElementById('all-cards').innerHTML =
    FILTERED.map(function(r){ return makeCard(r, true); }).join('') ||
    '<div style="text-align:center;padding:40px;color:var(--text-mute)">No transcripts match the filters</div>';
  attachCardListeners();
}

function attachCardListeners() {
  document.querySelectorAll('.tcard').forEach(function(card){
    card.addEventListener('click', function(){
      const sym = card.dataset.sym;
      const r = (DATA.transcripts || []).find(function(t){ return t.symbol === sym; });
      if (r) showDetail(r);
    });
  });
}

function showDetail(r) {
  const pos = (r.key_positives || []).map(function(p){ return '<li class="bul-pos">' + p + '</li>'; }).join('');
  const neg = (r.key_concerns || []).map(function(c){ return '<li class="bul-neg">' + c + '</li>'; }).join('');
  const themes = (r.themes || []).map(function(t){ return '<span class="theme-chip">' + t + '</span>'; }).join(' ');
  document.getElementById('modal-content').innerHTML =
    '<button class="detail-close" onclick="document.getElementById(\'modal\').classList.remove(\'open\')">×</button>' +
    '<h2 style="margin:0 0 4px 0;font-size:18px;">' + r.symbol + '  <span style="color:var(--text-dim);font-weight:400;font-size:14px;">' + (r.name || '') + '</span></h2>' +
    '<div style="font-family:var(--font-mono);font-size:11px;color:var(--text-dim);margin-bottom:14px;">' +
      'Q' + r.quarter + ' FY' + r.fiscal_year + ' · ' + r.transcript_date + ' · ' + r.model +
    '</div>' +
    '<div style="display:flex;gap:20px;margin-bottom:14px;font-size:14px;">' +
      '<div><span style="color:var(--text-dim);font-size:11px;">Sentiment</span><br><span style="font-size:24px;font-weight:700;color:' + (r.overall_sentiment > 0 ? 'var(--pos)' : 'var(--neg)') + '">' + (r.overall_sentiment > 0 ? '+' : '') + r.overall_sentiment + '</span></div>' +
      '<div><span style="color:var(--text-dim);font-size:11px;">Confidence</span><br><span style="font-size:24px;font-weight:700;">' + r.confidence_score + '</span></div>' +
      '<div><span style="color:var(--text-dim);font-size:11px;">Guidance</span><br><span class="guide-chip ' + (r.forward_guidance || 'none') + '" style="font-size:13px;padding:4px 10px;">' + (r.forward_guidance || 'NONE').toUpperCase() + '</span></div>' +
    '</div>' +
    '<div style="margin-bottom:14px;color:var(--text);font-style:italic;border-left:3px solid var(--accent);padding-left:12px;line-height:1.6;">' + (r.one_line_summary || '') + '</div>' +
    (pos ? '<div style="margin-top:14px;"><strong style="color:var(--pos);font-size:13px;">Key Positives</strong><ul class="detail-bullets">' + pos + '</ul></div>' : '') +
    (neg ? '<div style="margin-top:8px;"><strong style="color:var(--neg);font-size:13px;">Key Concerns</strong><ul class="detail-bullets">' + neg + '</ul></div>' : '') +
    (themes ? '<div style="margin-top:14px;"><strong style="color:var(--text-dim);font-size:11px;">Themes</strong><br><div style="margin-top:4px;">' + themes + '</div></div>' : '') +
    '<div style="margin-top:18px;padding-top:12px;border-top:1px solid var(--border);font-family:var(--font-mono);font-size:10px;color:var(--text-mute);">' +
      'Tokens: ' + (r.tokens_in || '-') + ' in / ' + (r.tokens_out || '-') + ' out · scored ' + fmtDate(r.scored_at) +
    '</div>';
  document.getElementById('modal').classList.add('open');
}

document.getElementById('search').addEventListener('input', applyFilters);
document.getElementById('filter-guidance').addEventListener('change', applyFilters);
document.getElementById('filter-sort').addEventListener('change', applyFilters);
document.getElementById('modal').addEventListener('click', function(e){
  if (e.target.id === 'modal') document.getElementById('modal').classList.remove('open');
});

load();
