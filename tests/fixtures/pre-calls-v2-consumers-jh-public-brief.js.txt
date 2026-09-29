/* Public research evidence; private account state never enters this renderer. */
(function (root) {
  'use strict';
  function state(doc, now = Date.now()) {
    if (!doc || doc.generation_method !== 'warehouse_deterministic_v1' || typeof doc.brief_md !== 'string' || doc.brief_md.trim().length < 120 || doc.call_verb !== 'WAIT' || doc.sizing_eligible !== false) return {valid:false};
    const age = now - Date.parse(doc.generated_at);
    return {valid:true, overdue:!Number.isFinite(age) || age < -300000 || age > 4.5 * 3600000,
      rows:Array.isArray(doc.evidence) ? doc.evidence : [],
      inventory:doc.evidence_inventory || {}, generated:doc.generated_at};
  }
  function bundlePath(ref) {
    return ref && /^[a-f0-9]{64}$/.test(ref.payload_sha256 || '') &&
      ref.run_id === 'calls-research-' + ref.payload_sha256 &&
      ref.bundle_key === 'data/calls-research-runs/' + ref.payload_sha256 + '.json' &&
      /^[a-f0-9]{64}$/.test(ref.bundle_sha256 || '') ? '/' + ref.bundle_key : null;
  }
  function sourcePath(source) { return /^data\/[a-z0-9-]+\.json$/.test(source || '') ? '/' + source : null; }
  function proofMatches(proof, ref) {
    return !!(proof && ref && proof.status === 'reproduced' && proof.run_id === ref.run_id &&
      proof.payload_sha256 === ref.payload_sha256 && proof.bundle_sha256 === ref.bundle_sha256);
  }
  function render(document, box, doc, now) {
    box.replaceChildren();
    const el = (tag, text, parent = box) => { const n = document.createElement(tag); n.textContent = text; parent.appendChild(n); return n; };
    el('h2', 'Evidence behind this brief');
    const view = state(doc, now);
    if (!view.valid) { el('p', 'Public brief unavailable. WAIT — no new allocation guidance.'); return; }
    const status = el('p', (view.overdue ? 'Overdue · ' : '') + 'WAIT · Research only · ' + view.generated);
    status.setAttribute('role', 'status');
    el('p', 'The brief records observations. It does not authorize a position change or override your existing risk controls.');
    const groups = Array.isArray(view.inventory.root_groups) ? view.inventory.root_groups : [];
    el('p', view.rows.length + ' measurements · ' + groups.length + ' mapped source roots · 0 eligible votes. Shared roots do not establish statistical independence.');
    const ref = doc.research_replay, path = bundlePath(ref);
    if (path) {
      const line = el('p', 'Frozen inputs retained · ');
      const link = el('a', 'Open replay record', line); link.href = path;
      const proof = el('span', ' · Independent replay check pending', line);
      proof.id = 'calls-replay-proof'; proof.setAttribute('role', 'status');
      const hash = el('small', 'Run ' + ref.run_id); hash.style.overflowWrap = 'anywhere';
    } else el('p', 'This legacy brief has no retained replay record.');
    const details = el('details', '');
    el('summary', 'Inspect measurements, dates and source overlap', details);
    const scroll = el('div', '', details); scroll.style.overflowX = 'auto'; scroll.tabIndex = 0;
    scroll.setAttribute('role', 'region'); scroll.setAttribute('aria-label', 'Brief evidence table');
    const table = el('table', '', scroll); table.style.width = '100%'; table.style.minWidth = '760px';
    const header = el('tr', '', el('thead', '', table));
    for (const label of ['Measurement / source', 'Value and unit', 'Observed', 'Quality', 'Shared roots']) {
      const th = el('th', label, header); th.setAttribute('scope', 'col');
    }
    const body = el('tbody', '', table);
    for (const row of view.rows) {
      const tr = el('tr', '', body), source = el('td', '', tr), sourceUrl = sourcePath(row.source);
      const name = String(row.series_id || 'Unidentified measurement').split('#').pop();
      const label = el(sourceUrl ? 'a' : 'span', name, source);
      if (sourceUrl) label.href = sourceUrl;
      source.style.overflowWrap = 'anywhere';
      const value = typeof row.value === 'number' && Number.isFinite(row.value) ? row.value.toLocaleString('en-US', {maximumFractionDigits:4}) : 'Unavailable';
      el('td', value + (row.unit ? ' ' + row.unit : ''), tr);
      el('td', row.observation_date || 'Unknown', tr);
      el('td', row.quality_status || 'Unverified', tr);
      el('td', Array.isArray(row.root_ids) ? row.root_ids.join(', ') : 'Unmapped', tr);
      if (row.note) tr.title = row.note;
    }
    el('p', 'Every measurement is monitor-only. A fresh source alone is insufficient for a decision vote or position size.', details);
    const prose = el('details', ''); el('summary', 'Read the brief, disagreements and limitations', prose);
    for (const line of doc.brief_md.split('\n')) {
      if (!line.trim() || line.startsWith('# ')) continue;
      const n = el(line.startsWith('## ') ? 'h3' : 'p', line.replace(/^## /, '').replace(/^\*\*|\*\*$/g, ''), prose);
      n.style.overflowWrap = 'anywhere';
    }
  }
  const api = {state, bundlePath, proofMatches, render};
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (!root.document) return;
  const document = root.document, main = document.querySelector('main'); if (!main) return;
  const box = document.createElement('section'); box.id = 'public-market-brief'; box.className = 'card section';
  box.style.cssText = 'margin:24px 0;padding:20px;line-height:1.65';
  const kpis = document.getElementById('kpi-row'); main.insertBefore(box, kpis ? kpis.nextSibling : null);
  box.textContent = 'Loading the public research brief…';
  async function refresh() {
    try {
      const response = await fetch('/data/ai-brief-public.json', {cache:'no-store', signal:AbortSignal.timeout(15000)});
      if (!response.ok) throw new Error('Brief unavailable');
      const doc = await response.json(); render(document, box, doc, Date.now());
      const ref = doc.research_replay, badge = document.getElementById('calls-replay-proof');
      if (!badge || !bundlePath(ref)) return;
      try {
        const response = await fetch('/data/calls-research-proofs/' + ref.payload_sha256 + '.json', {cache:'no-store', signal:AbortSignal.timeout(10000)});
        if (!response.ok) return;
        const proof = await response.json();
        badge.textContent = proofMatches(proof, ref) ? ' · Replay verified ' + proof.generated_at : ' · Replay check failed or does not match this run';
      } catch (_) { /* The retained record remains useful; verification stays pending. */ }
    } catch (_) { render(document, box, null, Date.now()); }
  }
  refresh(); setInterval(() => { if (document.visibilityState !== 'hidden') refresh(); }, 300000);
})(typeof globalThis === 'object' ? globalThis : this);
