/* JustHodl Gear A pane. Independent of the existing SageMaker desk renderer. */
(function () {
  'use strict';
  const root = document.getElementById('factory-pane');
  if (!root) return;
  const $ = id => document.getElementById('factory-' + id);
  const text = (id, value) => { const e = $(id); if (e) e.textContent = value; };
  const money = v => v == null ? 'Unavailable' : Number(v).toLocaleString(undefined, { maximumFractionDigits: 4 });
  const safe = v => String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const sourcePath = key => '/'+key.replace(/^\//, '');
  let state = null, timer = null;

  // Preserve numeric spellings while excluding only the ROOT checksum member.
  // JSON reserialization changes Python's 1.0 into JS's 1 and would break SHA256.
  function checksumBody(raw) {
    const s = raw.trim();
    if (s[0] !== '{' || s[s.length - 1] !== '}') throw Error('Invalid state envelope');
    const pieces = []; let start = 1, depth = 0, quoted = false, escape = false;
    for (let i = 1; i < s.length - 1; i++) {
      const ch = s[i];
      if (quoted) { if (escape) escape = false; else if (ch === '\\') escape = true; else if (ch === '"') quoted = false; continue; }
      if (ch === '"') quoted = true;
      else if ('[{'.includes(ch)) depth++;
      else if (']}'.includes(ch)) depth--;
      else if (ch === ',' && depth === 0) { pieces.push(s.slice(start, i)); start = i + 1; }
    }
    pieces.push(s.slice(start, -1));
    const kept = pieces.filter(part => !/^\s*"checksum"\s*:/.test(part));
    if (kept.length !== pieces.length - 1) throw Error('Missing integrity checksum');
    return '{' + kept.join(',') + '}';
  }
  async function verifyStateRaw(raw) {
    const doc = JSON.parse(raw);
    if (doc.schema_version !== 'student-state.v1' || !Number.isInteger(doc.state_version) || !Number.isInteger(doc.gen) || !Array.isArray(doc.skillbook)) throw Error('State schema mismatch');
    const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(checksumBody(raw)));
    const hex = Array.from(new Uint8Array(hash), x => x.toString(16).padStart(2, '0')).join('');
    if (hex !== doc.checksum) throw Error('State checksum mismatch');
    return doc;
  }
  async function checkedState(kind) {
    const payload = await api('view?kind=' + encodeURIComponent(kind));
    return verifyStateRaw(payload.raw);
  }
  async function publicState() {
    for (const url of ['/data/student-state.json', '/student-state.json']) {
      try {
        const r = await fetch(url, { cache: 'no-store' });
        if (!r.ok) continue;
        return await verifyStateRaw(await r.text());
      } catch (e) {}
    }
    return null;
  }
  function age(value) {
    const ms = Date.now() - Date.parse(value);
    return Number.isFinite(ms) ? Math.max(0, Math.round(ms / 60000)) + ' min ago' : 'Not yet observed';
  }
  function paint(doc) {
    text('status', doc.health.status + ' · tick ' + age(doc.generated_at));
    text('gen', doc.gen);
    text('fit', doc.fit ? Math.round(doc.fit.score * 100) + '%' : 'Awaiting exam');
    text('skills', doc.skillbook.length);
    text('weeks', doc.wall.independent_weeks || 0);
    text('pending', doc.wall.pending || 0);
    text('model', doc.model.status === 'blocked_no_verified_generative_model' ? 'Generative coder unavailable' : doc.model.status);
    text('objective', doc.objective);
    text('integrity', 'State v' + doc.state_version + ' · SHA-256 verified · ' + doc.checksum.slice(0, 12));
    text('budget', '1 experiment/day · 0 GPU jobs · 0 paid model calls · AWS service charges apply');
    text('exam-note', doc.fit ? doc.fit.n + ' protected coding cases; baseline ' + Math.round(doc.fit.baseline * 100) + '%. This measures one repair family.' : 'Only independently checked results appear here.');
    text('errors', doc.health.errors.length ? doc.health.errors.map(e => e.phase + ': ' + (e.detail || e.error)).join(' · ') : 'State and worker checks passed.');
    $('agents').innerHTML = doc.agents.map(a => '<article class="factory-agent"><b>' + safe(a.name) + '</b><span>' + safe(a.status.replaceAll('_', ' ')) + '</span><p>' + safe(a.purpose) + '</p></article>').join('');
    const funding = doc.outer_status.funding || {}, tape = doc.outer_status.tape || {};
    const rows = [...Object.entries(funding), ...Object.entries(tape)];
    $('sources').innerHTML = rows.map(([name, f]) => '<tr><td>' + safe(name.toUpperCase().replaceAll('_', ' ')) + '</td><td>' + safe(money(f.value)) + '</td><td>' + safe(f.unit || '—') + '</td><td>' + safe(f.observed_at ? f.observed_at.slice(0, 10) : 'Unavailable') + '</td><td>' + (f.retained ? 'Last good retained' : f.stale ? 'Stale / missing' : 'Observed') + '</td></tr>').join('');
    const oss = Object.values(doc.outer_status.oss || {});
    text('oss', oss.map(r => r.repository + ': ' + (r.license || 'license review required') + ' · ' + r.status).join(' | ') || 'OSS metadata awaiting first observation.');
    $('skillbook').innerHTML = doc.skillbook.length ? doc.skillbook.map(s => '<li><a href="' + safe('#factory-event=' + s.id) + '" target="_blank" rel="noopener">' + safe(s.summary) + '</a> · ' + s.independent_cases + ' cases</li>').join('') : '<li>No verified skills retained yet.</li>';
    const season = doc.season;
    text('season', season.id + ' · ' + season.weeks + ' weeks · America/New_York');
    text('crisis', season.crisis_definition.replaceAll('_', ' '));
    text('wall-status', (doc.wall.entries || 0) + ' entries · ' + (doc.wall.graded || 0) + ' outcomes · ' + (doc.wall.pending || 0) + ' pending. Official prints required.');
    const week = $('week');
    if (week && !week.value) week.value = season.starts_on;
    if ($('prediction-json') && !$('prediction-json').value) predictionTemplate();
  }
  async function wall() {
    try {
      let board;
      try {
        board = JSON.parse((await api('view?kind=board')).raw);
      } catch (authErr) {
        const r = await fetch('/factory/salon/board.json', { cache: 'no-store' });
        if (!r.ok) throw authErr;
        board = await r.json();
      }
      $('leaders').innerHTML = board.top50.length ? board.top50.map(r => '<tr><td>' + safe(r.agent) + '</td><td>' + Math.round(r.elo) + '</td><td>' + Math.round(r.score * 100) + '%</td><td>' + r.independent_weeks + '</td><td>' + r.graded_predictions + '</td></tr>').join('') : '<tr><td colspan="5">No held-out market results yet. Elo starts after grading.</td></tr>';
      text('invited', board.invited.length ? 'Invited: ' + board.invited.map(r => r.agent).join(', ') : 'Invite-only pilot: no guests invited yet.');
      $('entries').innerHTML = (board.entries || []).slice(-12).reverse().map(e => '<li><a href="#factory-event=' + encodeURIComponent(e.id) + '">' + safe(e.agent + ' · ' + e.symbol + ' · ' + e.week) + '</a> — ' + safe(e.direction + ' / ' + e.regime) + '</li>').join('') || '<li>The next entries open Monday at 09:30 ET and lock at 09:35.</li>';
    } catch (e) { text('wall-status', e.message); }
    await permalink();
  }
  async function permalink() {
    if (!location.hash.startsWith('#factory-event=')) return;
    const id = decodeURIComponent(location.hash.slice('#factory-event='.length));
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$/.test(id)) return;
    try {
      const event = JSON.parse((await api('view?kind=event&id=' + encodeURIComponent(id))).raw);
      text('event', JSON.stringify(event, null, 2));
      $('event-details').open = true;
      $('event-details').scrollIntoView({ behavior: 'smooth', block: 'center' });
    } catch (e) { text('event', e.message); }
  }
  async function refresh() {
    const token = window.JustHodlAuth && JustHodlAuth.getAccessToken ? await JustHodlAuth.getAccessToken() : null;
    let good = [];
    if (token) {
      const result = await Promise.allSettled(['state', 'mirror'].map(checkedState));
      good = result.filter(r => r.status === 'fulfilled').map(r => r.value).sort((a, b) => b.state_version - a.state_version);
    }
    if (!good.length) {
      const pub = await publicState();
      if (pub) {
        if (state && pub.state_version < state.state_version) return;
        state = pub;
        paint(state);
        await wall();
        text('status', (state.health && state.health.status ? state.health.status : 'live') + ' · public feed · tick ' + age(state.generated_at));
        return;
      }
      text('status', state ? 'Feed unavailable · retaining last verified state' : 'Sign in to post to the wall. Public factory feed is unavailable.');
      return;
    }
    if (good.length > 1 && good[0].state_version === good[1].state_version && good[0].checksum !== good[1].checksum) { text('status', 'State conflict · retaining last verified state'); return; }
    if (state && good[0].state_version < state.state_version) return;
    state = good[0]; paint(state); await wall();
  }
  async function api(action, body) {
    const token = window.JustHodlAuth && JustHodlAuth.getAccessToken ? await JustHodlAuth.getAccessToken() : null;
    if (!token) throw Error('Sign in to use the invite-only factory.');
    const r = await fetch('/api/v1/factory/' + action, { method: body ? 'POST' : 'GET',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token }, body: body ? JSON.stringify(body) : undefined });
    const doc = await r.json();
    if (!r.ok) throw Error(doc.error || 'Factory request failed');
    return doc;
  }
  function predictionTemplate() {
    if (!state) return;
    const symbol = $('symbol').value;
    $('prediction-json').value = JSON.stringify({ id: 'entry-' + Date.now(), week: $('week').value, symbol,
      direction: 'FLAT', regime: 'RANGE', crisis_probability: 0.1,
      direction_probabilities: { DOWN: 0.25, FLAT: 0.5, UP: 0.25 },
      regime_probabilities: { RANGE: 0.5, TRANSITION: 0.25, TREND: 0.25 },
      price_source: state.season.price_sources[symbol], data_cutoff: new Date().toISOString(), model_revision: 'guest-method-v1'
    }, null, 2);
  }
  $('symbol').addEventListener('change', predictionTemplate);
  $('week').addEventListener('change', predictionTemplate);
  $('refresh').addEventListener('click', refresh);
  $('connect').addEventListener('click', async () => {
    try { const r = await api('sandbox'); text('admission', 'Connected as ' + r.agent + '. Delayed tape and SOFR only.'); $('owner').hidden = !r.owner; await refresh(); }
    catch (e) { text('admission', e.message); }
  });
  for (const [id, action, input] of [['send-prediction', 'predictions', 'prediction-json'], ['send-trace', 'traces', 'trace-json'], ['send-invite', 'invites', 'invite-json']]) {
    $(id).addEventListener('click', async () => {
      try { const doc = await api(action, JSON.parse($(input).value)); text('admission', JSON.stringify(doc)); }
      catch (e) { text('admission', e.message); }
    });
  }
  for (const [id, enabled] of [['pause', false], ['resume', true]]) $(id).addEventListener('click', async () => {
    try { const doc = await api('control', { enabled }); text('admission', doc.enabled ? 'Factory resumed.' : 'Factory paused.'); await refresh(); }
    catch (e) { text('admission', e.message); }
  });
  root.querySelectorAll('[data-factory-view]').forEach(button => button.addEventListener('click', async () => {
    try {
      const payload = await api('view?kind=' + button.dataset.factoryView);
      text('event', payload.content_type === 'application/json' ? JSON.stringify(JSON.parse(payload.raw), null, 2) : payload.raw);
      $('event-details').open = true;
    } catch (e) { text('admission', e.message); }
  }));
  window.addEventListener('hashchange', permalink);
  document.addEventListener('visibilitychange', () => { clearInterval(timer); if (!document.hidden) { refresh(); timer = setInterval(refresh, 60000); } });
  refresh(); timer = setInterval(refresh, 60000);
})();
