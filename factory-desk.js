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
    for (const url of ['/data/ai-factory.json', '/data/factory-public.json', '/data/student-state.json', '/student-state.json']) {
      try {
        const r = await fetch(url, { cache: 'no-store' });
        if (!r.ok) continue;
        const raw = await r.text();
        try { return await verifyStateRaw(raw); } catch (e) {
          const doc = JSON.parse(raw);
          if (doc && Array.isArray(doc.agents) && Array.isArray(doc.skillbook || [])) {
            doc.schema_version = doc.schema_version || 'student-state.v1';
            doc.state_version = Number.isInteger(doc.state_version) ? doc.state_version : 0;
            doc.gen = Number.isInteger(doc.gen) ? doc.gen : 0;
            doc.skillbook = doc.skillbook || [];
            doc.wall = doc.wall || {};
            doc.season = doc.season || {};
            doc.health = doc.health || { status: 'live', errors: [] };
            doc.model = doc.model || { status: 'unknown' };
            doc.outer_status = doc.outer_status || {};
            doc.checksum = doc.checksum || 'public-projection';
            return doc;
          }
        }
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
    text('integrity', doc.checksum === 'public-projection' ? 'Public factory feed · exams stay private' : ('State v' + doc.state_version + ' · SHA-256 verified · ' + String(doc.checksum||'').slice(0, 12)));
    text('budget', '1 experiment/day · 0 GPU jobs · 0 paid model calls · AWS service charges apply');
    text('exam-note', doc.fit ? doc.fit.n + ' protected coding cases; baseline ' + Math.round(doc.fit.baseline * 100) + '%. This measures one repair family.' : 'Only independently checked results appear here.');
    text('errors', doc.health.errors.length ? doc.health.errors.map(e => e.phase + ': ' + (e.detail || e.error)).join(' · ') : 'State and worker checks passed.');
    $('agents').innerHTML = doc.agents.map(a => '<article class="factory-agent"><b>' + safe(a.name) + '</b><span>' + safe(a.status.replaceAll('_', ' ')) + '</span><p>' + safe(a.purpose) + '</p></article>').join('');
    paintRanks(doc);
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
  function paintRanks(doc) {
    const body = $('ranks');
    if (!body) return;
    const ranks = doc.ranks || {};
    const cards = Array.isArray(ranks.cards) ? ranks.cards : [];
    const pct = v => (v === null || v === undefined) ? '—' : Math.round(Number(v) * 100) + '%';
    body.innerHTML = cards.length ? cards.map(c => '<tr class="' + (c.status === 'retired' ? 'factory-retired' : '') + '"><td>' + safe(c.alias) +
      (c.co ? ' <small>← ' + safe(c.co) + '</small>' : '') + '</td><td>' + safe(c.rank || 'recruit') + '</td><td>' + safe(c.status || 'active') +
      '</td><td>' + (c.graded_window || 0) + '</td><td>' + (c.errors_window || 0) + '</td><td>' + pct(c.pass_rate) + '</td><td>' + pct(c.prior_pass_rate) +
      '</td><td>' + safe((c.last_verdict || 'awaiting window').replaceAll('_', ' ').replaceAll(':', ' · ')) + '</td></tr>').join('')
      : '<tr><td colspan="8" class="factory-muted">Awaiting the first discipline pass.</td></tr>';
    const d = doc.discipline || {};
    const changes = Array.isArray(d.changes) ? d.changes.slice(-5) : [];
    text('discipline', (ranks.active || 0) + ' active · ' + (ranks.retired || 0) + ' retired · last pass ' + (d.last_run_at ? age(d.last_run_at) : 'pending') +
      (changes.length ? ' · latest: ' + changes.map(x => x.alias + ' ' + x.decision + ' (' + x.reason + ')').join(' | ') : ' · no promotions or retirements yet'));
  }

  async function wall() {
    try {
      let board;
      try {
        board = JSON.parse((await api('view?kind=board')).raw);
      } catch (authErr) {
        const r = await fetch('/data/factory-board.json', { cache: 'no-store' });
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
  async function waitToken() {
    if (window.JustHodlAuth && JustHodlAuth.init) {
      try { await JustHodlAuth.init(); } catch (e) {}
    }
    if (window.JustHodlAuth && JustHodlAuth.getAccessToken) {
      for (let i = 0; i < 8; i++) {
        const token = await JustHodlAuth.getAccessToken();
        if (token) return token;
        await new Promise(r => setTimeout(r, 250));
      }
    }
    return null;
  }
  async function api(action, body) {
    const token = await waitToken();
    if (!token) throw Error('Still restoring your session — wait a second, you are signed in on the page.');
    const r = await fetch('/api/v1/factory/' + action, { method: body ? 'POST' : 'GET',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token }, body: body ? JSON.stringify(body) : undefined });
    const doc = await r.json();
    if (!r.ok) throw Error((doc.error || 'Factory request failed') + (doc.detail ? ' — ' + doc.detail : ''));
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
  
  let chatBusy = false;
  function paintChat(doc) {
    const log = $('chat-log');
    if (!log) return;
    const msgs = (doc && doc.messages) || [];
    if (doc && doc.workers) {
      const w = doc.workers;
      text('chat-workers', 'Workers: ' + (w.active || 0) + ' live / ' + (w.queued || 0) + ' queued · cap ' + (w.cap || 48));
    }
    if (!msgs.length) {
      log.textContent = 'Signed in. Talk to Student, Coder, Researcher, Investor, Deployer or a principle card. Spawn workers for a task — live cap 48, extras queue.';
      return;
    }
    log.innerHTML = msgs.map(m => {
      const mine = m.role === 'owner' || m.role === 'guest';
      const tag = mine ? ((m.from || '') + ' → ' + (m.to || '')) : ((m.from || 'brain') + (m.model ? ' · ' + m.model : ''));
      return '<div class="factory-msg ' + (mine ? 'me' : 'bot') + '"><small>' + safe(tag) + '</small>' + safe(m.text || '').replace(/\n/g,'<br>') + '</div>';
    }).join('');
    log.scrollTop = log.scrollHeight;
  }
  async function loadChat() {
    if (chatBusy) return;
    const log = $('chat-log');
    try { paintChat(await api('chat')); }
    catch (e) {
      if (log) log.textContent = (e && e.message) || 'Chat is waking up…';
    }
  }
  const form = $('chat-form');
  if (form) form.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const input = $('chat-in');
    const typed = (input && input.value || '').trim();
    if (!typed) return;
    chatBusy = true;
    const to = ($('chat-to') && $('chat-to').value) || 'model';
    const spawn = Number(($('spawn-n') && $('spawn-n').value) || 0);
    const send = $('chat-send');
    const log = $('chat-log');
    if (log && log.textContent && !log.querySelector('.factory-msg')) log.textContent = '';
    if (log) log.insertAdjacentHTML('beforeend', '<div class="factory-msg me"><small>you → ' + safe(to) + '</small>' + safe(typed) + '</div><div class="factory-msg bot" id="factory-chat-wait"><small>' + safe(to) + '</small>thinking…</div>');
    if (log) log.scrollTop = log.scrollHeight;
    if (input) input.value = '';
    if (send) send.disabled = true;
    try {
      const payload = { text: typed, to: to };
      if (spawn > 0) { payload.spawn = spawn; payload.role = to === 'coder' ? 'coder' : 'researcher'; payload.task = typed; }
      const doc = await api('chat', payload);
      paintChat(doc);
      if (doc.workers && doc.workers.note) text('admission', doc.workers.note);
      if (doc.workers && doc.workers.declared != null) text('chat-workers', 'Fleet ' + (doc.workers.declared||0) + ' declared · ' + (doc.workers.active||0) + ' live / ' + (doc.workers.queued||0) + ' queued · compute cap ' + (doc.workers.cap||8));
    } catch (e) {
      const wait = document.getElementById('factory-chat-wait');
      if (wait) wait.remove();
      if (log) log.insertAdjacentHTML('beforeend', '<div class="factory-msg bot"><small>desk</small>' + safe((e && e.message) || 'send failed') + '</div>');
      if (log) log.scrollTop = log.scrollHeight;
    }
    chatBusy = false;
    if (send) send.disabled = false;
    if (input) input.focus();
  });
  const LEARN = {
    code: { to: 'coder', text: 'Go learn how to learn how to code. Start at stage 0: deliberate practice, TDD as a learning device, rubber-duck until you can teach it. Then climb toward production-engineer caliber. Keep only what grades on the protected exam. Do not train weights. Do not binge tutorials.' },
    markets: { to: 'investor', text: 'Go learn financial markets — stocks, bonds, and the yield curve — from public sources and the warehouse delayed tape. No orders.' },
    investing: { to: 'investor', text: 'Go learn investing from Jesse Livermore, Wyckoff, George Soros, and Stanley Druckenmiller. Cite them. Evidence before size. No orders.' }
  };
  root.querySelectorAll('[data-learn]').forEach(btn => {
    btn.addEventListener('click', () => {
      const spec = LEARN[btn.getAttribute('data-learn')];
      if (!spec) return;
      const to = $('chat-to');
      const input = $('chat-in');
      if (to) to.value = spec.to;
      if (input) input.value = spec.text;
      if (form && form.requestSubmit) form.requestSubmit();
      else if (form) form.dispatchEvent(new Event('submit', { cancelable: true, bubbles: true }));
    });
  });
  const agentsRoot = $('agents');
  if (agentsRoot) agentsRoot.addEventListener('click', (ev) => {
    const card = ev.target.closest('.factory-agent');
    if (!card) return;
    const name = ((card.querySelector('b') || {}).textContent || '').trim().toLowerCase();
    const map = { student: 'student', coder: 'coder', researcher: 'researcher', investor: 'investor', deployer: 'deployer',
      livermore: 'livermore', wyckoff: 'wyckoff', soros: 'soros', druckenmiller: 'druckenmiller', 'your brain': 'model', brain: 'model' };
    const sel = $('chat-to');
    if (sel && map[name]) sel.value = map[name];
    agentsRoot.querySelectorAll('.factory-agent').forEach(c => c.classList.remove('on'));
    card.classList.add('on');
    const box = document.getElementById('factory-chat-box');
    if (box) box.scrollIntoView({ behavior: 'smooth', block: 'center' });
    const input = $('chat-in'); if (input) input.focus();
  });

  window.addEventListener('hashchange', permalink);
  // A12: one polling function for initial load and visibility recovery; chat settlement always rides the poll
  const poll = () => { refresh(); loadChat(); };
  document.addEventListener('visibilitychange', () => { clearInterval(timer); if (!document.hidden) { poll(); timer = setInterval(poll, 60000); } });
  (async () => {
    if (window.JustHodlAuth && JustHodlAuth.init) { try { await JustHodlAuth.init(); } catch (e) {} }
    await refresh();
    await loadChat();
    if (window.JustHodlAuth && JustHodlAuth.onChange) {
      JustHodlAuth.onChange(() => { refresh(); loadChat(); });
    }
  })();
  timer = setInterval(poll, 60000);
})();
