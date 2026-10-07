/* Shared research dossier. Native source strings are rendered as text only. */
(function (root) {
  'use strict';
  const CONTRACT = 'research-network.v1';
  const noAuthority = ['calls_eligible', 'forecast_qualified', 'sizing_eligible', 'execution_eligible', 'promotion_eligible'];
  const label = value => ({within_publication_sla:'publication current',context_only:'market context',adapter_mismatch:'source format changed',listed_security:'listed security',unknown:'publication clock unknown',empty:'no asset rows reported'})[value] || String(value || 'unavailable').replaceAll('_',' ');
  function validate(doc) {
    if (!doc || doc.contract !== CONTRACT || doc.access !== 'PUBLIC_RESEARCH' ||
        !/^[a-f0-9]{64}$/.test(doc.publication_id || '') ||
        noAuthority.some(k => doc[k] !== false) || doc.independent_investment_votes !== 0 ||
        doc.snapshot_key !== 'data/research-network/publications/' + doc.publication_id + '/manifest.json') {
      throw new Error('Research publication contract is unavailable.');
    }
    return doc;
  }
  function sourceURL(key) {
    if (typeof key !== 'string' || !/^data\/research-network\/(?:originals\/[a-f0-9]{64}\.json\.gz|publications\/[a-f0-9]{64}\/(?:manifest|[a-f0-9]{2})\.json)$/.test(key)) return null;
    return '/' + key;
  }
  function matches(doc, symbol) {
    return Object.entries(doc.entity_states || {}).filter(([, value]) => value.symbol === symbol.trim().toUpperCase()).map(([id]) => id);
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = {validate, sourceURL, matches};
  if (!root.document) return;
  const document = root.document;
  function el(tag, text, cls) {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    if (cls) node.className = cls;
    return node;
  }
  function details(title, value) {
    const node = el('details'); node.append(el('summary', title), el('pre', JSON.stringify(value, null, 2))); return node;
  }
  function link(text, key) {
    const url = sourceURL(key);
    if (!url) return el('span', 'Retained original unavailable');
    const node = el('a', text); node.href = url; node.target = '_blank'; node.rel = 'noopener'; return node;
  }
  async function fetchJSON(key) {
    const response = await fetch('/' + key, {cache: 'no-store', credentials: 'omit'});
    if (!response.ok) throw new Error('Research feed unavailable (HTTP ' + response.status + ').');
    const raw = await response.arrayBuffer();
    if (raw.byteLength > 64 * 1024 * 1024) throw new Error('Research feed exceeds the display limit.');
    return {doc: JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(raw)), raw};
  }
  async function hash(raw) {
    return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', raw))).map(v => v.toString(16).padStart(2, '0')).join('');
  }
  function mount() {
    const host = document.getElementById('research-network'); if (!host) return;
    host.classList.add('rn');
    host.append(el('h2', 'Connected research · Ticker 360'));
    const status = el('p', 'Loading shared research…'); status.id = 'rn-status'; status.setAttribute('role', 'status');
    const policy = el('p', 'Research can remain available during a risk hold. These observations do not authorize trades, sizing or promotion.', 'rn-note');
    const form = el('form'); form.className = 'rn-controls';
    const symbolLabel = el('label', 'Symbol '), input = el('input'); input.id = 'rn-symbol'; input.maxLength = 25; input.value = document.getElementById('tk')?.value || 'NVDA'; symbolLabel.append(input);
    const button = el('button', 'Review asset'); button.type = 'submit';
    const reload = el('button', 'Refresh research'); reload.type = 'button';
    const scopeLabel = el('label', 'Reported identity '), scopes = el('select'); scopes.id = 'rn-scope'; scopeLabel.append(scopes); scopeLabel.hidden = true;
    form.append(symbolLabel, button, reload, scopeLabel);
    const view = el('div'); view.id = 'rn-view';
    const overview = el('div'); overview.id = 'rn-overview';
    host.append(status, policy, form, view, overview);
    let manifest = null, generation = 0, searchGeneration = 0;
    function renderOverview(doc) {
      overview.replaceChildren(el('h3', 'Review from every angle'));
      const grid = el('div', undefined, 'rn-grid');
      for (const review of Object.values(doc.specialists || {})) {
        const card = el('details', undefined, 'rn-card');
        card.append(el('summary', review.title + ' · ' + label(review.status)), el('p', review.question));
        card.append(el('p', 'Missing inputs: ' + (review.missing?.join(', ') || 'none reported')));
        for (const sid of review.sources || []) {
          const source = doc.sources[sid]; if (!source) continue;
          const row = el('details');
          row.append(el('summary', sid + ' · ' + label(source.status) + ' · ' + label(source.publication_freshness)));
          row.append(el('p', 'Published: ' + (source.generated_at || 'unknown') + '. Observation freshness: unverified.'));
          row.append(link('Open complete archived source', source.snapshot_key), details('Source contract, clocks and summary', source));
          card.append(row);
        }
        grid.append(card);
      }
      overview.append(grid, details('Consumer processing receipts', doc.consumer_processing || {}),
                      details('Publication and source lineage', {publication_id: doc.publication_id, compiler: doc.compiler, dependency_receipt: doc.dependency_receipt}),
                      link('Open full network manifest', doc.snapshot_key));
    }
    function renderEntity(entity, doc) {
      view.replaceChildren(el('h3', entity.identity.symbol + ' · ' + label(entity.identity.scope)));
      const risk = doc.sources['khalid-risk'];
      view.append(el('p', 'Reported risk policy: ' + (risk?.summary?.capital_decision || risk?.reported_status || 'unavailable') + '. Risk publication: ' + label(risk?.publication_freshness) + '.'));
      view.append(el('p', 'Grouping uses reported symbols. Instrument identity and source independence are not yet qualified.', 'rn-note'));
      const thesis = entity.thesis || {};
      view.append(el('p', thesis.what_is_happening));
      const questions = [
        ['Why might it matter?', thesis.why_it_matters],
        ['Which independent evidence supports it?', thesis.independent_support],
        ['What contradicts it?', thesis.contradictions?.length ? thesis.contradictions : 'No opposing explicit directions detected; this is not proof of agreement.'],
        ['What is missing?', {angles: thesis.missing, data_concerns: thesis.data_concerns}],
        ['Which horizon applies?', thesis.horizons?.length ? thesis.horizons : 'Source horizons were not supplied.'],
        ['What would invalidate the thesis?', thesis.invalidation?.length ? thesis.invalidation : 'Invalidation criteria were not supplied.'],
        ['How does it affect my portfolio?', 'Open the authenticated portfolio risk view for holding-specific context. Public model portfolios are not your account.'],
        ['What changed since the last review?', thesis.changes]
      ];
      const questionsNode = el('div', undefined, 'rn-grid');
      for (const [question, answer] of questions) questionsNode.append(details(question, answer));
      view.append(questionsNode, el('h4', 'Attributed evidence'));
      for (const evidence of entity.evidence || []) {
        const source = doc.sources[evidence.source_id] || {};
        const row = el('details', undefined, 'rn-evidence');
        row.append(el('summary', evidence.source_id + ' · ' + label(source.publication_freshness)),
                   el('p', 'Original row: ' + evidence.pointer),
                   el('p', 'Reported observation clocks: ' + JSON.stringify(evidence.reported_clocks)),
                   link('Inspect complete original', source.snapshot_key),
                   details('All projected facts and qualification', evidence));
        view.append(row);
      }
      view.append(details('Complete dossier record', entity));
    }
    async function selectEntity(eid) {
      const serial = ++searchGeneration, doc = manifest;
      view.replaceChildren(el('p', 'Loading asset evidence…'));
      try {
        const item = doc.entity_states[eid], ref = doc.shards[item.shard];
        const expected = 'data/research-network/publications/' + doc.publication_id + '/' + item.shard + '.json';
        if (!/^[a-f0-9]{2}$/.test(item.shard) || ref.key !== expected || !sourceURL(ref.key)) throw new Error('Invalid dossier reference.');
        const {doc: shard, raw} = await fetchJSON(ref.key);
        if (await hash(raw) !== ref.sha256 || raw.byteLength !== ref.bytes || shard.publication_id !== doc.publication_id || shard.contract !== CONTRACT) throw new Error('Dossier source verification failed.');
        const entity = shard.entities?.[eid];
        if (!entity || entity.entity_id !== eid || noAuthority.some(k => entity[k] !== false)) throw new Error('Dossier qualification is unavailable.');
        if (serial !== searchGeneration || doc !== manifest) return;
        renderEntity(entity, doc);
      } catch (error) {
        if (serial === searchGeneration) view.replaceChildren(el('p', error.message));
      }
    }
    function search() {
      ++searchGeneration;
      if (!manifest) return;
      const ids = matches(manifest, input.value);
      scopes.replaceChildren(); scopeLabel.hidden = ids.length < 2;
      if (!ids.length) {view.replaceChildren(el('p', 'No research dossier for this reported symbol.')); return;}
      for (const id of ids) {const option = el('option', id); option.value = id; scopes.append(option);}
      if (ids.length > 1) {
        const option = el('option', 'Choose an identity scope'); option.value = ''; option.selected = true; scopes.prepend(option);
        view.replaceChildren(el('p', 'Several identity scopes report this symbol. Choose one to keep them separate.'));
      } else selectEntity(ids[0]);
    }
    async function load() {
      const serial = ++generation; ++searchGeneration; manifest = null;
      status.textContent = 'Loading shared research…'; view.replaceChildren(); overview.replaceChildren(); scopeLabel.hidden = true;
      try {
        const {doc} = await fetchJSON('data/research-network.json'); validate(doc);
        if (serial !== generation) return;
        manifest = doc;
        const age = (Date.now() - Date.parse(doc.generated_at)) / 3600000;
        const clock = Number.isFinite(age) && age >= 0 && age <= 26 ? 'within publication SLA' : 'stale or invalid publication clock';
        status.textContent = doc.available_sources + '/' + doc.source_count + ' sources available · ' + doc.entity_count + ' reported identities · ' + doc.generated_at + ' · ' + clock;
        renderOverview(doc); search();
      } catch (error) {if (serial === generation) status.textContent = error.message;}
    }
    form.addEventListener('submit', event => {event.preventDefault(); search();});
    scopes.addEventListener('change', () => {if (scopes.value) selectEntity(scopes.value);});
    reload.addEventListener('click', load);
    root.addEventListener('jh:dossier-symbol', event => {input.value = String(event.detail || ''); search();});
    load();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount); else mount();
})(typeof window === 'undefined' ? globalThis : window);
