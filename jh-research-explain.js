/* Received research evidence, not investment authority. No storage, polling or votes. */
(function (root) {
  'use strict';
  const own = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
  const object = x => x !== null && typeof x === 'object' && !Array.isArray(x);
  const printable = x => x === undefined ? 'Not reported' : typeof x === 'string' ? x : JSON.stringify(x, null, 2);
  const finite = x => typeof x === 'number' && Number.isFinite(x) && (!Number.isInteger(x) || Number.isSafeInteger(x));
  function numberField(row, key, aliases = []) {
    const selected = [key, ...aliases].find(k => object(row) && own(row, k));
    return {key:selected || key, value:selected && finite(row[selected]) ? row[selected] : null};
  }
  function collection(packet, keys) {
    const key = keys.find(k => object(packet) && own(packet, k));
    if (key === undefined) return {status:'unavailable', reason:'No recognized collection reported', key:null, rows:[]};
    if (!Array.isArray(packet[key])) return {status:'unavailable', reason:key + ' is not an array; no legacy fallback used', key, rows:[]};
    const rows = [], malformed = [];
    packet[key].forEach((value, index) => {
      const pointer = '/' + key.replace(/~/g, '~0').replace(/\//g, '~1') + '/' + index;
      if (object(value)) rows.push({value, pointer}); else malformed.push(pointer);
    });
    return {status:malformed.length ? 'partial' : 'available', key, rows, malformed,
      reason:malformed.length ? malformed.length + ' non-object records retained in the original packet' : rows.length + ' received records'};
  }
  function identity(row) {
    const ids = ['ticker', 'symbol'].filter(k => own(row, k)).map(k => row[k]);
    if (!ids.length || ids.some(x => typeof x !== 'string' || !x.trim())) return null;
    const normalized = ids.map(x => x.trim().toUpperCase());
    return new Set(normalized).size === 1 ? normalized[0] : null;
  }
  function select(packet, keys, ticker) {
    const found = collection(packet, keys);
    return {...found, matches:found.rows.filter(r => identity(r.value) === ticker)};
  }
  function eventTime(value) {
    if (typeof value !== 'string') return null;
    const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
    if (!m) return null;
    const [,year,month,day,hour,minute,second,,offsetHour,offsetMinute] = m;
    if (+year < 1000 || +month < 1 || +month > 12 || +day < 1 || +day > new Date(Date.UTC(+year,+month,0)).getUTCDate() || +hour > 23 || +minute > 59 || +second > 59 || +(offsetHour || 0) > 23 || +(offsetMinute || 0) > 59) return null;
    const n = Date.parse(value); return Number.isFinite(n) ? n : null;
  }
  // Numeric tokens stay available verbatim in the original. Reject false zero and
  // unsafe integral projections before using JSON numbers in the displayed model.
  function checkedDecode(raw, io) {
    const source = new TextDecoder('utf-8', {fatal:true, ignoreBOM:true}).decode(raw);
    const tokens = /"(?:[^"\\]|\\.)*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/g;
    for (const m of source.matchAll(tokens)) {
      if (m[0][0] === '"') continue;
      const n = Number(m[0]), mantissa = m[0].split(/[eE]/)[0];
      if (n === 0 && /[1-9]/.test(mantissa)) throw Error('Numeric underflow: original retained, projection unavailable');
      if (Number.isInteger(n) && !Number.isSafeInteger(n)) throw Error('Unsafe integer: original retained, projection unavailable');
    }
    const data = io.decode(raw);
    if (!object(data)) throw Error('Expected a packet object; original retained');
    return {data, source};
  }
  async function receive(url, options = {}) {
    const io = options.io || root.JHEvidenceIO, fetcher = options.fetcher || root.fetch.bind(root);
    const receipt = {url, status:'unavailable', received_at:null, http_status:null, raw:null, data:null, sha256:null};
    try {
      const target = new URL(url, root.location?.href || 'https://justhodl.ai/');
      target.searchParams.set('exact', '1'); target.searchParams.set('nogen', '1');
      receipt.request_url = target.href;
      receipt.raw = await io.readComplete(async signal => {
        const response = await fetcher(target.href, {cache:'no-store', signal});
        receipt.http_status = response.status;
        return response;
      }, {timeoutMs:options.timeoutMs || 12000});
      receipt.received_at = new Date().toISOString();
      if (root.crypto?.subtle) receipt.sha256 = Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256', receipt.raw)), b => b.toString(16).padStart(2, '0')).join('');
      receipt.source = new TextDecoder('utf-8', {fatal:true, ignoreBOM:true}).decode(receipt.raw);
      Object.assign(receipt, checkedDecode(receipt.raw, io), {status:'received'});
    } catch (error) { receipt.reason = String(error.message || error); }
    return receipt;
  }
  function el(tag, text, className) {
    const n = document.createElement(tag);
    if (text !== undefined) n.textContent = String(text);
    if (className) n.className = className;
    return n;
  }
  function inspect(label, value, original = false) {
    const details = el('details', undefined, 'research-inspect');
    details.append(el('summary', label));
    // Expand on demand: the complete value is retained, with no slicing.
    details.addEventListener('toggle', () => {
      if (!details.open || details.querySelector('pre')) return;
      const pre = el('pre', original ? value : printable(value)); pre.tabIndex = 0;
      pre.setAttribute('aria-label', label); details.append(pre);
    });
    return details;
  }
  function packetClocks(packet) {
    if (!object(packet)) return 'Packet clocks unavailable';
    const parts = ['generated_at', 'as_of', 'observation_date', 'published_at'].filter(k => own(packet, k)).map(k => k + ': ' + printable(packet[k]));
    return parts.length ? parts.join(' · ') : 'Packet clocks not reported';
  }
  function sourceCard(key, receipt) {
    const card = el('section', undefined, 'research-source');
    card.append(el('h3', key + ' — ' + receipt.status), el('p', receipt.url || 'Research-only boundary', 'research-meta'));
    if (receipt.status === 'research_only_abstain') {
      card.append(el('p', 'No request and no investment vote. Use Momentum observations for its separate research record.'));
      return card;
    }
    card.append(el('p', receipt.reason || packetClocks(receipt.data), 'research-meta'));
    if (receipt.http_status !== null) card.append(el('p', 'HTTP ' + receipt.http_status + ' · received_at: ' + (receipt.received_at || 'unavailable'), 'research-meta'));
    if (object(receipt.data) && own(receipt.data, 'quality')) card.append(inspect('Reported quality (not independently verified)', receipt.data.quality));
    if (receipt.raw) {
      card.append(el('p', receipt.raw.byteLength + ' complete received bytes · SHA-256: ' + (receipt.sha256 || 'unavailable'), 'research-meta'));
      if (receipt.source !== undefined) card.append(inspect('Complete original JSON text', receipt.source, true));
      const button = el('button', 'Download original bytes'); button.type = 'button';
      button.addEventListener('click', () => {
        const href = URL.createObjectURL(new Blob([receipt.raw], {type:'application/octet-stream'}));
        const a = el('a'); a.href = href; a.download = key + '-received.json'; a.click();
        setTimeout(() => URL.revokeObjectURL(href), 1000);
      }); card.append(button);
    }
    return card;
  }
  function showSources(container, receipts) {
    container.replaceChildren(...Object.entries(receipts).map(([k, r]) => sourceCard(k, r)));
  }
  function receivedSummary(receipts) {
    const all = Object.values(receipts), withheld = all.filter(r => r.status === 'research_only_abstain').length;
    return all.filter(r => r.status === 'received').length + '/' + (all.length - withheld) + ' packets received. ' +
      (all.filter(r => r.status !== 'received' && r.status !== 'research_only_abstain').length) + ' unavailable. ' +
      (withheld ? withheld + ' research-only source not requested. ' : '') +
      'Receipt time is not observation freshness; source independence and portfolio authority are unqualified.';
  }
  const CROSS_COLLECTIONS = {
    compound:[['ranked']], asymmetric:[['top_setups']], master:[['top_tickers', 'ranked', 'rows'], ['unranked_tickers']],
    nobrainers:[['all_scored', 'top_setups', 'setups', 'tier_a', 'top_picks', 'candidates', 'results'], ['tier_b']],
    insiders:[['transactions', 'activity', 'events', 'records', 'trades'], ['sell_transactions'], ['clusters'], ['big_buys']],
    eps:[['request_records', 'all_qualifying', 'results', 'tickers']],
    prePump:[['all_qualifying', 'signals', 'candidates', 'results']], smartMoney:[['clusters', 'results']],
    deepValue:[['all_qualifying', 'results', 'picks']], optionsFlow:[['unusual', 'results']],
    microcap:[['request_records', 'all_qualifying', 'candidates', 'results']],
    pead:[['request_records', 'all_qualifying', 'signals', 'results']], volSqueeze:[['signals', 'candidates', 'results']],
    themes:[['themes']], themeTiers:[['themes', 'tiers']], revenue:[['request_records', 'all_qualifying', 'results']], filings:[['filings', 'results']]
  };
  function crossModel(receipts, ticker) {
    return Object.entries(receipts).map(([key, receipt]) => {
      const groups = receipt.status === 'received' ? (CROSS_COLLECTIONS[key] || []).map(keys => select(receipt.data, keys, ticker)) : [];
      return {key, receipt, groups, matches:groups.flatMap(g => g.matches.map(r => ({...r, collection:g.key})))};
    });
  }
  function scoreLine(row, key, aliases = []) {
    const score = numberField(row, key, aliases);
    return score.key + ': ' + (score.value === null ? 'Unavailable' : String(score.value));
  }
  function recordCard(key, record, packet) {
    const card = el('article', undefined, 'research-record');
    card.append(el('h3', key + ' · ' + record.pointer), el('p', packetClocks(packet), 'research-meta'));
    const row = record.value;
    if (key === 'master') {
      card.append(el('p', record.collection === 'unranked_tickers' ? 'Withheld from ranking' : 'Reported research ranking'), el('p', scoreLine(row, 'score'), 'research-value'));
      if (own(row, 'reasons')) card.append(inspect('Complete withholding reasons', row.reasons));
      if (own(row, 'score_calculation')) card.append(inspect('Complete reported calculation', row.score_calculation));
    } else if (key === 'compound') {
      card.append(el('p', scoreLine(row, 'compound_score', ['score']), 'research-value'));
      card.append(el('p', 'These are reported component inputs. Their sum is not necessarily the composite; overlapping systems are not independent evidence.'));
      if (object(row.scores)) {
        const list = el('dl', undefined, 'research-scores');
        for (const [system, value] of Object.entries(row.scores)) {
          list.append(el('dt', system), el('dd', finite(value) ? String(value) : 'Unavailable (see original)'));
        }
        card.append(list);
      } else card.append(el('p', 'Per-system scores unavailable; no default weights supplied.'));
      if (own(row, 'systems')) card.append(inspect('Reported system identifiers', row.systems));
      if (own(row, 'details')) card.append(inspect('Complete reported component details', row.details));
    } else if (key === 'asymmetric') {
      card.append(el('p', scoreLine(row, 'composite_score'), 'research-value'));
      card.append(el('p', 'Reported dimension results; omitted dimensions do not imply failure.'));
      if (own(row, 'dims_passed_list')) card.append(inspect('Reported passed dimensions', row.dims_passed_list));
    }
    card.append(inspect('Complete parsed record ' + record.pointer, row));
    return card;
  }
  function renderCross(container, receipts, rawTicker) {
    const ticker = typeof rawTicker === 'string' ? rawTicker.trim().toUpperCase() : '';
    const model = crossModel(receipts, ticker), count = model.reduce((n, m) => n + m.matches.length, 0);
    container.replaceChildren(el('h2', ticker ? ticker + ' — ' + count + ' matched source records' : 'Choose a ticker to inspect received records'));
    container.append(el('p', 'Matches are reported occurrences, not independent confirmations or an investment recommendation. No match does not establish that a ticker is outside the universe.'));
    for (const item of model) {
      const section = el('section', undefined, 'research-source');
      section.append(el('h3', item.key), el('p', item.receipt.status, 'research-meta'));
      for (const group of item.groups) section.append(el('p', (group.key || 'Collection') + ' — ' + group.status + ': ' + group.reason + (ticker ? '; ' + group.matches.length + ' matching records' : ''), 'research-meta'));
      if (!item.groups.length) section.append(el('p', item.receipt.reason || 'No ticker collection examined. Original packet access is below when available.'));
      if (ticker) {
        let shown = 0; const more = el('button', 'Show more matching records'); more.type = 'button';
        function page() {
          more.remove();
          for (const record of item.matches.slice(shown, shown + 50)) section.append(recordCard(item.key, record, item.receipt.data));
          shown += 50;
          if (shown < item.matches.length) { more.textContent = 'Show more matching records (' + (item.matches.length - shown) + ' remaining)'; section.append(more); }
        }
        more.addEventListener('click', page); page();
      }
      container.append(section);
    }
    return {ticker, matches:count};
  }
  function nowModel(receipts) {
    const events = [], snapshots = [], diagnostics = [];
    const append = (key, keys, kind, category) => {
      const receipt = receipts[key];
      if (receipt?.status !== 'received') { diagnostics.push(key + ': unavailable'); return; }
      const group = collection(receipt.data, keys);
      diagnostics.push(key + '/' + (group.key || keys[0]) + ': ' + group.status + ' — ' + group.reason);
      for (const row of group.rows) {
        const explicit = ['event_at', 'occurred_at', 'changed_at'].find(k => own(row.value, k));
        const ts = explicit ? eventTime(row.value[explicit]) : null;
        const entry = {key, category, kind, ...row, packet:receipt.data, eventClock:explicit ? explicit + ': ' + printable(row.value[explicit]) + (ts === null ? ' (invalid/unsupported clock; not sorted as dated)' : '') : 'Event time not reported', time:ts};
        (kind === 'event' ? events : snapshots).push(entry);
      }
    };
    append('compound', ['new_alerts'], 'event', 'convergence');
    append('opportunities', ['changes'], 'event', 'setup');
    const master = receipts.master;
    if (master?.status === 'received' && object(master.data.alerts)) {
      snapshots.push({key:'master', category:'setup', kind:'snapshot', pointer:'/alerts', value:master.data.alerts, packet:master.data});
      diagnostics.push('master/alerts: aggregate counters, not timestamped events');
    } else append('master', ['alerts'], 'event', 'setup');
    append('bestSetups', ['top_setups'], 'snapshot', 'setup');
    for (const key of ['funding', 'crypto', 'bondVol', 'signalBoard']) {
      const receipt = receipts[key];
      if (receipt?.status === 'received') snapshots.push({key, category:key === 'crypto' ? 'risk' : 'macro', kind:'snapshot', pointer:'', value:receipt.data, packet:receipt.data});
      else diagnostics.push(key + ': unavailable');
    }
    events.sort((a, b) => a.time === null ? b.time === null ? 0 : 1 : b.time === null ? -1 : b.time - a.time);
    return {events, snapshots, diagnostics};
  }
  function renderNow(container, model, filter = 'all') {
    container.replaceChildren();
    for (const [key, title] of [['events', 'Source-reported changes'], ['snapshots', 'Current packet snapshots — no change inferred']]) {
      const rows = model[key].filter(r => filter === 'all' || filter === r.category || filter === r.kind);
      const section = el('section', undefined, 'research-section'); section.append(el('h2', title + ' (' + rows.length + ')'));
      if (!rows.length) section.append(el('p', 'No matching records in the received, recognized collections. Check source availability below.'));
      let shown = 0;
      const more = el('button', 'Show more records'); more.type = 'button';
      function page() {
        more.remove();
        for (const r of rows.slice(shown, shown + 50)) {
          const card = el('article', undefined, 'research-record'), symbol = identity(r.value);
          card.append(el('h3', r.key + (symbol ? ' · ' + symbol : '') + ' · ' + (r.pointer || '(packet root)')));
          if (symbol) { const a = el('a', 'Open dossier'); a.href = '/dossier.html?t=' + encodeURIComponent(symbol); card.append(a); }
          card.append(el('p', r.kind === 'event' ? r.eventClock + ' · Source-reported change; not independently verified' : 'Snapshot only', 'research-meta'));
          card.append(el('p', packetClocks(r.packet), 'research-meta'));
          for (const k of ['reason', 'message', 'change', 'note', 'regime', 'risk_level']) if (own(r.value, k)) card.append(el('p', k + ': ' + printable(r.value[k])));
          for (const k of ['score', 'conviction', 'dump_risk_score']) if (own(r.value, k)) card.append(el('p', scoreLine(r.value, k)));
          card.append(inspect('Complete parsed record ' + (r.pointer || '(packet root)'), r.value)); section.append(card);
        }
        shown += 50;
        if (shown < rows.length) { more.textContent = 'Show more (' + (rows.length - shown) + ' remaining)'; section.append(more); }
      }
      more.addEventListener('click', page); page(); container.append(section);
    }
    const diagnostics = el('section', undefined, 'research-source'); diagnostics.append(el('h2', 'Collection availability'));
    for (const line of model.diagnostics) diagnostics.append(el('p', line, 'research-meta'));
    container.append(diagnostics);
  }
  const api = {object, finite, numberField, collection, identity, select, eventTime, checkedDecode, receive, inspect, packetClocks, sourceCard, showSources, receivedSummary, crossModel, renderCross, nowModel, renderNow};
  if (typeof module === 'object' && module.exports) module.exports = api; else root.JHResearchExplain = api;
})(typeof globalThis === 'object' ? globalThis : this);
