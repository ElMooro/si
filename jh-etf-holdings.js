(function (root) {
  'use strict';
  const kinds = {
    holdings: {contract: 'etf-holdings-original-research.v1', prefix: 'data/etf-holdings-research/', current: 'data/etf-holdings-research.json'},
    lookthrough: {contract: 'etf-holdings-lookthrough.v1', prefix: 'data/holdings-lookthrough-research/', current: 'data/flow-lookthrough.json'}
  };
  const prefix = kinds.holdings.prefix;
  const flags = ['calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified'];
  const esc = x => String(x ?? 'Unavailable').replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c]));
  const numeric = x => typeof x === 'string' && /^-?\d+(?:\.\d+)?$/.test(x) && Number.isFinite(Number(x)) ? Number(x) : null;
  function fmt(x) {
    if (numeric(x) === null) return 'Unavailable';
    const [whole, fraction] = x.split('.');
    return whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',') + (fraction === undefined ? '' : '.' + fraction);
  }
  const cash = x => Number.isFinite(x) ? x.toLocaleString('en-US', {style:'currency', currency:'USD'}) : 'Unavailable';
  const dates = x => Object.keys(x || {}).join(', ') || 'Unavailable';
  const kind = p => Object.keys(kinds).find(k => p?.contract === kinds[k].contract);
  const safe = k => typeof k === 'string' && /^data\/(?:etf-holdings|holdings-lookthrough)-research\/(?:runs|outputs|rows|snapshots|comparisons|memberships|directories)\/[a-f0-9]{64}\.json$/.test(k);
  function typed(p) {
    return !!(kind(p) && flags.every(f => p[f] === false) && p.call === null && p.portfolio_action === 'WAIT' &&
      p.quality?.independent_investment_votes === 0 && p.quality.weight_unit_certified === false &&
      p.funds && !Array.isArray(p.funds) && Object.keys(p.funds).length === p.quality.configured_funds &&
      Object.keys(p.funds).length > 0 && Object.keys(p.funds).length <= 1000 &&
      Object.entries(p.funds).every(([k, v]) => /^[A-Z][A-Z0-9.-]{0,14}$/.test(k) && v.ticker === k && flags.every(f => v[f] === false) && v.current && v.prior) &&
      Number.isFinite(Date.parse(p.generated_at)) && Number.isFinite(Date.parse(p.source_valid_until)) &&
      (!p.source_generated_at || Date.parse(p.source_generated_at) <= Date.parse(p.generated_at)));
  }
  function stable(v) {
    if (Array.isArray(v)) return v.map(stable);
    if (v && typeof v === 'object') return Object.fromEntries(Object.keys(v).sort().map(k => [k, stable(v[k])]));
    return v;
  }
  async function sha(raw) {
    return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256', raw)), b => b.toString(16).padStart(2, '0')).join('');
  }
  async function load(key, fetcher, signal) {
    if (!safe(key) && !Object.values(kinds).some(v => v.current === key)) throw Error('Unapproved holdings evidence path');
    const r = await fetcher('/' + key, {cache: safe(key) ? 'default' : 'no-store', signal});
    if (!r.ok) throw Error('Holdings evidence request failed');
    if (Number(r.headers?.get('content-length')) > 16 * 1024 * 1024) throw Error('Holdings evidence byte bound');
    let raw;
    if (r.body?.getReader) {
      const reader = r.body.getReader(), parts = [];let length = 0;
      try {
        for (;;) {const item = await reader.read();if (item.done) break;length += item.value.byteLength;
          if (length > 16 * 1024 * 1024) throw Error('Holdings evidence byte bound');parts.push(item.value);}
      } catch (error) {await reader.cancel();throw error;}
      const combined = new Uint8Array(length);let offset = 0;
      for (const part of parts) {combined.set(part, offset);offset += part.byteLength;}
      raw = combined.buffer;
    } else raw = await r.arrayBuffer();
    if (raw.byteLength > 16 * 1024 * 1024) throw Error('Holdings evidence byte bound');
    return {raw, doc: JSON.parse(new TextDecoder().decode(raw))};
  }
  async function retained(ref, group, fetcher, signal) {
    if (!ref || !/^[a-f0-9]{64}$/.test(ref.sha256) || ref.key !== prefix + group + '/' + ref.sha256 + '.json' ||
        !Number.isInteger(ref.bytes) || ref.bytes <= 0 || ref.bytes > 16 * 1024 * 1024) throw Error('Holdings artifact identity differs');
    const out = await load(ref.key, fetcher, signal);
    if (out.raw.byteLength !== ref.bytes || await sha(out.raw) !== ref.sha256) throw Error('Holdings artifact bytes differ');
    return out.doc;
  }
  async function verifyPacket(p, fetcher, signal) {
    if (!typed(p)) throw Error('Native dated holdings research required');
    const k = kind(p), base = kinds[k].prefix, key = p.replay?.manifest_key;
    if (!safe(key) || !key.startsWith(base + 'runs/')) throw Error('Holdings run path differs');
    const run = await load(key, fetcher, signal), m = run.doc;
    if (key !== base + 'runs/' + await sha(run.raw) + '.json' || m.contract !== 'etf-holdings-replay.v1' ||
        m.kind !== k || m.generated_at !== p.generated_at || m.output_sha256 !== p.replay.output_sha256) throw Error('Holdings run differs');
    const ref = m.output;
    if (ref?.key !== base + 'outputs/' + m.output_sha256 + '.json' || ref.sha256 !== m.output_sha256) throw Error('Holdings output identity differs');
    const out = await load(ref.key, fetcher, signal);
    if (out.raw.byteLength !== ref.bytes || await sha(out.raw) !== ref.sha256) throw Error('Holdings output bytes differ');
    const {replay, ...body} = p;
    if (JSON.stringify(stable(body)) !== JSON.stringify(stable(out.doc))) throw Error('Current holdings body differs');
    return p;
  }
  async function parallel(items, fn) {
    const result = new Array(items.length);let index = 0;
    await Promise.all(Array.from({length: Math.min(6, items.length)}, async () => {
      while (index < items.length) {const i = index++;result[i] = await fn(items[i], i);}
    }));
    return result;
  }
  async function snapshot(p, ticker, role, fetcher, signal) {
    if (!['current', 'prior', 'retained_previous_current'].includes(role)) throw Error('Choose a snapshot basis');
    const ref = p.funds[ticker]?.[role]?.snapshot;
    const doc = await retained(ref, 'snapshots', fetcher, signal);
    if (doc.contract !== 'etf-holdings-snapshot.v1' || doc.ticker !== ticker || !Array.isArray(doc.parts) || !Array.isArray(doc.index_parts) ||
        !Number.isInteger(doc.indexed_rows) || doc.indexed_rows < 0 || doc.indexed_rows > 100000) throw Error('Selected holdings snapshot differs');
    const pages = await parallel(doc.index_parts, (r) => retained(r, 'snapshots', fetcher, signal));
    const rows = [];
    for (const part of pages) {
      if (part.contract !== 'etf-holdings-row-index.v1' || part.ticker !== ticker || part.row_offset !== rows.length || !Array.isArray(part.rows) || part.rows.length > 500) throw Error('Holdings index chain differs');
      rows.push(...part.rows);
    }
    if (rows.length !== doc.indexed_rows || new Set(rows.map(r => r.row_id)).size !== rows.length ||
        rows.some(r => !Number.isInteger(r.part) || r.part < 0 || r.part >= doc.parts.length)) throw Error('Holdings index coverage differs');
    return {...doc, row_index: rows, verified_snapshot_sha256: ref.sha256};
  }
  async function rowPart(doc, number, fetcher, signal) {
    const part = await retained(doc.parts[number], 'rows', fetcher, signal);
    if (part.contract !== 'etf-holdings-rows.v1' || part.ticker !== doc.ticker || part.processed_date !== doc.processed_date ||
        !Array.isArray(part.rows) || part.rows.length > 250) throw Error('Selected holdings row page differs');
    const expected = doc.row_index.filter(r => r.part === number);
    if (JSON.stringify(part.rows.map(r => r.row_id)) !== JSON.stringify(expected.map(r => r.row_id))) throw Error('Holdings row page coverage differs');
    return part.rows;
  }
  async function directory(p, fetcher, signal) {
    const doc = await retained(p.security_directory, 'directories', fetcher, signal);
    if (doc.contract !== 'etf-holdings-security-directory.v1' || !Array.isArray(doc.parts) || !Number.isInteger(doc.security_count) || doc.security_count > 300000) throw Error('Security directory differs');
    const pages = await parallel(doc.parts, r => retained(r, 'directories', fetcher, signal));
    const securities = [];
    for (const page of pages) {
      if (page.contract !== 'etf-holdings-directory-rows.v1' || page.row_offset !== securities.length || !Array.isArray(page.securities) || page.securities.length > 500) throw Error('Security directory chain differs');
      securities.push(...page.securities);
    }
    if (securities.length !== doc.security_count || new Set(securities.map(r => r.identity_key)).size !== securities.length) throw Error('Security directory coverage differs');
    return {...doc, securities};
  }
  async function memberships(dir, identity, fetcher, signal) {
    const index = dir.securities.find(r => r.identity_key === identity);
    if (!index || index.bucket !== identity.slice(0, 2)) throw Error('Choose a verified provider identity');
    const doc = await retained(dir.buckets[index.bucket], 'memberships', fetcher, signal);
    const row = doc.securities?.find(r => r.identity_key === identity);
    if (doc.contract !== 'etf-holdings-memberships.v1' || doc.bucket !== index.bucket || !row ||
        !Array.isArray(row.memberships) || row.portfolio_weight !== null || row.inferred_trade_usd !== null) throw Error('Membership evidence differs');
    return row;
  }
  async function comparison(p, ticker, identity, fetcher, signal) {
    const summary = await retained(p.funds[ticker]?.comparison, 'comparisons', fetcher, signal);
    if (summary.contract !== 'etf-holdings-position-comparison.v1' || summary.ticker !== ticker || !Array.isArray(summary.parts)) throw Error('Position comparison identity differs');
    if (!identity) return {summary, row: null};
    const parts = await parallel(summary.parts, r => retained(r, 'comparisons', fetcher, signal));
    const rows = [];
    for (const part of parts) {
      if (part.contract !== 'etf-holdings-position-comparison-rows.v1' || part.ticker !== ticker ||
          part.row_offset !== rows.length || !Array.isArray(part.rows) || part.rows.length > 250) throw Error('Comparison row chain differs');
      rows.push(...part.rows);
    }
    if (rows.length !== summary.compared_identities) throw Error('Comparison coverage differs');
    return {summary, row: rows.find(r => r.identity_key === identity) || null};
  }
  function table(head, rows, label, trustedFirst=false) {
    return '<div class="xr-scroll" role="region" aria-label="' + esc(label) + '" tabindex="0"><table><thead><tr>' +
      head.map(x => '<th scope="col">' + esc(x) + '</th>').join('') + '</tr></thead><tbody>' + rows.map(row => '<tr>' +
      row.map((v, i) => '<' + (i ? 'td' : 'th scope="row"') + '>' + (i === 0 && trustedFirst ? v : esc(v)) + '</' + (i ? 'td' : 'th') + '>').join('') + '</tr>').join('') + '</tbody></table></div>';
  }
  function sourceState(p, at=Date.now()) {
    return Date.parse(p.generated_at) <= at && at < Date.parse(p.source_valid_until) ? 'Dated research; units remain unqualified' : 'Retained research; source check overdue';
  }
  function searchRows(doc, query) {
    const q = query.trim().toLowerCase();
    return doc.row_index.filter(r => [r.constituent_ticker, r.constituent_name, r.isin, r.figi, r.us_code, r.sedol, r.asset_class, r.security_type].join(' ').toLowerCase().includes(q));
  }
  function searchSecurities(doc, query) {
    const q = query.trim().toLowerCase();
    return doc.securities.filter(r => [...r.names, ...r.tickers, ...Object.values(r.identifiers || {})].join(' ').toLowerCase().includes(q));
  }
  function snapshotView(doc, role, at=Date.now()) {
    const q = doc.quality, age = q.effective_age_days || {};
    return '<h3>' + esc(doc.ticker) + ' · ' + esc(role === 'current' ? 'current collection' : role === 'prior' ? 'earlier query cutoff' : 'previous retained snapshot') + '</h3>' +
      '<p>Holdings effective ' + esc(dates(doc.effective_dates)) + '. Processed ' + esc(doc.processed_date) + '. Original acquisition ' + esc(doc.source_acquired_at) + '.</p>' +
      '<p>' + esc(q.status) + ' · ' + doc.indexed_rows + ' reconstructed rows. Missing ticker: ' + (q.missing_ticker_rows ?? 'unknown') +
      '; missing identity: ' + (q.missing_identity_rows ?? 'unknown') + '; duplicate identity rows: ' + (q.duplicate_identity_rows ?? 'unknown') + '.</p>' +
      '<p>Observation ages: ' + esc(Object.entries(age).map(([d,v]) => d + ': ' + v + ' days at compilation').join(' · ') || 'Unavailable') +
      '. Current fund ownership is not confirmed. ' + (at >= Date.parse(doc.source_valid_until) ? 'The source check is overdue.' : 'Source check due ' + esc(doc.source_valid_until) + '.') + '</p>' +
      '<p>Raw reported weight sum: ' + fmt(doc.weight_audit?.raw_observed_sum_decimal) + '. This is not a certified NAV fraction and is not normalized to 100%. Trading currency does not certify the currency of reported market value.</p>';
  }
  function rowTable(rows) {
    return table(['Holding', 'Name', 'Effective date', 'Source weight · raw', 'Reported position units', 'Reported market value · currency unverified', 'Trading currency'],
      rows.map(r => ['<button type="button" data-hd-row="' + esc(r.row_id) + '">' + esc(r.constituent_ticker || 'No ticker') + '</button>', r.constituent_name,
        r.effective_date, fmt(r.weight_raw_decimal), fmt(r.shares_held_raw_decimal), fmt(r.market_value_raw_decimal), r.currency_traded]), 'Complete indexed holding observations', true);
  }
  function rowDetail(row) {
    return '<h3>' + esc(row.constituent_name || row.constituent_ticker || 'Unnamed source row') + '</h3>' +
      table(['Provider field', 'Retained value'], Object.entries(row).filter(([k]) => !['source','field_errors'].includes(k)).map(([k,v]) => [k, v]), 'Exact holding row values') +
      '<p>Source page ' + row.source.page + ', original row ' + row.source.row_index + ', SHA-256 ' + esc(row.source.sha256) + '.</p>' +
      '<p>Rejected fields: ' + esc(row.field_errors.join(', ') || 'None') + '. Position units are unadjusted; a change does not establish a trade.</p>';
  }
  function memberView(row) {
    return '<h3>' + esc(row.names.join(' / ') || row.tickers.join(' / ')) + '</h3><p>' +
      row.observed_funds.length + ' distinct configured funds report this exact provider identity, on dates ' + esc(row.effective_dates.join(', ')) +
      '. These dated memberships are not independent votes or portfolio allocation weights.</p>' +
      table(['Fund', 'Effective / processed', 'Source weight · raw', 'Reported position units', 'Reported value · currency unverified', 'Snapshot coverage', 'Source check'],
        row.memberships.map(r => [r.fund, r.effective_date + ' / ' + r.processed_date, fmt(r.weight_raw_decimal), fmt(r.shares_held_raw_decimal),
          fmt(r.market_value_raw_decimal), r.snapshot_complete ? 'Complete returned snapshot' : 'Partial source',
          Date.now() >= Date.parse(r.source_valid_until) ? 'Overdue' : 'Due ' + r.source_valid_until]), 'Dated exact-identity fund membership');
  }
  function scenario(p, ticker, doc, row, exposure, fraction, shock, cost, at=Date.now()) {
    if (!typed(p) || Date.parse(p.generated_at) > at || !doc || doc.ticker !== ticker ||
        doc.verified_snapshot_sha256 !== p.funds[ticker]?.current?.snapshot?.sha256 ||
        doc.quality?.status !== 'complete_returned_snapshot' || !(at < Date.parse(doc.source_valid_until)) ||
        !row || !doc.row_index.some(r => r.row_id === row.row_id)) throw Error('Select a holding from a currently verified source snapshot');
    if (![exposure, fraction, shock, cost].every(v => typeof v === 'number' && Number.isFinite(v)) ||
        exposure === 0 || Math.abs(exposure) > 1e12 || Math.abs(fraction) > 1000 || shock < -100 || shock > 1000 || cost < 0 || cost > 1e10) throw Error('Enter bounded signed exposure, exposure fraction, shock and nonnegative costs');
    const assumedExposure = exposure * fraction / 100, gross = assumedExposure * shock / 100;
    return {fund: ticker, row_id: row.row_id, assumed_holding_exposure_usd: assumedExposure,
      gross_change_usd: gross, net_change_usd: gross - cost, costs_usd: cost, source_weight_used: false,
      scope: 'Your assumed exposure and shock; not measured look-through exposure, a forecast, or a suggested allocation.'};
  }
  function render(p) {
    return '<h2>Trace holdings before using them</h2><p data-hd-clock class="xr-state">' + esc(sourceState(p)) + ' · WAIT means abstention.</p>' +
      '<p>' + p.quality.complete_returned_snapshots + ' of ' + p.quality.configured_funds + ' configured funds have complete returned current snapshots; ' +
      p.quality.reconstructed_current_rows + ' rows reconstructed. Compiled ' + esc(p.generated_at) + '.</p>' +
      '<p>No ticker is required to retain a row. Bonds, cash, swaps and other derivative records remain visible. The dates below describe the reported holdings, not today’s ownership.</p>' +
      '<div class="xr-controls"><label>Inspect fund<select data-hd-fund aria-label="Inspect fund">' + Object.keys(p.funds).sort().map(t => '<option' + (t === 'SPY' ? ' selected' : '') + '>' + t + '</option>').join('') +
      '</select></label><label>Snapshot basis<select data-hd-basis aria-label="Snapshot basis"><option value="current">Current collection</option><option value="prior">Earlier query cutoff</option><option value="retained_previous_current">Previous retained snapshot, if available</option></select></label></div>' +
      '<div data-hd-snapshot role="status">Verifying the selected holdings index…</div><label>Find a holding by name, ticker or identifier<input data-hd-filter type="search" aria-label="Find a holding by name, ticker or identifier" autocomplete="off"></label>' +
      '<div data-hd-table></div><div class="hd-pager"><button type="button" data-hd-previous>Previous rows</button><button type="button" data-hd-next>Next rows</button></div>' +
      '<div data-hd-detail role="status">Select a holding to inspect its exact source values.</div><button type="button" data-hd-compare>Inspect current versus earlier positions</button><div data-hd-comparison role="status"></div>' +
      '<h2>Find the same reported security across funds</h2><p>Search the complete configured sample. Matching uses provider identifiers and listing attributes; ticker similarity alone does not merge instruments.</p>' +
      '<label>Security name, ticker or identifier<input data-hd-security-query type="search" aria-label="Security name, ticker or identifier" autocomplete="off"></label><button type="button" data-hd-search>Search all holdings</button>' +
      '<div data-hd-search-results role="status"></div><div class="hd-pager"><button type="button" data-hd-search-previous>Previous matches</button><button type="button" data-hd-search-next>Next matches</button></div><div data-hd-memberships role="status"></div>' +
      '<h2>Evidence and definitions</h2><p><a href="/' + esc(p.replay.manifest_key) + '">Retained calculation run</a> · <a href="/data/etf-holdings-research-verification.json">Deployment acceptance</a></p>' +
      '<details><summary>Read the measurement limits and source definitions</summary>' + Object.entries(p.methodology).map(([k,v]) => '<p><b>' + esc(k) + '</b>: ' + esc(v) + '</p>').join('') +
      '</details><button type="button" data-hd-refresh>Refresh and verify</button>';
  }

  function bindScenario(host, getState) {
    const form = host.querySelector('[data-hd-scenario]'), output = host.querySelector('[data-hd-scenario-output]');
    const invalidate = () => {output.textContent = 'Selection, source or assumptions changed; recalculate your hypothetical scenario.';};
    form.oninput = invalidate;
    form.onsubmit = e => {
      e.preventDefault();
      try {
        const state = getState();
        if (state.basis !== 'current') throw Error('Select the current collection before calculating a hypothetical scenario');
        const values = ['exposure', 'fraction', 'shock', 'cost'].map(name => {
          const value = form.elements[name].value;if (value.trim() === '') throw Error('Complete every scenario assumption');return Number(value);
        });
        const result = scenario(state.packet, state.ticker, state.snapshot, state.row, ...values);
        output.innerHTML = '<p>Assumed exposure to this holding: <b>' + cash(result.assumed_holding_exposure_usd) + '</b>. ' +
          'Gross change: <b>' + cash(result.gross_change_usd) + '</b>; after your costs: <b>' + cash(result.net_change_usd) + '</b>.</p><p>' + esc(result.scope) +
          ' The source’s raw weight was not used. This linear scenario does not model derivative repricing, leverage paths, liquidity, tax or financing.</p>';
      } catch (error) {output.textContent = error.message;}
    };
    return invalidate;
  }

  async function boot(host) {
    const panel = host.querySelector('[data-etf-holdings]'), mode = host.dataset.holdingsMode;
    if (!kinds[mode]) return;
    let packet = null, snap = null, selected = null, dir = null, controller = null, ticker = 'SPY', basis = 'current';
    let epoch = 0, rowEpoch = 0, securityEpoch = 0, publicationEpoch = 0, page = 0, searchPage = 0, matches = [], displayed = [], rowsCache = new Map();
    const fetcher = root.fetch.bind(root), signal = () => controller?.signal;
    const q = selector => panel.querySelector(selector);
    const invalidate = bindScenario(host, () => ({packet, ticker, basis, snapshot: snap, row: selected}));
    const fail = (selector, error) => {const target = q(selector);if (target) target.textContent = error.message;};
    async function showRows() {
      const token = ++rowEpoch, generation = epoch, current = snap, cache = rowsCache;
      selected = null;invalidate();q('[data-hd-detail]').textContent = 'Select a holding to inspect its exact source values.';
      q('[data-hd-comparison]').textContent = '';
      if (!current) {q('[data-hd-table]').textContent = 'No verified snapshot selected.';return;}
      const found = searchRows(current, q('[data-hd-filter]').value), count = Math.max(1, Math.ceil(found.length / 30));
      page = Math.max(0, Math.min(page, count - 1));const visible = found.slice(page * 30, page * 30 + 30);
      q('[data-hd-previous]').disabled = page === 0;q('[data-hd-next]').disabled = page + 1 >= count;
      q('[data-hd-table]').textContent = 'Verifying selected source rows…';
      try {
        const numbers = [...new Set(visible.map(r => r.part))];
        const pages = await parallel(numbers, async number => {
          if (!cache.has(number)) cache.set(number, rowPart(current, number, fetcher, signal()));
          try {return await cache.get(number);} catch (error) {cache.delete(number);throw error;}
        });
        if (token !== rowEpoch || generation !== epoch || current !== snap) return;
        const byId = new Map(pages.flat().map(r => [r.row_id, r]));displayed = visible.map(r => byId.get(r.row_id));
        if (displayed.some(r => !r)) throw Error('Selected holding is missing from its retained row page');
        q('[data-hd-table]').innerHTML = '<p>' + found.length + ' of ' + current.indexed_rows + ' rows match. Page ' + (page + 1) + ' of ' + count + '.</p>' + rowTable(displayed);
        q('[data-hd-table]').querySelectorAll('[data-hd-row]').forEach(button => {button.onclick = () => {
          selected = displayed.find(r => r.row_id === button.dataset.hdRow);invalidate();
          q('[data-hd-comparison]').textContent = '';
          q('[data-hd-detail]').innerHTML = rowDetail(selected);
        };});
      } catch (error) {if (token === rowEpoch && generation === epoch) fail('[data-hd-table]', error);}
    }
    async function inspect() {
      const generation = ++epoch;rowEpoch++;snap = null;selected = null;page = 0;rowsCache = new Map();invalidate();
      ticker = q('[data-hd-fund]').value;basis = q('[data-hd-basis]').value;
      q('[data-hd-filter]').value = '';q('[data-hd-snapshot]').textContent = 'Verifying the complete holding index…';
      q('[data-hd-table]').textContent = '';q('[data-hd-detail]').textContent = '';q('[data-hd-comparison]').textContent = '';
      q('[data-hd-compare]').disabled = true;
      try {
        if (!packet.funds[ticker][basis]) throw Error('This fund has no separately retained snapshot for the chosen basis');
        const found = await snapshot(packet, ticker, basis, fetcher, signal());if (generation !== epoch) return;
        snap = found;q('[data-hd-snapshot]').innerHTML = snapshotView(snap, basis);q('[data-hd-compare]').disabled = false;
        await showRows();
      } catch (error) {if (generation === epoch) fail('[data-hd-snapshot]', error);}
    }
    async function comparePositions() {
      const generation = epoch, fund = ticker, requestedRow = selected;q('[data-hd-comparison]').textContent = 'Verifying reported position comparisons…';
      try {
        const compared = await comparison(packet, fund, requestedRow?.identity_key, fetcher, signal()), summary = compared.summary;
        let html = '<p>Effective dates: ' + esc(summary.prior_effective_dates.join(', ')) + ' → ' + esc(summary.current_effective_dates.join(', ')) +
          '. ' + esc(summary.scope) + '</p>' + table(['Identity status', 'Count'], Object.entries(summary.identity_status_counts), 'Reported position comparison coverage');
        if (requestedRow?.identity_key) {
          const row = compared.row;
          html += row ? table(['Selected identity', 'Status', 'Unadjusted position-unit change', 'Raw weight change'],
            [[requestedRow.constituent_ticker || requestedRow.constituent_name, row.status, fmt(row.shares_held_change_raw_decimal), fmt(row.weight_change_raw_decimal)]], 'Selected unadjusted position comparison') : '<p>No unambiguous selected identity comparison.</p>';
        } else html += '<p>Select an identified holding to see its individual position comparison.</p>';
        if (generation === epoch && requestedRow === selected) q('[data-hd-comparison]').innerHTML = html;
      } catch (error) {if (generation === epoch && requestedRow === selected) fail('[data-hd-comparison]', error);}
    }
    function showMatches() {
      const count = Math.max(1, Math.ceil(matches.length / 25));searchPage = Math.max(0, Math.min(searchPage, count - 1));
      q('[data-hd-search-previous]').disabled = searchPage === 0;q('[data-hd-search-next]').disabled = searchPage + 1 >= count;
      const visible = matches.slice(searchPage * 25, searchPage * 25 + 25);
      q('[data-hd-search-results]').innerHTML = '<p>' + matches.length + ' of ' + dir.security_count + ' exact provider identities match. Page ' + (searchPage + 1) + ' of ' + count + '.</p>' +
        table(['Security', 'Reported names', 'Observed funds', 'Effective dates'], visible.map(r => [
          '<button type="button" data-hd-identity="' + esc(r.identity_key) + '">' + esc(r.tickers.join(' / ') || 'No ticker') + '</button>',
          r.names.join(' / '), r.observed_fund_count, r.effective_dates.join(', ')]), 'Searchable reported security identities', true);
      q('[data-hd-search-results]').querySelectorAll('[data-hd-identity]').forEach(button => {button.onclick = async () => {
        const token = ++securityEpoch;q('[data-hd-memberships]').textContent = 'Verifying dated fund memberships…';
        try {const found = await memberships(dir, button.dataset.hdIdentity, fetcher, signal());if (token === securityEpoch) q('[data-hd-memberships]').innerHTML = memberView(found);}
        catch (error) {if (token === securityEpoch) fail('[data-hd-memberships]', error);}
      };});
    }
    async function search() {
      const token = ++securityEpoch, expected = packet;q('[data-hd-search-results]').textContent = 'Verifying the complete security directory…';
      q('[data-hd-memberships]').textContent = '';
      try {
        const found = dir || await directory(packet, fetcher, signal());if (token !== securityEpoch || packet !== expected) return;
        dir = found;matches = searchSecurities(dir, q('[data-hd-security-query]').value);searchPage = 0;showMatches();
      } catch (error) {if (token === securityEpoch) fail('[data-hd-search-results]', error);}
    }
    async function refresh() {
      epoch++;rowEpoch++;securityEpoch++;controller?.abort();controller = new AbortController();
      const publication = ++publicationEpoch, activeSignal = controller.signal;
      packet = null;snap = null;selected = null;dir = null;invalidate();
      panel.textContent = 'Checking retained ETF holdings evidence…';
      try {
        const loaded = await load(kinds[mode].current, fetcher, activeSignal);
        const candidate = await verifyPacket(loaded.doc, fetcher, activeSignal);
        if (publication !== publicationEpoch) return;
        packet = candidate;panel.innerHTML = render(packet);
        q('[data-hd-fund]').onchange = inspect;q('[data-hd-basis]').onchange = inspect;
        q('[data-hd-filter]').oninput = () => {page = 0;void showRows();};
        q('[data-hd-previous]').onclick = () => {page--;void showRows();};q('[data-hd-next]').onclick = () => {page++;void showRows();};
        q('[data-hd-compare]').onclick = comparePositions;q('[data-hd-search]').onclick = search;
        q('[data-hd-search-previous]').onclick = () => {searchPage--;showMatches();};
        q('[data-hd-search-next]').onclick = () => {searchPage++;showMatches();};
        q('[data-hd-search-previous]').disabled = true;q('[data-hd-search-next]').disabled = true;
        q('[data-hd-security-query]').oninput = () => {securityEpoch++;q('[data-hd-search-results]').textContent = 'Run search for the edited query.';q('[data-hd-memberships]').textContent = '';q('[data-hd-search-previous]').disabled = true;q('[data-hd-search-next]').disabled = true;};
        q('[data-hd-refresh]').onclick = refresh;await inspect();
      } catch (error) {if (publication === publicationEpoch && error.name !== 'AbortError') {packet = null;panel.textContent = 'Holdings evidence unavailable: ' + error.message;}}
    }
    root.setInterval(() => {
      if (packet && q('[data-hd-clock]')) q('[data-hd-clock]').textContent = sourceState(packet) + ' · WAIT means abstention.';
      if (snap && Date.now() >= Date.parse(snap.source_valid_until)) invalidate();
    }, 60000);
    await refresh();
  }

  const api = {kinds, typed, safe, load, retained, verifyPacket, snapshot, rowPart, directory, memberships, comparison,
    searchRows, searchSecurities, snapshotView, rowTable, rowDetail, memberView, scenario, render, sourceState, table, esc, cash, bindScenario, boot};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.JHHoldings = api;
  if (root.document) root.document.addEventListener('DOMContentLoaded', () => {
    root.document.querySelectorAll('main[data-holdings-mode]').forEach(host => {void boot(host);});
  });
})(typeof globalThis !== 'undefined' ? globalThis : this);
