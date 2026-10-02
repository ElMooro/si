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
  const MAX_EVIDENCE_BYTES = 16 * 1024 * 1024;
  function evidenceByteLimit(value) {
    if (!Number.isInteger(value) || value <= 0 || value > MAX_EVIDENCE_BYTES) throw Error('Holdings evidence byte bound');
    return Math.min(value, MAX_EVIDENCE_BYTES);
  }
  async function load(key, fetcher, signal, maxBytes=MAX_EVIDENCE_BYTES) {
    maxBytes = evidenceByteLimit(maxBytes);
    if (!safe(key) && !Object.values(kinds).some(v => v.current === key)) throw Error('Unapproved holdings evidence path');
    const r = await fetcher('/' + key, {cache: safe(key) ? 'default' : 'no-store', signal});
    if (!r.ok) throw Error('Holdings evidence request failed');
    if (Number(r.headers?.get('content-length')) > maxBytes) throw Error('Holdings evidence byte bound');
    let raw;
    if (r.body?.getReader) {
      const reader = r.body.getReader(), parts = [];let length = 0;
      try {
        for (;;) {const item = await reader.read();if (item.done) break;length += item.value.byteLength;
          if (length > maxBytes) throw Error('Holdings evidence byte bound');parts.push(item.value);}
      } catch (error) {await reader.cancel();throw error;}
      const combined = new Uint8Array(length);let offset = 0;
      for (const part of parts) {combined.set(part, offset);offset += part.byteLength;}
      raw = combined.buffer;
    } else raw = await r.arrayBuffer();
    if (raw.byteLength > maxBytes) throw Error('Holdings evidence byte bound');
    return {raw, doc: JSON.parse(new TextDecoder().decode(raw))};
  }
  async function retained(ref, group, fetcher, signal) {
    if (!ref || !/^[a-f0-9]{64}$/.test(ref.sha256) || ref.key !== prefix + group + '/' + ref.sha256 + '.json' ||
        !Number.isInteger(ref.bytes) || ref.bytes <= 0 || ref.bytes > 16 * 1024 * 1024) throw Error('Holdings artifact identity differs');
    const out = await load(ref.key, fetcher, signal, ref.bytes);
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
    evidenceByteLimit(ref.bytes);
    const out = await load(ref.key, fetcher, signal, ref.bytes);
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
      '; missing identity: ' + (q.missing_identity_rows ?? 'unknown') + '; duplicate identity rows: ' + (q.duplicate_identity_rows ?? 'unknown') +
      '; rows with rejected fields: ' + esc(Number.isSafeInteger(q.rows_with_field_errors) && q.rows_with_field_errors >= 0 ? q.rows_with_field_errors : 'Unavailable') + '.</p>' +
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
    return ownershipPanel() + heatPanel() + '<h2>Trace holdings before using them</h2><p data-hd-clock class="xr-state">' + esc(sourceState(p)) + ' · WAIT means abstention.</p>' +
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

  // Opt-in bounded cohort, never a ranking of a partially downloaded universe.
  const heatLimits = {funds: 8, requests: 40, bytes: 8 * 1024 * 1024};
  const identityFields = ['figi','isin','us_code','sedol','exchange','currency_traded','asset_class','security_type'];
  function heatSelection(p, text) {
    const funds = [...new Set(text.toUpperCase().split(/[\s,]+/).filter(Boolean))];
    if (!funds.length || funds.length > heatLimits.funds || funds.some(t => !p.funds[t])) throw Error('Choose 1–8 configured fund tickers, separated by commas.');
    return funds.sort();
  }
  function heatReason(p, s, date, at) {
    const q = s.quality || {}, ds = Object.keys(s.effective_dates || {});
    if (!Number.isFinite(Date.parse(p.generated_at)) || Date.parse(p.generated_at) > at) return 'Future or invalid publication';
    if (q.status !== 'complete_returned_snapshot' || q.pagination_complete !== true) return 'Incomplete returned snapshot';
    if (!Number.isFinite(Date.parse(s.source_acquired_at)) || Date.parse(s.source_acquired_at) > at ||
        !Number.isFinite(Date.parse(s.source_valid_until)) || !(at < Date.parse(s.source_valid_until))) return 'Source check unavailable, future or expired';
    if (ds.length !== 1 || ds[0] !== date || date > new Date(at).toISOString().slice(0,10)) return 'Effective date outside chosen cohort';
    if (q.missing_identity_rows !== 0 || q.duplicate_identity_rows !== 0 || q.rows_with_field_errors !== 0) return 'Identity or field coverage unresolved';
    return null;
  }
  function heatMeasure(p, selected, snapshots, date, at=Date.now()) {
    const coverage = selected.map(t => {
      const s = snapshots[t];if (!s) throw Error('Whole selected sample must finish before ranking');
      return {fund:t, reason:heatReason(p,s,date,at), dates:dates(s.effective_dates), tags:JSON.stringify(p.funds[t].configured_tag_unverified||{}), acquired:s.source_acquired_at, expiry:s.source_valid_until};
    });
    const eligible = new Set(coverage.filter(r => !r.reason).map(r => r.fund)), identities = new Map();
    let unidentified = 0;
    for (const t of selected) {
      const s = snapshots[t], seenRows = new Set(), seenIdentities = new Set();
      if (s.rows.length !== s.indexed_rows) throw Error('Whole snapshot row count differs');
      for (const row of s.rows) {
        if (!row.row_id || seenRows.has(row.row_id)) throw Error('Duplicate source row');seenRows.add(row.row_id);
        if (!row.identity_key) {unidentified++;if (eligible.has(t)) throw Error('Qualified snapshot has unidentified rows');continue;}
        const tuple = JSON.stringify(identityFields.map(k => row[k] ?? null));
        let rec = identities.get(row.identity_key);
        if (rec && rec.tuple !== tuple) throw Error('Provider identity collision');
        if (!rec) {rec={identity:row.identity_key,tuple,name:row.constituent_name,ticker:row.constituent_ticker,asset:row.asset_class,raw:new Set(),qualified:new Set()};identities.set(row.identity_key,rec);}
        if (eligible.has(t) && (seenIdentities.has(row.identity_key) || row.effective_date !== date || row.processed_date !== s.processed_date)) throw Error('Qualified identity or date coverage differs');
        seenIdentities.add(row.identity_key);rec.raw.add(t);if (eligible.has(t)) rec.qualified.add(t);
      }
    }
    const rows=[...identities.values()].map(r=>({...r,raw:[...r.raw],qualified:[...r.qualified]}));
    rows.sort((a,b)=>eligible.size ? b.qualified.length-a.qualified.length || a.identity.localeCompare(b.identity) : a.identity.localeCompare(b.identity));
    return {coverage,selected:selected.length,configured:Object.keys(p.funds).length,eligible:eligible.size,date,unidentified,rows};
  }
  function heatSession() {
    const cache=new Map();let cacheBytes=0, controller=null, generation=0;
    return {
      cancel(){generation++;controller?.abort();},
      async run(p, selected, date, fetcher, progress=()=>{}, at=null) {
        this.cancel();const token=generation;controller=new AbortController();const signal=controller.signal;
        let requests=0, bytes=0;const snapshots={};
        const read=async(ref,group)=>{
          if (signal.aborted) throw new DOMException('Cancelled','AbortError');
          if (!ref || !Number.isInteger(ref.bytes) || ref.bytes<=0 || ref.bytes>heatLimits.bytes) throw Error('Snapshot exceeds the heatmap byte budget; choose a smaller sample');
          const key=JSON.stringify(ref);
          if (cache.has(key)) return cache.get(key);
          if (requests+1>heatLimits.requests || bytes+ref.bytes>heatLimits.bytes) throw Error('Heatmap request/byte budget reached; no partial ranking. Choose a smaller sample.');
          requests++;bytes+=ref.bytes;const doc=await retained(ref,group,fetcher,signal);
          if (signal.aborted) throw new DOMException('Cancelled','AbortError');
          while(cacheBytes+ref.bytes>heatLimits.bytes && cache.size){const [k,v]=cache.entries().next().value;cacheBytes-=v.bytes;cache.delete(k);}
          cache.set(key,{doc,bytes:ref.bytes});cacheBytes+=ref.bytes;return cache.get(key);
        };
        for (const t of selected) {
          const ref=p.funds[t].current.snapshot, s=(await read(ref,'snapshots')).doc;
          if(s.contract!=='etf-holdings-snapshot.v1'||s.ticker!==t||!Array.isArray(s.parts)||!Number.isInteger(s.indexed_rows)||s.indexed_rows<0||s.indexed_rows>100000) throw Error('Heatmap snapshot contract differs');
          const rows=[];
          for(const part of s.parts){const d=(await read(part,'rows')).doc;
            if(d.contract!=='etf-holdings-rows.v1'||d.ticker!==t||d.processed_date!==s.processed_date||d.row_offset!==rows.length||!Array.isArray(d.rows)||d.rows.length>250) throw Error('Heatmap row chain differs');
            rows.push(...d.rows);
          }
          snapshots[t]={...s,rows};progress({completed:Object.keys(snapshots).length,total:selected.length,requests,bytes});
        }
        if(token!==generation||signal.aborted) throw new DOMException('Cancelled','AbortError');
        return {...heatMeasure(p,selected,snapshots,date,at ?? Date.now()),requests,bytes};
      }
    };
  }
  function heatPanel() {
    return '<section data-hd-heat aria-label="Selected fund membership heatmap"><h2>Reported membership heatmap</h2>'+
      '<p>Choose up to eight funds and one effective date. Rankings cover only the eligible funds in your selected sample, never the whole market. Only your selected funds are loaded.</p>'+
      '<label>Selected configured funds<input data-hd-heat-funds aria-label="Heatmap fund tickers" placeholder="SPY, QQQ, IWM" autocomplete="off"></label>'+
      '<label>Common effective date<input data-hd-heat-date aria-label="Heatmap effective date" type="date"></label>'+
      '<button type="button" data-hd-heat-load>Load / retry selected sample</button> <button type="button" data-hd-heat-cancel>Cancel</button>'+
      '<div data-hd-heat-status role="status">Not loaded. At most 40 additional verified objects / 8 MiB per attempt; cached objects are reused in this page.</div><div data-hd-heat-result></div></section>';
  }
  function heatView(m) {
    const qualified=m.rows.filter(r=>r.qualified.length), shown=(m.eligible?qualified:m.rows).slice(0,12);
    return '<p><b>'+m.eligible+' eligible / '+m.selected+' selected / '+m.configured+' configured funds.</b> Effective-date cohort '+esc(m.date)+'. '+m.unidentified+' unidentified rows excluded.</p>'+
      table(['Fund','Qualification / exclusion','Effective dates','Configured tags · unverified','Source acquired','Source expires'],m.coverage.map(r=>[r.fund,r.reason||'Complete, fresh, identity-resolved dated snapshot',r.dates,r.tags,r.acquired,r.expiry]),'Heatmap coverage')+
      '<p>'+ (m.eligible?'Highest membership counts within the eligible selected sample.':'Qualified ranking unavailable. Unranked raw observations below may be partial or stale.')+
      ' Showing '+shown.length+' of '+(m.eligible?qualified.length:m.rows.length)+' identities. No aggregate dollars, allocation weights, trading events or favorability score. Counts describe reported rows, including zero or short positions; they do not certify long ownership.</p><div class="hd-heat-grid">'+
      shown.map(r=>'<article class="hd-heat-tile" style="border-left-width:'+ (m.eligible?2+Math.round(10*r.qualified.length/m.eligible):2)+'px;background:rgba(43,133,180,'+(m.eligible?0.06+0.24*r.qualified.length/m.eligible:0.06)+')"><b>'+esc(r.ticker||r.name||'Unlabelled identity')+'</b><p>'+esc(r.asset||'Asset type unknown')+' · source classification</p><p>Qualified '+(m.eligible?r.qualified.length+' / '+m.eligible:'Unavailable')+' · raw observed '+r.raw.length+' / '+m.selected+'</p><p>'+esc(r.qualified.join(', ')||'No qualified membership')+'</p><small>'+esc(r.identity)+'</small></article>').join('')+'</div>'+
      '<p>Use the existing fund and security inspectors below for full rows and exact current/prior dates. Membership comparisons are reported observations; quantity and raw weight differences remain separate. Thirty-day query cutoffs are not daily changes; same-date revisions and corporate actions are not trades. Weight and value units remain unqualified. Fund overlap, fund-of-funds and leveraged/inverse strategies are not combined into exposure.</p>';
  }

  // Summary limits are browser constants, never expanded by retained declarations.
  const ownershipLimits = Object.freeze({manifest:524288, page:262144, records:100000, pages:768, total:50331648, sessionPages:32, sessionBytes:8388608});
  const count = (v,max) => Number.isInteger(v) && v>=0 && v<=max;
  const isoDay = v => typeof v==='string' && /^\d{4}-\d{2}-\d{2}$/.test(v) && Number.isFinite(Date.parse(v)) && new Date(v).toISOString().slice(0,10)===v;
  const same = (a,b) => JSON.stringify(stable(a))===JSON.stringify(stable(b));
  function summaryRef(ref, limit) {
    if(!ref || !/^[a-f0-9]{64}$/.test(ref.sha256) || ref.key!==prefix+'directories/'+ref.sha256+'.json' || !count(ref.bytes,limit) || ref.bytes===0) throw Error('Summary reference/size differs');
  }
  async function ownershipManifest(p, fetcher, signal) {
    if(!p.ownership_summary) throw Error('Ownership summary not available in this publication. Existing inspectors remain available.');
    const ref=p.ownership_summary;
    if(ref.status!=='complete'||ref.policy!=='qualified-membership.v1') throw Error('Ownership summary unavailable: '+(ref.reason||'unsupported policy/status'));
    summaryRef(ref.manifest,ownershipLimits.manifest);
    const m=await retained(ref.manifest,'directories',fetcher,signal), configured=Object.keys(p.funds).sort();
    if(m.contract!=='etf-qualified-membership-summary.v1'||m.policy!==ref.policy||!same(Object.keys(m.funds||{}).sort(),configured)||m.configured_fund_count!==configured.length ||
       !Number.isFinite(Date.parse(m.generated_at)) || Date.parse(m.generated_at)>Date.parse(p.generated_at) ||
       flags.some(f=>m[f]!==false)||m.call!==null||m.portfolio_action!=='WAIT'||m.independent_investment_votes!==0 ||
       !['source_classifications_verified','corporate_actions_verified','weight_unit_certified','market_value_currency_certified'].every(k=>m[k]===false)||
       !Array.isArray(m.cohorts)||m.cohorts.length>ownershipLimits.pages||!count(m.record_count,ownershipLimits.records)||!count(m.page_count,ownershipLimits.pages)) throw Error('Summary manifest contract differs');
    for(const t of configured){const f=m.funds[t],original=p.funds[t];
      const current=original.current,quality=current.quality,stamp=Date.parse(m.generated_at);
      if(f.qualification_exclusion===null&&(quality.status!=='complete_returned_snapshot'||quality.pagination_complete!==true||
         !['missing_identity_rows','duplicate_identity_rows','rows_with_field_errors'].every(k=>quality[k]===0)||
         Object.keys(current.effective_dates).length!==1||!Object.keys(current.effective_dates).every(d=>isoDay(d)&&Date.parse(d)<=stamp)||
         !Number.isFinite(Date.parse(current.source_acquired_at))||Date.parse(current.source_acquired_at)>stamp||!(stamp<Date.parse(current.source_valid_until))||
         !isoDay(current.processed_date)||Date.parse(current.processed_date)>Date.parse(current.source_acquired_at)))throw Error('Summary qualification differs from source metadata');
      if(!same(f.current_snapshot,original.current.snapshot)||!same(f.prior_snapshot,original.prior.snapshot)||!same(f.comparison,original.comparison)||
         !same(f.current_effective_dates,Object.keys(original.current.effective_dates).sort())||!same(f.prior_effective_dates,Object.keys(original.prior.effective_dates).sort())||
         f.source_acquired_at!==original.current.source_acquired_at||f.source_valid_until!==original.current.source_valid_until||
         !(f.qualification_exclusion===null||typeof f.qualification_exclusion==='string')) throw Error('Summary fund provenance differs');
    }
    let records=0,pages=0,bytes=ref.manifest.bytes;const ids=new Set(),keys=new Set();
    for(const g of m.cohorts){
      if(!['current_membership','dated_membership_comparison'].includes(g.kind)||!Array.isArray(g.effective_dates)||g.effective_dates.length!==(g.kind==='current_membership'?1:2)||!g.effective_dates.every(isoDay)||
         !Array.isArray(g.eligible_funds)||!same(g.eligible_funds,[...new Set(g.eligible_funds)].sort())||g.eligible_funds.some(t=>!m.funds[t])||g.eligible_fund_count!==g.eligible_funds.length||
         g.configured_fund_count!==configured.length||!count(g.record_count,ownershipLimits.records)||!Array.isArray(g.parts)||g.parts.length!==Math.ceil(g.record_count/200)||
         ![g.source_valid_until,g.lower_bound_valid_until].every(d=>d===null||typeof d==='string'&&Number.isFinite(Date.parse(d)))) throw Error('Summary cohort/count contract differs');
      const digest=await sha(new TextEncoder().encode(JSON.stringify(stable({kind:g.kind,effective_dates:g.effective_dates}))));
      if(g.cohort_id!==digest||ids.has(digest))throw Error('Summary cohort identity differs');ids.add(digest);
      if(g.kind==='current_membership'){
        const eligible=configured.filter(t=>m.funds[t].qualification_exclusion===null&&same(m.funds[t].current_effective_dates,g.effective_dates));
        const expiry=eligible.length?Math.min(...eligible.map(t=>Date.parse(m.funds[t].source_valid_until))):null;
        if(!same(eligible,g.eligible_funds)||(expiry===null?g.source_valid_until!==null:Date.parse(g.source_valid_until)!==expiry)||
           g.ranking_scope!==(eligible.length?'exact_within_eligible_date_cohort':'unranked_reported_observations'))throw Error('Summary eligible denominator differs');
      }
      for(const r of g.parts){summaryRef(r,ownershipLimits.page);if(keys.has(r.key))throw Error('Duplicate summary page');keys.add(r.key);bytes+=r.bytes;}
      records+=g.record_count;pages+=g.parts.length;
    }
    if(records!==m.record_count||pages!==m.page_count||pages>ownershipLimits.pages||records>ownershipLimits.records||bytes>ownershipLimits.total)throw Error('Summary total coverage/byte bound differs');
    return m;
  }
  function ownershipState(m,g,view,at=Date.now()) {
    if(!['qualified','raw','lower'].includes(view))return 'Choose a supported measurement.';
    if(!g||g.kind!=='current_membership')return 'Choose an exact economic-date cohort.';
    if(Date.parse(m.generated_at)>at)return 'Publication clock is in the future.';
    if(view==='qualified'&&(!g.eligible_fund_count||g.source_valid_until===null||!(at<Date.parse(g.source_valid_until))))return 'Qualified ranking unavailable: no eligible funds or source-check deadline expired.';
    if(view==='lower'&&(g.lower_bound_valid_until===null||!(at<Date.parse(g.lower_bound_valid_until))))return 'Fresh known-presence lower bound unavailable: separate deadline missing or expired.';
    return null;
  }
  function ownershipRows(doc,g,index,seen,previous) {
    if(doc.contract!=='etf-qualified-membership-rows.v1'||doc.cohort_id!==g.cohort_id||doc.row_offset!==index*200||!Array.isArray(doc.rows)||doc.rows.length!==Math.min(200,g.record_count-index*200))throw Error('Summary page count/offset differs');
    let prev=previous;const next=new Set(seen);
    for(const r of doc.rows){
      if(!/^[a-f0-9]{64}$/.test(r.identity_key)||next.has(r.identity_key)||!Array.isArray(r.reported_tickers)||r.reported_tickers.length>1000||!r.reported_tickers.every(t=>typeof t==='string'&&t.length<=512)||
         ![r.source_asset_class,r.source_security_type].every(v=>v===null||typeof v==='string'&&v.length<=512)||
         !count(r.raw_observed_fund_count,g.configured_fund_count)||!count(r.known_presence_lower_bound,r.raw_observed_fund_count)||
         (g.eligible_fund_count?!count(r.qualified_fund_count,Math.min(g.eligible_fund_count,r.known_presence_lower_bound)):r.qualified_fund_count!==null)||
         !['observed_in_both_count','observed_only_in_current_count','observed_only_in_prior_count'].every(k=>r[k]===null))throw Error('Summary row/count contract differs');
      if(prev&&((prev.qualified_fund_count||0)<(r.qualified_fund_count||0)||((prev.qualified_fund_count||0)===(r.qualified_fund_count||0)&&prev.identity_key>=r.identity_key)))throw Error('Summary ordering differs');
      next.add(r.identity_key);prev=r;
    }
    return {seen:next,previous:prev};
  }
  function ownershipSession() {
    let token=0,controller=null,manifest=null,cohort=null,index=0,cursor=0,seen=new Set(),previous=null,requests=0,bytes=0;
    const cache=new Map();let cacheBytes=0;
    function cancel(){token++;controller?.abort();}
    async function read(ref,limit,fetcher,signal){summaryRef(ref,limit);if(signal.aborted)throw Error('Cancelled');
      const key=JSON.stringify(ref);if(cache.has(key))return cache.get(key).doc;
      if(bytes+ref.bytes>ownershipLimits.sessionBytes||requests>=ownershipLimits.sessionPages)throw Error('Summary session budget reached; reload metadata to start another bounded attempt.');
      bytes+=ref.bytes;requests++;const doc=await retained(ref,'directories',fetcher,signal);
      if(signal.aborted)throw Error('Cancelled');
      while(cacheBytes+ref.bytes>ownershipLimits.sessionBytes&&cache.size){const [k,v]=cache.entries().next().value;cacheBytes-=v.bytes;cache.delete(k);}
      cache.set(key,{doc,bytes:ref.bytes});cacheBytes+=ref.bytes;return doc;
    }
    const position=()=>({page:cursor,canPrevious:cursor>1,canNext:!!cohort&&cursor<Math.min(cohort.parts.length,ownershipLimits.sessionPages)});
    async function move(target,view,fetcher,at){cancel();const generation=token;controller=new AbortController();
      const error=ownershipState(manifest,cohort,view,at??Date.now());if(error)throw Error(error);
      if(target<1)throw Error('No previous cohort page');
      if(target>cohort.parts.length)throw Error('No more cohort pages');
      if(target>ownershipLimits.sessionPages)throw Error('32-page browsing bound reached; use the source evidence for the remaining records.');
      const doc=await read(cohort.parts[target-1],ownershipLimits.page,fetcher,controller.signal);
      if(generation!==token)throw Error('Cancelled');
      // Only extend the validated prefix forwards. Revisits use the same verified
      // immutable reference, never re-add identities or weaken cross-page checks.
      const extending=target>index;
      const validated=ownershipRows(doc,cohort,target-1,extending?seen:new Set(),extending?previous:null);
      const expired=ownershipState(manifest,cohort,view,at??Date.now());if(expired)throw Error(expired);
      if(extending){seen=validated.seen;previous=validated.previous;index=target;}
      cursor=target;
      return {rows:doc.rows,...position(),from:(cursor-1)*200+1,to:(cursor-1)*200+doc.rows.length,loaded:seen.size,total:cohort.record_count,requests,bytes};
    }
    return {cancel,position,
      async open(p,fetcher){cancel();const generation=token;controller=new AbortController();manifest=null;cohort=null;cursor=index=0;seen=new Set();previous=null;requests=bytes=0;
        const activeSignal=controller.signal;await verifyPacket(p,fetcher,activeSignal);
        // Metadata verifier performs its own bounded retained read (not page cache).
        const m=await ownershipManifest(p,fetcher,activeSignal);
        if(generation!==token)throw Error('Cancelled');manifest=m;return m;
      },
      select(id){cancel();cohort=manifest?.cohorts.find(g=>g.cohort_id===id&&g.kind==='current_membership');cursor=index=0;seen=new Set();previous=null;if(!cohort)throw Error('Choose a cohort');return cohort;},
      next(view,fetcher,at=null){return move(cursor+1,view,fetcher,at);},
      previous(view,fetcher,at=null){return move(cursor-1,view,fetcher,at);}
    };
  }
  function ownershipPanel(){return '<section data-hd-ownership aria-label="Configured ownership cohorts"><h2>Configured-universe membership cohorts</h2><p>Rank reported memberships only within eligible funds sharing one economic date. A fresh source check does not establish current ownership. No whole-market ranking, trades or capital-flow inference.</p><button type="button" data-own-open>Load / retry summary metadata</button> <button type="button" data-own-cancel>Cancel</button><div data-own-status role="status">Not loaded. Existing publications may not yet contain a summary. No membership shards are fetched by this panel.</div><div data-own-controls hidden><label>Economic-date cohort<select data-own-cohort aria-label="Economic-date cohort"><option value="">Choose a date explicitly</option></select></label><label>Measurement<select data-own-view aria-label="Ownership measurement"><option value="qualified">Qualified memberships · eligible cohort only</option><option value="raw">Raw historical observations · unranked</option><option value="lower">Fresh known-presence lower bounds · unranked</option></select></label><button type="button" data-own-previous disabled>Previous</button> <button type="button" data-own-next disabled>Next · up to 200 records</button></div><div data-own-coverage></div><div data-own-result></div></section>';}
  function ownershipCoverage(m,g,at=Date.now()){
    const excluded={};for(const f of Object.values(m.funds))if(f.qualification_exclusion)excluded[f.qualification_exclusion]=(excluded[f.qualification_exclusion]||0)+1;
    const otherDates=Object.values(m.funds).filter(f=>f.qualification_exclusion===null&&!same(f.current_effective_dates,g.effective_dates)).length;
    const day=g.effective_dates[0],age=Math.floor((at-Date.parse(day))/86400000);
    return '<p><b>'+g.eligible_fund_count+' eligible / '+m.configured_fund_count+' configured funds in this date cohort.</b> Economic date '+esc(day)+' · '+age+' days before this viewing date. '+(age<0?'Future economic date — not current ownership.':age>7?'Historical economic date — do not treat as current ownership.':'Reported dated observations; current ownership is not confirmed.')+'</p><p>Compiled '+esc(m.generated_at)+'. Eligible ranking expires '+esc(g.source_valid_until)+'. Fresh lower bounds separately expire '+esc(g.lower_bound_valid_until)+'.</p><p>Eligible funds: '+esc(g.eligible_funds.join(', ')||'None')+'. '+otherDates+' funds qualified on other dates; they are not in this denominator. Other dates are separate cohorts.</p>'+table(['Global exclusion reason','Configured funds'],Object.entries(excluded),'Summary exclusions')+'<details><summary>Per-fund source clocks and exclusions</summary>'+table(['Fund','Economic dates','Source acquired','Source expires','Qualification / exclusion'],Object.entries(m.funds).map(([t,f])=>[t,f.current_effective_dates.join(', '),f.source_acquired_at,f.source_valid_until,f.qualification_exclusion||'Eligible only within its own economic-date cohort']),'Per-fund summary provenance')+'</details><p>Source classes are unverified; unknown remains unknown. Includes reported zero/short positions, cash, bonds and derivatives. No unit-qualified dollar/quantity totals, inferred asset classes, weight conversions, fund-of-funds expansion or leverage/inverse netting. Use existing inspectors for dated membership comparisons and separate quantity/weight observations.</p>';
  }
  function ownershipView(m,g,result,view,at=Date.now()){
    const error=ownershipState(m,g,view,at);if(error)return '<p>'+esc(error)+'</p>';
    const rows=view==='qualified'?result.rows:[...result.rows].sort((a,b)=>a.identity_key.localeCompare(b.identity_key));
    const title=view==='qualified'?'Qualified reported memberships within eligible date cohort':view==='raw'?'Raw historical observations · not ranked':'Fresh known-presence lower bounds · not ranked';
    return '<p>'+title+'. Page '+result.page+'. Displayed records '+((result.page-1)*200+1)+'–'+((result.page-1)*200+result.rows.length)+'. The eligible-fund denominator is '+g.eligible_fund_count+', not the number of displayed records. '+result.loaded+' / '+result.total+' retained records verified. Only this page is displayed; unloaded records are not a separate universe or a new ranking. '+(view!=='qualified'?'Page membership follows the retained qualified-order pagination; this is not a top raw/lower-bound list. Raw and lower-bound counts can include ineligible funds; they are not the qualified denominator.':'Counts and order are supplied by the verified producer for this exact cohort.')+'</p>'+table(['Reported ticker / exact identity','Source class · unverified',view==='qualified'?'Qualified funds / eligible':view==='raw'?'Raw observed funds':'Known-presence lower bound'],rows.map(r=>[(r.reported_tickers.join(' / ')||'No ticker')+' · '+r.identity_key,r.source_asset_class??'Unknown',view==='qualified'?r.qualified_fund_count+' / '+g.eligible_fund_count:view==='raw'?r.raw_observed_fund_count:r.known_presence_lower_bound]),title);
  }
  function bindOwnership(panel,p,fetcher){
    const host=panel.querySelector('[data-hd-ownership]'),q=s=>host.querySelector(s),session=ownershipSession();let generation=0,m=null,g=null,result=null,timer=null;
    const navigation=()=>{const state=session.position();q('[data-own-previous]').disabled=!state.canPrevious;q('[data-own-next]').disabled=!state.canNext;};
    const clear=()=>{generation++;session.cancel();clearTimeout(timer);result=null;q('[data-own-result]').textContent='';navigation();};
    const paint=()=>{navigation();if(!m||!g)return;q('[data-own-coverage]').innerHTML=ownershipCoverage(m,g);if(result)q('[data-own-result]').innerHTML=ownershipView(m,g,result,q('[data-own-view]').value);
      clearTimeout(timer);const deadline=q('[data-own-view]').value==='qualified'?g.source_valid_until:q('[data-own-view]').value==='lower'?g.lower_bound_valid_until:null;
      if(deadline&&Date.parse(deadline)>Date.now())timer=setTimeout(paint,Math.min(2147483647,Date.parse(deadline)-Date.now()));};
    q('[data-own-cancel]').onclick=()=>{clear();q('[data-own-status]').textContent='Cancelled. Choose a cohort again or retry metadata.';};
    q('[data-own-open]').onclick=async()=>{clear();m=g=null;q('[data-own-controls]').hidden=true;q('[data-own-coverage]').textContent='';const token=generation;q('[data-own-status]').textContent='Verifying publication and bounded summary metadata…';
      try{const found=await session.open(p,fetcher);if(token!==generation)return;m=found;navigation();q('[data-own-controls]').hidden=false;q('[data-own-cohort]').innerHTML='<option value="">Choose a date explicitly</option>'+m.cohorts.filter(c=>c.kind==='current_membership').sort((a,b)=>b.effective_dates[0].localeCompare(a.effective_dates[0])).map(c=>'<option value="'+c.cohort_id+'">'+esc(c.effective_dates[0])+' · '+c.eligible_fund_count+' / '+m.configured_fund_count+' eligible</option>').join('');q('[data-own-status]').textContent='Metadata verified. Choose an economic date; no date is selected automatically. Each click loads at most 200 records / 256 KiB. At most 32 page requests / 8 MiB of page bytes per attempt across all cohorts. Separate proof/metadata bounds: two objects up to 16 MiB each plus a 512 KiB summary manifest.';}
      catch(e){if(token===generation)q('[data-own-status]').textContent='Summary unavailable: '+e.message;}};
    const select=()=>{clear();g=null;try{g=session.select(q('[data-own-cohort]').value);paint();q('[data-own-status]').textContent='Date selected; load a page explicitly.';}catch(e){navigation();q('[data-own-coverage]').textContent='';q('[data-own-status]').textContent=e.message;}};
    q('[data-own-cohort]').onchange=select;q('[data-own-view]').onchange=select;
    const navigate=direction=>async()=>{const token=++generation;result=null;q('[data-own-result]').textContent='';q('[data-own-status]').textContent='Verifying '+direction+' retained page…';
      try{const r=await session[direction](q('[data-own-view]').value,fetcher);if(token!==generation)return;result=r;paint();q('[data-own-status]').textContent='Verified '+r.loaded+' / '+r.total+' records; displaying '+r.from+'–'+r.to+'. '+r.requests+' page requests / '+r.bytes+' bytes this attempt.'+(r.page===ownershipLimits.sessionPages&&r.to<r.total?' Browsing limit reached (32 pages); remaining records are not loaded.':'');}
      catch(e){if(token===generation){navigation();q('[data-own-status]').textContent='Summary page unavailable: '+e.message;}}};
    q('[data-own-next]').onclick=navigate('next');q('[data-own-previous]').onclick=navigate('previous');
    return clear;
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
    const heat = heatSession();let heatEpoch=0, heatUntil=Infinity, clearOwnership=()=>{};
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
      clearOwnership();heatEpoch++;heat.cancel();epoch++;rowEpoch++;securityEpoch++;controller?.abort();controller = new AbortController();
      const publication = ++publicationEpoch, activeSignal = controller.signal;
      packet = null;snap = null;selected = null;dir = null;invalidate();
      panel.textContent = 'Checking retained ETF holdings evidence…';
      try {
        const loaded = await load(kinds[mode].current, fetcher, activeSignal);
        const candidate = await verifyPacket(loaded.doc, fetcher, activeSignal);
        if (publication !== publicationEpoch) return;
        packet = candidate;panel.innerHTML = render(packet);
        try {clearOwnership=bindOwnership(panel,packet,fetcher);} catch(e) {q('[data-own-status]').textContent='Summary panel unavailable: '+e.message;}
        const clearHeat=()=>{heatEpoch++;heat.cancel();q('[data-hd-heat-result]').textContent='';q('[data-hd-heat-status]').textContent='Selection changed or cancelled; load to recompute the whole selected sample.';};
        q('[data-hd-heat-funds]').oninput=clearHeat;q('[data-hd-heat-date]').oninput=clearHeat;q('[data-hd-heat-cancel]').onclick=clearHeat;
        q('[data-hd-heat-load]').onclick=async()=>{
          heat.cancel();const token=++heatEpoch;q('[data-hd-heat-result]').textContent='';
          try{
            const chosen=heatSelection(packet,q('[data-hd-heat-funds]').value), date=q('[data-hd-heat-date]').value;
            if(!/^\d{4}-\d{2}-\d{2}$/.test(date))throw Error('Choose an effective-date cohort.');
            q('[data-hd-heat-status]').textContent='Loading complete selected snapshots; no ranking yet…';
            const m=await heat.run(packet,chosen,date,fetcher,p=>{if(token===heatEpoch)q('[data-hd-heat-status]').textContent=p.completed+' / '+p.total+' fund snapshots loaded; '+p.requests+' requests, '+p.bytes+' declared bytes. No partial ranking.';});
            if(token!==heatEpoch)return;
            heatUntil=Math.min(...m.coverage.filter(r=>!r.reason).map(r=>Date.parse(r.expiry)));q('[data-hd-heat-result]').innerHTML=heatView(m);q('[data-hd-heat-status]').textContent='Selected sample verified: '+m.requests+' network requests, '+m.bytes+' bytes. Qualification evaluated '+new Date().toISOString()+'.';
          }catch(e){if(token===heatEpoch)q('[data-hd-heat-status]').textContent='Heatmap unavailable: '+e.message;}
        };
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
      if(Date.now()>=heatUntil && q('[data-hd-heat-result]')?.textContent){heatEpoch++;heat.cancel();q('[data-hd-heat-result]').textContent='';q('[data-hd-heat-status]').textContent='Qualification clock advanced; reload the selected sample (verified cache reused).';}
      if (snap && Date.now() >= Date.parse(snap.source_valid_until)) invalidate();
    }, 60000);
    await refresh();
  }

  const api = {ownershipLimits, ownershipManifest, ownershipState, ownershipRows, ownershipSession, ownershipPanel, ownershipCoverage, ownershipView, bindOwnership, heatLimits, heatSelection, heatReason, heatMeasure, heatSession, heatView, kinds, typed, safe, load, retained, verifyPacket, snapshot, rowPart, directory, memberships, comparison,
    searchRows, searchSecurities, snapshotView, rowTable, rowDetail, memberView, scenario, render, sourceState, table, esc, cash, bindScenario, boot};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.JHHoldings = api;
  if (root.document) root.document.addEventListener('DOMContentLoaded', () => {
    root.document.querySelectorAll('main[data-holdings-mode]').forEach(host => {void boot(host);});
  });
})(typeof globalThis !== 'undefined' ? globalThis : this);
