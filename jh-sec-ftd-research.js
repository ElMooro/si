(function (root) {
  'use strict';
  const PREFIX = 'data/sec-ftd-research/', CONTRACT = 'sec-ftd-original-research.v1', CURRENT = 'data/squeeze-fuel.json';
  const HASH = /^[a-f0-9]{64}$/, FLAGS = ['calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified'];
  const COMPILERS = {
    sec_ftd_source: '69e9ad4792d5b97dc302aaa61d0f69715fa26b3db690b143c6ff100e6a123bfe',
    sec_ftd_measurements: '3536dce70f4ed7df4028d2b890e32176f4dc9e270fd8bb2a6cb24f91362195a0',
    sec_ftd_research_model: '32ab46ad21b4b7b9368bdd61df15557a8da78249bdcf0fb986af7d442baa4e54',
    sec_ftd_research_store: '0533b9341ab5d2422736ac7ab1144de1e6851dbffb88206fc28acdf2abe56c60'
  };
  const FIELDS = ['source_index', 'source_line', 'settlement_date', 'reported_symbol', 'reported_description',
    'fail_balance_shares', 'reported_previous_day_price', 'previous_reported_date', 'previous_record_present',
    'previous_fail_balance_shares', 'adjacent_balance_change_shares', 'adjacent_balance_change_pct', 'comparison_status'];
  function canonical(v) {
    if (Array.isArray(v)) return '[' + v.map(canonical).join(',') + ']';
    if (v && typeof v === 'object') return '{' + Object.keys(v).sort().map(k => JSON.stringify(k) + ':' + canonical(v[k])).join(',') + '}';
    return JSON.stringify(v);
  }
  async function hash(bytes) {
    return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256', bytes)), n => n.toString(16).padStart(2, '0')).join('');
  }
  async function get(fetcher, key, ref) {
    if (key !== CURRENT && !new RegExp('^' + PREFIX + '(?:runs|inputs|outputs|records)/[a-f0-9]{64}\\.json$').test(key)) throw Error('Invalid research artifact path');
    const response = await fetcher('/' + key, {cache: 'no-store', credentials: 'same-origin'});
    if (!response.ok) throw Error('Recorded research is unavailable (HTTP ' + response.status + ')');
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length > 8 * 1024 * 1024) throw Error('Research artifact exceeds its size limit');
    const digest = await hash(bytes);
    if (ref && (ref.key !== key || ref.bytes !== bytes.length || ref.sha256 !== digest)) throw Error('Recorded artifact verification failed');
    if (key !== CURRENT && key.split('/').pop().slice(0, 64) !== digest) throw Error('Recorded artifact identity differs');
    return JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(bytes));
  }
  function valid(p) {
    return p && p.contract === CONTRACT && FLAGS.every(k => p[k] === false)
      && ['call', 'signal', 'score', 'fail_age_days', 'short_sale_origin', 'forced_buy_in_probability', 'short_interest', 'short_float_pct'].every(k => p[k] === null)
      && p.decision?.abstain === true && p.decision?.eligible_votes === 0 && p.decision?.verb === 'WAIT'
      && Object.keys(p.by_ticker || {}).length === 0 && ['board', 'rows', 'top_picks'].every(k => Array.isArray(p[k]) && p[k].length === 0)
      && canonical(p.point_fields) === canonical(FIELDS);
  }
  async function load(fetcher, pinned) {
    let reference, current = null;
    if (pinned !== null && pinned !== undefined) {
      if (!HASH.test(pinned)) throw Error('Invalid recorded run');
      reference = PREFIX + 'runs/' + pinned + '.json';
    } else {
      current = await get(fetcher, CURRENT);
      if (!valid(current) || !current.replay) throw Error('Native SEC settlement research is not available yet');
      reference = current.replay.manifest_key;
    }
    const run = await get(fetcher, reference);
    if (run.contract !== 'sec-ftd-original-replay.v1' || !HASH.test(run.output_sha256)
        || canonical(Object.keys(run.compilers || {}).sort()) !== canonical(Object.keys(COMPILERS).sort())) throw Error('Unsupported recorded calculation');
    for (const [name, digest] of Object.entries(COMPILERS)) {
      const ref = run.compilers[name];
      if (ref.sha256 !== digest || ref.key !== PREFIX + 'compilers/' + digest + '.py') throw Error('Unqualified calculation version');
    }
    const packet = await get(fetcher, run.output.key, run.output);
    if (!valid(packet) || run.output.sha256 !== run.output_sha256 || packet.generated_at !== run.generated_at) throw Error('Recorded publication differs from its run');
    if (current) {
      const body = {...current}; delete body.replay;
      if (current.replay.output_sha256 !== run.output_sha256 || await hash(new TextEncoder().encode(canonical(body))) !== run.output_sha256) throw Error('Current head differs from its recorded publication');
    }
    return {packet, run, reference, runId: reference.split('/').pop().slice(0, 64)};
  }
  function findIssues(state, query) {
    const value = String(query).trim().toUpperCase(), exact = state.packet.issues.filter(item => item.cusip === value);
    if (exact.length) return exact;
    const symbols = state.packet.symbols.find(item => item.symbol === value);
    return symbols ? state.packet.issues.filter(item => symbols.record_ids.includes(item.record_id)) : [];
  }
  async function record(fetcher, state, cusip) {
    const issue = state.packet.issues.find(item => item.cusip === cusip);
    if (!issue || !HASH.test(issue.record_id)) throw Error('Choose an exact reported CUSIP from this record');
    const ref = state.packet.record_shards[issue.record_id.slice(0, 2)];
    if (!ref) throw Error('Missing recorded CUSIP bucket');
    const shard = await get(fetcher, ref.key, ref), row = shard.records?.[issue.record_id];
    if (shard.contract !== 'sec-ftd-record-shard.v1' || !row || row.reported_cusip !== cusip || FLAGS.some(k => row[k] !== false)
        || canonical(row.dates) !== canonical(state.packet.dates)) throw Error('Invalid descriptive CUSIP record');
    if (await hash(new TextEncoder().encode(canonical(['SEC_CNS_REPORTED_CUSIP', cusip]))) !== issue.record_id) throw Error('Reported identity differs');
    return {row, artifact: ref, issue};
  }
  function observation(state, selected, stamp) {
    if (!state.packet.dates.includes(stamp)) throw Error('Invalid reported settlement date');
    const raw = selected.row.observations.find(point => point[2] === stamp);
    if (!raw) return {date: stamp, point: null, source: null, missing_reason: 'This CUSIP is not reported on this date; no zero balance is imputed.'};
    if (raw.length !== FIELDS.length) throw Error('Incomplete recorded observation');
    const point = Object.fromEntries(FIELDS.map((key, i) => [key, raw[i]])), source = state.packet.sources[point.source_index];
    if (!Number.isInteger(point.source_index) || !source || !source.reported_dates[stamp] || !Number.isInteger(point.source_line)
        || point.source_line < 2 || point.source_line >= source.integrity_controls.record_count_source_line) throw Error('Recorded source locator differs');
    return {date: stamp, point, source, missing_reason: null};
  }
  function decimal(value) {
    if (typeof value !== 'string' || !/^[-+]?\d{1,18}(?:\.\d{1,8})?$/.test(value.trim())) throw Error('Use a decimal number with at most eight decimal places');
    const text = value.trim(), negative = text[0] === '-', parts = text.replace(/^[-+]/, '').split('.');
    return (negative ? -1n : 1n) * BigInt(parts[0] + (parts[1] || '').padEnd(8, '0'));
  }
  function exact(value, scale) {
    const negative = value < 0n; let text = (negative ? -value : value).toString().padStart(scale + 1, '0');
    if (scale) text = text.slice(0, -scale) + '.' + text.slice(-scale);
    return (negative ? '-' : '') + text.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
  }
  function scenario(a) {
    const q = decimal(a.shares), p = decimal(a.price), shock = decimal(a.shock), cost = decimal(a.cost);
    if (p <= 0n || cost < 0n || shock < -100n * 10n**8n || shock > 1000n * 10n**8n) throw Error('Use a positive price, nonnegative costs, and a price change from −100% to 1,000%');
    return {pnl_usd: exact(q * p * shock - cost * 10n**18n, 26), signed_notional_usd: exact(q * p, 16), assumptions: {...a}, forecast: false};
  }
  const api = {PREFIX, CONTRACT, CURRENT, COMPILERS, FIELDS, load, findIssues, record, observation, scenario, canonical, hash};
  if (typeof module !== 'undefined' && module.exports) module.exports = api; else root.JHSecFtdResearch = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
