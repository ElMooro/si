(function (root) {
  'use strict';
  const CONTRACT = 'etf-desk-original-research.v1', PREFIX = 'data/etf-desk-research/', CURRENT = 'data/etf-desk-research.json';
  const flags = ['forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible'];
  const MAX = 16 * 1024 * 1024;
  const esc = v => String(v ?? 'Unavailable').replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c]));
  const decimal = v => typeof v === 'string' && /^-?\d+(?:\.\d+)?$/.test(v) && v.length <= 310 && Number.isFinite(Number(v));
  const exact = v => decimal(v) ? v.replace(/^(\-?\d+)(.*)$/, (_, a, b) => a.replace(/\B(?=(\d{3})+(?!\d))/g, ',') + b) : 'Unavailable';
  const ticker = t => typeof t === 'string' && /^[A-Z][A-Z0-9.-]{0,14}$/.test(t);
  const clock = value => typeof value === 'string' && Number.isFinite(Date.parse(value));
  const current = (until, at=Date.now()) => clock(until) && at < Date.parse(until);
  const permissions = p => !!p && flags.every(k => p[k] === false) && p.call === null && p.portfolio_action === 'WAIT';
  function typed(p) {
    return !!(p?.contract === CONTRACT && permissions(p) && clock(p.generated_at) && p.quality?.independent_investment_votes === 0 &&
      p.quality.weight_unit_certified === false && p.quality.profile_fee_scale_certified === false &&
      p.funds && !Array.isArray(p.funds) && Object.keys(p.funds).length === p.quality.configured_funds &&
      Object.keys(p.funds).length > 0 && Object.keys(p.funds).length <= 1000 &&
      Object.entries(p.funds).every(([t, f]) => ticker(t) && f.ticker === t && permissions(f) &&
        f.flows?.ticker === t && permissions(f.flows) && f.holdings?.ticker === t && permissions(f.holdings) &&
        f.profiles?.current?.ticker === t && f.profiles?.prior?.ticker === t && f.holdings.current && f.holdings.prior) &&
      ['flows','holdings'].every(k => clock(p.canonical_sources?.[k]?.generated_at) && Date.parse(p.canonical_sources[k].generated_at) <= Date.parse(p.generated_at)));
  }
  const safe = key => typeof key === 'string' && (/^data\/etf-desk-research\/(?:runs|outputs|profiles)\/[a-f0-9]{64}\.json$/.test(key) ||
    /^data\/provider-flow-research\/histories\/[a-f0-9]{64}\.json$/.test(key));
  function stable(v) {
    if (Array.isArray(v)) return v.map(stable);
    if (v && typeof v === 'object') return Object.fromEntries(Object.keys(v).sort().map(k => [k, stable(v[k])]));
    return v;
  }
  async function sha(raw) {return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256', raw)), n => n.toString(16).padStart(2, '0')).join('');}
  async function load(key, fetcher, signal) {
    if (key !== CURRENT && !safe(key)) throw Error('Unapproved desk evidence path');
    const r = await fetcher('/' + key, {cache:key === CURRENT ? 'no-store' : 'default', signal});
    if (!r.ok) throw Error('Desk evidence request failed');
    if (Number(r.headers?.get('content-length')) > MAX) throw Error('Desk response byte bound');
    let raw;
    if (r.body?.getReader) {
      const reader = r.body.getReader(), parts = [];let length = 0;
      try {
        for (;;) {const item = await reader.read();if (item.done) break;
          length += item.value.byteLength;if (length > MAX) throw Error('Desk response byte bound');parts.push(item.value);}
      } catch (error) {await reader.cancel();throw error;}
      const joined = new Uint8Array(length);let offset = 0;
      for (const part of parts) {joined.set(part, offset);offset += part.byteLength;}raw = joined.buffer;
    } else raw = await r.arrayBuffer();
    if (raw.byteLength > MAX) throw Error('Desk response byte bound');
    return {raw, doc:JSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(raw))};
  }
  async function retained(ref, group, fetcher, signal) {
    const prefix = group === 'histories' ? 'data/provider-flow-research/' : PREFIX;
    if (!['histories','profiles','outputs'].includes(group) || !/^[a-f0-9]{64}$/.test(ref?.sha256) ||
      ref.key !== prefix + group + '/' + ref.sha256 + '.json' || !Number.isInteger(ref.bytes) || ref.bytes <= 0 || ref.bytes > MAX) throw Error('Desk artifact identity differs');
    const item = await load(ref.key, fetcher, signal);
    if (item.raw.byteLength !== ref.bytes || await sha(item.raw) !== ref.sha256) throw Error('Desk artifact bytes differ');
    return item.doc;
  }
  async function verifyPacket(p, fetcher, signal) {
    if (!typed(p)) throw Error('Native dated ETF desk required');
    const key = p.replay?.manifest_key;
    if (!safe(key) || !key.startsWith(PREFIX + 'runs/')) throw Error('Desk run identity differs');
    const item = await load(key, fetcher, signal), run = item.doc;
    if (key !== PREFIX + 'runs/' + await sha(item.raw) + '.json' || run.contract !== 'etf-desk-replay.v1' ||
      run.generated_at !== p.generated_at || run.output_sha256 !== p.replay.output_sha256 || run.output?.sha256 !== run.output_sha256) throw Error('Desk run differs');
    const output = await retained(run.output, 'outputs', fetcher, signal), {replay, ...body} = p;
    if (JSON.stringify(stable(body)) !== JSON.stringify(stable(output))) throw Error('Current desk body differs');
    return p;
  }
  async function recordedRun(id, fetcher, signal) {
    if (typeof id !== 'string' || !/^[a-f0-9]{64}$/.test(id)) throw Error('Exact recorded ETF desk run required');
    const key=PREFIX+'runs/'+id+'.json', item=await load(key,fetcher,signal),run=item.doc;
    if (await sha(item.raw)!==id || run.contract!=='etf-desk-replay.v1' || run.output?.sha256!==run.output_sha256) throw Error('Recorded ETF desk run differs');
    const body=await retained(run.output,'outputs',fetcher,signal);
    return verifyPacket({...body,replay:{manifest_key:key,output_sha256:run.output_sha256}},fetcher,signal);
  }
  function recordedUrl(p,t) {
    const key=p.replay?.manifest_key;
    if(!typed(p)||!ticker(t)||!p.funds[t]||!safe(key)||!key.startsWith(PREFIX+'runs/'))throw Error('Choose a recorded fund');
    return '/etf.html?fund='+encodeURIComponent(t)+'&run='+key.split('/').pop().slice(0,-5);
  }
  async function profile(p, t, role, fetcher, signal) {
    if (!typed(p) || !ticker(t) || !['current','prior','retained_previous_current'].includes(role)) throw Error('Choose a dated desk profile');
    const ref = p.funds[t]?.profiles?.[role]?.snapshot;
    const doc = await retained(ref, 'profiles', fetcher, signal);
    if (doc.contract !== 'etf-desk-profile-snapshot.v1' || doc.ticker !== t || !Array.isArray(doc.profiles) || doc.profiles.length > 40000) throw Error('Selected profile differs');
    return doc;
  }
  async function history(p, t, fetcher, signal) {
    if (!typed(p) || !ticker(t)) throw Error('Choose a desk fund');
    const doc = await retained(p.funds[t]?.flows?.history, 'histories', fetcher, signal);
    if (doc.contract !== 'provider-flow-history.v1' || doc.ticker !== t || !Array.isArray(doc.history) || doc.history.length > 50000) throw Error('Selected flow history differs');
    return doc;
  }
  function eligibility(f, at=Date.now()) {
    return {profile:!!(f?.profiles?.current?.quality?.current_profile_eligible && current(f.profiles.current.source_valid_until, at)),
      flow:!!(f?.quality?.flow_current_eligible && current(f.flows?.source_valid_until, at)),
      holdings:!!(f?.holdings?.current?.quality?.status === 'complete_returned_snapshot' && current(f.holdings.current.source_valid_until, at))};
  }
  function windowValue(f, n, at=Date.now()) {
    const w = f?.flows?.aligned_windows?.[String(n)];
    return eligibility(f, at).flow && w?.status === 'matched_reporting_window' && decimal(w.flow_usd_decimal) ? w.flow_usd_decimal : null;
  }
  function table(headers, rows, caption, html=false) {
    return '<div class="xr-scroll" tabindex="0" role="region" aria-label="' + esc(caption) + '"><table><caption>' + esc(caption) + '</caption><thead><tr>' +
      headers.map(v => '<th scope="col">' + esc(v) + '</th>').join('') + '</tr></thead><tbody>' + rows.map(row => '<tr>' +
        row.map((v,i) => '<td>' + (html && i === 0 ? v : esc(v)) + '</td>').join('') + '</tr>').join('') + '</tbody></table></div>';
  }
  const text = (f,k) => f.profiles.current.summary?.text?.[k]?.value;
  function inventory(p, query='', at=Date.now()) {
    if (!typed(p)) throw Error('Native desk inventory required');
    const search = String(query).trim().toLowerCase(), funds = Object.values(p.funds).filter(f =>
      [f.ticker,text(f,'description'),text(f,'issuer')].some(v => String(v || '').toLowerCase().includes(search)));
    return '<p>' + funds.length + ' of ' + p.quality.configured_funds + ' configured funds shown. All configured funds remain represented when a source is unavailable.</p>' +
      table(['Fund','Name','Profile effective','Holdings effective','Source checks · profile / flow / holdings','Matched 5 observations · USD'], funds.map(f => {
        const e = eligibility(f,at);
        return ['<button type="button" data-ed-select="' + esc(f.ticker) + '">' + esc(f.ticker) + '</button>',text(f,'description'),f.profiles.current.effective_date,
          Object.keys(f.holdings.current.effective_dates || {}).join(', ') || 'Unavailable',
          ['profile','flow','holdings'].map(k => e[k] ? 'Current' : 'Unavailable / overdue').join(' / '), exact(windowValue(f,5,at))];
      }), 'Complete configured ETF desk inventory', true);
  }
  function summary(p,at=Date.now()) {
    if (!typed(p)) throw Error('Native desk summary required');
    const counts = Object.values(p.funds).reduce((a,f) => {const e=eligibility(f,at);for (const k of Object.keys(e)) a[k]+=Number(e[k]);return a;},{profile:0,flow:0,holdings:0});
    return '<p class="xr-state">Research · WAIT means abstention · 0 independent investment votes.</p><p>Compiled ' + esc(p.generated_at) +
      '. Current source checks: profiles ' + counts.profile + '/' + p.quality.configured_funds + ', flows ' + counts.flow + '/' + p.quality.configured_funds +
      ', complete returned holdings ' + counts.holdings + '/' + p.quality.configured_funds + '.</p><p>' + p.quality.canonical_overlap +
      ' funds reuse the canonical flow and holdings evidence; ' + p.quality.additional_funds.length + ' have separately retained originals. These views share one provider root.</p>' +
      '<p>Source-check freshness does not mean today’s holdings. Profile, constituent and flow dates remain separate. Fee scales, profile AUM currency and holding-weight units are unqualified.</p>';
  }
  function provenance(source) {
    return 'Page ' + (source?.page ?? 'unavailable') + ', row ' + (source?.row_index ?? 'unavailable') +
      (source?.field ? ', field ' + source.field : '') + (source?.member_key ? ', member ' + source.member_key : '') + '; SHA-256 ' + (source?.sha256 || 'unavailable');
  }
  function profileView(doc, role) {
    let html = '<h3>' + esc(doc.ticker) + ' · ' + esc(role === 'current' ? 'current profile collection' : role === 'prior' ? 'earlier profile query' : 'previous retained profile') + '</h3>' +
      '<p>Profile effective ' + esc(doc.effective_date) + '; processed ' + esc(doc.processed_date) + '; acquired ' + esc(doc.source_acquired_at) +
      '. ' + esc(doc.quality?.status) + '. This date does not describe the constituents.</p>';
    if (!doc.profiles.length) return html + '<p>No complete profile available for this acquisition.</p>';
    for (const row of doc.profiles) {
      html += '<p>' + esc(provenance(row.source)) + '</p>' + table(['Field','Reported value','Status','Unit','Evidence'],
        Object.entries(row.numeric).map(([k,c]) => [k,exact(c.value_decimal),c.status,c.unit + (c.unit_certified ? ' · documented' : ' · unqualified'),provenance(c.source)]), 'Original numeric profile fields') +
        '<details><summary>All profile descriptions and classifications</summary>' + table(['Field','Reported text','Status','Evidence'],
          Object.entries(row.text).map(([k,c]) => [k,c.value,c.status,provenance(c.source)]),'Original profile text fields') + '</details>';
      for (const [field,view] of Object.entries(row.exposures)) {
        html += '<details><summary>' + esc(field) + ' · ' + esc(view.status) + '</summary><p>Raw reported sum ' + exact(view.raw_observed_sum_decimal) +
          '. Scale unqualified; values are not normalized.</p>' + table(['Reported category','Raw value','Status','Evidence'],
          view.entries.map(c => [c.label,exact(c.raw_decimal),c.status,provenance(c.source)]),field + ' original entries') +
          (view.raw_type === 'array' ? '<pre>' + esc(JSON.stringify(view.raw_structure,null,2)) + '</pre>' : '') + '</details>';
      }
      html += '<p>Rejected fields: ' + esc(row.field_errors.join(', ') || 'None') + '.</p>';
    }
    return html;
  }
  function fundView(p,t,at=Date.now()) {
    if (!typed(p) || !p.funds[t]) throw Error('Configured desk fund required');
    const f=p.funds[t],e=eligibility(f,at),legacy=f.legacy_history;
    return '<h2>' + esc(t) + ' · ' + esc(text(f,'description') || 'ETF source detail') + '</h2><p>Profile ' + esc(f.profiles.current.effective_date) +
      ' · holdings ' + esc(Object.keys(f.holdings.current.effective_dates || {}).join(', ') || 'Unavailable') + ' · latest flow ' + esc(f.flows.latest_effective_date) + '.</p>' +
      table(['Window · reporting observations','Matched flow · USD','Start / end','Coverage'],[1,5,21].map(n => {
        const w=f.flows.aligned_windows[String(n)];return [n,exact(windowValue(f,n,at)),[w.start_date,w.end_date].join(' / '),e.flow ? w.status : 'Source unavailable or overdue'];
      }),'Matched fund-flow windows') + '<p>Observed fund creations/redemptions do not establish underlying stock purchases, investor identity or future returns.</p>' +
      '<details><summary>Preserved earlier flow history</summary><p>' + esc(legacy.status) + '. ' + esc(legacy.reported_rows) + ' reported rows, ' +
      esc(legacy.reported_first_date) + ' through ' + esc(legacy.reported_last_date) +
      '. The whole predecessor is retained separately; it is not merged into the verified source tape.</p>' +
      (legacy.retained_original ? '<p>Retained original SHA-256 ' + esc(legacy.retained_original.sha256) + '; ' + legacy.retained_original.bytes + ' bytes.</p>' : '') + '</details>';
  }
  function scenario(p,t,position,shock,cost,at=Date.now()) {
    if (!typed(p) || !p.funds[t] || Date.parse(p.generated_at)>at || !Object.values(eligibility(p.funds[t],at)).some(Boolean)) throw Error('Select a fund with a current source check');
    if (![position,shock,cost].every(v => typeof v === 'number' && Number.isFinite(v)) || position===0 || Math.abs(position)>1e12 || shock < -100 || shock>1000 || cost<0 || cost>1e10) throw Error('Enter bounded signed position, assumed fund return and nonnegative costs');
    const gross=position*shock/100;
    return {fund:t,position_usd:position,assumed_fund_return_pct:shock,costs_usd:cost,gross_change_usd:gross,net_change_usd:gross-cost,
      run:p.replay,source_profile_weight_or_fee_used:false,scope:'Your assumed fund-level return and costs. No forecast, automatic leverage multiplier, measured holdings allocation or suggested position.'};
  }
  const api={CONTRACT,PREFIX,CURRENT,MAX,esc,exact,decimal,ticker,typed,safe,load,retained,verifyPacket,recordedRun,recordedUrl,profile,history,
    eligibility,windowValue,table,inventory,summary,provenance,profileView,fundView,scenario};
  if (typeof module !== 'undefined' && module.exports) module.exports=api;
  root.JHEtfDeskResearch=api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
