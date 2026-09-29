/* Public research evidence; private account state never enters this renderer. */
(function (root) {
  'use strict';
 function day(s){if(typeof s!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(s)||s.startsWith('0000-'))return null;const n=Date.parse(s+'T00:00:00Z');return Number.isFinite(n)&&new Date(n).toISOString().slice(0,10)===s?n:null;}
 function clock(s){if(typeof s!=='string')return null;const m=/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|([+-])(\d{2}):(\d{2}))$/.exec(s);
  if(!m||day(m[1])===null||Number(m[2])>23||Number(m[3])>59||Number(m[4])>59||(m[5]!=='Z'&&(Number(m[7])>23||Number(m[8])>59)))return null;const n=Date.parse(s);return Number.isFinite(n)?n:null;
 }

 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw)),x=>x.toString(16).padStart(2,'0')).join('');}
 function strictJSON(source){
  // Reject duplicate identities and overflow rather than accepting the last key.
  let i=0;const number=/-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function ws(){while(/[\x20\t\r\n]/.test(source[i]||'x'))i++;}
  function string(){const start=i++;for(;i<source.length;i++){if(source[i]==='\\'){i++;continue;}if(source[i]==='"'){const text=JSON.parse(source.slice(start,++i));for(let n=0;n<text.length;n++){const c=text.charCodeAt(n);if(c>=0xD800&&c<=0xDBFF){const next=text.charCodeAt(++n);if(!(next>=0xDC00&&next<=0xDFFF))throw Error('Invalid Unicode scalar');}else if(c>=0xDC00&&c<=0xDFFF)throw Error('Invalid Unicode scalar');}return text;}}throw Error('Incomplete JSON string');}
  function value(depth){
   if(depth>128)throw Error('JSON nesting exceeds bound');ws();const c=source[i];
   if(c==='"')return string();
   if(c==='{'||c==='['){const object=c==='{',out=object?{}:[],seen=new Set(),end=object?'}':']';i++;ws();if(source[i]===end){i++;return out;}
    for(;;){ws();let key;if(object){if(source[i]!=='"')throw Error('JSON key required');key=string();if(seen.has(key))throw Error('Duplicate JSON key');seen.add(key);ws();if(source[i++]!==':')throw Error('JSON colon required');}
     const item=value(depth+1);if(object)Object.defineProperty(out,key,{value:item,enumerable:true,writable:true,configurable:true});else out.push(item);
     ws();if(source[i]===end){i++;return out;}if(source[i++]!==',')throw Error('Incomplete JSON structure');}
   }
   for(const [token,v]of [['true',true],['false',false],['null',null]])if(source.startsWith(token,i)){i+=token.length;return v;}
   number.lastIndex=i;const m=number.exec(source);if(!m)throw Error('Invalid JSON value');i=number.lastIndex;const n=Number(m[0]);if(!Number.isFinite(n))throw Error('Nonfinite JSON number');return n;
  }
  const out=value(0);ws();if(i!==source.length)throw Error('Trailing JSON content');return out;
 }

  async function readJSON(response) {
    if (!response.ok) { try { await response.body?.cancel(); } catch (_) {} throw Error('Research response unavailable'); }
    const raw = await response.arrayBuffer();
    const doc = strictJSON(new TextDecoder('utf-8', {fatal:true, ignoreBOM:true}).decode(raw));
    if (!doc || Array.isArray(doc) || typeof doc !== 'object') throw Error('Research object required');
    return {raw, doc};
  }

  function state(doc, now = Date.now()) {
    if (!doc || !['warehouse_deterministic_v1','warehouse_deterministic_v2'].includes(doc.generation_method) || typeof doc.brief_md !== 'string' || doc.brief_md.trim().length < 120 || doc.call_verb !== 'WAIT' || doc.sizing_eligible !== false) return {valid:false};
    const generated = clock(doc.generated_at), age = generated === null ? NaN : now - generated;
    const clockStatus = !Number.isFinite(age) ? 'invalid' : age < -300000 ? 'future' : age > 4.5 * 3600000 ? 'overdue' : 'current';
    return {valid:true, overdue:clockStatus !== 'current', clockStatus,
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
  function originalPath(ref, provider='fred') {
    if(!['fred','fr2004','ecb','tic'].includes(provider))return null;
    return ref && typeof ref.sha256 === 'string' && /^[a-f0-9]{64}$/.test(ref.sha256) &&
      typeof ref.key === 'string' && new RegExp('^data/evidence/'+provider+'/[a-f0-9]{64}/'+ref.sha256+'\\.bin\\.gz$').test(ref.key) ? '/'+ref.key : null;
  }
  async function proofMatches(proof, raw, now = Date.now()) {
    try {
      const doc = strictJSON(new TextDecoder('utf-8', {fatal:true, ignoreBOM:true}).decode(raw));
      const ref = doc?.research_replay, binding = proof?.public_object, audited = clock(proof?.generated_at), published = clock(doc?.generated_at);
      return !!(state(doc, now).valid && bundlePath(ref) &&
        proof?.schema_version === 'calls-research-replay-proof.v1' && proof.status === 'reproduced' &&
        proof.call_verb === 'WAIT' && proof.sizing_eligible === false && proof.decision_eligible === false && proof.private_account_data_read === false &&
        proof.run_id === ref.run_id && proof.payload_sha256 === ref.payload_sha256 && proof.bundle_sha256 === ref.bundle_sha256 &&
        typeof doc.snapshot_id === 'string' && doc.snapshot_id.length > 0 && proof.snapshot_id === doc.snapshot_id &&
        proof.publication_generated_at === doc.generated_at && audited !== null && published !== null &&
        Number.isFinite(now) && audited <= now + 300000 && audited >= published - 300000 &&
        binding?.key === 'data/ai-brief-public.json' && Number.isSafeInteger(binding.bytes) && binding.bytes === raw.byteLength &&
        /^[a-f0-9]{64}$/.test(binding.sha256 || '') && binding.sha256 === await sha(raw) &&
        proof.brief_sha256 === await sha(new TextEncoder().encode(doc.brief_md)));
    } catch (_) { return false; }
  }
  function render(document, box, doc, now) {
    box.replaceChildren();
    const el = (tag, text, parent = box) => { const n = document.createElement(tag); n.textContent = text; parent.appendChild(n); return n; };
    el('h2', 'Evidence behind this brief');
    const view = state(doc, now);
    if (!view.valid) { el('p', 'Public brief unavailable. WAIT — no new allocation guidance.'); return () => {}; }
    const status = el('p', ''); status.setAttribute('role', 'status');
    const updateAge = (at = Date.now()) => {
      const current = state(doc, at), prefix = {invalid:'Clock unavailable · ',future:'Future clock · ',overdue:'Overdue · ',current:''}[current.clockStatus];
      status.textContent = prefix + 'WAIT · Research only · ' + view.generated;
    };
    updateAge(now);
    el('p', 'Measurements and source-quality assessments below describe this brief at publication. Replaying it does not refresh its observations.');
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
    for (const label of ['Measurement / source', 'Value and unit', 'Observed', 'Quality at publication', 'Shared roots']) {
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
    const originals = doc.original_source_lineage?.liquidity_flow;
    if (originals) {
      const panel=el('details','',details);el('summary','Liquidity: original sources and calculation',panel);
      if(originals.status!=='verified')el('p','Original-source replay unavailable. Reported liquidity is unqualified for current research use.',panel);
      else {
        el('p','WALCL − WTREGEN − RRPONTSYD · USD billions. Retained originals reconstructed for this brief; the independent run check is shown above.',panel);
        el('p',originals.current_use?.eligible===true?'At brief publication, descriptive use passed the declared source clocks. No allocation permission.':'Historical reconstruction only; current use is withheld.',panel);
        for(const sid of ['WALCL','WTREGEN','RRPONTSYD']) {
          const source=originals.sources?.[sid],component=originals.latest_reconstructed?.components?.[sid];
          const observation=originals.observations?.[component?.observation_id];
          if(!source)continue;
          el('h4',sid+' · '+(sid==='RRPONTSYD'?'Fed reverse-repo operations':'Fed H.4.1'),panel);
          el('p','Observed '+(observation?.observation_date || 'unavailable')+' · original reported value '+(observation?.reported_native_value ?? 'unavailable')+' '+(observation?.native_unit || '')+'. Acquired '+(source.acquired_at || 'unavailable')+'.',panel);
          for(const kind of ['definition','observations']) {
            const ref=source.originals?.[kind],href=originalPath(ref);
            if(href){const link=el('a','Inspect original '+sid+' '+kind,panel);link.href=href;link.style.display='block';}
          }
        }
        el('p','WALCL and WTREGEN share H.4.1. Repeated observations and shared sources do not become independent votes. Original publication timing and point-in-time history remain unverified; the frozen record retains all calculations.',panel);
      }
    }
    const fails=doc.original_source_lineage?.settlement_fails;
    if(fails) {
      const panel=el('details','',details);el('summary','Settlement fails: original reports, scopes and overlap',panel);
      if(fails.status!=='verified')el('p','FR2004 original replay unavailable. Reported fails remain unqualified for current research use.',panel);
      else {
        el('p','Complete FR2004 source replay: '+String(fails.coverage?.original_rows ?? 'unknown')+' original rows across '+String(fails.coverage?.original_series ?? 'unknown')+' series. Every scope total is checked in original USD millions before conversion.',panel);
        for(const id of ['ust_ex_tips','treasury_incl_tips']) {
          const scope=fails.scopes?.[id];if(!scope)continue;
          el('h4',scope.label || id,panel);
          const values=scope.reported?.exact_usd_bn || {};
          el('p','Observed '+(scope.reported?.as_of || 'unavailable')+' · FTD '+(values.ftd ?? 'unavailable')+' + FTR '+(values.ftr ?? 'unavailable')+' = gross '+(values.gross ?? 'unavailable')+' USD billions.',panel);
          el('p',scope.current_use?.eligible===true?'At brief publication, descriptive use passed original-source clocks. No allocation permission.':'Historical report only; current research use is withheld.',panel);
        }
        el('p','Treasury including TIPS contains the Treasury excluding TIPS headline. Never add these scopes or treat them as separate evidence. Two-sided cumulative fails are not unique defaults, losses or capital flows.',panel);
        for(const observation of Object.values(fails.observations || {})) {
          el('p',String(observation.series_id || 'Unknown series')+' · source row (zero-based) '+String(observation.original_row ?? 'unavailable')+' · '+String(observation.observation_date || 'unavailable')+' · '+String(observation.reported_native_value ?? 'missing')+' reported USD millions.',panel);
        }
        el('p','Original observations acquired '+String(fails.source_acquired_at || 'unavailable')+'. Actual publication times and holiday adjustments remain unverified.',panel);
        for(const [name,source] of Object.entries(fails.original_sources || {})) {
          const href=originalPath(source?.evidence,'fr2004');
          if(href){const link=el('a','Inspect original FR2004 '+name,panel);link.href=href;link.style.display='block';}
        }
        const full=fails.complete_source_output_key;
        if(typeof full==='string' && /^data\/fails-research\/outputs\/[a-f0-9]{64}\.json$/.test(full)) {
          const link=el('a','Inspect complete source histories and all six asset classes',panel);link.href='/'+full;link.style.display='block';
        }
        el('p','This panel shows the two Calls scopes. Complete source histories remain in the immutable output above; all were replayed. Shared rows are not independent events or a validated forecast.',panel);
      }
    }
    const ciss=doc.original_source_lineage?.ciss;
    if(ciss) {
      const panel=el('details','',details);el('summary','CISS: original ECB rows and contribution reconciliation',panel);
      if(ciss.status!=='verified')el('p','ECB original replay unavailable. Reported CISS remains unqualified for current research use.',panel);
      else {
        panel.style.overflowWrap='anywhere';
        el('p','Complete retained ECB histories: '+String(ciss.coverage?.original_series ?? 'unknown')+' series and '+String(ciss.coverage?.original_rows ?? 'unknown')+' source rows. Values are dimensionless index points, not crisis probabilities.',panel);
        el('p',ciss.current_headline_research_eligible===true?'At brief publication, all seven headline and contribution legs passed their source clocks and same-date arithmetic. Descriptive use only.':'Historical reconstruction only; current CISS research use is withheld.',panel);
        const proof=ciss.independent_arithmetic || {},counts=proof.headline_panel || {};
        el('p','Historical contribution sums: '+String(counts.matched ?? 'unknown')+' matched, '+String(counts.mismatch ?? 'unknown')+' mismatched, '+String(counts.unavailable ?? 'unknown')+' unavailable. Incomplete component panels are never filled with zero.',panel);
        const labels={SS_CIN:'Composite index',SS_BMN:'Bond-market contribution',SS_EMN:'Equity-market contribution',SS_FIN:'Financial-intermediary contribution',SS_FXN:'Foreign-exchange contribution',SS_MMN:'Money-market contribution',SS_CON:'Correlation contribution'};
        for(const [key,series] of Object.entries(ciss.series || {})) {
          const point=ciss.observations?.[series.latest_occurrence],source=ciss.original_sources?.[key];
          el('h4',labels[key.split('.')[6]] || 'ECB series',panel);
          el('small',key,panel);
          el('p','Observed '+String(point?.observation_period || 'unavailable')+' · original value '+String(point?.reported_decimal ?? 'missing')+' · status '+String(point?.observation_status || 'unknown')+' · source row (zero-based) '+String(point?.original_row ?? 'unavailable')+'. Acquired '+String(source?.acquired_at || 'unavailable')+'.',panel);
          const href=originalPath(source,'ecb');
          if(href){const link=el('a','Inspect original ECB '+(labels[key.split('.')[6]] || key),panel);link.href=href;link.style.display='block';}
          const comparisons=el('details','',panel);el('summary','Inspect dated changes and baseline rows',comparisons);
          for(const [window,change] of Object.entries(series.comparisons || {})) {
            el('p',window+': '+String(change.value ?? 'unavailable')+' index points; target '+String(change.target_date || 'unavailable')+'; baseline '+String(change.baseline_period || 'unavailable')+'; original row (zero-based) '+String(change.baseline_source_row ?? 'unavailable')+'.',comparisons);
          }
        }
        const manifest=ciss.source_replay?.manifest_key;
        if(typeof manifest==='string' && /^data\/ciss-research\/runs\/[a-f0-9]{64}\.json$/.test(manifest)) {
          const link=el('a','Open complete source manifest and all retained histories',panel);link.href='/'+manifest;link.style.display='block';
        }
        el('p','The composite and its six contributions share one ECB measurement system. They do not count as seven independent votes. These are current retrieved vintages; historical publication availability, distribution statistics and forecasting value remain unqualified.',panel);
      }
    }
    const tic=doc.original_source_lineage?.tic;
    if(tic) {
      const panel=el('details','',details);el('summary','Treasury capital flows: transactions, holders and original months',panel);
      if(tic.status!=='verified')el('p','TIC original replay unavailable. Reported transactions remain unqualified for current research use.',panel);
      else {
        panel.style.overflowWrap='anywhere';
        const coverage=tic.coverage || {},proof=tic.independent_arithmetic || {};
        el('p','Complete Treasury archive: '+String(coverage.archive_series_retained ?? 'unknown')+' series retained; '+String(coverage.core_series ?? 'unknown')+' core series and '+String(coverage.original_core_rows ?? 'unknown')+' original rows checked. Other series remain unqualified.',panel);
        el('p',tic.current_use?.eligible===true?'At brief publication, descriptive use passed native source clocks and calendar checks. No allocation permission.':'Historical reconstruction only; current TIC research use is withheld.',panel);
        el('p','Independent arithmetic: '+String(proof.windows ?? 'unknown')+' windows and '+String(proof.reconciliations ?? 'unknown')+' component reconciliations checked. '+String(proof.incomplete_windows ?? 'unknown')+' incomplete historical windows remain unavailable.',panel);
        const net=tic.net_cross_border_definition || {},holders=tic.reported_holder_splits || {};
        el('p','Net cross-border long-term flow = foreign net purchases of U.S. securities minus U.S. net purchases of foreign securities, over the same twelve months: '+String(net.value ?? 'unavailable')+' USD billions.',panel);
        el('p','Foreign holder split, twelve months: official '+String(holders.official?.sum_12m ?? 'unavailable')+'; private '+String(holders.private?.sum_12m ?? 'unavailable')+' USD billions. All twelve monthly holder decompositions reconciled: '+(holders.rolling_twelve_months_reconciled===true?'yes':'no')+'.',panel);
        const labels={total:'Foreign purchases: all U.S. long-term securities',treasuries:'Long-term Treasuries',agency_bonds:'Agency bonds',corporate_bonds:'Corporate bonds',equities:'Equities',short_treasury:'Short-term Treasuries',us_abroad:'U.S. purchases of foreign long-term securities',official:'Foreign official holders',private:'Foreign private holders',total_prior_nonoverlapping_12m:'Previous non-overlapping twelve months'};
        for(const role of Object.keys(labels)) {
          const window=tic.windows?.[role];if(!window)continue;
          const detail=el('details','',panel);el('summary',labels[role]+' · '+String(window.total_usd_bn_decimal ?? 'unavailable')+' USD billions',detail);
          el('p','Exact window: '+String(window.months?.[0] || 'unavailable')+' to '+String(window.months?.at(-1) || 'unavailable')+'. Missing months: '+(window.missing_months?.join(', ') || 'none')+'.',detail);
          for(const month of window.months || []) {
            const point=tic.observations?.[window.original_observations?.[month]];
            el('p',month+' · native value '+String(point?.reported_native_value ?? 'missing')+' USD millions · archive series index '+String(point?.original_series_index ?? 'unavailable')+' · original row '+String(point?.original_row ?? 'unavailable')+' (zero-based).',detail);
          }
        }
        const bulk=tic.original_sources?.bulk,href=originalPath(bulk?.evidence,'tic');
        el('p','Original archive acquired '+String(bulk?.acquired_at || 'unavailable')+'. Nominal release-calendar dates are not verified initial publication times.',panel);
        if(href){const link=el('a','Inspect complete original Treasury archive',panel);link.href=href;link.style.display='block';}
        const full=tic.complete_source_output_key,manifest=tic.source_replay?.manifest_key;
        for(const [key,kind,label] of [[full,'outputs','Inspect complete native output and history shards'],[manifest,'runs','Inspect source manifest, compiler pins and FRED comparisons']]) {
          if(typeof key==='string' && new RegExp('^data/tic-research/'+kind+'/[a-f0-9]{64}\\.json$').test(key)) {
            const link=el('a',label,panel);link.href='/'+key;link.style.display='block';
          }
        }
        el('p','The asset classes, holder groups and total overlap. Never add them as separate flows. FRED redistributes CSLT and supplies no additional independent economic observation. These transactions are not valuation changes, investable cash or a forecast; current retrieved vintages do not establish historical information availability.',panel);
      }
    }
    const prose = el('details', ''); el('summary', 'Read the brief, disagreements and limitations', prose);
    for (const line of doc.brief_md.split('\n')) {
      if (!line.trim() || line.startsWith('# ')) continue;
      const n = el(line.startsWith('## ') ? 'h3' : 'p', line.replace(/^## /, '').replace(/^\*\*|\*\*$/g, ''), prose);
      n.style.overflowWrap = 'anywhere';
    }
    return updateAge;
  }
  const api = {state, bundlePath, originalPath, proofMatches, readJSON, render};
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (!root.document) return;
  const document = root.document, main = document.querySelector('main'); if (!main || document.getElementById('public-market-brief')) return;
  const box = document.createElement('section'); box.id = 'public-market-brief'; box.className = 'card section';
  box.style.cssText = 'margin:24px 0;padding:20px;line-height:1.65';
  const kpis = document.getElementById('kpi-row'); main.insertBefore(box, kpis ? kpis.nextSibling : null);
  box.textContent = 'Loading the public research brief…';
  let refreshing = false, updateAge = () => {};
  async function refresh() {
    if (refreshing) return;
    refreshing = true;
    try {
      const response = await fetch('/data/ai-brief-public.json', {cache:'no-store', redirect:'error', signal:AbortSignal.timeout(15000)});
      const packet = await readJSON(response), doc = packet.doc; updateAge = render(document, box, doc, Date.now());
      const ref = doc.research_replay, badge = document.getElementById('calls-replay-proof');
      if (!badge || !bundlePath(ref)) return;
      try {
        const response = await fetch('/data/calls-research-proofs/' + ref.payload_sha256 + '.json', {cache:'no-store', redirect:'error', signal:AbortSignal.timeout(10000)});
        const proof = (await readJSON(response)).doc;
        const matched = await proofMatches(proof, packet.raw);
        badge.textContent = matched ? ' · Replay verified for these exact brief bytes · ' + proof.generated_at : ' · Replay not verified for this displayed brief';
      } catch (_) { /* The retained record remains useful; verification stays pending. */ }
    } catch (_) { updateAge = render(document, box, null, Date.now()); }
    finally { refreshing = false; }
  }
  let networkTimer = null, ageTimer = null;
  function startTimers() {
    if (networkTimer !== null) return;
    networkTimer = setInterval(() => { if (document.visibilityState !== 'hidden') refresh(); }, 300000);
    ageTimer = setInterval(() => updateAge(), 60000);
  }
  function stopTimers() {
    if (networkTimer !== null) clearInterval(networkTimer);
    if (ageTimer !== null) clearInterval(ageTimer);
    networkTimer = ageTimer = null;
  }
  document.addEventListener?.('visibilitychange', () => updateAge());
  root.addEventListener?.('pagehide', stopTimers);
  root.addEventListener?.('pageshow', () => { updateAge(); startTimers(); });
  refresh(); startTimers();
})(typeof globalThis === 'object' ? globalThis : this);
