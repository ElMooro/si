/* Company context from the same immutable accounting evidence as the statement desk. */
(function (root) {
  'use strict';
  const esc = value => String(value ?? 'Unavailable').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const explain = value => String(value || 'Unavailable').replace(/_/g,' ');
  function section(ticker) {
    return '<section class="block" id="recorded-statements" data-ticker="'+esc(ticker)+'"><style>#recorded-statements a{color:var(--accent,#47d7ed);overflow-wrap:anywhere}#recorded-statements summary{cursor:pointer;line-height:1.5}#recorded-statements td,#recorded-statements th{padding:7px;text-align:left;vertical-align:top}#recorded-statements details{margin:8px 0}#recorded-statements p{line-height:1.6}</style><h3>Recorded financial statements <span class="pill" data-statement-state>verifying</span></h3><div data-statement-content aria-live="polite">Verifying issuer identity, retained records and exact accounting arithmetic…</div></section>';
  }
  function clocks(packet, now) {
    const dates = [packet.generated_at, packet.source_acquisition_started_at, packet.source_acquisition_completed_at,
      packet.identity_index?.requested_at, packet.identity_index?.received_at].map(Date.parse);
    if (dates.some(t => !Number.isFinite(t) || t > now) || dates[1] > dates[2] || dates[3] > dates[4]) throw Error('Accounting source clocks are missing, inconsistent or future-dated.');
    // Acquisition age is not the age of a fiscal period or a filing.
    return now - Math.min(...dates) <= 48 * 3600 * 1000;
  }
  function identity(row) { return row.identity || row.identity_evidence?.[0]?.reported_identity || {}; }
  function newest(records, period) {
    const rows = records.filter(row => row.request_period === period);
    const dated = rows.filter(row => /^\d{4}-\d{2}-\d{2}$/.test(identity(row).date || ''));
    // Do not hide an unqualified newer row by substituting an older calculation.
    const day = dated.map(row => identity(row).date).sort().at(-1);
    const choices = dated.filter(row => identity(row).date === day);
    return {rows, choices, selected: choices.length === 1 ? choices[0] : null};
  }
  function deskLink(state, symbol, period, row) {
    return '/statement-research.html?run='+state.runId+'&symbol='+encodeURIComponent(symbol)
      +'&period='+period+(row?'&record='+row.record_id:'');
  }
  function periodHtml(state, selected, period) {
    const choice = newest(selected.shard.records, period), row = choice.selected;
    const title = period === 'annual' ? 'Annual request' : 'Quarterly request';
    if (!row) return '<div><h4>'+title+'</h4><p>'+(choice.rows.length
      ? 'No unique newest reported period and filing identity can be selected. Inspect the retained records; no older value is substituted.'
      : 'No statements were returned for this request period.')+'</p><a href="'+esc(deskLink(state, selected.summary.symbol, period))+'">Inspect all '+choice.rows.length+' retained records</a></div>';
    const i = identity(row);
    let html = '<details'+(period === 'annual'?' open':'')+'><summary>'+title+' · reported period ended '+esc(i.date)+' · '+esc(i.reportedCurrency)+' · '+esc(explain(row.status))+'</summary>'
      +'<p>Reported CIK '+esc(i.cik)+' · fiscal '+esc(i.fiscalYear)+' '+esc(i.period)+' · filing '+esc(i.filingDate)+' · provider accepted '+esc(i.acceptedDate)+' (timezone unverified).</p>';
    if (!row.measurements) html += '<p>Calculations unavailable: '+esc(explain(row.status))+'. Original identity evidence remains inspectable.</p>';
    else {
      html += '<div style="overflow:auto" role="region" aria-label="Recorded '+period+' accounting measurements" tabindex="0"><table style="width:100%;min-width:540px"><thead><tr><th>Measurement / inspect inputs</th><th>Exact recorded value</th><th>Unit</th><th>State</th></tr></thead><tbody>';
      for (const metric of Object.values(row.measurements.metrics)) {
        const inputs = metric.inputs.map(input => {
          const coord = row.source_rows.find(c => c.source_id === input.source_id && c.source_row === input.source_row && c.endpoint === input.endpoint);
          const origin = coord && state.packet.sources[coord.capture_id];
          return '<li>'+esc(input.endpoint)+' / '+esc(input.field)+' = '+esc(input.reported_value)
            +(origin?'<br>Acquired '+esc(origin.received_at)+' · original JSON row '+esc(input.source_row)+' (zero-based).<br>SHA-256 <span style="overflow-wrap:anywhere">'+esc(input.source_id)+'</span><br>Source request <span style="overflow-wrap:anywhere">'+esc(origin.request.url)+'</span>':' · no unique matching source row')+'</li>';
        }).join('');
        html += '<tr><th scope="row" style="text-align:left"><details><summary>'+esc(metric.definition)+'</summary><ul style="max-width:440px">'+inputs+'</ul></details></th><td>'+esc(metric.value)+'</td><td>'+esc(metric.unit)+'</td><td>'+esc(explain(metric.status))+'</td></tr>';
      }
      html += '</tbody></table></div>';
    }
    return html+'<p><a href="'+esc(deskLink(state, selected.summary.symbol, period, row))+'">Open this exact filing identity, full history and assumed-exposure calculator</a> · '+choice.rows.length+' retained '+period+' records.</p></details>';
  }
  function paint(body, pill, state, selected, current) {
    const packet = state.packet;
    body.innerHTML = '<p>Descriptive accounting measurements from retained provider statements. No health grade, fraud conclusion, forecast or position size is inferred. Amount scale and original SEC filing values have not been independently audited.</p>'
      +'<p>Capture '+esc(packet.generated_at)+' · sources acquired '+esc(packet.source_acquisition_started_at)+' through '+esc(packet.source_acquisition_completed_at)+'. '
      +(current?'Within the 48-hour acquisition review window.':'Acquisition review overdue.')+' Reporting periods and filing dates below describe the underlying information lag; capture time does not make them current.</p>'
      +'<p>Current ticker/CIK correspondence was checked against the SEC index acquired '+esc(packet.identity_index.received_at)+'. Historical security continuity is unverified. Annual and quarterly requests remain separate; no TTM or annualization is inferred.</p>'
      +periodHtml(state, selected, 'annual')+periodHtml(state, selected, 'quarter')
      +'<p>Arithmetic uses exact reported decimals, rounded half-even to twelve places. Zero remains zero; missing inputs and nonpositive denominators stay unavailable.</p>'
      +'<p><a href="/'+esc(state.reference)+'">Verified immutable run and compiler versions</a> · <a href="/'+esc(state.run.output.key)+'">Complete captured population and source inventory</a> · <a href="/'+esc(selected.artifact.key)+'">Complete issuer evidence</a></p>';
    const inspector = root.document.createElement('div'); body.append(inspector);
    root.JHResearchInspection.show(inspector, [{label:selected.summary.symbol+' · complete verified issuer history', source:'/'+selected.artifact.key, document:selected.shard}]);
    pill.textContent = current ? 'descriptive' : 'acquisition review overdue';
  }
  async function load(ticker) {
    const section = root.document.getElementById('recorded-statements');
    if (!section || section.dataset.ticker !== ticker) return;
    const body = section.querySelector('[data-statement-content]'), pill = section.querySelector('[data-statement-state]');
    const active = () => root.document.getElementById('recorded-statements') === section && section.dataset.ticker === ticker;
    try {
      const api = root.JHStatementResearch;
      if (!api || !root.JHResearchInspection) throw Error('Accounting verification is unavailable.');
      const fetcher = root.fetch.bind(root), state = await api.load(fetcher, null);
      if (!active()) return;
      clocks(state.packet, Date.now());
      if (!state.packet.issuers.some(row => row.symbol === ticker)) {
        body.textContent = ticker+' is outside this retained '+state.packet.reported_names+'-name population. No statement or issuer is substituted. Full company-response fields remain available in the complete returned-data inspector.';
        pill.textContent = 'outside captured population'; return;
      }
      const selected = await api.record(fetcher, state, ticker);
      if (!active()) return;
      paint(body, pill, state, selected, clocks(state.packet, Date.now()));
    } catch (error) {
      if (!active()) return;
      body.textContent = 'Recorded accounting evidence could not be verified. '+String(error?.message || 'Verification unavailable.').slice(0,180)+' No legacy health score is substituted.';
      pill.textContent = 'unavailable';
    }
  }
  const api = {section,load,clocks,newest,periodHtml};
  root.JHEquityStatements = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
