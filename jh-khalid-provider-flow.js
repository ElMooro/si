/* Informational provider evidence only. No reads, scoring, or portfolio actions. */
(function (root) {
  'use strict';
  const periods = ['1', '5', '21'];
  const decimal = x => typeof x === 'string' && /^-?\d{1,24}(?:\.\d{1,50})?$/.test(x) && Number.isFinite(Number(x)) && Math.abs(Number(x)) <= 1e18;
  const safe = k => typeof k === 'string' && /^data\/(provider-flow|capital-radar)-research\/(runs|histories)\/[a-f0-9]{64}\.json$/.test(k);
  const money = x => decimal(x) ? Number(x).toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 2}) : 'Unavailable';
  function view(p, n = '5', query = '', now = Date.now()) {
    if (!periods.includes(n)) throw Error('Choose 1, 5 or 21 reporting observations');
    const valid = p && p.contract === 'khalid-provider-flow-evidence.v1' && p.informational_only === true && p.independent_investment_votes === 0 && ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'].every(k => p[k] === false);
    const fresh = valid && Date.parse(p.source_generated_at) <= Date.parse(p.generated_at) && Date.parse(p.generated_at) <= now && now < Date.parse(p.source_valid_until);
    if (!fresh || !['available','partial'].includes(p.status) || !Array.isArray(p.funds) || p.funds.length > 500) return {status: 'Unavailable', reasons: valid ? (fresh ? (p.reasons || []).join(', ') : 'Source expired, missing or future-dated') : 'Native evidence is not present in this engine packet', funds: [], baskets: []};
    const q = String(query).trim().toUpperCase();
    const funds = p.funds.filter(r => r && typeof r.ticker === 'string' && (r.ticker + ' ' + r.category + ' ' + r.subcategory).toUpperCase().includes(q)).map(r => {
      const w = r.windows?.[n] || {}, current = Date.parse(r.source_acquired_at) <= now && now < Date.parse(r.source_valid_until);
      const available = current && w.status === 'available' && w.available_observations === Number(n) && decimal(w.flow_usd_decimal);
      return {ticker: r.ticker, tag: (r.category || 'unverified') + ' / ' + (r.subcategory || 'unverified'),
        effective: r.effective_date, processed: r.processed_date, acquired: r.source_acquired_at,
        window: [w.start_date, w.end_date].join(' → '), coverage: (w.available_observations ?? 0) + '/' + n,
        amount: available ? money(w.flow_usd_decimal) : 'Unavailable',
        state: available ? 'Reported fund flow' : (!current ? 'Source expired or unavailable' : (w.reasons || ['Unavailable']).join(', ')), history: safe(r.history_key) ? r.history_key : null};
    });
    const baskets = (Array.isArray(p.baskets) ? p.baskets : []).slice(0,100).filter(r => String(r.name).toUpperCase().includes(q)).map(r => {
      const w = r.windows?.[n] || {}, complete = w.status === 'available', partial = w.status === 'partial';
      return {name: r.name, coverage: (w.included_count ?? 0) + '/' + (w.required_count ?? 0),
        window: [w.start_date, w.end_date].join(' → '),
        amount: complete ? money(w.flow_usd_decimal) : partial ? money(w.observed_subset_flow_usd_decimal) + ' (partial subset)' : 'Unavailable',
        state: complete ? 'Complete configured basket' : (w.reasons || ['Unavailable']).join(', '), excluded: (w.excluded || []).join(', ')};
    });
    return {status: p.status === 'partial' ? 'Partial coverage' : 'Available', reasons: (p.reasons || []).join(', '), funds, baskets};
  }
  function render(host, packet) {
    if (!host) return;
    if (host._providerTimer) root.clearInterval(host._providerTimer);
    host.replaceChildren();
    const doc = host.ownerDocument;
    const el = (tag,text) => {const x=doc.createElement(tag);if(text!=null)x.textContent=String(text);return x;};
    const status=el('p');status.setAttribute('role','status');host.append(status);
    host.append(el('p','Provider-reported ETF creations/redemptions. Informational evidence only: zero investment votes and no change to rankings, confidence, readiness or capital permissions. Configured baskets are not complete industries; ETF flows do not establish constituent stock purchases. Leveraged and inverse funds remain separate.'));
    const provenance=el('p');host.append(provenance);
    const controls=el('div');controls.className='k-provider-controls';
    const periodLabel=el('label','Reporting observations '), select=el('select');select.setAttribute('aria-label','Provider reporting observations');
    periods.forEach(n=>{const o=el('option',n+' observations');o.value=n;select.append(o);});select.value='5';periodLabel.append(select);
    const queryLabel=el('label','Find ETF, configured tag or basket '), query=el('input');query.type='search';query.setAttribute('aria-label','Find provider evidence');queryLabel.append(query);controls.append(periodLabel,queryLabel);host.append(controls);
    const body=el('div');host.append(body);
    function link(parent,text,path){const a=el('a',text);a.href='/'+path;parent.append(a);}
    function table(title,heads,rows){body.append(el('h4',title));const wrap=el('div');wrap.className='k-provider-scroll';wrap.tabIndex=0;wrap.setAttribute('role','region');wrap.setAttribute('aria-label',title);const t=el('table'),head=el('thead'),hr=el('tr');heads.forEach(h=>{const th=el('th',h);th.scope='col';hr.append(th);});head.append(hr);t.append(head);const tb=el('tbody');rows.forEach(row=>{const tr=el('tr');row.forEach((v,i)=>{const cell=el(i?'td':'th');if(!i)cell.scope='row';if(v&&typeof v==='object'&&v.link)link(cell,'Retained history',v.link);else cell.textContent=v==null?'Unavailable':String(v);tr.append(cell);});tb.append(tr);});t.append(tb);wrap.append(t);body.append(wrap);}
    function update(){const v=view(packet,select.value,query.value);status.textContent=v.status+(v.reasons?' · '+v.reasons:'');body.replaceChildren();provenance.replaceChildren();
      provenance.append(el('span','Economic common end: '+(packet?.reference_end_date || 'Unavailable')+' · Provider compilation: '+(packet?.source_generated_at || 'Unavailable')+' · Radar compilation: '+(packet?.generated_at || 'Unavailable')+' · Source expires: '+(packet?.source_valid_until || 'Unavailable')+'. '));
      link(provenance,'Canonical provider research','data/provider-fund-flow-research.json');provenance.append(el('span',' · '));link(provenance,'Radar projection','data/capital-flow-radar.json');
      if(safe(packet?.replay_key)){provenance.append(el('span',' · '));link(provenance,'Retained run reference',packet.replay_key);}
      if(!v.funds.length&&!v.baskets.length){body.append(el('p','No matching available evidence. Missing observations are not zero.'));return;}
      table('Fund observations', ['ETF','Configured tag (unverified)','Effective / processed','Acquired UTC','Reporting range','Count','Flow USD','State','Provenance'],v.funds.map(r=>[r.ticker,r.tag,r.effective+' / '+r.processed,r.acquired,r.window,r.coverage,r.amount,r.state,r.history?{link:r.history}:'Unavailable']));
      table('Configured ETF baskets — overlapping groups cannot be added',['Basket','Funds included / required','Reporting range','Flow USD','State','Excluded funds'],v.baskets.map(r=>[r.name,r.coverage,r.window,r.amount,r.state,r.excluded||'None']));
    }
    select.addEventListener('change',update);query.addEventListener('input',update);update();host._providerTimer=root.setInterval(update,60000);
  }
  const api={view,render};if(typeof module==='object'&&module.exports)module.exports=api;root.JHKhalidProviderFlow=api;
})(typeof window==='object'?window:globalThis);
