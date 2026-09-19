(function(root, factory) {
  const api = factory(typeof module === 'object' && module.exports ? require('./jh-tic-research.js') : root.TicResearch);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.FlowResearch = api;
})(typeof window !== 'undefined' ? window : this, function(base) {
  'use strict';
  const {esc, num, fmt, signed} = base;
  const CONTRACT = 'global-flow-research.v1', PREFIX = 'data/flow-desk-research/';
  const KIND = {issuer_classification:'Reported issuer classifications', configured_basket:'Configured research baskets', configured_sector_fund:'Configured sector funds', configured_geography:'Configured geography baskets'};
  function table(headers, rows) {
    return `<div class="table-scroll"><table><thead><tr>${headers.map(v=>`<th>${esc(v)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>`<tr>${r.map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  }
  function million(value) { return num(value) === null ? '—' : signed(Number(value) / 1e6); }
  function boundary(p) {
    if (p?.contract !== CONTRACT || p.call !== null || p.calls_eligible !== false || p.sizing_eligible !== false || p.execution_eligible !== false || p.additional_independent_votes !== 0 || !p.groups || !p.funds) throw Error('Flow research permission contract differs');
    return p;
  }
  function status(p, at=Date.now(), pinned=false) {
    if (pinned) return 'historical snapshot';
    const source=Date.parse(p.source_generated_at), generated=Date.parse(p.generated_at), observed=Date.parse(p.source_reference_date+'T00:00:00Z');
    if (![source,generated,observed].every(Number.isFinite) || [source,generated,observed].some(v=>v>at+300000) || at-source>48*3600000 || at-observed>5*86400000 || p.quality.status==='stale_source') return 'source check expired';
    return 'partial fund coverage';
  }
  function summary(p, at, pinned) {
    boundary(p); const q=p.quality;
    const cards=[['Source configured funds',q.configured_funds,'Includes uncovered source funds'],['Source histories',q.source_histories,'Immutable issuer evidence'],['Aligned estimates',q.aligned_funds,'Same five-observation period'],['Issuer classifications',q.issuer_classified_funds,'Separate from configured baskets']];
    return {state:status(p,at,pinned),cards:cards.map(([name,v,note])=>`<article class="metric"><div>${esc(name)}</div><strong>${fmt(v,0)}</strong><span>${esc(note)}</span></article>`).join('')};
  }
  function detail(p,id) {
    const g=p.groups[id]; if(!g) throw Error('Group outside retained research universe');
    const rows=g.configured_members.map(t=>{
      const v=p.funds[t], tax=v?.issuer_classification, related=p.overlapping_memberships[t]||[];
      const link=v?.history?`<a href="${esc(v.source_snapshot)}">${esc(t)}</a>`:esc(t);
      return [link,esc(v?.identity?.fund_name || v?.identity?.issuer || 'No reviewed native history'),
        million(v?.comparison?.net_flow_5d_usd),esc(tax?.status==='reported_issuer_classification'?tax.asset_class+' / '+tax.sub_asset_class:'Unreviewed classification'),
        esc(v?.observation_date || 'unavailable'),fmt(related.length,0)];
    });
    return `<h2>${esc(g.label)}</h2><p>${esc(KIND[g.kind])} · ${esc(g.scope)}</p><p><strong>${million(g.net_flow_5d_usd)} USD million</strong> · ${g.coverage_count} / ${g.configured_count} funds · ${esc(g.period?.start_date||'unavailable')} → ${esc(g.period?.end_date||'unavailable')}.</p>`+
      table(['Fund / evidence','Identity','Aligned estimate · USD m','Issuer classification','Latest native date','Group memberships'],rows)+
      '<p class="dim">One fund can belong to several displayed groups. Adding those groups would count it repeatedly. Geography baskets describe fund mandates; they do not measure national capital flows.</p>';
  }
  function history(doc,page=0) {
    if(doc?.contract!=='flow-desk-history.v1'||!Array.isArray(doc.rows))throw Error('Flow group history contract differs');
    const rows=[...doc.rows].reverse(), selected=rows.slice(page*24,page*24+24);
    return {count:rows.length,pages:Math.ceil(rows.length/24),html:table(['Issuer date','1-observation estimate · USD m','5-observation estimate · USD m','5-observation start','Covered funds in 5-observation subtotal'],selected.map(r=>[esc(r.date),million(r.one_observation_decimal),million(r.five_observation_decimal),esc(r.five_start_date||'unavailable'),esc(r.five_covered_members.join(', ')||'none')]))};
  }
  function chart(doc,window='five',range=260,width=1040) {
    const rows=range?doc.rows.slice(-range):doc.rows, field=window==='one'?'one_observation_decimal':'five_observation_decimal', members=window==='one'?'one_covered_members':'five_covered_members';
    const valid=rows.filter(r=>num(r[field])!==null); if(!valid.length)return '<p>No covered-fund estimates in this selection. Missing values are not zero.</p>';
    const low=Math.min(0,...valid.map(r=>Number(r[field])/1e6)), high=Math.max(0,...valid.map(r=>Number(r[field])/1e6)), span=high-low||1;
    const W=Math.max(320,Math.min(1040,Number(width)||1040)), H=260,L=78,R=14,T=20,B=35, first=Date.parse(rows[0].date),last=Date.parse(rows.at(-1).date);
    const x=d=>L+(Date.parse(d)-first)/(last-first||1)*(W-L-R), y=v=>T+(high-v)/span*(H-T-B); let paths=[],path='',previous=null;
    for(const r of rows){const coverage=r[members].join('|');if(num(r[field])===null||previous!==null&&coverage!==previous){if(path)paths.push(path);path='';}if(num(r[field])!==null)path+=(path?' L':'M')+x(r.date).toFixed(2)+' '+y(Number(r[field])/1e6).toFixed(2);previous=coverage;}if(path)paths.push(path);
    return `<svg role="img" aria-label="Covered-fund NAV-valued share changes in USD millions; changing membership breaks the line" viewBox="0 0 ${W} ${H}"><line x1="${L}" x2="${W-R}" y1="${y(0)}" y2="${y(0)}" stroke="var(--bd)"/>${[low,high].map(v=>`<text x="${L-8}" y="${y(v)+4}" text-anchor="end">${esc(fmt(v,1))}</text>`).join('')}${paths.map(d=>`<path d="${d}" stroke="var(--cyan)" stroke-width="2" fill="none"/>`).join('')}<text x="${L}" y="${H-5}">${esc(rows[0].date)}</text><text x="${W-R}" y="${H-5}" text-anchor="end">${esc(rows.at(-1).date)}</text></svg>`+
      '<p class="dim">USD millions · actual date axis. Missing values or a change in covered members breaks the line. The latest chart date may be later than the common comparison period. Current fund membership and historical issuer vintage; not a point-in-time strategy backtest.</p>';
  }
  function scenario(positions,days) {
    if(!Array.isArray(positions)||!positions.length||positions.length>12||!Number.isInteger(days)||days<1||days>3650)throw Error('Enter 1–12 long positions and a horizon of 1–3650 whole days.');
    const rows=positions.map(p=>{
      if(typeof p.label!=='string'||!p.label.trim()||p.label.trim().length>80)throw Error('Name each scenario position (up to 80 characters).');
      for(const key of ['exposure','pricePct','fxPct','income','costs'])if(typeof p[key]!=='number'||!Number.isFinite(p[key]))throw Error('Enter every assumption, including explicit zeros.');
      if(p.exposure<=0||p.exposure>1e12||p.pricePct< -100||p.pricePct>1000||p.fxPct< -100||p.fxPct>1000||p.income<0||p.income>1e12||p.costs<0||p.costs>1e12)throw Error('Scenario inputs outside the displayed bounds.');
      const combined=(1+p.pricePct/100)*(1+p.fxPct/100)-1, price=p.exposure*combined;
      return {inputs:{...p,label:p.label.trim()},usd_price_return:combined,price_and_fx_pnl:price,net_pnl:price+p.income-p.costs};
    });
    const exposure=rows.reduce((s,r)=>s+r.inputs.exposure,0), net=rows.reduce((s,r)=>s+r.net_pnl,0);
    return {positions:rows,horizon_days:days,currency:'USD',initial_market_value:exposure,net_pnl:net,net_return:net/exposure,
      formula:'market_value_USD * ((1 + local_price_change_pct/100) * (1 + USD_per_local_currency_change_pct/100) - 1) + cash_income_USD - costs_USD',
      forecast:false,automatic_allocation:null,fund_flow_to_return_coefficient:null,probability:null,
      limitation:'Entered long-position scenario. Price changes exclude separately entered cash distributions. FX is USD per unit of local currency. No probability, tax, margin, execution or automatic leveraged-fund compounding model.'};
  }
  async function verified(fetcher,ref) {
    if(!ref||!new RegExp('^'+PREFIX+'(?:outputs|histories)/[a-f0-9]{64}\\.json$').test(ref.key)||!Number.isInteger(ref.bytes)||ref.bytes<1||ref.bytes>16*1024*1024||!/^[a-f0-9]{64}$/.test(ref.sha256))throw Error('Retained flow artifact identity differs');
    const response=await fetcher('/'+ref.key,{cache:'no-store'});if(!response.ok)throw Error('Retained flow research unavailable');
    const raw=await response.arrayBuffer(),sha=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',raw)),v=>v.toString(16).padStart(2,'0')).join('');
    if(raw.byteLength!==ref.bytes||sha!==ref.sha256)throw Error('Retained flow research hash differs');return JSON.parse(new TextDecoder().decode(raw));
  }
  return {CONTRACT,PREFIX,KIND,esc,num,fmt,signed,million,table,boundary,status,summary,detail,history,chart,scenario,verified};
});
