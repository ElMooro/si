/* JustHodl CDS desk renderer (shared by /cds.html and /intelligence/#cds).
   Data: data/cds-desk.json (packet), data/cds-desk-history.json (every name's daily series + free long records since 2006),
   /assets/cds-world.json (static Natural Earth 110m outlines keyed by ISO3).  Descriptive only: no calls, no sizing. */
(function(root){
'use strict';
const S3='https://justhodl-dashboard-live.s3.amazonaws.com/';
const KEYS={packet:'data/cds-desk.json',history:'data/cds-desk-history.json',world:'/assets/cds-world.json'};
const esc=v=>String(v==null?'':v).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const fin=v=>typeof v==='number'&&Number.isFinite(v);
const bp=(v,dp=0)=>fin(v)?v.toFixed(dp):'—';
const sgn=(v,dp=1)=>fin(v)?(v>0?'+':'')+v.toFixed(dp):'—';
const chg=v=>fin(v)?`<span class="${v>0?'t-neg':v<0?'t-pos':'t-mute'}">${v>0?'+':''}${v.toFixed(1)}</span>`:'<span class="t-mute">—</span>';
const pct=v=>{if(!fin(v))return '—';const n=Math.round(v),t=n%10,h=n%100;return n+((h>=11&&h<=13)?'th':t===1?'st':t===2?'nd':t===3?'rd':'th');};
const z=v=>fin(v)?`<span class="${v>=2?'t-neg':v<=-2?'t-pos':''}">${v>0?'+':''}${v.toFixed(1)}</span>`:'—';
const sd=v=>fin(v)?` data-sort="${v}"`:' data-sort=""';
const spark=pts=>{if(!Array.isArray(pts)||pts.length<2)return '';const ys=pts.map(p=>p[1]).filter(fin);if(ys.length<2)return '';const lo=Math.min(...ys),hi=Math.max(...ys),w=90,h=22,n=ys.length;const path=ys.map((y,i)=>`${i?'L':'M'}${(i/(n-1)*w).toFixed(1)},${(hi===lo?h/2:h-2-(y-lo)/(hi-lo)*(h-4)).toFixed(1)}`).join(' ');return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" aria-hidden="true"><path d="${path}" fill="none" stroke="${ys[n-1]>ys[0]?'var(--neg)':'var(--pos)'}" stroke-width="1.2"/></svg>`;};
const sparkChg=pts=>{if(!Array.isArray(pts)||pts.length<2)return null;const ys=pts.map(p=>p[1]).filter(fin);return ys.length<2?null:ys[ys.length-1]-ys[0];};
const SRC={quoted:'quoted print',multi_coupon:'two-coupon print',multi_coupon_trailing:'two-coupon (90d)',single_branch:'single feasible branch',trailing:'own level ≤7d',trailing_long:'own level ≤180d',comove:'index co-movement',curve_shape:'curve shape',index:'index quote'};
const GRP={sovereign:'Sov',us_corp:'US',global_corp:'Global'};
const LONG_ORDER=[['baa10y','Baa − 10Y (Moody\'s)'],['ofr_credit','OFR credit stress'],['ofr_em','OFR emerging markets'],['gz_spread','GZ credit spread'],['ebp','Excess bond premium'],['ofr_fsi','OFR FSI (total)'],['aaa10y','Aaa − 10Y'],['ofr_funding','OFR funding'],['ofr_vol','OFR volatility']];
const STATUS_LABEL={liquid:'liquid',thin:'thin',unpriced:'either / or',dormant:'dormant',not_in_tape:'not in tape'};
const STATUS_CLASS={liquid:'pos',thin:'info',unpriced:'warning',dormant:'mute',not_in_tape:'mute'};
const statusPill=s=>`<span class="pill ${STATUS_CLASS[s]||'mute'}" title="${esc(STATUS_HELP[s]||'')}">${esc(STATUS_LABEL[s]||s||'—')}</span>`;
const STATUS_HELP={liquid:'priced on >=12 of the last 20 file days',thin:'priced, but on fewer than 12 of the last 20 file days',unpriced:'prints seen in the last 30 days, but every one carries an upfront whose payer DTCC does not disclose; both feasible levels are shown',dormant:'no listable print in the last 30 days; last dated level shown',not_in_tape:'known sovereign CDS issuer with no public print since the tape began (2024-09)'};
// which free long record (since 2006) a name is compared against in the history modal: same bp unit for Baa-10Y, index units for OFR
function proxyFor(group,tier,key){
 if(group==='index'){if(/EM/.test(key))return 'ofr_em';if(/HY|XOVER/.test(key))return 'baa10y';if(/ITRAXX/.test(key))return 'ofr_credit';return 'baa10y';}
 if(group==='sovereign')return tier==='DM'?'ofr_credit':'ofr_em';
 if(group==='us_corp')return 'baa10y';
 return 'ofr_credit';
}
// time-axis SVG line chart.  pts=[[iso,value]]; opts {h,bars,color,label,markers:[{date,value,label}],hline:{value,label},series2:[[iso,v]],axis2:bool,color2,label2,band:[[iso,hi,lo]],xmin}
function lineChart(pts,opts){
 opts=opts||{};
 const W=920,H=opts.h||240,L=54,R=opts.axis2?58:16,T=18,B=30;
 const data=(pts||[]).filter(p=>Array.isArray(p)&&fin(p[1]));
 const s2=(opts.series2||[]).filter(p=>Array.isArray(p)&&fin(p[1]));
 const band=(opts.band||[]).filter(p=>Array.isArray(p)&&fin(p[1])&&fin(p[2]));
 if(data.length<2&&s2.length<2&&band.length<2)return '<div class="t-mute" style="font-family:var(--font-mono);font-size:11px">not enough points</div>';
 const allT=data.map(p=>Date.parse(p[0])).concat(s2.map(p=>Date.parse(p[0])),band.map(p=>Date.parse(p[0])));
 let t0=Math.min(...allT),t1=Math.max(...allT);if(opts.xmin)t0=Math.min(t0,Date.parse(opts.xmin));if(t1===t0)t1=t0+864e5;
 const ys=data.map(p=>p[1]).concat(opts.hline&&fin(opts.hline.value)?[opts.hline.value]:[]).concat((opts.markers||[]).map(m=>m.value).filter(fin)).concat(opts.axis2?[]:s2.map(p=>p[1])).concat(opts.axis2?[]:band.flatMap(p=>[p[1],p[2]]));
 let lo=ys.length?Math.min(...ys):0,hi=ys.length?Math.max(...ys):1;if(hi===lo){hi+=1;lo-=1;}
 const pad=(hi-lo)*0.08;lo=opts.bars?Math.min(0,lo):lo-pad;hi+=pad;
 const x=t=>L+(t-t0)/Math.max(1,t1-t0)*(W-L-R),y=v=>T+(hi-v)/(hi-lo)*(H-T-B);
 const fmt=v=>Math.abs(v)>=100?v.toFixed(0):Math.abs(v)>=10?v.toFixed(1):v.toFixed(2);
 let g=`<svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" style="display:block;font-family:var(--font-mono)" role="img" aria-label="${esc(opts.label||'time series')}">`;
 for(let i=0;i<=4;i++){const v=lo+(hi-lo)*i/4,yy=y(v);g+=`<line x1="${L}" x2="${W-R}" y1="${yy.toFixed(1)}" y2="${yy.toFixed(1)}" stroke="rgba(28,35,48,.9)" stroke-width="1"/><text x="${L-6}" y="${(yy+3).toFixed(1)}" text-anchor="end" font-size="9.5" fill="#5b6479">${fmt(v)}</text>`;}
 const y0=new Date(t0).getUTCFullYear(),y1=new Date(t1).getUTCFullYear(),step=(y1-y0)>12?2:1;
 if((y1-y0)>3){for(let yr=y0+1;yr<=y1;yr++){if((yr-y0)%step)continue;const xx=x(Date.UTC(yr,0,1));if(xx<L||xx>W-R)continue;g+=`<line x1="${xx.toFixed(1)}" x2="${xx.toFixed(1)}" y1="${T}" y2="${H-B}" stroke="rgba(28,35,48,.6)"/><text x="${xx.toFixed(1)}" y="${H-B+14}" text-anchor="middle" font-size="9.5" fill="#5b6479">${yr}</text>`;}}
 else{for(let t=new Date(t0);t.getTime()<=t1;t.setUTCMonth(t.getUTCMonth()+1,1)){const tt=Date.UTC(t.getUTCFullYear(),t.getUTCMonth(),1);if(tt<t0)continue;const xx=x(tt);g+=`<line x1="${xx.toFixed(1)}" x2="${xx.toFixed(1)}" y1="${T}" y2="${H-B}" stroke="rgba(28,35,48,.6)"/><text x="${xx.toFixed(1)}" y="${H-B+14}" text-anchor="middle" font-size="9" fill="#5b6479">${new Date(tt).toISOString().slice(2,7)}</text>`;}}
 const color=opts.color||'var(--info)';
 // second axis (right) for a series in another unit
 let y2=y;
 if(opts.axis2&&(s2.length||band.length)){const v2=s2.map(p=>p[1]).concat(band.flatMap(p=>[p[1],p[2]]));let lo2=Math.min(...v2),hi2=Math.max(...v2);if(hi2===lo2){hi2+=1;lo2-=1;}const p2=(hi2-lo2)*0.08;lo2-=p2;hi2+=p2;y2=v=>T+(hi2-v)/(hi2-lo2)*(H-T-B);for(let i=0;i<=4;i++){const v=lo2+(hi2-lo2)*i/4;g+=`<text x="${W-R+6}" y="${(y2(v)+3).toFixed(1)}" font-size="9.5" fill="${opts.color2||'#f5c451'}">${fmt(v)}</text>`;}if(opts.label2)g+=`<text x="${W-R+6}" y="${T-6}" font-size="9" fill="${opts.color2||'#f5c451'}">${esc(opts.label2)}</text>`;}
 if(opts.bars&&data.length){const bw=Math.max(1,(W-L-R)/data.length-1);data.forEach(p=>{const xx=x(Date.parse(p[0])),yy=y(p[1]);g+=`<rect x="${(xx-bw/2).toFixed(1)}" y="${yy.toFixed(1)}" width="${bw.toFixed(1)}" height="${(y(Math.max(lo,0))-yy).toFixed(1)}" fill="${color}" opacity=".7"><title>${p[0]}: ${fmt(p[1])}</title></rect>`;});}
 else if(data.length>1){const path=data.map((p,i)=>`${i?'L':'M'}${x(Date.parse(p[0])).toFixed(1)},${y(p[1]).toFixed(1)}`).join(' ');g+=`<path d="${path} L${x(Date.parse(data[data.length-1][0])).toFixed(1)},${y(lo).toFixed(1)} L${x(Date.parse(data[0][0])).toFixed(1)},${y(lo).toFixed(1)} Z" fill="${color}" opacity=".07"/><path d="${path}" fill="none" stroke="${color}" stroke-width="1.3"/>`;}
 if(band.length>1){const up=band.map((p,i)=>`${i?'L':'M'}${x(Date.parse(p[0])).toFixed(1)},${y2(p[1]).toFixed(1)}`).join(' '),dn=band.slice().reverse().map(p=>`L${x(Date.parse(p[0])).toFixed(1)},${y2(p[2]).toFixed(1)}`).join(' ');g+=`<path d="${up} ${dn} Z" fill="var(--orange)" opacity=".12"/><path d="${up}" fill="none" stroke="var(--orange)" stroke-width="1" stroke-dasharray="3 2"/><path d="${band.map((p,i)=>`${i?'L':'M'}${x(Date.parse(p[0])).toFixed(1)},${y2(p[2]).toFixed(1)}`).join(' ')}" fill="none" stroke="var(--orange)" stroke-width="1" stroke-dasharray="3 2"/>`;}
 if(s2.length>1){const p2=s2.map((p,i)=>`${i?'L':'M'}${x(Date.parse(p[0])).toFixed(1)},${y2(p[1]).toFixed(1)}`).join(' ');g+=`<path d="${p2}" fill="none" stroke="${opts.color2||'var(--gold)'}" stroke-width="${opts.axis2?1.6:1.1}"${opts.axis2?'':' stroke-dasharray="3 2"'}/>`;const l2=s2[s2.length-1];g+=`<circle cx="${x(Date.parse(l2[0])).toFixed(1)}" cy="${y2(l2[1]).toFixed(1)}" r="2.8" fill="${opts.color2||'var(--gold)'}"/>`;}
 (opts.markers||[]).forEach(m=>{if(!fin(m.value)||!m.date)return;const xx=x(Date.parse(m.date)),yy=y(m.value);const anchor=xx>W-150?'end':'start';g+=`<circle cx="${xx.toFixed(1)}" cy="${yy.toFixed(1)}" r="3" fill="var(--neg)"/><text x="${(xx+(anchor==='end'?-6:6)).toFixed(1)}" y="${(yy-5).toFixed(1)}" text-anchor="${anchor}" font-size="9.5" fill="#ff3d5a">${esc(m.label)} ${fmt(m.value)}</text>`;});
 if(opts.hline&&fin(opts.hline.value)){const yy=y(opts.hline.value);g+=`<line x1="${L}" x2="${W-R}" y1="${yy.toFixed(1)}" y2="${yy.toFixed(1)}" stroke="var(--gold)" stroke-width="1" stroke-dasharray="5 4"/><text x="${W-R}" y="${(yy-4).toFixed(1)}" text-anchor="end" font-size="10" fill="#f5c451">${esc(opts.hline.label)}</text>`;}
 if(data.length>1&&!opts.bars){const last=data[data.length-1];g+=`<circle cx="${x(Date.parse(last[0])).toFixed(1)}" cy="${y(last[1]).toFixed(1)}" r="2.8" fill="${color}"/>`;}
 return g+'</svg>';
}
// ---- table tools: every <th> sorts asc/desc, every single-table panel gets a CSV button (idempotent) ----
function enhanceTables(rootEl){
 (rootEl||document).querySelectorAll('table').forEach(tbl=>{
  if(!tbl.tBodies.length||!tbl.tHead)return;
  tbl.tHead.querySelectorAll('th').forEach((th,i)=>{if(th.classList.contains('sortable'))return;th.classList.add('sortable');th.dataset.col=i;th.title=(th.title?th.title+' · ':'')+'click to sort (asc/desc)';th.insertAdjacentHTML('beforeend','<span class="sort-ind">⇅</span>');});
  const panel=tbl.closest('.panel'),h3=panel&&panel.querySelector(':scope > h3');
  if(h3&&!h3.querySelector('.csv-btn[data-csv]')&&panel.querySelectorAll('table').length===1){const b=document.createElement('button');b.type='button';b.className='csv-btn';b.dataset.csv='1';b.textContent='⇩ csv';b.title='download this table as CSV (current sort order)';(h3.querySelector('.tag')||h3).appendChild(b);}
 });
}
function sortTableBy(th){
 const tbl=th.closest('table'),col=+th.dataset.col,body=tbl.tBodies[0];if(!body)return;
 const dir=th.dataset.dir==='desc'?'asc':'desc';
 tbl.tHead.querySelectorAll('th').forEach(x=>{x.classList.remove('sorted');if(x!==th)delete x.dataset.dir;const ind=x.querySelector('.sort-ind');if(ind)ind.textContent='⇅';});
 th.dataset.dir=dir;th.classList.add('sorted');const ind=th.querySelector('.sort-ind');if(ind)ind.textContent=dir==='desc'?'▼':'▲';
 const rows=[...body.rows].filter(r=>r.cells.length>1);
 const parse=td=>{if(!td)return {n:NaN,s:''};const raw=(td.dataset.sort!=null?td.dataset.sort:td.textContent).trim();if(raw===''||raw==='—'||raw==='-')return {n:NaN,s:''};const m=raw.replace(/,/g,'').match(/^[+\-−]?\d+(\.\d+)?/);if(m&&!/^\d{4}-\d{2}-\d{2}/.test(raw))return {n:parseFloat(m[0].replace('−','-')),s:raw.toLowerCase()};return {n:NaN,s:raw.toLowerCase()};};
 const vals=rows.map(r=>parse(r.cells[col]));
 const numeric=vals.filter(v=>!Number.isNaN(v.n)).length>=Math.max(1,vals.filter(v=>v.s!=='').length/2);
 const idx=rows.map((_,i)=>i);
 idx.sort((a,b)=>{const va=vals[a],vb=vals[b];if(numeric){const na=Number.isNaN(va.n),nb=Number.isNaN(vb.n);if(na&&nb)return 0;if(na)return 1;if(nb)return -1;return dir==='desc'?vb.n-va.n:va.n-vb.n;}if(va.s===''&&vb.s==='')return 0;if(va.s==='')return 1;if(vb.s==='')return -1;return dir==='desc'?vb.s.localeCompare(va.s):va.s.localeCompare(vb.s);});
 idx.forEach(i=>body.appendChild(rows[i]));
}
function tableToCsv(tbl){
 const cell=td=>{const t=(td.dataset.sort!=null?td.dataset.sort:td.textContent).replace(/\s+/g,' ').trim();return /[",\n]/.test(t)?'"'+t.replace(/"/g,'""')+'"':t;};
 const head=[...tbl.tHead.rows[0].cells].map(th=>cell({textContent:th.textContent.replace(/[⇅▼▲]/g,''),dataset:{}}));
 const lines=[head.join(',')];
 [...tbl.tBodies[0].rows].forEach(r=>{if(r.cells.length>1)lines.push([...r.cells].map(cell).join(','));});
 return lines.join('\n');
}
// ---- desk skeleton ----
function shell(){
 return `<div class="kpi-grid" id="cdsdesk-kpis"><div class="loading spin">loading cds-desk</div></div>
<div class="panel"><h3>Index strip <span class="tag">on-the-run 5Y · median of the day's quoted prints · click a name for history vs 2006</span></h3><div id="cdsdesk-indices"></div></div>
<div class="panel"><h3>Where today sits vs 2006 → <span class="tag" id="cdsdesk-long-tag">weekly · free government records that still carry 2008 · loading…</span></h3><div id="cdsdesk-long-ctl" style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px"></div><div id="cdsdesk-long"></div><div id="cdsdesk-long-peaks" style="margin-top:10px"></div></div>
<div class="panel"><h3>World sovereign CDS map <span class="tag" id="cdsdesk-map-tag">5Y · colour = level · hatched = either/or · grey = dormant or not in tape · click a country for history</span></h3><div id="cdsdesk-map"></div></div>
<div class="panel"><h3>Sovereign 5Y CDS <span class="tag" id="cdsdesk-sov-tag">whole known universe · USD senior · click a chip to filter, a header to sort, a name for history</span></h3><div id="cdsdesk-sov-chips"></div><div id="cdsdesk-sov"></div></div>
<div class="panel"><h3>U.S. corporate 5Y CDS <span class="tag" id="cdsdesk-us-tag">every USD name in the tape · click a sector to filter</span></h3><div id="cdsdesk-us-chips"></div><div id="cdsdesk-us"></div></div>
<div class="panel"><h3>Global (non-U.S.) corporate 5Y CDS <span class="tag" id="cdsdesk-gl-tag">EUR / JPY / USD names · click a sector to filter</span></h3><div id="cdsdesk-gl-chips"></div><div id="cdsdesk-gl"></div></div>
<div class="panel"><h3>Active but unpriced — why <span class="tag" id="cdsdesk-unpriced-tag">prints seen, upfront sign unresolved or last level older than 10 days</span></h3><div id="cdsdesk-unpriced"></div></div>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:14px">
 <div class="panel" style="margin-bottom:0"><h3>Biggest 1-day moves <span class="tag">bp vs prior priced day · under 1000bp</span></h3><div id="cdsdesk-movers"></div></div>
 <div class="panel" style="margin-bottom:0"><h3>At 1-year wides <span class="tag">≥95th pct of own priced days · ≥60 days</span></h3><div id="cdsdesk-wides"></div></div>
 <div class="panel" style="margin-bottom:0"><h3>At 1-year tights <span class="tag">≤5th pct of own priced days</span></h3><div id="cdsdesk-tights"></div></div>
 <div class="panel" style="margin-bottom:0"><h3>Inverted curves <span class="tag" id="cdsdesk-term-tag">1Y above 5Y on the same day's sign-fixed prints</span></h3><div id="cdsdesk-term"></div></div>
</div>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:14px;margin-top:14px">
 <div class="panel" style="margin-bottom:0"><h3>CDS − bond basis <span class="tag">CDX vs ICE BofA cash OAS · bp · 1y</span></h3><div id="cdsdesk-basis"></div></div>
 <div class="panel" style="margin-bottom:0"><h3>Tape activity <span class="tag">single-name prints per file day · 120d</span></h3><div id="cdsdesk-activity"></div></div>
</div>
<div class="interp" id="cdsdesk-interp" style="margin-top:14px"></div>
<div id="cdsdesk-modal" style="display:none;position:fixed;inset:0;background:rgba(5,8,12,.78);z-index:50;align-items:center;justify-content:center;padding:20px" role="dialog" aria-modal="true"><div class="panel" style="max-width:1000px;width:100%;margin:0;max-height:92vh;overflow:auto"><h3><span id="cdsdesk-modal-title">history</span><span class="tag"><button type="button" id="cdsdesk-modal-close" class="csv-btn">close ✕</button></span></h3><div id="cdsdesk-modal-body"></div></div></div>`;
}
// ---- universe tables ----
const nameCell=(r,label)=>`<td class="t-info cds-clickable" data-key="${esc(r.key)}" data-name="${esc(label||r.name)}" data-group="${esc(r.group||'')}" data-tier="${esc(r.tier||'')}" title="click: history since 2006">${esc(label||r.name)}</td>`;
const level=r=>fin(r.points_upfront)?`${bp(r.points_upfront,1)} pts <span class="t-mute">(${bp(r.spread_bp)}bp eq.)</span>`:bp(r.spread_bp,1);
const cands=u=>(u.candidates||[]).length?u.candidates.map(c=>`<span title="${c.days} day(s) with two feasible branches at the ${c.coupon_bp}bp coupon">${bp(c.below_coupon_bp)} <span class="t-mute">or</span> ${bp(c.above_coupon_bp)} <span class="t-mute">@${c.coupon_bp}c</span></span>`).join('<br>'):'<span class="t-mute">no derivable level</span>';
const evidence=r=>{if(r.status==='unpriced')return '<span class="t-mute">sign undisclosed</span>';if(r.status==='dormant'||r.status==='not_in_tape')return '<span class="t-mute">—</span>';const s=SRC[r.anchor_source]||r.anchor_source||'—';const ev=r.sign_evidence;return `<span class="pill ${ev==='firm'?'pos':ev==='inferred'?'warning':'mute'}" title="spread basis: ${esc(r.spread_basis||'')}">${esc(s)}${ev==='inferred'?' · inferred':''}</span>`;};
// merge priced rows, unpriced and dormant records (and, for sovereigns, the known-universe list) into one row set with a shared shape
function universeRows(group,g){
 const byKey=new Map();
 (g.rows||[]).forEach(r=>byKey.set(r.key,{...r,group}));
 (g.unpriced||[]).forEach(u=>{if(!byKey.has(u.key))byKey.set(u.key,{...u,group,status:'unpriced'});});
 (g.dormant||[]).forEach(u=>{if(!byKey.has(u.key))byKey.set(u.key,{...u,group,status:'dormant'});});
 if(group==='sovereign'){
  (g.universe||[]).forEach(e=>{const r=byKey.get(e.key);if(r){r.name=e.name||r.name;r.iso3=e.iso3;r.region=e.region||r.region;r.tier=e.tier||r.tier;}else{const k=e.key||('NA:'+e.iso3);byKey.set(k,{...e,key:k,group,status:e.status||'not_in_tape'});}});
 }
 return [...byKey.values()];
}
function chipRow(label,field,items,current,fmtItem){
 return `<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:8px;align-items:center"><span class="t-mute" style="font-family:var(--font-mono);font-size:10px;text-transform:uppercase;letter-spacing:.5px;min-width:52px">${esc(label)}</span><button type="button" class="seg-btn cds-chip${current==='all'?' active':''}" data-filter="${field}" data-value="all">all</button>${items.map(it=>`<button type="button" class="seg-btn cds-chip${current===it.value?' active':''}" data-filter="${field}" data-value="${esc(it.value)}" title="${esc(it.title||'')}">${fmtItem(it)}</button>`).join('')}</div>`;
}
function universeTable(rows,cols,emptyMsg){
 const head=`<tr><th>Name</th>${cols.map(c=>`<th>${c}</th>`).join('')}<th>Status</th><th title="priced level, or the two levels an unsigned upfront could mean">5Y (bp)</th><th>1d</th><th>1w</th><th>1m</th><th>3m</th><th>1y pct</th><th>1y range</th><th>z 90d</th><th title="5Y minus 1Y, same-day sign-fixed prints">5Y−1Y</th><th>Prints 30d</th><th>Last print</th><th title="how the upfront sign was fixed">Sign evidence</th><th>120d</th></tr>`;
 const body=rows.map(r=>{
  const priced=r.status==='liquid'||r.status==='thin';
  const lvl=priced?`<b>${level(r)}</b>`:r.status==='unpriced'?`${cands(r)}${fin(r.last_spread_bp)?`<div class="t-mute" style="font-size:10px">last fixed ${bp(r.last_spread_bp,1)} · ${esc(r.last_priced_date||'')}</div>`:''}`:r.status==='dormant'?`<span class="t-mute">last ${bp(r.last_spread_bp,1)} · ${esc(r.last_priced_date||'')}</span>`:'<span class="t-mute">no public print since 2024-09</span>';
  const lvlSort=priced?r.spread_bp:(r.candidates&&r.candidates.length?r.candidates[0].above_coupon_bp:r.last_spread_bp);
  const lastCell=r.status==='not_in_tape'?'<td class="t-mute" data-sort="">—</td>':`<td class="t-mute" data-sort="${esc(r.last_date||'')}">${esc((r.last_date||'').slice(5))}${priced?` · ${r.n_last??0}p${r.ambiguous_last?` <span title="prints with unresolved sign">(${r.ambiguous_last}?)</span>`:''}`:''}</td>`;
  return `<tr data-status="${esc(r.status)}" title="${esc(r.read||r.why||'')}">${nameCell(r)}${cols.map(c=>`<td class="t-mute">${esc(r[c==='Region'?'region':c==='Tier'?'tier':'sector']||'')}</td>`).join('')}<td data-sort="${esc(r.status)}">${statusPill(r.status)}</td><td${sd(lvlSort)}>${lvl}</td><td${sd(r.chg_1d_bp)}>${chg(r.chg_1d_bp)}</td><td${sd(r.chg_1w_bp)}>${chg(r.chg_1w_bp)}</td><td${sd(r.chg_1m_bp)}>${chg(r.chg_1m_bp)}</td><td${sd(r.chg_3m_bp)}>${chg(r.chg_3m_bp)}</td><td${sd(r.pct_rank_1y)}>${pct(r.pct_rank_1y)}</td><td${sd(r.hi_1y_bp)} class="t-mute">${fin(r.hi_1y_bp)?`${bp(r.lo_1y_bp)}–${bp(r.hi_1y_bp)}`:'—'}</td><td${sd(r.z_90d)}>${z(r.z_90d)}</td><td${sd(r.slope_1y5y_bp)}>${fin(r.slope_1y5y_bp)?`<span class="${r.slope_1y5y_bp<0?'t-neg':''}">${sgn(r.slope_1y5y_bp,0)}</span>`:'<span class="t-mute">—</span>'}</td><td${sd(r.trades_30d)}>${r.trades_30d??'—'}</td>${lastCell}<td data-sort="${esc(r.anchor_source||r.status)}">${evidence(r)}</td><td${sd(sparkChg(r.spark))}>${spark(r.spark)}</td></tr>`;
 }).join('');
 return `<div style="overflow-x:auto;max-height:640px;overflow-y:auto"><table style="white-space:nowrap"><thead>${head}</thead><tbody>${body||`<tr><td colspan="${cols.length+16}" class="t-mute">${esc(emptyMsg||'nothing matches this filter')}</td></tr>`}</tbody></table></div>`;
}
const STATUS_ORDER={liquid:0,thin:1,unpriced:2,dormant:3,not_in_tape:4};
const sortUniverse=rows=>rows.sort((a,b)=>(STATUS_ORDER[a.status]??9)-(STATUS_ORDER[b.status]??9)||((b.spread_bp??-1)-(a.spread_bp??-1))||((b.trades_30d??0)-(a.trades_30d??0))||String(a.name).localeCompare(String(b.name)));
function countBy(rows,field){const m=new Map();rows.forEach(r=>{const k=r[field]||'Other';const c=m.get(k)||{value:k,n:0,priced:0,unpriced:0,med:[]};c.n++;if(r.status==='liquid'||r.status==='thin'){c.priced++;if(fin(r.spread_bp))c.med.push(r.spread_bp);}else if(r.status==='unpriced')c.unpriced++;m.set(k,c);});return [...m.values()].map(c=>{c.med.sort((a,b)=>a-b);c.median=c.med.length?c.med[Math.floor(c.med.length/2)]:null;return c;}).sort((a,b)=>b.n-a.n);}
const chipText=c=>`${esc(c.value)} <span class="t-mute">· ${c.n}${c.priced?` · ${c.priced} priced`:''}${fin(c.median)?` · med ${bp(c.median)}bp`:''}</span>`;
const STATUS_ITEMS=rows=>['liquid','thin','unpriced','dormant','not_in_tape'].map(s=>({value:s,n:rows.filter(r=>r.status===s).length})).filter(s=>s.n).map(s=>({value:s.value,n:s.n,title:STATUS_HELP[s.value]}));
// ---- world map ----
function colorFor(v){if(!fin(v))return null;const l=Math.log(Math.max(5,Math.min(5000,v)));const stops=[[Math.log(10),[34,211,238]],[Math.log(60),[0,230,118]],[Math.log(150),[251,191,36]],[Math.log(400),[251,146,60]],[Math.log(1500),[255,61,90]]];if(l<=stops[0][0])return `rgb(${stops[0][1]})`;for(let i=1;i<stops.length;i++){if(l<=stops[i][0]){const t=(l-stops[i-1][0])/(stops[i][0]-stops[i-1][0]),a=stops[i-1][1],b=stops[i][1];return `rgb(${a.map((c,j)=>Math.round(c+(b[j]-c)*t)).join(',')})`;}}return `rgb(${stops[stops.length-1][1]})`;}
function worldMap(world,rows){
 if(!world||!world.paths)return '<div class="t-mute" style="font-family:var(--font-mono);font-size:11px">map outlines not loaded</div>';
 const byIso=new Map();rows.forEach(r=>{if(r.iso3)byIso.set(r.iso3,r);});
 const W=+world.w||960,H=+world.h||415;
 let g=`<svg viewBox="0 0 ${W} ${H}" width="100%" style="display:block;max-height:520px" role="img" aria-label="world sovereign CDS map"><defs><pattern id="cds-hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="6" height="6" fill="#2a3548"/><line x1="0" y1="0" x2="0" y2="6" stroke="#fb923c" stroke-width="2"/></pattern></defs>`;
 for(const [iso,d] of Object.entries(world.paths)){
  const r=byIso.get(iso);let fill='#141a24',cls='',title=esc((world.names||{})[iso]||iso)+': not a known sovereign CDS issuer';
  if(r){const priced=r.status==='liquid'||r.status==='thin';if(priced){fill=colorFor(r.spread_bp);title=`${esc(r.name)}: ${bp(r.spread_bp,1)}bp (${esc(r.last_date||'')}, ${esc(r.status)})${fin(r.chg_1d_bp)?`, 1d ${sgn(r.chg_1d_bp)}`:''}`;}
   else if(r.status==='unpriced'){fill='url(#cds-hatch)';title=`${esc(r.name)}: either/or — ${(r.candidates||[]).map(c=>`${bp(c.below_coupon_bp)} or ${bp(c.above_coupon_bp)} @${c.coupon_bp}c`).join('; ')||'no derivable level'}${fin(r.last_spread_bp)?` · last fixed ${bp(r.last_spread_bp,1)} on ${esc(r.last_priced_date)}`:''}`;}
   else if(r.status==='dormant'){fill='#3a4356';title=`${esc(r.name)}: dormant · last ${bp(r.last_spread_bp,1)}bp on ${esc(r.last_priced_date||'')}`;}
   else{fill='#232b3a';title=`${esc(r.name)}: known issuer, no public print since 2024-09`;}
   cls=` class="cds-clickable cds-map-country" data-key="${esc(r.key)}" data-name="${esc(r.name)}" data-group="sovereign" data-tier="${esc(r.tier||'')}"`;}
  g+=`<path d="${d}" fill="${fill}" stroke="#0a0e14" stroke-width=".6"${cls}><title>${title}</title></path>`;
 }
 // labels for priced sovereigns
 rows.forEach(r=>{const c=world.centroids&&world.centroids[r.iso3];if(!c||!(r.status==='liquid'||r.status==='thin'))return;g+=`<text x="${c[0]}" y="${c[1]+3}" text-anchor="middle" font-size="8.5" font-family="var(--font-mono)" fill="#e6ecf3" stroke="#0a0e14" stroke-width="2" paint-order="stroke" pointer-events="none">${bp(r.spread_bp)}</text>`;});
 g+='</svg>';
 const legend=[[10,'10'],[30,'30'],[60,'60'],[100,'100'],[150,'150'],[250,'250'],[400,'400'],[800,'800'],[1500,'1500+']].map(([v,l])=>`<span style="display:inline-flex;align-items:center;gap:4px;font-family:var(--font-mono);font-size:10px;color:var(--text-dim)"><i style="display:inline-block;width:14px;height:10px;background:${colorFor(v)};border-radius:2px"></i>${l}</span>`).join('');
 return g+`<div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:8px;align-items:center">${legend}<span style="display:inline-flex;align-items:center;gap:4px;font-family:var(--font-mono);font-size:10px;color:var(--text-dim)"><i style="display:inline-block;width:14px;height:10px;background:repeating-linear-gradient(45deg,#2a3548 0 3px,#fb923c 3px 5px);border-radius:2px"></i>either / or (sign undisclosed)</span><span style="display:inline-flex;align-items:center;gap:4px;font-family:var(--font-mono);font-size:10px;color:var(--text-dim)"><i style="display:inline-block;width:14px;height:10px;background:#3a4356;border-radius:2px"></i>dormant</span><span style="display:inline-flex;align-items:center;gap:4px;font-family:var(--font-mono);font-size:10px;color:var(--text-dim)"><i style="display:inline-block;width:14px;height:10px;background:#232b3a;border-radius:2px"></i>known issuer, not in tape</span></div>`;
}
// ---- state + rendering ----
const CDS={packet:null,history:null,historyPromise:null,world:null,worldPromise:null,longKey:'baa10y',base:S3,
 f:{sovRegion:'all',sovTier:'all',sovStatus:'all',usSector:'all',usStatus:'all',glSector:'all',glStatus:'all'}};
const byId=id=>document.getElementById(id);
async function fetchJson(key){const url=(key.startsWith('/')?key:CDS.base+key)+'?cb='+Math.floor(Date.now()/60000);const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(key+' '+r.status);return r.json();}
function loadHistory(){if(CDS.history)return Promise.resolve(CDS.history);if(!CDS.historyPromise)CDS.historyPromise=fetchJson(KEYS.history).then(h=>{CDS.history=h;return h;});return CDS.historyPromise;}
function loadWorld(){if(CDS.world)return Promise.resolve(CDS.world);if(!CDS.worldPromise)CDS.worldPromise=fetchJson(KEYS.world).then(w=>{CDS.world=w;return w;}).catch(()=>null);return CDS.worldPromise;}
function applyFilters(rows,fRegion,fTier,fStatus,fSector){return rows.filter(r=>(fRegion==null||fRegion==='all'||(r.region||'Other')===fRegion)&&(fTier==null||fTier==='all'||(r.tier||'Other')===fTier)&&(fStatus==null||fStatus==='all'||r.status===fStatus)&&(fSector==null||fSector==='all'||(r.sector||'Other')===fSector));}
function renderSovereign(){
 const d=CDS.packet;if(!d)return;const sov=d.groups.sovereign||{},f=CDS.f;
 const all=sortUniverse(universeRows('sovereign',sov));
 const regions=countBy(all,'region'),tiers=countBy(all,'tier').sort((a,b)=>({DM:0,EM:1,Frontier:2}[a.value]??9)-({DM:0,EM:1,Frontier:2}[b.value]??9));
 const rows=applyFilters(all,f.sovRegion,f.sovTier,f.sovStatus,null);
 const cov=sov.coverage||{},nP=all.filter(r=>r.status==='liquid'||r.status==='thin').length,nU=all.filter(r=>r.status==='unpriced').length,nD=all.filter(r=>r.status==='dormant').length;
 byId('cdsdesk-sov-tag').textContent=`${cov.known??all.length} known sovereign issuers · ${cov.in_tape??(nP+nU+nD)} in the public tape · ${nP} priced · ${nU} either/or · ${nD} dormant · showing ${rows.length} · as of ${d.as_of||''}`;
 byId('cdsdesk-sov-chips').innerHTML=chipRow('Region','sovRegion',regions,f.sovRegion,chipText)+chipRow('Tier','sovTier',tiers,f.sovTier,chipText)+chipRow('Status','sovStatus',STATUS_ITEMS(all),f.sovStatus,c=>`${esc(STATUS_LABEL[c.value]||c.value)} <span class="t-mute">· ${c.n}</span>`);
 byId('cdsdesk-sov').innerHTML=universeTable(rows,['Region','Tier'],'no sovereign matches this filter');
 enhanceTables(byId('cdsdesk-sov').parentElement);
 const mapRows=applyFilters(all,f.sovRegion,f.sovTier,'all',null);
 byId('cdsdesk-map-tag').textContent=`5Y · ${all.filter(r=>r.status==='liquid'||r.status==='thin').length} priced · ${all.filter(r=>r.status==='unpriced').length} either/or · ${all.filter(r=>r.status==='not_in_tape').length} known issuers with no public print · click a country for history`;
 loadWorld().then(w=>{byId('cdsdesk-map').innerHTML=worldMap(w,mapRows);});
}
function renderCorp(group,prefix,sectorField,statusField){
 const d=CDS.packet;if(!d)return;const g=d.groups[group]||{},f=CDS.f;
 const all=sortUniverse(universeRows(group,g));
 const sectors=countBy(all,'sector');
 const rows=applyFilters(all,null,null,f[statusField],f[sectorField]);
 const nP=all.filter(r=>r.status==='liquid'||r.status==='thin').length,nL=all.filter(r=>r.status==='liquid').length,nU=all.filter(r=>r.status==='unpriced').length,nD=all.filter(r=>r.status==='dormant').length;
 byId(`cdsdesk-${prefix}-tag`).textContent=`${all.length} names in the tape · ${nP} priced (${nL} liquid) · ${nU} either/or · ${nD} dormant · median ${bp(g.median_bp)}bp · showing ${rows.length}`;
 byId(`cdsdesk-${prefix}-chips`).innerHTML=chipRow('Sector',sectorField,sectors,f[sectorField],chipText)+chipRow('Status',statusField,STATUS_ITEMS(all),f[statusField],c=>`${esc(STATUS_LABEL[c.value]||c.value)} <span class="t-mute">· ${c.n}</span>`);
 byId(`cdsdesk-${prefix}`).innerHTML=universeTable(rows,['Sector'],'no name matches this filter');
 enhanceTables(byId(`cdsdesk-${prefix}`).parentElement);
}
function renderLong(h){
 const tag=byId('cdsdesk-long-tag'),ctl=byId('cdsdesk-long-ctl'),box=byId('cdsdesk-long'),pk=byId('cdsdesk-long-peaks');
 const long=h&&h.long,ser=long&&long.series||{};
 if(!Object.keys(ser).length){tag.textContent='history file not published yet';box.innerHTML='<div class="t-mute" style="font-family:var(--font-mono);font-size:11px">data/cds-desk-history.json will appear after the next engine run</div>';return;}
 const avail=LONG_ORDER.filter(([k])=>ser[k]);if(!ser[CDS.longKey])CDS.longKey=avail[0][0];
 ctl.innerHTML=avail.map(([k,lab])=>`<button type="button" class="seg-btn${k===CDS.longKey?' active':''}" data-long="${k}">${lab}</button>`).join('');
 ctl.querySelectorAll('[data-long]').forEach(b=>b.addEventListener('click',()=>{CDS.longKey=b.dataset.long;renderLong(h);}));
 const s=ser[CDS.longKey],map=s.cdx_ig_map,unit=s.unit||'';
 const markers=(s.peaks||[]).map(p=>({date:p.date,value:p.value,label:p.episode.replace(/ \d{4}.*$/,'')}));
 const hline=map&&fin(map.mapped_baa10y_bp)&&map.usable?{value:map.mapped_baa10y_bp,label:`CDX IG ${map.cdx_ig_bp}bp ≈ ${map.mapped_baa10y_bp}bp here`}:null;
 tag.textContent=`${s.name} · ${s.source} · last ${s.last.date}`;
 box.innerHTML=lineChart(s.points,{h:250,markers,hline,color:'var(--info)',label:s.name});
 const gfc=(s.peaks||[]).find(p=>p.episode.startsWith('GFC'));
 const isIndex=/^index/.test(unit)||CDS.longKey==='ebp'||s.last.value<=0;
 let read=`<b>${esc(s.name)}</b> is <b>${s.last.value}</b> ${esc(unit)} (${s.last.date}) — the <b>${pct(s.pct_rank_since_2006)} percentile</b> of every week since 2006${gfc?(isIndex?`, ${(s.last.value-gfc.value).toFixed(2)} below its 2008 peak (${gfc.value} on ${gfc.date})`:`, ${(100*s.last.value/gfc.value).toFixed(0)}% of its 2008 peak (${gfc.value} on ${gfc.date})`):''}.`;
 if(map)read+=` Mapping the live CDX IG print (${map.cdx_ig_bp}bp on ${map.cdx_ig_date}) onto this record with a straight-line fit over the ${map.fit.n}-day overlap (r² ${map.fit.r2}${map.usable?'':' — too weak to lean on'}) puts it at <b>${map.mapped_baa10y_bp}bp</b>, the <b>${pct(map.pct_rank_since_2006)} percentile</b> since 2006${fin(map.share_of_gfc_peak)?` and ${(100*map.share_of_gfc_peak).toFixed(0)}% of the GFC peak`:''}.`;
 pk.innerHTML=`<table><thead><tr><th>Episode</th><th>Peak date</th><th>Peak</th><th>Today</th><th>${isIndex?'Today − peak':'Today / peak'}</th></tr></thead><tbody>${(s.peaks||[]).map(p=>`<tr><td class="t-info">${esc(p.episode)}</td><td class="t-mute">${p.date}</td><td><b>${p.value}</b></td><td>${s.last.value}</td><td>${isIndex?(s.last.value-p.value).toFixed(2):p.value>0?(100*s.last.value/p.value).toFixed(0)+'%':'—'}</td></tr>`).join('')}</tbody></table><div class="t-mute" style="font-family:var(--font-mono);font-size:10.5px;margin-top:8px">${read}<br>${(long.limits||[]).map(esc).join(' · ')}</div>`;
 enhanceTables(pk);
}
// history modal: the name's own DTCC record (2024-09 →) drawn over the group's free long record back to 2006
function modalHtml(key,name,group,tier,h,packet){
 const s=h&&h.names&&h.names[key];const long=h&&h.long&&h.long.series||{};
 const pk=proxyFor(group,tier,key),px=long[pk];
 const pts=s&&s.points||[];
 const coupons=s&&s.branches?Object.keys(s.branches).sort((a,b)=>(s.branches[b].length-s.branches[a].length)):[];
 const band=coupons.length?s.branches[coupons[0]].map(p=>[p[0],p[1],p[2]]):[];
 let html='';
 if(px){
  const markers=(px.peaks||[]).map(p=>({date:p.date,value:p.value,label:p.episode.replace(/ \d{4}.*$/,'')}));
  const own=pts.length>1?pts:[];
  html+=`<div style="font-family:var(--font-mono);font-size:10.5px;color:var(--text-dim);margin-bottom:4px">2006 → today: <b style="color:#4fb7e8">${esc(px.name)}</b> (${esc(px.unit||'')}, left axis, ${esc(px.source||'')}) with <b style="color:#f5c451">${esc(name)}</b> 5Y CDS (bp, right axis) from the first public print${own.length?` ${esc(own[0][0])}`:''}${band.length?` · orange band = the two feasible levels of the unsigned upfront @${coupons[0]}c`:''}</div>`;
  html+=lineChart(px.points,{h:280,color:'#4fb7e8',label:px.name,markers,series2:own,axis2:true,color2:'#f5c451',label2:'bp',band:own.length?[]:band,xmin:'2006-01-01'});
  const gfc=(px.peaks||[]).find(p=>p.episode.startsWith('GFC'));
  html+=`<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:8px"><span class="pill info">${esc(px.name)} ${px.last.value} ${esc(px.unit||'')} · ${pct(px.pct_rank_since_2006)} pct since 2006</span>${gfc?`<span class="pill neg">2008 peak ${gfc.value} on ${gfc.date}</span>`:''}${(px.peaks||[]).filter(p=>!p.episode.startsWith('GFC')).map(p=>`<span class="pill mute">${esc(p.episode)}: ${p.value}</span>`).join('')}</div>`;
 }
 if(pts.length>1){
  const ys=pts.map(p=>p[1]),lo=Math.min(...ys),hi=Math.max(...ys),last=pts[pts.length-1],loD=pts[ys.indexOf(lo)][0],hiD=pts[ys.indexOf(hi)][0];
  html+=`<div style="font-family:var(--font-mono);font-size:10.5px;color:var(--text-dim);margin:14px 0 4px">Own record, every priced day in the DTCC bank</div>`+lineChart(pts,{h:240,color:'#f5c451',label:name,markers:[{date:hiD,value:hi,label:'high'}],band})+
   `<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px"><span class="pill info">last ${last[1]}bp · ${last[0]}</span><span class="pill mute">${pts.length} priced days · ${s.first} → ${s.last}</span><span class="pill neg">high ${hi}bp · ${hiD}</span><span class="pill pos">low ${lo}bp · ${loD}</span><span class="pill mute">${s.n} prints in bank</span></div>`;
 }else if(band.length>1){
  html+=`<div style="font-family:var(--font-mono);font-size:10.5px;color:var(--text-dim);margin:14px 0 4px">No print ever fixed the sign: both feasible levels per day @${coupons[0]}c (orange band)</div>`+lineChart([],{h:220,label:name,band,axis2:true,color2:'var(--orange)',label2:'bp'})+`<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:10px"><span class="pill warning">either / or · ${band.length} days with two branches</span><span class="pill mute">${s?s.n:0} prints in bank</span></div>`;
 }else if(!px){html+='<div class="t-mute" style="font-family:var(--font-mono);font-size:11px">no stored history for this name yet (file not published, or no public print since 2024-09)</div>';}
 else if(!s){html+='<div class="t-mute" style="font-family:var(--font-mono);font-size:11px;margin-top:10px">This issuer has no public DTCC print since the tape began (2024-09), so only the group record is drawn.</div>';}
 html+=`<div class="t-mute" style="font-family:var(--font-mono);font-size:10.5px;margin-top:10px">No free source carries this name's own CDS before 2024-09: the DTCC public tape starts there and every long single-name CDS history is licensed (Markit/ICE, Bloomberg). The 2006 → panel therefore draws the free ${px?esc(px.name):'group'} record the name is compared against (same bp unit for Baa−10Y; OFR series are stress indexes), with the name's real prints overlaid on their own axis — it places today against 2008 / 2011 / 2020 for the name's group, not for the name itself.${h&&h.as_of?' History as of '+esc(h.as_of)+'.':''}</div>`;
 return html;
}
function openHistory(key,name,group,tier){
 const modal=byId('cdsdesk-modal'),body=byId('cdsdesk-modal-body'),title=byId('cdsdesk-modal-title');
 if(!modal)return;
 title.textContent=`${name} · 5Y CDS vs 2006 →`;body.innerHTML='<div class="loading spin">loading history</div>';modal.style.display='flex';
 loadHistory().then(h=>{body.innerHTML=modalHtml(key,name,group,tier,h,CDS.packet);}).catch(e=>{body.innerHTML=`<div class="t-mute" style="font-family:var(--font-mono);font-size:11px">history file unavailable (${esc(e.message)})</div>`;});
}
function render(d){
 const kp=byId('cdsdesk-kpis');if(!kp)return;
 if(!d||!d.groups){kp.innerHTML='<div class="kpi warning"><div class="label">CDS desk</div><div class="val">Unavailable</div><div class="sub">data/cds-desk.json not published yet</div></div>';return;}
 CDS.packet=d;
 const g=d.groups,sov=g.sovereign||{},us=g.us_corp||{},gl=g.global_corp||{},br=d.breadth||{},hist=d.history||{},term=d.term||{};
 const idx=Object.fromEntries((d.indices||[]).map(i=>[i.key,i]));const ig=idx['CDX.NA.IG'],hy=idx['CDX.NA.HY'];
 const widerShare=fin(br.n)&&br.n?Math.round(100*br.wider/br.n):null;
 const map=hist.cdx_ig_vs_2006,bh=hist.basis&&hist.basis.hy,bi=hist.basis&&hist.basis.ig;
 const cov=sov.coverage||{};
 kp.innerHTML=`
  <div class="kpi ${widerShare==null?'info':widerShare>=65?'neg':widerShare<=35?'pos':'gold'}"><div class="label">Breadth (1d)</div><div class="val">${widerShare==null?'—':widerShare+'% wider'}</div><div class="sub">${br.wider??0} wider · ${br.tighter??0} tighter · median ${sgn(br.median_chg_1d_bp)}bp</div></div>
  <div class="kpi accent"><div class="label">Sovereign median</div><div class="val">${bp(sov.median_bp)}bp</div><div class="sub">${sov.n_priced??sov.n_liquid??0} priced of ${cov.in_tape??sov.n_tracked??0} in tape${cov.known?` · ${cov.known} known issuers`:''} · 1d ${sgn(sov.median_chg_1d_bp)}bp</div></div>
  <div class="kpi info"><div class="label">U.S. corp median</div><div class="val">${bp(us.median_bp)}bp</div><div class="sub">${us.n_priced??us.n_liquid??0} priced of ${us.n_tracked??0} names · global ${gl.n_priced??gl.n_liquid??0} of ${gl.n_tracked??0} · 1d ${sgn(us.median_chg_1d_bp)}bp</div></div>
  <div class="kpi gold"><div class="label">CDX IG / HY</div><div class="val">${ig?bp(ig.spread_bp,1):'—'} / ${hy?bp(hy.spread_bp):'—'}</div><div class="sub">${ig?`IG ${pct(ig.pct_rank_1y)} pct 1y`:''}${hy?` · HY px ${bp(hy.price,2)} · ${pct(hy.pct_rank_1y)} pct`:''}</div></div>
  <div class="kpi ${map&&map.usable?(map.pct_rank_since_2006>=75?'neg':map.pct_rank_since_2006<=25?'pos':'gold'):'info'}" id="cdsdesk-kpi-long"><div class="label">CDX IG vs 2006→</div><div class="val">${map&&map.usable?pct(map.pct_rank_since_2006)+' pct':'—'}</div><div class="sub">${map?`≈ ${map.mapped_baa10y_bp}bp Baa−10Y · ${fin(map.share_of_gfc_peak)?Math.round(100*map.share_of_gfc_peak)+'% of 2008 peak':''} · fit r² ${map.fit?map.fit.r2:'—'}${map.usable?'':' (weak)'}`:'needs history file'}</div></div>
  <div class="kpi ${bh?(bh[1]<-40?'warning':'info'):'info'}" id="cdsdesk-kpi-basis"><div class="label">CDS − bond basis</div><div class="val">${bh?sgn(bh[1],0):'—'}<span style="font-size:13px"> HY</span> ${bi?sgn(bi[1],0):'—'}<span style="font-size:13px"> IG</span></div><div class="sub">${bh?`CDX HY ${bp(bh[2])} vs HY OAS ${bp(bh[3])}bp · ${bh[0]}`:'CDX vs ICE BofA cash OAS'}</div></div>
  <div class="kpi ${fin(br.n_z_over_2)&&br.n_z_over_2>=10?'warning':'info'}"><div class="label">Stretched names</div><div class="val">${br.n_z_over_2??'—'}</div><div class="sub">≥2σ above own 90d mean · ${br.n_z_under_minus2??0} at ≤−2σ · ${fin(br.share_above_90d_mean)?Math.round(br.share_above_90d_mean*100)+'% above mean':''}</div></div>
  <div class="kpi ${(term.n_inverted||0)>=5?'neg':'info'}"><div class="label">Inverted curves</div><div class="val">${term.n_inverted??'—'}<span style="font-size:13px"> / ${term.n_with_curve??'—'}</span></div><div class="sub">1Y above 5Y · ${(d.wides_1y||[]).length} names at 1y wides · ${(d.tights_1y||[]).length} at 1y tights</div></div>`;
 byId('cdsdesk-indices').innerHTML=`<div style="overflow-x:auto"><table><thead><tr><th>Index</th><th>5Y spread</th><th>Price</th><th>1d</th><th>1w</th><th>1m</th><th>3m</th><th>1y pct</th><th>1y range</th><th>z 90d</th><th>Prints</th><th>Expiry</th><th>120d</th></tr></thead><tbody>${(d.indices||[]).map(i=>`<tr>${nameCell({key:'IDX:'+i.key,name:i.name,group:'index'})}<td${sd(i.spread_bp)}><b>${bp(i.spread_bp,1)}</b></td><td${sd(i.price)}>${bp(i.price,2)}</td><td${sd(i.chg_1d_bp)}>${chg(i.chg_1d_bp)}</td><td${sd(i.chg_1w_bp)}>${chg(i.chg_1w_bp)}</td><td${sd(i.chg_1m_bp)}>${chg(i.chg_1m_bp)}</td><td${sd(i.chg_3m_bp)}>${chg(i.chg_3m_bp)}</td><td${sd(i.pct_rank_1y)}>${pct(i.pct_rank_1y)}</td><td${sd(i.hi_1y_bp)} class="t-mute">${bp(i.lo_1y_bp)}–${bp(i.hi_1y_bp)}</td><td${sd(i.z_90d)}>${z(i.z_90d)}</td><td>${i.n_last??'—'}</td><td class="t-mute">${esc(i.expiry||'')}</td><td${sd(sparkChg(i.spark))}>${spark(i.spark)}</td></tr>`).join('')||'<tr><td colspan="13" class="t-mute">no index prints</td></tr>'}</tbody></table></div>`;
 renderSovereign();renderCorp('us_corp','us','usSector','usStatus');renderCorp('global_corp','gl','glSector','glStatus');
 const movers=[...(sov.movers_1d||[]).map(m=>({...m,grp:'Sov',group:'sovereign'})),...(us.movers_1d||[]).map(m=>({...m,grp:'US',group:'us_corp'})),...(gl.movers_1d||[]).map(m=>({...m,grp:'Global',group:'global_corp'}))].filter(m=>fin(m.chg_1d_bp)).sort((a,b)=>Math.abs(b.chg_1d_bp)-Math.abs(a.chg_1d_bp)).slice(0,12);
 byId('cdsdesk-movers').innerHTML=`<table><thead><tr><th>Name</th><th>Group</th><th>5Y</th><th>1d</th><th>Prints</th></tr></thead><tbody>${movers.map(m=>`<tr>${nameCell(m)}<td class="t-mute">${m.grp}</td><td${sd(m.spread_bp)}>${bp(m.spread_bp,1)}</td><td${sd(m.chg_1d_bp)}>${chg(m.chg_1d_bp)}</td><td>${m.n_last??'—'}</td></tr>`).join('')||'<tr><td colspan="5" class="t-mute">no priced moves</td></tr>'}</tbody></table>`;
 const extremes=(arr,rangeKey,rangeHead)=>`<table><thead><tr><th>Name</th><th>Group</th><th>5Y</th><th>1y pct</th><th>${rangeHead}</th><th>1m</th></tr></thead><tbody>${(arr||[]).map(r=>`<tr>${nameCell(r)}<td class="t-mute">${GRP[r.group]||r.group}</td><td${sd(r.spread_bp)}><b>${bp(r.spread_bp,1)}</b></td><td${sd(r.pct_rank_1y)}>${pct(r.pct_rank_1y)}</td><td${sd(r[rangeKey])}>${bp(r[rangeKey],1)}</td><td${sd(r.chg_1m_bp)}>${chg(r.chg_1m_bp)}</td></tr>`).join('')||'<tr><td colspan="6" class="t-mute">none today</td></tr>'}</tbody></table>`;
 byId('cdsdesk-wides').innerHTML=extremes(d.wides_1y,'hi_1y_bp','1y high');byId('cdsdesk-tights').innerHTML=extremes(d.tights_1y,'lo_1y_bp','1y low');
 byId('cdsdesk-term-tag').textContent=`${term.n_inverted??0} of ${term.n_with_curve??0} names with a same-day 1Y print`;
 byId('cdsdesk-term').innerHTML=`<table><thead><tr><th>Name</th><th>Group</th><th>5Y</th><th>5Y−1Y</th></tr></thead><tbody>${(term.inverted||[]).map(r=>`<tr>${nameCell(r)}<td class="t-mute">${GRP[r.group]||r.group}</td><td${sd(r.spread_bp)}>${bp(r.spread_bp,1)}</td><td${sd(r.slope_1y5y_bp)}><span class="t-neg">${sgn(r.slope_1y5y_bp,0)}</span></td></tr>`).join('')||'<tr><td colspan="4" class="t-mute">no inverted curves among names with a sign-fixed 1Y print</td></tr>'}</tbody></table><div class="t-mute" style="font-family:var(--font-mono);font-size:10.5px;margin-top:8px">${esc(term.note||'')}</div>`;
 // unpriced: why, with both branches
 const unp=[...(sov.unpriced||[]).map(u=>({...u,grp:'Sov',group:'sovereign'})),...(us.unpriced||[]).map(u=>({...u,grp:'US',group:'us_corp'})),...(gl.unpriced||[]).map(u=>({...u,grp:'Global',group:'global_corp'}))].sort((a,b)=>b.trades_30d-a.trades_30d);
 byId('cdsdesk-unpriced-tag').textContent=`${unp.length} names · ${unp.filter(u=>fin(u.last_spread_bp)).length} with a dated earlier level · ${unp.filter(u=>!(u.candidates||[]).length).length} with no derivable level (capped blocks) · sorted by prints`;
 byId('cdsdesk-unpriced').innerHTML=unp.length?`<div style="overflow-x:auto;max-height:520px;overflow-y:auto"><table><thead><tr><th>Name</th><th>Group</th><th>Sector / region</th><th>Prints 30d</th><th>Days active</th><th>Ambiguous</th><th>Last print</th><th title="the two spreads the unsigned upfront could mean: below the coupon or above it">Either … or (bp)</th><th>Last fixed level</th><th>Fixed on</th><th>Why unpriced</th></tr></thead><tbody>${unp.map(u=>`<tr>${nameCell(u)}<td class="t-mute">${u.grp}</td><td class="t-mute">${esc(u.sector||u.region||'')}</td><td${sd(u.trades_30d)}>${u.trades_30d??'—'}</td><td${sd(u.days_active_30d)}>${u.days_active_30d??'—'}</td><td${sd(u.ambiguous_30d)}>${u.ambiguous_30d??'—'}</td><td class="t-mute" data-sort="${esc(u.last_date||'')}">${esc(u.last_date||'')}</td><td${sd((u.candidates||[]).length?u.candidates[0].above_coupon_bp:null)}>${cands(u)}</td><td${sd(u.last_spread_bp)}>${fin(u.last_spread_bp)?`<b>${bp(u.last_spread_bp,1)}</b>`:'<span class="t-mute">never</span>'}</td><td class="t-mute" data-sort="${esc(u.last_priced_date||'')}">${esc(u.last_priced_date||'—')}</td><td class="t-mute" style="font-size:10.5px;white-space:nowrap">${esc(u.why||'')}</td></tr>`).join('')}</tbody></table></div>
  <div class="t-mute" style="font-family:var(--font-mono);font-size:10.5px;margin-top:8px"><b>Why "derived" and "either / or" exist:</b> the public DTCC file carries each print's coupon, notional, maturity and upfront amount, but not which side paid it. A quoted spread field is filled on only a minority of single-name prints (most dealers report upfront-only), so for the rest the engine solves the ISDA standard model for the two spreads consistent with the unsigned upfront — one below the coupon, one above — and needs other evidence (a quoted print, a second coupon, a single feasible branch, the name's own recent fixed level, or co-movement with its index) to pick one. Where the only prints are block trades with a capped notional, no level can be derived at all and the name is listed with no candidates. Listed rather than guessed.</div>`:'<span class="t-mute">every active name is priced</span>';
 const basisBox=byId('cdsdesk-basis');basisBox.innerHTML='<div class="t-mute" style="font-family:var(--font-mono);font-size:11px">loading history…</div>';
 const act=d.activity&&d.activity.rows||[];
 if(act.length>1){const prints=act.map(r=>[r[0],r[1]]);const l=act[act.length-1],avg=act.slice(-20).reduce((s,r)=>s+r[1],0)/Math.min(20,act.length);byId('cdsdesk-activity').innerHTML=lineChart(prints,{h:170,bars:true,color:'var(--accent)',label:'single-name prints per day'})+`<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:8px"><span class="pill info">${l[0]}: ${l[1]} prints</span><span class="pill mute">${l[2]} with a resolved spread (${l[1]?Math.round(100*l[2]/l[1]):0}%)</span><span class="pill mute">${l[4]} names priced</span><span class="pill mute">5Y notional ${fin(l[3])?'$'+(l[3]/1000).toFixed(1)+'bn':'—'} (blocks at cap)</span><span class="pill ${l[1]>avg*1.4?'warning':'mute'}">${(100*l[1]/avg-100).toFixed(0)}% vs 20d avg</span></div><div class="t-mute" style="font-family:var(--font-mono);font-size:10.5px;margin-top:8px">Prints are a liquidity gauge: single-name volume spikes when hedgers rush, and a rising share of unresolved signs usually means more upfront-heavy (distressed) trading.</div>`;}
 else byId('cdsdesk-activity').innerHTML='<span class="t-mute">activity block not published yet</span>';
 const m=d.method||{},src=d.source||{};
 byId('cdsdesk-interp').innerHTML=`<b>What this is:</b> 5Y CDS levels measured from the real-time public trade reports every U.S. swap dealer must disseminate (${esc(src.law||'')}), via the DTCC files ${esc(src.files||'')}. History in the bank: ${esc(src.first_day||'')} → ${esc(src.last_day||'')} (${src.n_days??'—'} days); tables carry 120-day sparklines, click any name for its record drawn against the free 2006 → series of its group. <b>Universe:</b> ${esc(m.universe_rule||'')}. Spread rule: ${esc(m.spread_rule||'')}. Model: ${esc(m.model||'')}. Liquid = ${esc(m.liquid_rule||'')}. <b>Sign rule:</b> ${esc(m.sign_rule||'')}.<br><b>Long view:</b> the DTCC tape starts 2024-09 and FRED's ICE BofA spread series were cut to three years in April 2026, so the "since 2006" panels use free government records that still carry 2008 (Moody's Baa−10Y, OFR Financial Stress Index, Fed GZ spread / excess bond premium) and map today's CDX IG onto them with a disclosed fit. <b>Limits:</b> ${(m.limitations||[]).map(esc).join(' · ')}. <b>No call, no sizing</b> — descriptive measurements only (decision.call = ${d.decision&&d.decision.call===null?'null':esc(d.decision&&d.decision.call)}).`;
 enhanceTables(byId('cdsdesk-kpis').parentElement);
 loadHistory().then(h=>{
  renderLong(h);
  const bs=h&&h.long&&h.long.basis||{};
  const one=(k,lab)=>{const b=bs[k];if(!b)return '';const pts=b.points.map(p=>[p[0],p[1]]);const lv=b.last;return `<div style="margin-bottom:10px"><div style="font-family:var(--font-mono);font-size:10.5px;color:var(--text-dim);margin-bottom:4px">${lab}: <b class="${lv[1]<0?'t-neg':'t-pos'}">${sgn(lv[1],0)}bp</b> on ${lv[0]} · CDS ${bp(lv[2])} vs cash ${bp(lv[3])} · 1y mean ${sgn(b.mean_bp,0)} · ${pct(b.pct_rank)} pct</div>${lineChart(pts,{h:120,color:lv[1]<0?'var(--neg)':'var(--pos)',label:lab+' basis',hline:{value:0,label:'0'}})}</div>`;};
  basisBox.innerHTML=(one('hy','CDX HY − HY OAS')+one('ig','CDX IG − IG OAS'))||'<div class="t-mute" style="font-family:var(--font-mono);font-size:11px">basis not published yet (needs the history file)</div>';
  if(bs.hy)basisBox.insertAdjacentHTML('beforeend',`<div class="t-mute" style="font-family:var(--font-mono);font-size:10.5px">${esc(bs.hy.read||'')}</div>`);
  const lb=h&&h.long&&h.long.series&&h.long.series.baa10y,lm=lb&&lb.cdx_ig_map,kl=byId('cdsdesk-kpi-long');
  if(kl&&lm&&lm.usable){kl.className='kpi '+(lm.pct_rank_since_2006>=75?'neg':lm.pct_rank_since_2006<=25?'pos':'gold');kl.innerHTML=`<div class="label">CDX IG vs 2006→</div><div class="val">${pct(lm.pct_rank_since_2006)} pct</div><div class="sub">≈ ${lm.mapped_baa10y_bp}bp Baa−10Y · ${fin(lm.share_of_gfc_peak)?Math.round(100*lm.share_of_gfc_peak)+'% of 2008 peak':''} · fit r² ${lm.fit?lm.fit.r2:'—'}</div>`;}
  else if(kl&&lb&&lb.last){const gfc=(lb.peaks||[]).find(p=>p.episode.startsWith('GFC'));kl.className='kpi '+(lb.pct_rank_since_2006>=75?'neg':lb.pct_rank_since_2006<=25?'pos':'gold');kl.innerHTML=`<div class="label">IG credit vs 2006→</div><div class="val">${pct(lb.pct_rank_since_2006)} pct</div><div class="sub">Baa−10Y ${lb.last.value}bp (${lb.last.date})${gfc?` · ${Math.round(100*lb.last.value/gfc.value)}% of 2008 peak`:''}${lm?` · CDX IG fit too weak (r² ${lm.fit?lm.fit.r2:'—'})`:''}</div>`;}
  const kb=byId('cdsdesk-kpi-basis');
  if(kb&&(bs.hy||bs.ig)){const lh=bs.hy&&bs.hy.last,li=bs.ig&&bs.ig.last;kb.className='kpi '+(lh&&lh[1]<-40?'warning':'info');kb.innerHTML=`<div class="label">CDS − bond basis</div><div class="val">${lh?sgn(lh[1],0):'—'}<span style="font-size:13px"> HY</span> ${li?sgn(li[1],0):'—'}<span style="font-size:13px"> IG</span></div><div class="sub">${lh?`CDX HY ${bp(lh[2])} vs HY OAS ${bp(lh[3])}bp · ${lh[0]}`:'CDX vs ICE BofA cash OAS'}</div>`;}
 }).catch(()=>{const t=byId('cdsdesk-long-tag');if(t)t.textContent='history file unavailable';});
}
let wired=false;
function wire(){
 if(wired||typeof document==='undefined')return;wired=true;
 document.addEventListener('click',e=>{
  const th=e.target.closest('th.sortable');if(th){sortTableBy(th);return;}
  const csv=e.target.closest('.csv-btn[data-csv]');
  if(csv){const panel=csv.closest('.panel'),tbl=panel&&panel.querySelector('table');if(!tbl)return;const name=(panel.querySelector('h3')?panel.querySelector('h3').childNodes[0].textContent:'table').trim().replace(/[^a-z0-9]+/gi,'-').toLowerCase();const blob=new Blob([tableToCsv(tbl)],{type:'text/csv'});const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=`justhodl-${name}-${new Date().toISOString().slice(0,10)}.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),2000);return;}
  const chip=e.target.closest('.cds-chip[data-filter]');
  if(chip){const f=chip.dataset.filter;CDS.f[f]=chip.dataset.value;if(f.startsWith('sov'))renderSovereign();else if(f.startsWith('us'))renderCorp('us_corp','us','usSector','usStatus');else renderCorp('global_corp','gl','glSector','glStatus');return;}
  const n=e.target.closest('.cds-clickable[data-key]');
  if(n){openHistory(n.dataset.key,n.dataset.name||n.textContent,n.dataset.group||'',n.dataset.tier||'');if(byId('cdsdesk-root')&&byId('cdsdesk-root').dataset.auto==='1'&&typeof history!=='undefined')history.replaceState(null,'','#name='+encodeURIComponent(n.dataset.key));return;}
  if(e.target.id==='cdsdesk-modal-close'||e.target.id==='cdsdesk-modal'){const m=byId('cdsdesk-modal');if(m)m.style.display='none';}
 });
 document.addEventListener('keydown',e=>{if(e.key==='Escape'){const m=byId('cdsdesk-modal');if(m)m.style.display='none';}});
}
function findName(key){
 const d=CDS.packet;if(!d)return null;
 if(key.startsWith('IDX:')){const i=(d.indices||[]).find(i=>'IDX:'+i.key===key);return i?{key,name:i.name,group:'index',tier:''}:null;}
 for(const [g,v] of Object.entries(d.groups||{})){const r=universeRows(g,v).find(r=>r.key===key);if(r)return {key,name:r.name,group:g,tier:r.tier||''};}
 return null;
}
function openFromHash(){
 if(typeof location==='undefined')return;const m=/[#&]name=([^&]+)/.exec(location.hash||'');if(!m)return;
 const key=decodeURIComponent(m[1]),f=findName(key);if(f)openHistory(f.key,f.name,f.group,f.tier);
}
function mount(el,opts){
 opts=opts||{};if(opts.base)CDS.base=opts.base;
 if(el&&!byId('cdsdesk-kpis'))el.innerHTML=shell();
 wire();
 if(opts.packet){render(opts.packet);openFromHash();return Promise.resolve(opts.packet);}
 if(opts.fetch===false)return Promise.resolve(null);
 return fetchJson(KEYS.packet).then(p=>{render(p);openFromHash();return p;}).catch(e=>{const kp=byId('cdsdesk-kpis');if(kp)kp.innerHTML=`<div class="kpi warning"><div class="label">CDS desk</div><div class="val">Unavailable</div><div class="sub">${esc(e.message)}</div></div>`;return null;});
}
const api={render,mount,findName,shell,lineChart,universeRows,universeTable,worldMap,modalHtml,proxyFor,colorFor,enhanceTables,sortTableBy,tableToCsv,countBy,applyFilters,state:CDS,KEYS};
if(typeof module!=='undefined'&&module.exports)module.exports=api;
if(root&&typeof root.document!=='undefined'){root.JH_CDS=api;root.enhanceTables=enhanceTables;root.sortTableBy=sortTableBy;root.tableToCsv=tableToCsv;
 root.document.addEventListener('DOMContentLoaded',()=>{const el=root.document.getElementById('cdsdesk-root');if(el&&el.dataset.auto==='1')mount(el,{base:el.dataset.base||S3});});}
})(typeof window!=='undefined'?window:null);
