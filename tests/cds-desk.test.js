const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const ui=require('../jh-cds-desk.js');
const packet=()=>JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/cds-desk-v130.json'),'utf8'));
test('sovereign universe merges priced, either/or, dormant and not-in-tape issuers into one row set with region and tier',()=>{
 const p=packet(),rows=ui.universeRows('sovereign',p.groups.sovereign);
 const st=new Set(rows.map(r=>r.status));
 assert.ok(st.has('liquid')||st.has('thin'));assert.ok(st.has('unpriced'));assert.ok(st.has('not_in_tape'));
 assert.ok(rows.every(r=>r.region&&r.tier&&r.key));
 const html=ui.universeTable(rows,['Region','Tier'],'none');
 assert.match(html,/either \/ or/);assert.match(html,/not in tape/);assert.match(html,/no public print since 2024-09/);
 assert.doesNotMatch(html,/undefined|NaN|\[object Object\]/);
 // the unsigned upfront is shown as both feasible levels, never as one guessed number
 const u=rows.find(r=>r.status==='unpriced'&&(r.candidates||[]).length);
 if(u){assert.match(html,new RegExp(`${Math.round(u.candidates[0].below_coupon_bp)} <span class="t-mute">or<\\/span> ${Math.round(u.candidates[0].above_coupon_bp)}`));}
});
test('chips filter by region, tier, status and sector; "all" resets',()=>{
 const p=packet(),rows=ui.universeRows('sovereign',p.groups.sovereign);
 const regions=ui.countBy(rows,'region');assert.ok(regions.length>=3);
 const one=regions[0].value,sub=ui.applyFilters(rows,one,'all','all',null);
 assert.ok(sub.length>0&&sub.length<rows.length&&sub.every(r=>r.region===one));
 assert.equal(ui.applyFilters(rows,'all','all','all',null).length,rows.length);
 assert.ok(ui.applyFilters(rows,'all','all','not_in_tape',null).every(r=>r.status==='not_in_tape'));
 const us=ui.universeRows('us_corp',p.groups.us_corp),sectors=ui.countBy(us,'sector').map(s=>s.value);
 const sect=sectors[0];assert.ok(ui.applyFilters(us,null,null,'all',sect).every(r=>(r.sector||'Other')===sect));
});
test('AI and software are separate sectors in the packet and the US table carries a sign-evidence column',()=>{
 const p=packet(),sect=p.groups.us_corp.sectors.map(s=>s.sector);
 assert.ok(sect.includes('AI & semis'));assert.ok(sect.includes('Software & internet'));
 const html=ui.universeTable(ui.universeRows('us_corp',p.groups.us_corp),['Sector'],'none');
 assert.match(html,/Sign evidence/);assert.match(html,/quoted print|two-coupon|single feasible branch|own level/);
});
test('world map colours priced sovereigns, hatches either/or, greys dormant / not-in-tape and ignores unknown ISO codes',()=>{
 const p=packet(),rows=ui.universeRows('sovereign',p.groups.sovereign);
 const world={w:960,h:415,paths:Object.fromEntries(rows.filter(r=>r.iso3).map(r=>[r.iso3,'M0,0 10,0 10,10Z']).concat([['XXX','M0,0 1,1Z']])),names:{XXX:'Nowhere'},centroids:{}};
 const svg=ui.worldMap(world,rows);
 assert.match(svg,/url\(#cds-hatch\)/);assert.match(svg,/not a known sovereign CDS issuer/);
 const priced=rows.find(r=>(r.status==='liquid'||r.status==='thin')&&r.iso3);
 if(priced)assert.ok(svg.includes(`data-key="${priced.key}"`)&&svg.includes(ui.colorFor(priced.spread_bp)));
 assert.doesNotMatch(svg,/undefined|NaN/);
 assert.equal(ui.colorFor(null),null);assert.notEqual(ui.colorFor(20),ui.colorFor(800));
});
test('history modal draws the group record from 2006 with the name on its own axis and states the pre-2024 limit honestly',()=>{
 const pts=[];for(let y=2006;y<=2026;y++)pts.push([`${y}-01-06`,100+y-2006]);
 const h={as_of:'2026-10-08',long:{series:{ofr_em:{name:'OFR EM stress',unit:'index',source:'OFR',points:pts,last:{date:'2026-01-06',value:120},pct_rank_since_2006:40,peaks:[{episode:'GFC 2008',date:'2008-11-20',value:130}]},baa10y:{name:'Baa-10Y',unit:'bp',source:'FRED',points:pts,last:{date:'2026-01-06',value:120},pct_rank_since_2006:40,peaks:[]},ofr_credit:{name:'OFR credit',unit:'index',source:'OFR',points:pts,last:{date:'2026-01-06',value:120},pct_rank_since_2006:40,peaks:[]}}},
  names:{'REPUBLIC OF TURKEY':{points:[['2024-09-03',280],['2026-10-08',250]],n:900,first:'2024-09-03',last:'2026-10-08'},'STATE OF QATAR':{points:[],n:40,first:null,last:null,branches:{'100':[['2026-09-01',162,43],['2026-10-01',160,44]]}}}};
 const a=ui.modalHtml('REPUBLIC OF TURKEY','Turkey','sovereign','EM',h,{});
 assert.match(a,/2006 → today/);assert.match(a,/OFR EM stress/);assert.match(a,/GFC/);assert.match(a,/No free source carries this name's own CDS before 2024-09/);assert.match(a,/Own record, every priced day/);
 // v1.4: the name's tape record is a range glyph in the right margin (high/low/last), never a time-compressed second line on the 20-year axis
 assert.match(a,/high 280/);assert.match(a,/low 250/);assert.match(a,/Turkey prints 2024-09/);assert.doesNotMatch(a,/stroke-width="1.6"/);
 const b=ui.modalHtml('STATE OF QATAR','Qatar','sovereign','EM',h,{});
 assert.match(b,/No print ever fixed the sign/);assert.match(b,/either \/ or/);
 const c=ui.modalHtml('UNKNOWN','Canada','sovereign','DM',h,{});
 assert.match(c,/no public DTCC print since the tape began/);
 assert.equal(ui.proxyFor('us_corp','',''),'baa10y');assert.equal(ui.proxyFor('sovereign','Frontier',''),'ofr_em');assert.equal(ui.proxyFor('sovereign','DM',''),'ofr_credit');assert.equal(ui.proxyFor('index','','IDX:CDX.EM'),'ofr_em');
 // v1.4: a euro-area sovereign is drawn against its own ECB SovCISS record when the history file carries it
 const longES={...h.long.series,sovciss_ESP:{name:'ECB SovCISS · Spain',unit:'index',source:'ECB',iso3:'ESP',points:pts,last:{date:'2026-10-08',value:0.05},pct_rank_since_2006:50,peaks:[{episode:'Euro crisis 2011-12',date:'2012-07-24',value:0.99}]}};
 assert.equal(ui.proxyFor('sovereign','DM','KINGDOM OF SPAIN','ESP',longES),'sovciss_ESP');assert.equal(ui.proxyFor('sovereign','DM','KINGDOM OF SPAIN','ESP',h.long.series),'ofr_credit');
 const d=ui.modalHtml('KINGDOM OF SPAIN','Spain','sovereign','DM',{...h,long:{series:longES},names:{'KINGDOM OF SPAIN':{points:[['2025-02-14',31.8],['2026-10-02',23.5]],n:100,first:'2025-02-14',last:'2026-10-02'}}},{},'ESP');
 assert.match(d,/ECB SovCISS · Spain/);assert.match(d,/this country.s own/);assert.match(d,/Euro crisis/);assert.doesNotMatch(d,/undefined|NaN/);
 for(const html of [a,b,c])assert.doesNotMatch(html,/undefined|NaN|\[object Object\]/);
});
test('sovereign rows carry IMF fundamentals when the packet has them: Debt\/GDP column, map tooltip, modal pills',()=>{
 const d=packet();const sov=d.groups.sovereign;const ita=sov.universe.find(e=>e.iso3==='ARG');assert.ok(ita);
 ita.fund={year:'2025',weo_year:'2026',debt_gdp:137.1,debt_gdp_weo:138.4,fiscal_bal_gdp:-3.1,cab_gdp:1.2,gdp_growth:0.5,inflation:1.6};
 d.fundamentals={n_sovereigns:1,year:'2025'};
 const rows=ui.universeRows('sovereign',sov);const r=rows.find(r=>r.iso3==='ARG');assert.equal(r.fund.debt_gdp,137.1);
 const tbl=ui.universeTable(rows,['Region','Tier','Debt\/GDP'],'none');assert.match(tbl,/137\.1%/);assert.match(tbl,/IMF WEO 2025: debt 137\.1% of GDP/);
 const world={w:100,h:50,paths:{ARG:'M0 0h10v10h-10z'},names:{ARG:'Argentina'}};assert.match(ui.worldMap(world,rows),/debt\/GDP 137\.1% \(IMF 2025\)/);
 const dash=ui.universeTable(rows.filter(r=>r.iso3!=='ARG').slice(0,3),['Debt\/GDP'],'none');assert.doesNotMatch(dash,/undefined|NaN/);
});
test('ECB SovCISS and IMF figures are visible outside the modal: table columns and map modes',()=>{
 const d=packet();const sov=d.groups.sovereign;const e=sov.universe.find(e=>e.iso3==='ARG');
 e.fund={year:'2025',weo_year:'2026',debt_gdp:80.3,debt_gdp_weo:78,fiscal_bal_gdp:-0.5,cab_gdp:1.2,gdp_growth:5,inflation:30};
 ui.state.history={long:{series:{sovciss_ARG:{iso3:'ARG',last:{date:'2026-10-08',value:0.265},pct_rank_since_2006:84,peaks:[{episode:'Euro crisis 2011-12',date:'2012-07-24',value:0.99}]}}}};
 try{
  const rows=ui.universeRows('sovereign',sov);const r=rows.find(r=>r.iso3==='ARG');assert.equal(r.stress.v,0.265);assert.equal(r.stress.p,84);
  const tbl=ui.universeTable(rows,['Region','Tier','Debt/GDP','Fiscal / CA','ECB stress'],'none');
  assert.match(tbl,/<th title="[^"]*ECB SovCISS[^"]*">ECB stress<\/th>/);assert.match(tbl,/0\.27<\/b> <span class="t-mute">· 84th/);assert.match(tbl,/-0\.5<\/span> <span class="t-mute">\/<\/span> <span class="">\+1\.2/);
  assert.doesNotMatch(tbl,/undefined|NaN/);
  const world={w:100,h:50,paths:{ARG:'M0 0h10v10h-10z',XXX:'M0 0h1v1z'},names:{XXX:'Nowhere'},centroids:{ARG:[5,5]}};
  const debt=ui.worldMap(world,rows,'debt');assert.match(debt,/debt 80\.3% of GDP \(IMF WEO 2025\) → 78% 2026e/);assert.ok(debt.includes(ui.mapColor('debt',80.3)));assert.match(debt,/>80%<\/text>/);
  const fis=ui.worldMap(world,rows,'fiscal');assert.match(fis,/fiscal balance -0\.5% of GDP/);
  const st=ui.worldMap(world,rows,'stress');assert.match(st,/ECB SovCISS 0\.265 \(2026-10-08\) · 84th pct since 2006/);
  const cds=ui.worldMap(world,rows,'cds');assert.match(cds,/cds-hatch/);
  [debt,fis,st,cds].forEach(h=>assert.doesNotMatch(h,/undefined|NaN/));
  assert.equal(ui.MAP_MODES.length,4);assert.notEqual(ui.mapColor('debt',20),ui.mapColor('debt',150));assert.equal(ui.mapColor('fiscal',null),null);
 }finally{ui.state.history=null;}
});
test('line chart supports a second axis and a two-branch band without NaN coordinates',()=>{
 const svg=ui.lineChart([['2006-01-01',1],['2016-01-01',2],['2026-01-01',3]],{axis2:true,series2:[['2024-09-01',200],['2026-01-01',150]],band:[['2024-09-01',300,40],['2026-01-01',280,42]],markers:[{date:'2008-11-01',value:2.5,label:'GFC'}]});
 assert.match(svg,/<svg/);assert.doesNotMatch(svg,/NaN|undefined/);assert.match(svg,/stroke-dasharray="3 2"/);assert.match(svg,/GFC/);
});
test('descriptive doctrine: packet fixture carries no call and the desk text never promises direction',()=>{
 const p=packet();assert.equal(p.decision.call,null);assert.equal(p.decision.sizing_eligible,false);
 const src=fs.readFileSync(path.join(__dirname,'../jh-cds-desk.js'),'utf8');
 assert.doesNotMatch(src,/\b(buy|sell|target price|forecast|predict)\b/i);
});
