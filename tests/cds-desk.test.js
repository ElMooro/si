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
  assert.match(tbl,/<th title="[^"]*SovCISS[^"]*CISS[^"]*CLIFS[^"]*">ECB stress<\/th>/);assert.match(tbl,/0\.27<\/b> <span class="t-mute">· 84th · sov/);assert.match(tbl,/-0\.5<\/span> <span class="t-mute">\/<\/span> <span class="">\+1\.2/);
  assert.doesNotMatch(tbl,/undefined|NaN/);
  const world={w:100,h:50,paths:{ARG:'M0 0h10v10h-10z',XXX:'M0 0h1v1z'},names:{XXX:'Nowhere'},centroids:{ARG:[5,5]}};
  const debt=ui.worldMap(world,rows,'debt');assert.match(debt,/debt 80\.3% of GDP \(IMF WEO 2025\) → 78% 2026e/);assert.ok(debt.includes(ui.mapColor('debt',80.3)));assert.match(debt,/>80%<\/text>/);
  const fis=ui.worldMap(world,rows,'fiscal');assert.match(fis,/fiscal balance -0\.5% of GDP/);
  const st=ui.worldMap(world,rows,'stress');assert.match(st,/ECB SovCISS 0\.265 \(2026-10-08, sovereign-market stress\) · 84th pct since 2006/);
  const cds=ui.worldMap(world,rows,'cds');assert.match(cds,/cds-hatch/);
  [debt,fis,st,cds].forEach(h=>assert.doesNotMatch(h,/undefined|NaN/));
  assert.equal(ui.MAP_MODES.length,6);assert.notEqual(ui.mapColor('debt',20),ui.mapColor('debt',150));assert.equal(ui.mapColor('fiscal',null),null);
  // v1.5: packet-side `stress` (any ECB family) wins over the history lookup and names its family; CISS / CLIFS fall back by priority
  const e2=sov.universe.find(e=>e.iso3==='ECU');e2.stress={family:'ciss',value:0.008,date:'2026-10-08',pct_rank_since_2006:15.6,frequency:'daily',gfc_peak:0.896};
  const rows2=ui.universeRows('sovereign',sov);const r2=rows2.find(r=>r.iso3==='ECU');assert.equal(r2.stress.fam,'ciss');assert.equal(r2.stress.tag,'sys');
  const tbl2=ui.universeTable(rows2,['ECB stress'],'none');assert.match(tbl2,/0\.01<\/b> <span class="t-mute">· 16th · sys/);assert.match(tbl2,/ECB CISS 0\.008 on 2026-10-08 \(systemic financial stress, daily\)[^"]*2008 peak 0\.896/);
  ui.state.history={long:{series:{clifs_ECU:{iso3:'ECU',frequency:'monthly',last:{date:'2026-08-31',value:0.093},pct_rank_since_2006:55.6,peaks:[]},ciss_ARG:{iso3:'ARG',last:{date:'2026-10-08',value:0.02},pct_rank_since_2006:30,peaks:[]},sovciss_ARG:{iso3:'ARG',last:{date:'2026-10-08',value:0.265},pct_rank_since_2006:84,peaks:[]}}}};
  delete e2.stress;const rows3=ui.universeRows('sovereign',sov);assert.equal(rows3.find(r=>r.iso3==='ECU').stress.fam,'clifs');assert.equal(rows3.find(r=>r.iso3==='ARG').stress.fam,'sovciss');
  assert.equal(ui.proxyFor('sovereign','EM','ECUADOR','ECU',ui.state.history.long.series),'clifs_ECU');assert.equal(ui.proxyFor('sovereign','EM','ARG','ARG',ui.state.history.long.series),'sovciss_ARG');
  const st2=ui.worldMap(world,rows3,'stress');assert.match(st2,/ECB SovCISS 0\.265 \(2026-10-08, sovereign-market stress\)/);assert.doesNotMatch(st2,/undefined|NaN/);
 }finally{ui.state.history=null;}
});
test('v1.6 risk layers: rating / reserves / private-debt / bank-holding columns, rating + private-debt map modes, four new panels render from the packet',()=>{
 const d=packet();const sov=d.groups.sovereign;const e=sov.universe.find(e=>e.iso3==='ARG');
 e.rating={consensus:'CCC+',notch:5,n_agencies:4,neg:1,pos:1,last_action:'2026-09-25',agencies:{"S&P":{rating:'CCC+',outlook:'Stable',date:'2025-11-01'},"Moody's":{rating:'Caa1',outlook:'Positive',date:'2026-01-10'}}};
 e.ara={year:'2025',ara:0.34,res_months_imports:3.0,res_std_cover:0.5,res_m2_pct:9.1};
 e.pdebt={year:'2024',private_debt_gdp:36.2,hh_debt_gdp:4.1,nfc_debt_gdp:32.1};
 const fra=sov.universe.find(e=>e.iso3==='CYP');assert.ok(fra);
 fra.banks={period:'2025-S2',ea_banks_eur_bn:789.9,share_of_ea_sov_book_pct:20.2,home_banks_eur_bn:642.3,home_banks_share_pct:81.3,chg_yoy_pct:9.4};
 fra.eba={period:'2026-06-30',home_bias_pct:45.2,other_eu_pct:30.1,amortised_cost_pct:57.3,long_10y_pct:18.2,total_eur_bn:1200.5};
 const rows=ui.universeRows('sovereign',sov);const r=rows.find(r=>r.iso3==='ARG');assert.equal(r.rating.notch,5);assert.equal(r.ara.ara,0.34);
 const cols=['Region','Tier','Rating','Debt/GDP','Fiscal / CA','Reserves','Private debt','Banks hold','ECB stress'];
 const tbl=ui.universeTable(rows,cols,'none');
 assert.match(tbl,/<b class="t-neg">CCC\+<\/b> <span class="t-mute">· 4<\/span> <span class="pill neg"[^>]*>1 neg<\/span>/);
 assert.match(tbl,/<b class="t-neg">0\.34×<\/b> ARA <span class="t-mute">· 3\.0m<\/span>/);
 assert.match(tbl,/36%<\/span> <span class="t-mute">· hh 4 · nfc 32<\/span>/);
 assert.match(tbl,/<b>€790bn<\/b> <span class="t-mute">· own 81\.3%<\/span> <span class="pill mute" title="EBA home bias">home 45%<\/span>/);
 assert.match(tbl,/IMF assesses reserve adequacy for emerging markets only/);
 assert.doesNotMatch(tbl,/undefined|NaN|\[object Object\]/);
 const world={w:100,h:50,paths:{ARG:'M0 0h10v10h-10z',CYP:'M20 0h10v10h-10z'},names:{},centroids:{ARG:[5,5],CYP:[25,5]}};
 const rm=ui.worldMap(world,rows,'rating');assert.match(rm,/CCC\+ consensus \(notch 5, 4 agencies, 1 negative outlook\) · ESMA/);assert.match(rm,/>CCC\+<\/text>/);assert.ok(rm.includes(ui.mapColor('rating',5)));assert.notEqual(ui.mapColor('rating',5),ui.mapColor('rating',21));
 const pm=ui.worldMap(world,rows,'pdebt');assert.match(pm,/private debt 36% of GDP \(IMF GDD 2024\)/);
 [rm,pm].forEach(h=>assert.doesNotMatch(h,/undefined|NaN/));
 assert.equal(ui.titleCase('BOLIVIA, PLURINATIONAL STATE OF'),'Bolivia, Plurinational State of');
 // panels: fake a minimal DOM
 const els={};const mk=id=>els[id]||(els[id]={id,innerHTML:'',textContent:''});
 global.document={getElementById:id=>mk(id)};
 try{
  d.bank_sovereign={period:'2025-S2',ea_banks_total_sov_eur_bn:3902.8,read:'Who holds whom.',rows:[['CYP',789.9,20.2,642.3,81.3,9.4]],columns:['iso3','ea','share','home','home_share','yoy'],by_iso3:{CYP:{history:[['2024-12-31',722],['2025-06-30',760],['2025-12-31',789.9]]}},aggregates:{W0:{label:'all counterparties',eur_bn:3902.8}}};
  d.layers={status:{imf_ara_gdd:'ok',ecb_sup:'ok',esma:'ok',eba:'ok',ofr_form_pf:'ok',us_credit:'ok'},eba:{period:'2026-06-30',n_sovereigns:30,read:''},esma:{as_of:'2026-10-09',n_sovereigns:116,read:'Tape.',recent_actions:[['2026-10-05','KGZ','KYRGYZSTAN',"Moody's",'Upgrade','B2'],['2026-09-25','ARG','ARGENTINA','S&P','Downgrade','CCC']]}};
  d.hedge_funds={as_of:'2026-06-30',n_series:3,read:'Books.',series:{sov_gne:{label:'Sovereign GNE',mnemonic:'X',last:{date:'2026-06-30',value:2774},chg_yoy_pct:12.2,max:2774,max_date:'2026-06-30',points:[['2025-06-30',2473],['2025-12-31',2600],['2026-06-30',2774]]},cds_up250_p50:{label:'stress',mnemonic:'Y',last:{date:'2026-06-30',value:-3},chg_yoy_pct:null,max:0,max_date:'',points:[]}}};
  d.us_credit={cmdi:{market:{date:'2026-09-25',value:0.2,pct_rank_since_2005:48.8,max:0.81,tail:[['2026-09-11',0.19],['2026-09-18',0.21],['2026-09-25',0.2]]},ig:{date:'2026-09-25',value:0.25,pct_rank_since_2005:56,max:0.9},read:'Gauge.'},fdic:{quarter:'2026-06-30',read:'Banks.',aggregates:{n_banks:4313,uninsured_share:40.42,htm_loss_bn:216.9,htm_loss_to_equity:8.22,cre_to_tier1:132.1,n_cre_gt300_tier1:1363,n_htm_loss_gt50_equity:9,noncurrent_ratio:0.94},series:{SYS_HTM_LOSS_TO_EQUITY:{tail:[['2026-03-31',8.0],['2026-06-30',8.22]]}},screen:[['AMERICAN BUSINESS BANK','CA',4417,76,19,477,0,0,0,0,95]]}};
  ui.renderLayers(d);
  assert.match(els['cdsdesk-nexus'].innerHTML,/<b>€790bn<\/b>/);assert.match(els['cdsdesk-nexus'].innerHTML,/81%/);assert.match(els['cdsdesk-nexus'].innerHTML,/45%/);
  assert.match(els['cdsdesk-ratings'].innerHTML,/Kyrgyzstan/);assert.match(els['cdsdesk-ratings'].innerHTML,/Downgrade/);assert.doesNotMatch(els['cdsdesk-ratings'].innerHTML,/KYRGYZSTAN/);
  assert.match(els['cdsdesk-hf'].innerHTML,/\$2\.77tn/);assert.match(els['cdsdesk-hf'].innerHTML,/\+12% y\/y/);assert.match(els['cdsdesk-hf'].innerHTML,/-3\.0%/);
  assert.match(els['cdsdesk-uscredit'].innerHTML,/MARKET <b>0\.20<\/b>/);assert.match(els['cdsdesk-uscredit'].innerHTML,/\$217bn/);assert.match(els['cdsdesk-uscredit-tag'].textContent,/4,313 banks/);assert.match(els['cdsdesk-uscredit'].innerHTML,/AMERICAN BUSINESS BANK/);
  Object.values(els).forEach(el=>assert.doesNotMatch(el.innerHTML+el.textContent,/undefined|NaN|\[object Object\]/));
  // empty packet: every panel degrades to a pending note, never throws
  Object.values(els).forEach(el=>{el.innerHTML='';});ui.renderLayers(Object.assign({},packet(),{layers:{status:{}}}));
  assert.match(els['cdsdesk-nexus'].innerHTML,/not in this packet yet/);assert.match(els['cdsdesk-hf'].innerHTML,/not attached yet/);
 }finally{delete global.document;}
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
