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
 const b=ui.modalHtml('STATE OF QATAR','Qatar','sovereign','EM',h,{});
 assert.match(b,/No print ever fixed the sign/);assert.match(b,/either \/ or/);
 const c=ui.modalHtml('UNKNOWN','Canada','sovereign','DM',h,{});
 assert.match(c,/no public DTCC print since the tape began/);
 assert.equal(ui.proxyFor('us_corp','',''),'baa10y');assert.equal(ui.proxyFor('sovereign','Frontier',''),'ofr_em');assert.equal(ui.proxyFor('sovereign','DM',''),'ofr_credit');assert.equal(ui.proxyFor('index','','IDX:CDX.EM'),'ofr_em');
 for(const html of [a,b,c])assert.doesNotMatch(html,/undefined|NaN|\[object Object\]/);
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
