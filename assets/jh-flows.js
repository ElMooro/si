/* jh-flows.js — JustHodl shared statement-flow (Sankey) renderer.
   ops 5013. One dependency-free engine for the GuruFocus-style
   Income / Balance-Sheet / Cash-Flow breakdown diagrams, usable by any
   research engine:

     JHFlows.statements(el, doc.statement_flows, {ticker:'AAOI'})

   or, for custom diagrams:

     JHFlows.render(el, {title, nodes:[{id,label,val,col,color}],
                         links:[{s,t,val,color,dash}], anchor, note})

   Geometry rule: node heights and ribbon widths are proportional to
   |value|; when a side's ribbons exceed the node (real accounting —
   e.g. APIC > total equity when retained earnings are negative) the
   ribbons compress to fit while the LABELS always carry the true
   numbers. Residual lines come pre-computed server-side — nothing is
   hidden to make a picture balance. */
(function(){
'use strict';
var C={bl:'#60a5fa',gn:'#34d399',rd:'#f87171',am:'#fbbf24',cy:'#22d3ee',
       gy:'#8fa3c0',dim:'#5d6b82',ink:'#dfe7f3'};
function fmt(v){
  if(v==null||isNaN(v))return '\u2014';
  var a=Math.abs(v),s=v<0?'-':'';
  if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';
  if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';
  if(a>=1e3)return s+'$'+(a/1e3).toFixed(0)+'K';
  return s+'$'+a.toFixed(0);
}
function esc(s){return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function colOf(v,fallback){return v<0?C.rd:(fallback||C.bl);}

function render(el, spec){
  var nodes=spec.nodes.filter(function(n){return n&&n.val!=null&&Math.abs(n.val)>1e-9;});
  var byId={};nodes.forEach(function(n){byId[n.id]=n;});
  var links=(spec.links||[]).filter(function(l){return byId[l.s]&&byId[l.t]&&Math.abs(l.val)>1e-9;});
  if(!nodes.length){el.innerHTML='<div style="color:'+C.dim+'">no flow data</div>';return;}
  var cols=1+Math.max.apply(null,nodes.map(function(n){return n.col;}));
  var W=980,L=182,R=196,T=14,B=26,NW=11,GAP=13;
  var colNodes=[];for(var c=0;c<cols;c++)colNodes.push(nodes.filter(function(n){return n.col===c;}));
  var Hbase=spec.h||430;
  var scale=1e9;
  colNodes.forEach(function(cn){if(!cn.length)return;
    var sum=cn.reduce(function(a,n){return a+Math.abs(n.val);},0);
    var s=(Hbase-T-B-GAP*(cn.length-1))/sum;
    if(s<scale)scale=s;});
  var H=Hbase;
  function X(c){return L+c*((W-L-R-NW)/Math.max(1,cols-1));}
  colNodes.forEach(function(cn){
    var hSum=cn.reduce(function(a,n){return a+Math.max(3,Math.abs(n.val)*scale);},0)+GAP*(cn.length-1);
    var y=T+Math.max(0,(H-T-B-hSum)/2);
    cn.forEach(function(n){n._h=Math.max(3,Math.abs(n.val)*scale);n._y=y;n._x=X(n.col);y+=n._h+GAP;});});
  // ribbon anchors with per-side compression
  nodes.forEach(function(n){n._oOff=0;n._iOff=0;
    var out=links.filter(function(l){return l.s===n.id;}).reduce(function(a,l){return a+Math.abs(l.val)*scale;},0);
    var inn=links.filter(function(l){return l.t===n.id;}).reduce(function(a,l){return a+Math.abs(l.val)*scale;},0);
    n._oK=out>n._h?n._h/out:1;n._iK=inn>n._h?n._h/inn:1;});
  var svg='';
  links.forEach(function(l){
    var s=byId[l.s],t=byId[l.t];
    var w0=Math.max(1.5,Math.abs(l.val)*scale*s._oK),w1=Math.max(1.5,Math.abs(l.val)*scale*t._iK);
    var ys=s._y+s._oOff, yt=t._y+t._iOff; s._oOff+=w0; t._iOff+=w1;
    var x0=s._x+NW, x1=t._x, mx=(x0+x1)/2;
    var col=l.color||colOf(l.val,byId[l.s].color);
    svg+='<path d="M'+x0.toFixed(1)+' '+ys.toFixed(1)
      +' C'+mx.toFixed(1)+' '+ys.toFixed(1)+' '+mx.toFixed(1)+' '+yt.toFixed(1)+' '+x1.toFixed(1)+' '+yt.toFixed(1)
      +' L'+x1.toFixed(1)+' '+(yt+w1).toFixed(1)
      +' C'+mx.toFixed(1)+' '+(yt+w1).toFixed(1)+' '+mx.toFixed(1)+' '+(ys+w0).toFixed(1)+' '+x0.toFixed(1)+' '+(ys+w0).toFixed(1)+' Z"'
      +' fill="'+col+'" opacity="0.42"'+(l.dash?' stroke="'+col+'" stroke-dasharray="4 3" stroke-opacity=".8" fill-opacity=".18"':'')+'>'
      +'<title>'+esc(byId[l.s].label)+' \u2192 '+esc(byId[l.t].label)+': '+fmt(l.val)+'</title></path>';});
  var anchor=spec.anchor;
  nodes.forEach(function(n){
    var col=n.color||colOf(n.val,C.bl);
    svg+='<rect x="'+n._x.toFixed(1)+'" y="'+n._y.toFixed(1)+'" width="'+NW+'" height="'+n._h.toFixed(1)+'" rx="2" fill="'+col+'"><title>'+esc(n.label)+': '+fmt(n.val)+'</title></rect>';
    var pct=(anchor&&Math.abs(anchor)>0)?' ('+(100*n.val/anchor).toFixed(1)+'%)':'';
    var left=n.col===0, xT=left?n._x-7:n._x+NW+7, ta=left?'end':'start';
    var yT=Math.max(T+8,Math.min(H-B,n._y+n._h/2-2));
    svg+='<text x="'+xT.toFixed(1)+'" y="'+yT.toFixed(1)+'" font-size="10" font-weight="700" fill="'+col+'" text-anchor="'+ta+'">'+esc(n.label)+'</text>'
       +'<text x="'+xT.toFixed(1)+'" y="'+(yT+11).toFixed(1)+'" font-size="9" fill="'+C.gy+'" text-anchor="'+ta+'">'+fmt(n.val)+esc(n.pct===false?'':pct)+'</text>';});
  el.innerHTML=(spec.title?'<div style="font-weight:800;color:'+C.ink+';margin:0 0 4px">'+esc(spec.title)+'</div>':'')
    +'<svg viewBox="0 0 '+W+' '+H+'" style="width:100%;display:block" role="img" aria-label="'+esc(spec.title||'flow diagram')+'">'+svg+'</svg>'
    +(spec.note?'<div style="color:'+C.dim+';font-size:10px;margin-top:4px">'+esc(spec.note)+'</div>':'');
}

/* ── semantic → diagram mappings ─────────────────────────────────── */
function mapIncome(I,tk){
  var n=[],l=[],c0=(I.segments&&I.segments.length)?0:-1;
  if(c0===0)I.segments.forEach(function(s,i){n.push({id:'sg'+i,label:s.name,val:s.val,col:0,color:C.bl});l.push({s:'sg'+i,t:'rev',val:s.val});});
  var b=c0+1;
  n.push({id:'rev',label:'Revenue',val:I.revenue,col:b,color:C.bl});
  n.push({id:'gp',label:'Gross Profit',val:I.gross_profit,col:b+1,color:I.gross_profit<0?C.rd:C.gn});
  n.push({id:'cogs',label:'COGS',val:I.cogs,col:b+1,color:C.rd});
  l.push({s:'rev',t:'gp',val:I.gross_profit,color:I.gross_profit<0?C.rd:C.gn});
  l.push({s:'rev',t:'cogs',val:I.cogs,color:C.rd});
  if(I.opex_total!=null){
    n.push({id:'op',label:'Operating Income',val:I.operating_income,col:b+2,color:colOf(I.operating_income,C.gn)});
    n.push({id:'ox',label:'Total Operating Expense',val:I.opex_total,col:b+2,color:C.rd});
    l.push({s:'gp',t:'op',val:I.operating_income,color:colOf(I.operating_income,C.gn)});
    l.push({s:'gp',t:'ox',val:I.opex_total,color:C.rd});
    if(I.sga){n.push({id:'sga',label:'SG&A',val:I.sga,col:b+3,color:C.rd});l.push({s:'ox',t:'sga',val:I.sga,color:C.rd});}
    if(I.rnd){n.push({id:'rnd',label:'R&D',val:I.rnd,col:b+3,color:C.rd});l.push({s:'ox',t:'rnd',val:I.rnd,color:C.rd});}
    if(Math.abs(I.other_opex)>0.002*I.revenue){n.push({id:'oox',label:'Other Operating Expense',val:I.other_opex,col:b+3,color:colOf(-I.other_opex,C.rd)});l.push({s:'ox',t:'oox',val:I.other_opex,color:C.rd});}
  }
  if(I.pretax!=null){
    n.push({id:'pt',label:'Pretax Income',val:I.pretax,col:b+3,color:colOf(I.pretax,C.gn)});
    l.push({s:'op',t:'pt',val:I.operating_income,color:colOf(I.operating_income,C.gn)});
    if(Math.abs(I.interest_net)>0.001*I.revenue){n.push({id:'int',label:'Net Interest Income',val:I.interest_net,col:b+2,color:colOf(I.interest_net,C.gn)});l.push({s:'int',t:'pt',val:I.interest_net,color:colOf(I.interest_net,C.gn)});}
    if(Math.abs(I.other_income)>0.001*I.revenue){n.push({id:'oi',label:'Other Income (Expense)',val:I.other_income,col:b+2,color:colOf(I.other_income,C.gn)});l.push({s:'oi',t:'pt',val:I.other_income,color:colOf(I.other_income,C.gn)});}
    n.push({id:'ni',label:'Net Income',val:I.net_income,col:b+4,color:colOf(I.net_income,C.gn)});
    n.push({id:'tax',label:'Tax'+(I.tax_rate_pct!=null?' \u00b7 rate '+I.tax_rate_pct+'%':''),val:I.tax,col:b+4,color:colOf(-I.tax,C.rd)});
    l.push({s:'pt',t:'ni',val:I.net_income,color:colOf(I.net_income,C.gn)});
    l.push({s:'pt',t:'tax',val:I.tax,color:colOf(-I.tax,C.rd)});
  }
  return {title:'Income Statement Breakdown \u00b7 '+tk,nodes:n,links:l,anchor:I.revenue,h:440,
          note:'percentages are of revenue \u00b7 negative figures shown in red with true signs'};
}
function mapBalance(B,tk){
  var n=[],l=[],A=B.total_assets;
  function comp(id,label,val,target,color){if(Math.abs(val)<=0.002*A)return;
    n.push({id:id,label:label,val:val,col:0,color:color||C.gn});l.push({s:id,t:target,val:val,color:color||C.gn});}
  comp('cash','Total Cash & ST Inv',B.cur.cash_sti,'ca');
  comp('rec','Total Receivables',B.cur.receivables,'ca');
  comp('inv','Total Inventories',B.cur.inventory,'ca');
  comp('ocu','Other Current Assets',B.cur.other,'ca');
  comp('ppe','Net PPE',B.lt.ppe,'lta');
  comp('gw','Goodwill',B.lt.goodwill,'lta');
  comp('itg','Intangible Assets',B.lt.intangibles,'lta');
  comp('lti','LT Investments',B.lt.lt_invest,'lta');
  comp('olt','Other LT Assets',B.lt.other,'lta');
  n.push({id:'ca',label:'Total Current Assets',val:B.cur.total,col:1,color:C.gn});
  n.push({id:'lta',label:'Total LT Assets',val:B.lt.total,col:1,color:C.gn});
  n.push({id:'ta',label:'Total Assets',val:A,col:2,color:C.bl});
  l.push({s:'ca',t:'ta',val:B.cur.total,color:C.gn});l.push({s:'lta',t:'ta',val:B.lt.total,color:C.gn});
  n.push({id:'tl',label:'Total Liabilities',val:B.liab.total,col:3,color:C.rd});
  n.push({id:'te',label:'Total Stockholders Equity',val:B.equity.total,col:3,color:colOf(B.equity.total,C.gn)});
  l.push({s:'ta',t:'tl',val:B.liab.total,color:C.rd});l.push({s:'ta',t:'te',val:B.equity.total,color:colOf(B.equity.total,C.gn)});
  n.push({id:'cl',label:'Total Current Liabilities',val:B.liab.cur_total,col:4,color:C.rd});
  n.push({id:'ll',label:'Total LT Liabilities',val:B.liab.lt_total,col:4,color:C.rd});
  l.push({s:'tl',t:'cl',val:B.liab.cur_total,color:C.rd});l.push({s:'tl',t:'ll',val:B.liab.lt_total,color:C.rd});
  function liab5(id,label,val,src){if(Math.abs(val)<=0.002*A)return;
    n.push({id:id,label:label,val:val,col:5,color:C.rd});l.push({s:src,t:id,val:val,color:C.rd});}
  liab5('ap','Acct Payable & Accr. Exp',B.liab.ap,'cl');
  liab5('std','ST Debt',B.liab.st_debt,'cl');
  liab5('drev','Deferred Tax & Revenue',B.liab.deferred_rev,'cl');
  liab5('ocl','Other Current Liabilities',B.liab.other_cur,'cl');
  liab5('ltd','Long-Term Debt',B.liab.lt_debt,'ll');
  liab5('oll','Other LT Liabilities',B.liab.other_lt,'ll');
  function eq4(id,label,val){if(Math.abs(val)<=0.0008*A)return;
    n.push({id:id,label:label,val:val,col:4,color:colOf(val,C.gn)});l.push({s:'te',t:id,val:val,color:colOf(val,C.gn)});}
  eq4('cs','Common Stock',B.equity.common);
  eq4('apic','Additional Paid-In Capital',B.equity.apic);
  eq4('re','Retained Earnings',B.equity.retained);
  eq4('aoci','Accumulated Other Compr.',B.equity.aoci);
  eq4('oeq','Other Equity',B.equity.other);
  return {title:'Balance Sheet Breakdown \u00b7 '+tk,nodes:n,links:l,anchor:A,h:470,
          note:'percentages are of total assets \u00b7 assets green, liabilities red \u00b7 negative equity components (e.g. retained deficit) in red with true signs'};
}
function mapCash(Cf,tk){
  var n=[],l=[];
  function op(id,label,val){if(Math.abs(val)<1)return;
    var pos=val>=0;n.push({id:id,label:label,val:val,col:0,color:colOf(val,C.gn)});
    l.push({s:id,t:pos?'opi':'opo',val:val,color:colOf(val,C.gn)});}
  op('ni','NI from Cont. Operations',Cf.ni);op('dda','D&A',Cf.dda);
  op('wc','Change in Working Capital',Cf.wc_change);op('dt','Deferred Tax',Cf.deferred_tax);
  op('sbc','Stock-Based Compensation',Cf.sbc);op('oop','Other Operating Activities',Cf.other_operating);
  var opi=[Cf.ni,Cf.dda,Cf.wc_change,Cf.deferred_tax,Cf.sbc,Cf.other_operating].filter(function(v){return v>0;}).reduce(function(a,b){return a+b;},0);
  var opo=[Cf.ni,Cf.dda,Cf.wc_change,Cf.deferred_tax,Cf.sbc,Cf.other_operating].filter(function(v){return v<0;}).reduce(function(a,b){return a+b;},0);
  n.push({id:'opi',label:'Operating Inflow',val:opi,col:1,color:C.gn});
  n.push({id:'opo',label:'Operating Outflow',val:opo,col:1,color:C.rd});
  n.push({id:'cfo',label:'CF from Operations',val:Cf.cfo,col:2,color:colOf(Cf.cfo,C.gn)});
  l.push({s:'opi',t:'cfo',val:opi,color:C.gn});l.push({s:'opo',t:'cfo',val:opo,color:C.rd});
  if(Math.abs(Cf.capex)>1){n.push({id:'cap',label:'CapEx',val:Cf.capex,col:0,color:C.rd});l.push({s:'cap',t:'cfi',val:Cf.capex,color:C.rd});}
  if(Math.abs(Cf.other_investing)>1){n.push({id:'oiv',label:'Other Investing Activities',val:Cf.other_investing,col:0,color:colOf(Cf.other_investing,C.gn)});l.push({s:'oiv',t:'cfi',val:Cf.other_investing,color:colOf(Cf.other_investing,C.gn)});}
  n.push({id:'cfi',label:'CF from Investing',val:Cf.cfi,col:2,color:colOf(Cf.cfi,C.gn)});
  function fin(id,label,val){if(Math.abs(val)<1)return;
    n.push({id:id,label:label,val:val,col:0,color:colOf(val,C.gn)});l.push({s:id,t:'cff',val:val,color:colOf(val,C.gn)});}
  fin('iss','Net Issuance of Stock',Cf.stock_issued);fin('rep','Stock Repurchased',Cf.stock_repurchased);
  fin('dbt','Net Issuance of Debt',Cf.debt_net);fin('div','Dividends Paid',Cf.dividends);
  fin('ofn','Other Financing Activities',Cf.other_financing);
  n.push({id:'cff',label:'CF from Financing',val:Cf.cff,col:2,color:colOf(Cf.cff,C.gn)});
  n.push({id:'fcf',label:'Free Cash Flow',val:Cf.fcf,col:3,color:colOf(Cf.fcf,C.gn)});
  l.push({s:'cfo',t:'fcf',val:Cf.cfo,color:colOf(Cf.cfo,C.gn)});
  l.push({s:'cfi',t:'fcf',val:Cf.capex,color:C.rd,dash:true});
  n.push({id:'chg',label:'Net Change in Cash',val:Cf.net_change,col:3,color:colOf(Cf.net_change,C.gn)});
  l.push({s:'cfo',t:'chg',val:Cf.cfo,color:colOf(Cf.cfo,C.gn)});
  l.push({s:'cfi',t:'chg',val:Cf.cfi,color:colOf(Cf.cfi,C.gn)});
  l.push({s:'cff',t:'chg',val:Cf.cff,color:colOf(Cf.cff,C.gn)});
  if(Math.abs(Cf.fx)>1){n.push({id:'fx',label:'FX Effects',val:Cf.fx,col:2,color:colOf(Cf.fx,C.gn)});l.push({s:'fx',t:'chg',val:Cf.fx,color:colOf(Cf.fx,C.gn)});}
  if(Cf.begin_cash!=null){n.push({id:'beg',label:'Beginning Cash',val:Cf.begin_cash,col:3,color:C.gn});l.push({s:'beg',t:'end',val:Cf.begin_cash,color:C.gn});}
  if(Cf.end_cash!=null){n.push({id:'end',label:'Ending Cash',val:Cf.end_cash,col:4,color:colOf(Cf.end_cash,C.gn)});l.push({s:'chg',t:'end',val:Cf.net_change,color:colOf(Cf.net_change,C.gn)});}
  return {title:'Cashflow Statement Breakdown \u00b7 '+tk,nodes:n,links:l,anchor:null,h:470,
          note:'FCF = CFO + CapEx (dashed tributary) \u00b7 inflows green, outflows red \u00b7 beginning + net change (+FX) = ending cash'};
}

function statements(el, sf, opts){
  opts=opts||{};var tk=opts.ticker||'';
  if(!sf||!sf.available){el.innerHTML='<div style="color:'+C.dim+'">statement flows unavailable \u2014 '
    +esc((sf||{}).reason||'no data')+'. Nothing is invented in its place.</div>';return;}
  var idx=0;
  function draw(){
    var p=sf.periods[idx];
    var pills=sf.periods.map(function(q,i){
      return '<button data-i="'+i+'" style="background:'+(i===idx?'rgba(34,211,238,.15)':'transparent')
        +';border:1px solid '+(i===idx?C.cy:'#232c3f')+';color:'+(i===idx?C.cy:C.dim)
        +';border-radius:6px;padding:2px 10px;font-size:.64rem;font-weight:700;cursor:pointer;margin-right:6px;font-family:inherit">'
        +esc(q.label)+(q.kind==='quarter'?' \u00b7 Q':'')+'</button>';}).join('');
    var h='<div style="margin:0 0 10px">'+pills+'</div>';
    h+='<div class="jhf-slot" id="jhf-i"></div><div class="jhf-slot" id="jhf-b" style="margin-top:16px"></div><div class="jhf-slot" id="jhf-c" style="margin-top:16px"></div>';
    h+='<div style="color:'+C.dim+';font-size:10px;margin-top:6px">'+esc(sf.note||'')+'</div>';
    el.innerHTML=h;
    var slots=[['jhf-i',p.income,mapIncome,'income statement'],
               ['jhf-b',p.balance,mapBalance,'balance sheet'],
               ['jhf-c',p.cashflow,mapCash,'cash-flow statement']];
    slots.forEach(function(s){var host=el.querySelector('#'+s[0]);
      if(!s[1]){host.innerHTML='<div style="color:'+C.dim+'">'+s[3]+' \u2014 not reconcilable for '+esc(p.label)+'; not drawn.</div>';return;}
      render(host,s[2](s[1],tk+' \u00b7 '+p.label));
      if(s[1].recon_ok===false)host.insertAdjacentHTML('beforeend',
        '<div style="color:'+C.am+';font-size:10px">\u26a0 source lines do not fully reconcile for this period \u2014 figures shown as reported.</div>');});
    Array.prototype.forEach.call(el.querySelectorAll('button[data-i]'),function(b){
      b.onclick=function(){idx=+b.getAttribute('data-i');draw();};});
  }
  draw();
}

window.JHFlows={render:render,statements:statements,v:'5013'};
})();
