/* ETF consumers use the verified native desk. No raw-provider fill or unit guessing. */
(function(w){
  'use strict';
  if(w.JHEtfFuse)return;
  let cached=null,pending=null,at=0;const histories=new Map();
  const api=()=>{if(!w.JHEtfDeskResearch)throw Error('Native ETF evidence client unavailable');return w.JHEtfDeskResearch;};
  const bare=s=>{const t=String(s||'').trim().toUpperCase().split(':').pop();return /^[A-Z][A-Z0-9.-]{0,14}$/.test(t)?t:'';};
  const num=v=>v!==null && v!==undefined && v!=='' && typeof v!=='boolean' && Number.isFinite(Number(v))?Number(v):null;
  async function desk(force=false){
    if(!force && cached && Date.now()-at<180000)return cached;
    if(pending)return pending;
    pending=(async()=>{const A=api(),loaded=await A.load(A.CURRENT,w.fetch.bind(w)),p=await A.verifyPacket(loaded.doc,w.fetch.bind(w));
      cached=p;at=Date.now();return p;})();
    try{return await pending;}finally{pending=null;}
  }
  function adapt(p,t){
    const f=p.funds[t];if(!f)return null;
    const A=api(),summary=f.profiles.current.summary,text=k=>summary?.text?.[k]?.value;
    const values={};for(const n of [1,5,21])values['flow_'+n+'d']=num(A.windowValue(f,n));
    return {native:true,ticker:t,fund:f,packet:p,name:text('description'),issuer:text('issuer'),asset_class:text('asset_class'),benchmark:text('primary_benchmark'),
      ...values,flow_label:values.flow_1d===null?'UNAVAILABLE':values.flow_1d>0?'REPORTED INFLOW':values.flow_1d<0?'REPORTED OUTFLOW':'REPORTED ZERO',
      flow_asof:p.reference.end_date,flow_effective:f.flows.latest_effective_date,aum:null,er:null,hhi:null,top:[],
      source_check_current:A.eligibility(f),scope:'Fund-level reporting observations; no underlying stock purchases or forecast.',
      call:null,portfolio_action:'WAIT',calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false};
  }
  async function of(t){return adapt(await desk(),bare(t));}
  async function fullHist(t,boundPacket){
    t=bare(t);if(!t)return [];
    const p=boundPacket||await desk();if(!api().typed(p))throw Error('Bound native desk required');
    const f=p.funds[t];if(!f)return [];
    const key=f.flows.history.key;
    if(histories.has(key))return histories.get(key);
    const h=await api().history(p,t,w.fetch.bind(w));
    const rows=h.history.filter(r=>api().decimal(r.flow_decimal)).map(r=>({d:r.date,f:num(r.flow_decimal),n:num(r.nav_decimal),s:num(r.shares_decimal),
      raw_decimal:r.flow_decimal,processed_date:r.processed_date,source_rows:r.source_rows,history_sha256:f.flows.history.sha256,
      retrospective:true,point_in_time_eligible:false}));
    histories.set(key,rows);return rows;
  }
  async function derived(){return {contract:'etf-desk-context.v1',native:true,packet:await desk(),by_ticker:{},verdicts:{}};}
  async function ofDerived(t){const row=await of(t);return row?{native:true,fund:row.fund,packet:row.packet}:null;}
  async function constituents(t){const row=await of(t);return {native:true,ticker:bare(t),n:row?.fund?.holdings?.current?.quality?.returned_rows??null,
    rows:[],snapshot:row?.fund?.holdings?.current?.snapshot,scope:'Complete dated rows are inspected on the ETF research desk; no ticker-only consolidation.'};}
  function fmtUsd(v){v=num(v);return v===null?'Unavailable':v.toLocaleString('en-US',{style:'currency',currency:'USD',maximumFractionDigits:2});}
  const fmtEr=()=> 'Scale unqualified';
  const wgt=v=>v===null || v===undefined?'Unavailable':String(v)+' raw; scale unqualified';
  const live=async()=>null,reverse=async()=>[],reverseFromDesk=()=>[],impliedDemand=()=>null,census=async()=>null,rankVs=()=>null;
  const isFund=row=>!!(row?.native && row.fund?.ticker===row.ticker);
  const mergeLive=row=>row||{},histFrom=row=>row?.native?row.flow_hist||[]:[];
  const compactToRows=()=>[],polyRows=j=>Array.isArray(j?.results)?j.results:[];
  function mergeHist(a,b){
    const all=[...(a||[]),...(b||[])],by=new Map();
    for(const row of all){if(!/^[a-f0-9]{64}$/.test(row.history_sha256||''))throw Error('Verified native history required');
      if(by.has(row.d) && JSON.stringify(by.get(row.d))!==JSON.stringify(row))throw Error('Do not merge different source vintages');by.set(row.d,row);}
    return [...by.values()].sort((x,y)=>x.d.localeCompare(y.d));
  }
  const markers=()=>[]; // Effective-date reporting is retrospective, never a buy/sell marker.
  function ymd(t) {
    var d = new Date((Number(t) || 0) * 1000);
    if (!isFinite(d.getTime())) return "";
    return d.toISOString().slice(0, 10);
  }
  function barStep(bars) {
    if (!bars || bars.length < 2) return 86400;
    var gaps = [], i, n = Math.min(bars.length - 1, 80);
    for (i = bars.length - n; i < bars.length; i++) if (i > 0) gaps.push(bars[i].time - bars[i - 1].time);
    if (!gaps.length) return 86400;
    gaps.sort(function (a, b) { return a - b; });
    return gaps[Math.floor(gaps.length / 2)] || 86400;
  }
  function isIntraBars(bars) {
    var step = barStep(bars);
    if (step < 20 * 3600) return true;
    var n = Math.min(bars.length - 1, 40), small = 0, i;
    for (i = bars.length - n; i < bars.length; i++) {
      if (i > 0 && bars[i].time - bars[i - 1].time < 20 * 3600) small++;
    }
    return n > 0 && small >= n * 0.35;
  }
  function alignHist(hist, bars) {
    /* Map official daily fund_flow onto whatever tick the chart pulled.
       Daily: 1:1 on UTC session date. Weekly/monthly/multi-day: SUM the
       prints whose dates fall in [bar.time, nextBar.time). Intraday: the
       day's total sits on the last bar of that session so the histogram
       is one accurate print per day, not a fake per-minute flow. */
    if (!hist || !hist.length || !bars || !bars.length) return [];
    var byDay = {}, i;
    hist.forEach(function (h) {
      if (!h || !h.d) return;
      var d = String(h.d).slice(0, 10);
      var f = num(h.f);
      if (!d || f == null) return;
      byDay[d] = (byDay[d] == null ? 0 : byDay[d]) + f;
    });
    var step = barStep(bars);
    var intra = isIntraBars(bars);
    var out = [];
    if (intra) {
      var lastOf = {};
      for (i = 0; i < bars.length; i++) lastOf[ymd(bars[i].time)] = i;
      Object.keys(lastOf).forEach(function (d) {
        if (byDay[d] == null) return;
        var b = bars[lastOf[d]];
        out.push({ time: b.time, value: byDay[d] / 1e9, raw: byDay[d], d: d, n: 1 });
      });
      out.sort(function (a, b) { return a.time - b.time; });
      return out;
    }
    var daily = step > 0 && step < 36 * 3600;
    for (i = 0; i < bars.length; i++) {
      var t0 = bars[i].time;
      var d0 = ymd(t0);
      if (!d0) continue;
      if (daily) {
        if (byDay[d0] == null) continue;
        out.push({ time: t0, value: byDay[d0] / 1e9, raw: byDay[d0], d: d0, n: 1 });
        continue;
      }
      var t1 = i + 1 < bars.length ? bars[i + 1].time : t0 + Math.max(step, 86400);
      var d1 = ymd(t1);
      if (!d1 || d1 <= d0) d1 = ymd(t0 + Math.max(step, 86400));
      var sum = 0, n = 0, d;
      for (d in byDay) {
        if (d >= d0 && d < d1) { sum += byDay[d]; n++; }
      }
      if (!n) continue;
      out.push({ time: t0, value: sum / 1e9, raw: sum, d: d0, n: n });
    }
    return out;
  }

  w.JHEtfFuse={native:true,desk,of,fullHist,derived,ofDerived,constituents,fmtUsd,fmtEr,wgt,num,bare,live,reverse,reverseFromDesk,impliedDemand,
    census,rankVs,isFund,mergeLive,histFrom,compactToRows,polyRows,mergeHist,markers,alignHist,
    holdingsIndex:async()=>({native:true,by_stock:{}}),invertHoldings:()=>({})};
})(window);
