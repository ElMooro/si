/* Descriptive Compound research. No ranking votes, polling, storage or trades. */
(function(root){
  'use strict';
  const E=typeof module==='object'&&module.exports?require('./jh-research-explain.js'):root.JHResearchExplain;
  const CONTRACT='compound-overlays.v1', own=(o,k)=>Object.prototype.hasOwnProperty.call(o,k);
  const printable=v=>v===undefined?'Not reported':typeof v==='string'?v:JSON.stringify(v);
  const display=v=>E.finite(v)?String(v):'Unavailable';
  function model(packet){
    const selected=E.collection(packet,['compound','ranked']),counts=new Map();
    for(const r of selected.rows){const name=E.identity(r.value);if(name)counts.set(name,(counts.get(name)||0)+1);}
    const rows=selected.rows.map(({value,pointer})=>{
      const name=E.identity(value),duplicate=!!name&&counts.get(name)>1,calc=value.desk_score_calculation;
      const current=packet?.overlay_contract===CONTRACT&&E.object(calc)&&calc.contract===CONTRACT&&calc.status==='usable'&&E.finite(calc.score)&&calc.score===value.desk_score&&E.finite(value.lifecycle_decay)&&value.lifecycle_decay>=0&&value.lifecycle_decay<=1;
      return {value,pointer,name,duplicate,metrics:{compound_score:E.numberField(value,'compound_score').value,
        desk_score:current&&!duplicate?E.numberField(value,'desk_score').value:null,
        lifecycle_decay:current&&!duplicate?E.numberField(value,'lifecycle_decay').value:null,
        n_systems:E.numberField(value,'n_systems').value},calculationAvailable:current&&!duplicate};
    });return {selected,rows,packet};
  }
  function sorted(rows,key='desk_score',query=''){
    const q=query.trim().toUpperCase();
    return rows.filter(r=>!q||[r.name||'',printable(r.value.systems)||''].join(' ').toUpperCase().includes(q)).slice().sort((a,b)=>{
      const av=a.metrics[key],bv=b.metrics[key],aa=E.finite(av),bb=E.finite(bv);
      if(aa!==bb)return aa?-1:1;if(aa&&av!==bv)return av>bv?-1:1;
      return a.pointer.localeCompare(b.pointer,undefined,{numeric:true});
    });
  }
  function cohorts(packet){
    const history=packet?.overlay_evidence?.history;
    if(packet?.overlay_contract!==CONTRACT||!Array.isArray(history?.cohorts))return [];
    const start=E.eventTime(history.window_start+'T00:00:00Z'),end=E.eventTime(history.window_end_exclusive+'T00:00:00Z');
    if(start===null||end===null||end-start!==90*86400000)return [];
    const counts=new Map();for(const c of history.cohorts){const date=c?.record?.d;counts.set(date,(counts.get(date)||0)+1);}
    const rows=[];
    for(const c of history.cohorts){const date=c?.record?.d,time=E.eventTime(date+'T00:00:00Z');
      if(c?.status!=='included'||counts.get(date)!==1||time===null||time<start||time>=end||!E.object(c.scores))continue;
      const values=Object.values(c.scores);if(!values.length||!values.every(E.finite))continue;
      rows.push({date,time,max:values.reduce((a,b)=>a>b?a:b),count:values.length,pointer:c.pointer});
    }return rows.sort((a,b)=>a.time-b.time);
  }
  function csv(rows){
    const cell=(value,numeric=false)=>{let s=value===null||value===undefined?'':String(value);if(!numeric&&/^[=+\-@\t\r]/.test(s))s="'"+s;return '"'+s.replace(/"/g,'""')+'"';};
    return [['symbol','lifecycle_adjusted_score','base_compound_score','system_count','lifecycle_factor','source_pointer','investment_use'].map(x=>cell(x)).join(','),...rows.map(r=>[
      cell(r.name||printable(r.value.symbol)),cell(r.metrics.desk_score,true),cell(r.metrics.compound_score,true),cell(r.metrics.n_systems,true),cell(r.metrics.lifecycle_decay,true),cell(r.pointer),cell('unqualified')].join(','))].join('\r\n');
  }
  function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=String(text);if(cls)n.className=cls;return n;}
  function renderRecord(r){
    const card=el('article',undefined,'convergence-record');card.dataset.pointer=r.pointer;
    const h=el('h3');if(r.name){const a=el('a',r.name);a.href='/why.html?t='+encodeURIComponent(r.name);h.append(a);}else h.textContent='Unresolved symbol';card.append(h);
    card.append(el('p',r.pointer+(r.duplicate?' · Duplicate symbol; adjusted score withheld':''),'research-meta'));
    const dl=el('dl',undefined,'convergence-metrics');
    for(const [key,title] of [['desk_score','Lifecycle-adjusted score'],['compound_score','Base Compound score'],['n_systems','Reported systems'],['lifecycle_decay','First-seen lifecycle factor']]){const pair=el('div');pair.append(el('dt',title),el('dd',display(r.metrics[key])));dl.append(pair);}card.append(dl);
    const list=v=>Array.isArray(v)&&v.every(x=>typeof x==='string')?v.join(' · '):printable(v);
    card.append(el('p','Systems: '+list(r.value.systems),'research-meta'),el('p','Families (declared taxonomy): '+list(r.value.families),'research-meta'));
    if(!r.calculationAvailable)card.append(el('p','Current lifecycle calculation unavailable. The base score is retained separately.'));
    if(r.value.family_pattern===true)card.append(el('p','At least four systems across three declared families; independence unverified.'));
    if(E.object(r.value.regime))card.append(el('p','Reported posture: '+(typeof r.value.regime.posture==='string'&&r.value.regime.posture.trim()?r.value.regime.posture:'Unavailable')+' · bottom minus top breadth: '+display(E.numberField(r.value.regime,'turn_net').value)+' percentage points.'));
    const change=E.numberField(r.value,'sparkline_change_pct').value;
    if(change!==null)card.append(el('p','Reported sparkline change: '+change+'% across two positions; calendar duration unavailable.'));
    for(const [key,label] of [['score_calculation','Base score calculation'],['desk_score_calculation','Lifecycle calculation'],['reversal_evidence','Reported reversal evidence'],['history_comparison','Historical comparison definition']])if(own(r.value,key))card.append(E.inspect(label,r.value[key]));
    if(own(r.value,'pctile_90d_all'))card.append(el('p','Reported percentile within retained prior score cohorts: '+display(E.numberField(r.value,'pctile_90d_all').value)+'. This is not forecast accuracy.'));
    card.append(E.inspect('Complete received record '+r.pointer,r.value));return card;
  }
  function renderHistory(container,packet){
    const rows=cohorts(packet);container.replaceChildren();
    if(!rows.length){container.append(el('p','No eligible reported score cohorts available. No history or zero value is inferred.'));return;}
    const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 900 190');svg.setAttribute('role','img');svg.setAttribute('aria-label','Maximum base score by eligible day; exact values in the table below');
    const vals=rows.map(r=>r.max),lo=vals.reduce((a,b)=>a<b?a:b),hi=vals.reduce((a,b)=>a>b?a:b);
    const scale=Math.max(Math.abs(lo),Math.abs(hi),1),slo=lo/scale,shi=hi/scale;
    const X=r=>30+840*(r.time-rows[0].time)/Math.max(86400000,rows.at(-1).time-rows[0].time);
    const Y=r=>lo===hi?90:150-120*(r.max/scale-slo)/(shi-slo);
    const line=document.createElementNS(ns,'polyline');line.setAttribute('points',rows.map(r=>X(r)+','+Y(r)).join(' '));line.setAttribute('stroke','#e0bd69');line.setAttribute('fill','none');line.setAttribute('stroke-width','2');svg.append(line);
    for(const r of rows){const dot=document.createElementNS(ns,'circle');dot.setAttribute('cx',X(r));dot.setAttribute('cy',Y(r));dot.setAttribute('r','4');dot.setAttribute('fill','#e0bd69');const title=document.createElementNS(ns,'title');title.textContent=r.date+' · '+r.max;dot.append(title);svg.append(dot);}container.append(svg);
    const wrap=el('div',undefined,'convergence-history-scroll');wrap.tabIndex=0;wrap.setAttribute('aria-label','Daily score values');const table=el('table');table.append(el('caption','Eligible retained daily cohorts — base scores'));const head=el('thead'),tr=el('tr');for(const t of ['Date','Highest base score','Names']){const th=el('th',t);th.scope='col';tr.append(th);}head.append(tr);table.append(head);const body=el('tbody');for(const r of rows){const tr=el('tr');for(const v of [r.date,r.max,r.count])tr.append(el('td',v));body.append(tr);}table.append(body);wrap.append(table);container.append(wrap);
  }
  async function start(){
    if(!document.getElementById('convergence-desk'))return;
    const urls={'compound-signals':'https://justhodl-dashboard-live.s3.amazonaws.com/data/compound-signals.json',
      'compound-history':'https://justhodl-dashboard-live.s3.amazonaws.com/data/compound-history.json'};
    const sources=Object.fromEntries(await Promise.all(Object.entries(urls).map(async ([key,url])=>[key,await E.receive(url)])));
    const receipt=sources['compound-signals'],packet=receipt.status==='received'?receipt.data:null,m=model(packet);
    const $=id=>document.getElementById(id),board=$('convergence-board'),sort=$('convergence-sort'),search=$('convergence-search'),more=$('convergence-more');let shown=24,current=[];
    $('convergence-status').textContent=E.receivedSummary(sources)+' · '+E.packetClocks(packet)+' · '+m.selected.reason;
    function draw(reset=true){if(reset)shown=24;current=sorted(m.rows,sort.value,search.value);board.replaceChildren(...current.slice(0,shown).map(renderRecord));more.hidden=shown>=current.length;$('convergence-count').textContent=Math.min(shown,current.length)+' of '+current.length+' matching received records · '+m.rows.length+' total object records';if(!current.length)board.append(el('p','No matching received records. Check collection and source availability below.'));}
    $('convergence-controls').addEventListener('submit',e=>{e.preventDefault();draw();});search.addEventListener('input',()=>draw());sort.addEventListener('change',()=>draw());more.addEventListener('click',()=>{shown+=24;draw(false);});
    $('convergence-csv').addEventListener('click',()=>{const href=URL.createObjectURL(new Blob([csv(current)],{type:'text/csv;charset=utf-8'})),a=el('a');a.href=href;a.download='convergence-research.csv';a.click();setTimeout(()=>URL.revokeObjectURL(href),1000);});
    draw();renderHistory($('convergence-history'),packet);
    if(E.object(packet)){const evidence=$('convergence-evidence');evidence.append(el('h2','Calculation evidence'));for(const [key,label] of [['input_evidence','Complete declared input populations'],['overlay_evidence','Complete context selection and excluded cohorts'],['withheld','Records withheld from base ranking']])if(own(packet,key))evidence.append(E.inspect(label,packet[key]));}
    E.showSources($('research-sources'),sources);document.documentElement.dataset.convergenceReady='true';
  }
  const api={model,sorted,cohorts,csv,renderRecord,renderHistory,start};
  if(typeof module==='object'&&module.exports)module.exports=api;else{root.JHConvergence=api;start().catch(()=>{document.getElementById('convergence-status').textContent='Research display unavailable. Original source links remain below.';document.documentElement.dataset.convergenceReady='error';});}
})(typeof globalThis==='object'?globalThis:this);
