/* Reported buyback accounting. Period ends are not publication timestamps. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports)module.exports=api;
  else {root.JHBuybackPane=api;api.mount(root);}
})(typeof window==='object'?window:globalThis,function(){
  'use strict';
  const CONTRACT='buyback-accounting-measurements.v1';
  const own=(value,key)=>value!==null&&typeof value==='object'&&Object.hasOwn(value,key);
  const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
  const number=value=>typeof value==='number'&&Number.isFinite(value)&&(!Number.isInteger(value)||Number.isSafeInteger(value))?value:null;
  function day(value){
    if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value)||value.startsWith('0000-'))return null;
    const stamp=Date.parse(value+'T00:00:00Z');
    return Number.isFinite(stamp)&&new Date(stamp).toISOString().slice(0,10)===value?value:null;
  }
  function selected(document){
    const tab=document.querySelector('#tabs .tab.on[data-id]');
    const value=tab?.getAttribute('data-id');
    if(typeof value!=='string')return '';
    const symbol=value.trim().toUpperCase().split(':').at(-1);
    return /^[A-Z0-9.\-]{1,20}$/.test(symbol)?symbol:'';
  }
  function observations(row){
    const source=row?.measurements?.cashflow_observations;
    if(!Array.isArray(source))return {status:'missing_or_invalid_observations',rows:[]};
    return {status:'reported_observations',rows:source.map((received,index)=>{
      const issues=[];const observation=object(received)?received:{};
      const end=day(observation.date),start=day(observation.start_date),unit=observation.reported_currency;
      const comparable=row?.measurement_contract===CONTRACT&&observation.eligible===true&&observation.reported_calendar_duration_aligned===true&&start!==null&&end!==null&&start<=end&&typeof unit==='string'&&/^[A-Z]{3}$/.test(unit);
      if(!comparable)issues.push('Quarter identity, duration or currency is unqualified.');
      const read=(key,field,sign)=>{
        const metric=observation.metrics?.[key];
        if(!comparable||!object(metric)||metric.status!=='reported_value'||metric.source_field!==field||metric.sign!==sign||metric.unit!==unit)return null;
        return number(metric.value);
      };
      // The producer already converted net issuance to signed net repurchases.
      const net=read('net_common_repurchases','netCommonStockIssuance','negative');
      const grossValue=read('gross_common_repurchases','commonStockRepurchased','magnitude');
      const gross=grossValue!==null&&grossValue>=0?grossValue:null;
      if(net===null)issues.push('Reported net repurchases unavailable; gross is separate.');
      const cap=number(row?.market_cap);
      const aligned=cap!==null&&cap>0&&day(row?.market_cap_asof)===end&&row?.market_cap_unit===unit;
      let ratio=null;
      if(net!==null&&aligned){ratio=number(net/cap*100);if(net!==0&&ratio===0)ratio=null;}
      if(!aligned)issues.push('Market cap date and currency do not match this quarter.');
      if(net!==null&&aligned&&ratio===null)issues.push('Ratio exceeds supported numeric precision.');
      return {source_index:index,start_date:start,end_date:end,unit:typeof unit==='string'?unit:null,net,gross,ratio,issues,received};
    })};
  }
  function reported(row){
    const source=row?.measurements?.cashflow_observations;
    if(!Array.isArray(source))return {status:'missing_or_invalid_observations',rows:[]};
    return {status:'reported_observations',rows:source.map((received,index)=>{
      const issues=[];const observation=object(received)?received:{};
      const end=day(observation.date),start=day(observation.start_date),unit=observation.reported_currency;
      const comparable=row?.measurement_contract===CONTRACT&&observation.eligible===true&&observation.reported_calendar_duration_aligned===true&&start!==null&&end!==null&&start<=end&&typeof unit==='string'&&/^[A-Z]{3}$/.test(unit);
      if(!comparable)issues.push('Quarter identity, duration or currency is unqualified.');
      const metricOk=typeof unit==='string'&&/^[A-Z]{3}$/.test(unit);
      const read=(key,field,sign)=>{
        const metric=observation.metrics?.[key];
        if(!metricOk||!object(metric)||metric.status!=='reported_value'||metric.source_field!==field||metric.sign!==sign||metric.unit!==unit)return null;
        return number(metric.value);
      };
      // The producer already converted net issuance to signed net repurchases.
      const net=read('net_common_repurchases','netCommonStockIssuance','negative');
      const grossValue=read('gross_common_repurchases','commonStockRepurchased','magnitude');
      const gross=grossValue!==null&&grossValue>=0?grossValue:null;
      if(net===null)issues.push('Reported net repurchases unavailable; gross is separate.');
      const cap=number(row?.market_cap);
      const aligned=cap!==null&&cap>0&&day(row?.market_cap_asof)===end&&row?.market_cap_unit===unit;
      let ratio=null,ratioNote=null;
      const scale=base=>{const q=number(net/base*100);if(q===null||(net!==0&&q===0))return null;return q;};
      if(net!==null&&aligned){ratio=scale(cap);}
      else if(net!==null&&typeof unit==='string'){
        const books=row?.provider_responses?.enterprise_values;
        const book=Array.isArray(books)?books.find(item=>object(item)&&day(item.date)===end&&number(item.marketCapitalization)>0):null;
        if(book){ratio=scale(number(book.marketCapitalization));if(ratio!==null)ratioNote='quarter cap';}
        else if(cap!==null&&cap>0&&row?.market_cap_unit===unit){ratio=scale(cap);if(ratio!==null)ratioNote='latest cap';}
      }
      if(!aligned)issues.push('Market cap date and currency do not match this quarter.');
      if(net!==null&&aligned&&ratio===null)issues.push('Ratio exceeds supported numeric precision.');
      return {source_index:index,start_date:start,end_date:end,unit:typeof unit==='string'?unit:null,net,gross,ratio,ratioNote,issues,received};
    })};
  }
  function mount(root){
    const document=root.document;if(!document)return;
    let cached=null,loadedAt=0,pending=null,generation=0,viewKey=null,guard=()=>false;
    const create=(tag,text,className='')=>{const el=document.createElement(tag);el.textContent=text;el.className=className;return el;};
    function audit(label,value,original=false){
      const details=create('details','','audit');details.appendChild(create('summary',label));
      details.addEventListener('toggle',()=>{if(!details.open||details.dataset.loaded)return;const pre=create('pre',original?value:JSON.stringify(value,null,2));pre.tabIndex=0;pre.setAttribute('aria-label',label);pre.style.cssText='white-space:pre-wrap;overflow-wrap:anywhere;max-height:220px;overflow:auto';details.appendChild(pre);details.dataset.loaded='true';});return details;
    }
    function item(){return Array.isArray(root.OSC)?root.OSC.find(row=>row?.id==='buyback'):null;}
    function ensure(){if(!Array.isArray(root.OSC))return;const existing=item();if(existing){existing.externalPane=true;return;}root.OSC.push({id:'buyback',n:'Buyback',on:0,cat:'Buyback',c:'#7ec8c4',externalPane:true});}
    function enabled(){const value=item()?.on;return value===1||value===true;}
    function pane(){
      let el=document.getElementById('jh-buyback-pane');if(el)return el;
      el=create('section','');el.id='jh-buyback-pane';el.tabIndex=0;el.setAttribute('aria-label','Reported buyback accounting');
      el.style.cssText='display:none;max-height:380px;overflow:auto;min-width:0;padding:10px;border-top:1px solid var(--line);background:var(--bg);color:var(--ink,var(--fg));flex:none;font-size:12px;line-height:1.5';
      const host=document.getElementById('oscwrap');if(host?.parentNode)host.parentNode.insertBefore(el,host.nextSibling);else (document.getElementById('stage')||document.body).appendChild(el);return el;
    }
    function receiptDetails(host,receipt,symbol){
      const el=create('details','','audit receipt-evidence');el.appendChild(create('summary','Source receipt and original bytes'));host.appendChild(el);
      const info=create('p','HTTP '+(receipt.http_status??'Unavailable')+' · Received at: '+(receipt.received_at||'Unavailable')+'. Receipt time is not observation freshness.');el.appendChild(info);
      const source=create('p','Source: '+(receipt.request_url||receipt.url||'Unavailable'));source.style.overflowWrap='anywhere';el.appendChild(source);
      if(!receipt.raw)return;
      const hash=create('p',receipt.raw.byteLength+' complete received bytes · SHA-256: '+(receipt.sha256||'Unavailable'));hash.style.overflowWrap='anywhere';el.appendChild(hash);
      if(typeof receipt.source==='string')el.appendChild(audit('Complete original JSON text',receipt.source,true));
      const button=create('button','Download original bytes');button.type='button';button.addEventListener('click',()=>{
        const url=root.URL.createObjectURL(new root.Blob([receipt.raw],{type:'application/octet-stream'}));
        const link=create('a','');link.href=url;link.download=symbol+'-buyback-received.json';link.click();root.setTimeout(()=>root.URL.revokeObjectURL(url),1000);
      });el.appendChild(button);
      el.appendChild(audit('Receipt metadata (capture only)',{request_url:receipt.request_url,received_at:receipt.received_at,http_status:receipt.http_status,sha256:receipt.sha256,byte_count:receipt.raw.byteLength,byte_limit:receipt.byte_limit,capture_page_commit:document.querySelector('meta[name="jh-build-commit"]')?.content||null,observation_freshness_verified:false,investment_authority:false}));
    }
    function load(force){
      if(pending)return pending;
      if(!force&&cached!==null&&Date.now()-loadedAt<300000)return Promise.resolve(cached);
      if(!root.JHNumericEvidence?.receive)throw new Error('Complete evidence reader unavailable');
      pending=root.JHNumericEvidence.receive('/data/buyback-engine.json',{io:root.JHEvidenceIO,fetcher:root.fetch.bind(root)}).then(receipt=>{
        cached=receipt.status==='received'?receipt:null;loadedAt=cached===null?0:Date.now();return receipt;
      }).finally(()=>{pending=null;});return pending;
    }
    function paint(el,receipt,symbol){
      el.replaceChildren();el.appendChild(titleBar(symbol));
      receiptDetails(el,receipt,symbol);
      if(receipt.status!=='received'){
        el.appendChild(create('p','Reported buybacks unavailable: '+(receipt.reason||'request failed')+'. No accounting projection is shown.'));
        const retry=create('button','Retry reported packet');retry.type='button';retry.addEventListener('click',()=>draw(true));el.appendChild(retry);return;
      }
      const packet=receipt.data;
      el.appendChild(create('p','Reported accounting periods are shown below. Publication availability is unverified. The line above is step-held on the price bars at each period end. It is reported cash, not a forecast.'));
      el.appendChild(create('p','Packet timestamp: '+(typeof packet?.generated_at==='string'?packet.generated_at:'Unavailable')+'. Amounts are reported context, not investment recommendations.'));
      const refresh=create('button','Refresh reported packet');refresh.type='button';refresh.addEventListener('click',()=>draw(true));el.appendChild(refresh);
      el.appendChild(audit('Complete received packet (parsed JSON)',packet));
      if(!object(packet)||!object(packet.tickers)){el.appendChild(create('p','Unavailable: missing or invalid ticker inventory.'));return;}
      if(!own(packet.tickers,symbol)){paintMissing(el,symbol);return;}
      const row=packet.tickers[symbol];el.appendChild(audit('Complete selected ticker record',row));
      if(!object(row)||row.symbol!==symbol){el.appendChild(create('p','Unavailable: missing or mismatched issuer identity.'));return;}
      const projected=reported(row);
      if(projected.status!=='reported_observations'){el.appendChild(create('p','Unavailable: missing or invalid reported quarter collection.'));return;}
      el.appendChild(create('p',projected.rows.length+' received accounting observations. Gross and net repurchases stay separate.'));
      remember(symbol, projected.rows);
      el.appendChild(levelStrip(projected.rows));
      el.appendChild(tableFor(projected.rows));
    }
    function periodLabel(observation){
      const start=observation.start_date, end=observation.end_date;
      if(start&&end)return start+' → '+end;
      if(end){
        const fiscal=observation.fiscal;
        return (typeof fiscal==='string'&&/^Q[1-4]$/.test(fiscal)?fiscal+' ending ':'Quarter ending ')+end;
      }
      return 'Unavailable';
    }
    function grouped(v){
      if(typeof v!=='number'||!Number.isFinite(v)||!Number.isInteger(v)||Math.abs(v)<1000)return String(v);
      const sign=v<0?'-':'';
      return sign+String(Math.abs(v)).replace(/\B(?=(\d{3})+(?!\d))/g,',');
    }
    function ratioText(observation){
      if(observation.ratio===null)return observation.capLabel||'Unavailable';
      const shown=Number.isInteger(observation.ratio)?String(observation.ratio):String(Math.round(observation.ratio*1000)/1000);
      return shown+(observation.ratioNote?('% '+observation.ratioNote):'%');
    }
    function tableFor(rows){
      const table=create('table','');table.style.cssText='width:100%;border-collapse:collapse;text-align:left';
      const header=create('tr','');for(const name of ['Reported period','Net repurchases','Gross repurchases','Quarter / matching-date cap'])header.appendChild(create('th',name));const thead=create('thead','');thead.appendChild(header);table.appendChild(thead);const body=create('tbody','');table.appendChild(body);
      for(const observation of rows){
        const tr=create('tr','');const cell=create('td',periodLabel(observation));
        if(observation.received)cell.appendChild(audit('Observation '+(observation.source_index+1)+' · full received record',observation.received));
        if(observation.issues&&observation.issues.length)cell.appendChild(create('p',observation.issues.join(' ')));tr.appendChild(cell);
        for(const value of [observation.net,observation.gross])tr.appendChild(create('td',value===null?'Unavailable':String(value)+' '+observation.unit));
        tr.appendChild(create('td',ratioText(observation)));for(const td of tr.children)td.style.cssText='vertical-align:top;padding:6px;overflow-wrap:anywhere;border-bottom:1px solid var(--line)';body.appendChild(tr);
      }
      return table;
    }
    function levelStrip(rows){
      const host=create('div','');host.className='jh-bb-level';
      host.style.cssText='position:relative;height:88px;margin:8px 0 10px;border:1px solid var(--line);overflow:hidden';
      const useRatio=rows.some(row=>typeof row.ratio==='number');
      const pts=[];
      rows.forEach(row=>{const v=useRatio?row.ratio:row.net;if(typeof v==='number')pts.push({v:v});});
      if(!pts.length){host.appendChild(create('div','No level yet'));return host;}
      let lo=0,hi=0;
      pts.forEach(p=>{if(p.v<lo)lo=p.v;if(p.v>hi)hi=p.v;});
      if(lo===hi){lo-=1;hi+=1;}
      const span=hi-lo, zero=(hi/span)*100, n=pts.length;
      const z=create('div','');z.style.cssText='position:absolute;left:0;right:52px;height:1px;background:var(--line);top:'+zero+'%';host.appendChild(z);
      pts.forEach((p,i)=>{
        const y=(hi-p.v)/span*100, up=p.v>=0, top=up?y:zero, h=Math.max(1,Math.abs(y-zero));
        const bar=create('div','');
        bar.style.cssText='position:absolute;width:7px;background:'+(up?'#089981':'#f23645')+';left:'+(n===1?8:(i/(n-1))*76)+'%;top:'+top+'%;height:'+h+'%';
        host.appendChild(bar);
      });
      const last=pts[pts.length-1];
      const lab=create('div',useRatio?String(Math.round(last.v*1000)/1000)+'%':grouped(last.v));
      lab.style.cssText='position:absolute;right:4px;top:4px;font:11px IBM Plex Mono,monospace;color:'+(last.v>=0?'#089981':'#f23645');
      host.appendChild(lab);
      host.appendChild(create('div',useRatio?'Buyback level':'Net cash level')).style.cssText='position:absolute;left:6px;top:4px;font-size:10px;color:var(--mut,#787b86)';
      return host;
    }
    function eventsOn(){return Array.isArray(root.INDS)&&root.INDS.some(i=>i&&i.id==='buyb'&&!i.hide&&(i.on===1||i.on===true));}
    function remember(symbol, rows){
      const plot=(rows||[]).filter(row=>row&&row.end_date&&(typeof row.ratio==='number'||typeof row.net==='number'));
      root.__jhBuybackPlot={symbol, rows:plot.map(row=>({date:row.end_date,ratio:typeof row.ratio==='number'?row.ratio:null,net:typeof row.net==='number'?row.net:null,note:row.ratioNote||null})), message:plot.length?'':'No buyback level for '+symbol};
      root.__jhBuybackSeries={symbol, rows:plot.filter(row=>typeof row.net==='number'&&row.net!==0).map(row=>({date:row.end_date,net:row.net}))};
      drawPlot();
      if(eventsOn()&&typeof root.paint==='function'&&Array.isArray(root.lastBars)){try{root.paint(root.lastBars);}catch(e){}}
    }
    function hookMarks(){
      const inst=root.jhInst;if(!inst||typeof inst.buybackMarks!=='function'||inst.buybackMarks.__jhBuy)return;
      const orig=inst.buybackMarks;
      function wrapped(d, pack, tkr){
        const mk=orig.apply(this, arguments), base=Array.isArray(mk)?mk.slice():[];
        const want=String(tkr||'').toUpperCase().split(':').pop();
        const series=root.__jhBuybackSeries;
        if(!series||series.symbol!==want||!Array.isArray(d))return base;
        const extra=[];
        series.rows.forEach(row=>{
          const target=Date.parse(row.date+'T00:00:00Z')/1000;
          if(!Number.isFinite(target))return;
          let best=null, bd=1e15;
          const sec=bar=>{
            if(!bar)return null;
            if(typeof bar.time==='number')return bar.time;
            if(typeof bar.time==='string')return Date.parse(bar.time.slice(0,10)+'T00:00:00Z')/1000;
            if(bar.time&&typeof bar.time==='object'&&bar.time.year)return Date.parse(bar.time.year+'-'+String(bar.time.month).padStart(2,'0')+'-'+String(bar.time.day).padStart(2,'0')+'T00:00:00Z')/1000;
            return null;
          };
          d.forEach(bar=>{
            const bt=sec(bar);
            if(bt==null||!Number.isFinite(bt))return;
            const diff=Math.abs(bt-target);if(diff<bd){bd=diff;best=bar;}
          });
          let gap=12*86400;
          if(d.length>2){const a=sec(d[d.length-1]),b=sec(d[d.length-2]);if(a!=null&&b!=null)gap=Math.max(4*86400,Math.abs(a-b)*1.6);}
          if(!best||bd>gap)return;
          extra.push({time:best.time, position:row.net>0?'belowBar':'aboveBar', color:row.net>0?'#089981':'#f23645', shape:'square', text:row.net>0?'BB':'ISS'});
        });
        return base.concat(extra).slice(-16);
      }
      wrapped.__jhBuy=true;inst.buybackMarks=wrapped;
    }
    let flows=null, flowsAt=0, flowPending=null, recordCache={};
    function cashExact(metric){
      const exact=metric&&metric.exact;
      if(!exact||typeof exact.numerator!=='string'||typeof exact.denominator!=='string')return null;
      if(!/^-?\d+$/.test(exact.numerator)||!/^[1-9]\d*$/.test(exact.denominator))return null;
      const n=Number(exact.numerator), d=Number(exact.denominator);
      if(!Number.isSafeInteger(n)||!Number.isSafeInteger(d))return null;
      return n/d;
    }
    function loadFlows(){
      if(flows&&Date.now()-flowsAt<300000)return Promise.resolve(flows);
      if(flowPending)return flowPending;
      flowPending=root.fetch('/data/share-flows.json',{cache:'no-store'}).then(response=>{if(!response.ok)throw new Error('HTTP '+response.status);return response.json();}).then(doc=>{flows=doc;flowsAt=Date.now();return doc;}).finally(()=>{flowPending=null;});
      return flowPending;
    }
    function loadCapital(symbol){
      if(recordCache[symbol])return Promise.resolve(recordCache[symbol]);
      return loadFlows().then(doc=>{
        const issuers=Array.isArray(doc&&doc.issuers)?doc.issuers:[];
        const hit=issuers.find(row=>row&&row.symbol===symbol);
        const key=hit&&hit.record&&hit.record.key;
        if(typeof key!=='string'||!/^data\/capital-structure-research\/records\/[a-f0-9]{64}\.json$/.test(key))return {rows:[]};
        return root.fetch('/'+key,{cache:'no-store'}).then(response=>{if(!response.ok)throw new Error('HTTP '+response.status);return response.json();}).then(file=>{
          const records=Array.isArray(file&&file.records)?file.records:[], rows=[];
          records.forEach(rec=>{
            if(!rec||rec.request_period!=='quarter')return;
            const end=day(rec.identity&&rec.identity.date);if(!end)return;
            const metrics=rec.measurements&&rec.measurements.metrics||{};
            const gross=cashExact(metrics.cash_repurchase_outflow), issued=cashExact(metrics.cash_common_stock_issuance);
            if(gross===null&&issued===null)return;
            const net=(gross===null?0:gross)-(issued===null?0:issued);
            const unit=rec.identity&&typeof rec.identity.reportedCurrency==='string'?rec.identity.reportedCurrency:'USD';
            const fiscal=rec.identity&&typeof rec.identity.period==='string'?rec.identity.period:null;
            rows.push({source_index:rows.length,start_date:null,end_date:end,fiscal,unit,net,gross,ratio:null,ratioNote:null,capLabel:'No quarter cap',issues:[],received:null});
          });
          rows.sort((a,b)=>a.end_date<b.end_date?-1:a.end_date>b.end_date?1:0);
          const quote=quoteCap(file);
          if(quote&&quote.cap>0){
            rows.forEach(row=>{
              if(typeof row.net!=='number')return;
              const q=number(row.net/quote.cap*100);
              if(q===null||(row.net!==0&&q===0))return;
              row.ratio=q;row.ratioNote='quote cap, not quarter-matched';row.capLabel='quote cap';
            });
          }
          const pack={rows,quote};recordCache[symbol]=pack;return pack;
        });
      });
    }
    function paintMissing(el,symbol){
      el.appendChild(create('p','No buyback-engine row for '+symbol+'. Loading the capital-structure record.'));
      const alive=guard;
      loadCapital(symbol).then(pack=>{
        if(alive!==guard||!guard())return;
        if(!pack||!pack.rows.length){root.__jhBuybackPlot={symbol,rows:[],message:'No buyback level for '+symbol};drawPlot();el.appendChild(create('p','No received record for the selected ticker.'));return;}
        remember(symbol, pack.rows);
        el.appendChild(create('p','Not in the buyback-engine packet. The level is reported cash: repurchase minus issuance. Positive is a repurchase, negative is issuance.'));
        el.appendChild(create('p','Quarter start dates are not in this capital-structure record. Each row is the reported statement end.'));
        if(pack.quote)el.appendChild(create('p','Latest quote cap '+grouped(pack.quote.cap)+' USD. The line uses that quote. It is not the market cap on each quarter end.'));
        el.appendChild(levelStrip(pack.rows));
        el.appendChild(tableFor(pack.rows));
      }).catch(()=>{if(alive===guard&&guard())el.appendChild(create('p','No received record for the selected ticker.'));});
    }
    function plotHost(){
      let el=document.getElementById('jh-buyback-plot');if(el)return el;
      el=document.createElement('div');el.id='jh-buyback-plot';
      el.style.cssText='display:none;height:118px;border-top:1px solid var(--line,#2a2e39);position:relative;background:var(--bg,#131722);flex:none;overflow:hidden';
      const osc=document.getElementById('oscwrap');
      if(osc&&osc.parentNode)osc.parentNode.insertBefore(el,osc);
      else (document.getElementById('stage')||document.body).appendChild(el);
      return el;
    }
    function barDay(t){
      if(typeof t==='number'&&Number.isFinite(t))return new Date(t*1000).toISOString().slice(0,10);
      if(typeof t==='string')return /^\d{4}-\d{2}-\d{2}/.test(t)?t.slice(0,10):null;
      if(t&&typeof t==='object'&&t.year)return t.year+'-'+String(t.month).padStart(2,'0')+'-'+String(t.day).padStart(2,'0');
      return null;
    }
    function heldPoints(series, bars){
      const pts=[];let q=0,y=null;
      const rows=series.slice().sort((a,b)=>a.date<b.date?-1:1);
      for(let i=0;i<bars.length;i++){
        const ds=barDay(bars[i]&&bars[i].time);if(!ds)continue;
        while(q<rows.length&&rows[q].date<=ds){y=rows[q].y;q++;}
        if(y!=null)pts.push({i,y});
      }
      return pts;
    }
    function drawPlot(){
      const host=plotHost();
      if(!enabled()){host.style.display='none';host.replaceChildren();return;}
      const symbol=selected(document);
      const pack=root.__jhBuybackPlot&&root.__jhBuybackPlot.symbol===symbol?root.__jhBuybackPlot:{rows:[],message:symbol?('Loading '+symbol+'…'):'Select a ticker'};
      const bars=Array.isArray(root.lastBars)?root.lastBars:[];
      const useRatio=(pack.rows||[]).some(row=>typeof row.ratio==='number');
      const series=(pack.rows||[]).map(row=>{
        const y=useRatio?row.ratio:row.net;
        return typeof y==='number'&&row.date?{date:row.date,y}:null;
      }).filter(Boolean);
      const pts=series.length&&bars.length?heldPoints(series,bars):[];
      host.style.display='block';host.replaceChildren();
      const head=document.createElement('div');
      head.style.cssText='position:absolute;left:8px;right:8px;top:2px;display:flex;align-items:center;gap:8px;z-index:2;font:11px IBM Plex Mono,monospace;color:#d1d4dc';
      const last=series.length?series[series.length-1]:null;
      const note=useRatio?((pack.rows.find(row=>row.note)||{}).note||'latest cap'):'net cash';
      const shown=last==null?(pack.message||'No buyback level'+(symbol?' for '+symbol:'')):(useRatio?((last.y>=0?'+':'')+String(Math.round(last.y*1000)/1000)+'%'):grouped(last.y));
      const title=document.createElement('span');
      title.textContent='Buyback'+(symbol?' · '+symbol:'')+'  '+shown+(last?'  '+note:'');
      title.style.cssText='min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:'+(last&&last.y<0?'#f23645':'#089981');
      const btn=document.createElement('button');
      btn.type='button';btn.textContent='Close';btn.setAttribute('aria-label','Close buyback');btn.title='Close buyback';
      btn.style.cssText='margin-left:auto;height:26px;padding:0 10px;border:1px solid #089981;border-radius:4px;background:#1e222d;color:#d1d4dc;font:600 12px IBM Plex Sans,sans-serif;cursor:pointer;flex:none';
      btn.addEventListener('click',shut);
      head.appendChild(title);head.appendChild(btn);host.appendChild(head);
      const w=host.clientWidth||640,h=118,svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
      svg.setAttribute('width',String(w));svg.setAttribute('height',String(h));svg.style.display='block';
      let min=0,max=0;pts.forEach(p=>{if(p.y<min)min=p.y;if(p.y>max)max=p.y;});
      if(min===max){min-=1;max+=1;}
      const pad=(max-min)*0.18;min-=pad;max+=pad;
      const X=i=>bars.length<2?8:8+(w-16)*(i/(bars.length-1));
      const Y=v=>26+(h-34)*(1-(v-min)/(max-min));
      const zero=document.createElementNS('http://www.w3.org/2000/svg','line');
      zero.setAttribute('x1','8');zero.setAttribute('x2',String(w-8));zero.setAttribute('y1',String(Y(0)));zero.setAttribute('y2',String(Y(0)));zero.setAttribute('stroke','#2a2e39');
      svg.appendChild(zero);
      if(pts.length){
        const path=document.createElementNS('http://www.w3.org/2000/svg','path');
        path.setAttribute('d',pts.map((p,i)=>(i?'L':'M')+X(p.i).toFixed(1)+' '+Y(p.y).toFixed(1)).join(' '));
        path.setAttribute('fill','none');path.setAttribute('stroke','#7ec8c4');path.setAttribute('stroke-width','1.6');
        svg.appendChild(path);
      }
      host.appendChild(svg);
    }
    function titleBar(symbol){
      const bar=create('div','');
      bar.style.cssText='position:sticky;top:0;z-index:5;display:flex;align-items:center;justify-content:space-between;gap:8px;background:var(--bg,#131722);padding:0 0 6px';
      const title=create('strong','Buyback accounting'+(symbol?' · '+symbol:''));
      title.style.cssText='min-width:0;overflow-wrap:anywhere';
      bar.appendChild(title);
      const btn=create('button','\u00d7 Close');
      btn.type='button';btn.setAttribute('aria-label','Close');btn.title='Close';
      btn.style.cssText='height:28px;border:1px solid var(--line,#2a2e39);border-radius:4px;background:#1e222d;color:#d1d4dc;font-size:13px;line-height:26px;cursor:pointer;flex:none;padding:0 8px';
      btn.addEventListener('click',shut);bar.appendChild(btn);return bar;
    }
    function shut(){
      const row=item();if(row)row.on=0;
      generation++;
      viewKey=JSON.stringify([false,eventsOn(),selected(document)]);
      const el=document.getElementById('jh-buyback-pane');
      if(el){el.style.display='none';el.replaceChildren();el.setAttribute('aria-busy','false');}
      const plot=document.getElementById('jh-buyback-plot');
      if(plot){plot.style.display='none';plot.replaceChildren();}
      document.querySelectorAll('[data-add="buyback"]').forEach(node=>node.classList.remove('on'));
    }
    function quoteCap(file){
      const snaps=Array.isArray(file&&file.snapshots)?file.snapshots:[];
      for(const sn of snaps){
        const facts=sn&&sn.reported&&Array.isArray(sn.reported.facts)?sn.reported.facts:[];
        const cap=facts.find(f=>f&&f.endpoint==='quote'&&f.field==='marketCap'&&f.status==='reported_numeric_value');
        const value=number(cap&&cap.reported&&cap.reported.value);
        if(value!==null&&value>0)return {cap:value};
      }
      return null;
    }
    function draw(force=false){
      ensure();hookMarks();const on=enabled(),ev=eventsOn(),symbol=selected(document),key=JSON.stringify([on,ev,symbol]);
      if(!on){const plot=document.getElementById('jh-buyback-plot');if(plot){plot.style.display='none';plot.replaceChildren();}}
      else if(force||key!==viewKey){root.__jhBuybackPlot={symbol,rows:[],message:symbol?('Loading '+symbol+'…'):'Select a ticker'};drawPlot();}
      else drawPlot();
      if(!force&&key===viewKey)return;viewKey=key;const request=++generation,el=pane();
      el.style.display=on?'block':'none';el.replaceChildren();el.setAttribute('aria-busy','false');
      if(!symbol){if(on){el.appendChild(titleBar(''));el.appendChild(create('p','Select a ticker tab to inspect its reported buybacks.'));}return;}
      if(!on&&!ev)return;
      if(on){el.appendChild(titleBar(symbol));el.appendChild(create('p','Loading reported buybacks for '+symbol+'…'));el.setAttribute('aria-busy','true');}
      const current=()=>request===generation&&(enabled()||eventsOn())&&selected(document)===symbol;
      guard=current;
      Promise.resolve().then(()=>load(force)).then(receipt=>{if(current())paint(el,receipt,symbol);}).catch(error=>{
        if(!current())return;
        el.replaceChildren();el.appendChild(titleBar(symbol));
        el.appendChild(create('p','Reported buybacks unavailable: '+(typeof error?.message==='string'?error.message:'request failed')));
        const retry=create('button','Retry reported packet');retry.type='button';retry.addEventListener('click',()=>draw(true));el.appendChild(retry);
      }).finally(()=>{if(current())el.setAttribute('aria-busy','false');});
    }
    // The pane has no price-bar projection, so it does not intercept chart paint.
    root.jhBuybackDraw=()=>draw(true);let timer=root.setInterval(()=>draw(),1000);draw();
    const tabs=document.getElementById('tabs');let observer=null;
    if(tabs&&root.MutationObserver){observer=new root.MutationObserver(()=>draw());observer.observe(tabs,{attributes:true,childList:true,subtree:true,attributeFilter:['class','data-id']});}
    root.addEventListener?.('pagehide',()=>{generation++;root.clearInterval(timer);timer=null;observer?.disconnect();});
    root.addEventListener?.('pageshow',()=>{if(timer!==null)return;timer=root.setInterval(()=>draw(),1000);if(tabs&&observer)observer.observe(tabs,{attributes:true,childList:true,subtree:true,attributeFilter:['class','data-id']});viewKey=null;draw();});
  }
  return {CONTRACT,number,day,selected,observations,mount};
});
