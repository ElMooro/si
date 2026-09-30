/* Presentation of quote records already in the loaded snapshot. No new requests. */
(function(root){
 'use strict';
 function mount(container,options={}){
  const doc=container.ownerDocument,model=options.model||root.JHPortfolioQuotes;
  const urls=options.urls||root.URL,makeBlob=options.makeBlob||((bytes)=>new Blob([bytes],{type:'application/octet-stream'}));
  let frame=Symbol('initial'),current=null,page=0,generation=0,objectURL=null,opener=null;
  const PAGE_SIZE=25,el=(tag,text)=>{const n=doc.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
  const statusLabels={MEASURED_PREVIOUS_CLOSE:'Previous close reported',NOT_ATTEMPTED:'Not attempted',UNAVAILABLE:'Unavailable'};
  const reasonLabels={COLLECTION_ACCEPTANCE_DEADLINE:'collection time budget reached',COLLECTION_BODY_RESERVATION_EXHAUSTED:'response storage budget reached',COLLECTION_TASK_FAILED:'quote task failed',PROVIDER_UNCONFIGURED:'provider unavailable',HTTP_ERROR:'provider request failed',RESPONSE_TICKER_MISMATCH:'provider returned a different ticker',INVALID_COMPLETE_JSON:'response could not be parsed',READ_DEADLINE:'response read deadline reached',TRANSPORT_OR_SOURCE_UNAVAILABLE:'source could not be read'};
  const button=(text,fn)=>{const n=el('button',text);n.type='button';n.addEventListener('click',fn);return n;};
  const title=el('h3'),coverage=el('p'),notice=el('p');coverage.setAttribute('role','status');coverage.setAttribute('aria-live','polite');
  const records=el('details'),summary=el('summary','Inspect retained quote records');
  const region=el('div');region.className='quote-table';region.tabIndex=0;region.setAttribute('role','region');region.setAttribute('aria-label','Retained previous-close quote records');
  const table=el('table'),head=el('thead'),tr=el('tr');
  for(const label of ['Symbol','Reported close','Bar window starts (UTC)','Reported result','Evidence']){const th=el('th',label);th.scope='col';tr.append(th);}
  head.append(tr);const body=el('tbody');table.append(head,body);region.append(table);
  const pager=el('nav');pager.className='quote-controls';pager.setAttribute('aria-label','Quote record pages');
  const previous=button('Previous quote page',()=>changePage(-1)),next=button('Next quote page',()=>changePage(1)),pageStatus=el('span');pageStatus.setAttribute('role','status');pager.append(previous,pageStatus,next);
  const panel=el('section');panel.className='quote-original';panel.hidden=true;panel.setAttribute('aria-label','Selected quote evidence');
  const panelTitle=el('h4'),close=button('Close quote evidence',()=>closePanel(true)),description=el('p'),status=el('p');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
  const sourceText=el('pre');sourceText.tabIndex=0;sourceText.setAttribute('aria-label','Original quote response text');sourceText.hidden=true;
  const download=el('a','Download verified quote response');download.hidden=true;
  const recordDetails=el('details'),recordSummary=el('summary','Complete recorded quote metadata'),recordText=el('pre');recordText.tabIndex=0;recordText.setAttribute('aria-label','Complete recorded quote record');
  recordDetails.append(recordSummary,recordText);panel.append(panelTitle,close,description,status,download,sourceText,recordDetails);
  records.append(summary,panel,region,pager);
  const collectionDetails=el('details'),collectionSummary=el('summary','Recorded collection details'),collectionText=el('pre');collectionText.tabIndex=0;collectionText.setAttribute('aria-label','Complete collection metadata');
  collectionDetails.append(collectionSummary,collectionText);
  container.replaceChildren(title,coverage,notice,records,collectionDetails);
  function release(){if(objectURL!==null){urls.revokeObjectURL(objectURL);objectURL=null;}}
  function closePanel(focus=false){
   ++generation;release();panel.hidden=true;sourceText.hidden=true;sourceText.textContent='';status.textContent='';description.textContent='';recordText.textContent='';recordDetails.open=false;
   download.hidden=true;download.removeAttribute('href');if(focus&&opener?.isConnected)opener.focus();opener=null;
  }
  async function inspect(row,trigger){
   closePanel();opener=trigger;const selected=++generation;panel.hidden=false;panelTitle.textContent=row.key+' · quote evidence';close.focus();
   description.textContent='Requested symbol: '+(row.requestedSymbol||'unavailable')+' · acquisition started: '+(row.started||'unavailable')+' · completed: '+(row.completed||'unavailable')+'. A bar-window timestamp is not the execution time of a trade.';
   recordText.textContent=JSON.stringify(row.record,null,2);
   if(!row.inspectable){status.textContent='No complete response with supported byte metadata is available. The recorded quote details remain inspectable.';return;}
   status.textContent='Verifying the complete retained response…';
   try{
    const result=await model.original(row);if(selected!==generation)return;
    objectURL=urls.createObjectURL(makeBlob(result.bytes));download.href=objectURL;download.download=result.filename;download.hidden=false;
    status.textContent='All bytes match the recorded length and SHA-256. '+result.interpretation+(result.text===null?' The response is not valid UTF-8; its exact bytes remain downloadable.':' Original text is shown literally without rounding numbers.');
    sourceText.textContent=result.text===null?'':result.text;sourceText.hidden=result.text===null;
   }catch(error){
    if(selected!==generation)return;release();status.textContent='Original response verification failed: '+(error instanceof Error?error.message:'unavailable');download.hidden=true;download.removeAttribute('href');sourceText.textContent='';sourceText.hidden=true;
   }
  }
  function showRows(){
   body.replaceChildren();const rows=current.rows,start=page*PAGE_SIZE;
   for(const row of rows.slice(start,start+PAGE_SIZE)){
    const tr=el('tr'),label=Object.hasOwn(statusLabels,row.status)?statusLabels[row.status]:'Unrecognized reported status';
    const reason=row.reason?(Object.hasOwn(reasonLabels,row.reason)?reasonLabels[row.reason]:'see recorded failure details'):null;
    const result=!row.identified?'Identity unavailable or inconsistent':label+(reason?' · '+reason:'');
    for(const value of [row.key,row.price===null?'Unavailable':String(row.price),row.windowStart||'Unavailable',result])tr.append(el('td',value));
    const td=el('td'),open=button('Inspect '+row.key,()=>inspect(row,open));td.append(open);tr.append(td);body.append(tr);
   }
   if(!rows.length){const tr=el('tr'),td=el('td','No quote records in this snapshot.');td.colSpan=5;tr.append(td);body.append(tr);}
   previous.disabled=page===0;next.disabled=start+PAGE_SIZE>=rows.length;pager.hidden=rows.length<=PAGE_SIZE;
   pageStatus.textContent=rows.length?'Quote records '+(start+1)+'–'+Math.min(start+PAGE_SIZE,rows.length)+' of '+rows.length:'No quote records';
  }
  function changePage(step){if(!current?.available)return;closePanel();page=Math.max(0,Math.min(Math.max(0,Math.ceil(current.rows.length/PAGE_SIZE)-1),page+step));showRows();}
  function render(snapshot){
   if(snapshot===frame)return;frame=snapshot;closePanel();page=0;current=model.view(snapshot);
   title.textContent=current.title;notice.textContent=current.detail;coverage.textContent=current.available?current.coverageText:'';
   records.open=false;records.hidden=!current.available;collectionDetails.open=false;collectionDetails.hidden=!current.available;
   collectionText.textContent=current.available?JSON.stringify(current.collection,null,2):'';body.replaceChildren();pager.hidden=true;
   if(current.available){summary.textContent='Inspect all '+current.rows.length+' retained/requested quote records';showRows();}
  }
  function clear(){frame=Symbol('cleared');render(null);}
  function destroy(){clear();container.replaceChildren();}
  return {render,clear,destroy};
 }
 const api={mount};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHPortfolioQuotesPage=api;
})(typeof globalThis==='object'?globalThis:this);
