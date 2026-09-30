/* Local presentation of research already included in a loaded portfolio snapshot. */
(function(root){
 'use strict';
 function mount(container,options={}){
  const doc=container.ownerDocument,model=options.model||root.JHPortfolioResearch;
  const urls=options.urls||root.URL,makeBlob=options.makeBlob||((bytes)=>new Blob([bytes],{type:'application/octet-stream'}));
  const pageSize=8;let frame=Symbol('initial'),generation=0,objectURL=null,sourcePage=0,current=null;
  const el=(tag,text,cls)=>{const node=doc.createElement(tag);if(text!==undefined)node.textContent=text;if(cls)node.className=cls;return node;};
  const button=(text,action)=>{const node=el('button',text);node.type='button';node.addEventListener('click',action);return node;};
  function release(){if(objectURL!==null){urls.revokeObjectURL(objectURL);objectURL=null;}}
  const title=el('h3'),notice=el('p'),totals=el('p'),cards=el('div',undefined,'research-sources');
  const pager=el('nav',undefined,'research-controls');pager.setAttribute('aria-label','Research source pages');
  const previous=button('Previous sources',()=>page(-1)),pageStatus=el('span'),next=button('Next sources',()=>page(1));
  pageStatus.setAttribute('role','status');pager.append(previous,pageStatus,next);
  const panel=el('section',undefined,'research-original');panel.hidden=true;panel.setAttribute('aria-label','Original retained source');
  const panelTitle=el('h4'),status=el('p');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
  const download=el('a','Download verified original bytes');download.hidden=true;
  const text=el('pre');text.tabIndex=0;text.setAttribute('aria-label','Original source text');text.hidden=true;
  const close=button('Close original source',()=>closePanel(true));let opener=null;
  panel.append(panelTitle,close,status,download,text);
  const diagnostics=el('details'),diagnosticTitle=el('summary','Source joins and row references');
  const joinNote=el('p','Six joins use four research sources. They are not six independent votes. Occurrence references are zero-based positions in the retained source arrays.');
  const tableRegion=el('div',undefined,'research-table');tableRegion.tabIndex=0;tableRegion.setAttribute('role','region');tableRegion.setAttribute('aria-label','Reported research join populations');
  const table=el('table'),head=el('thead'),headRow=el('tr');
  for(const label of ['Join','Source rows','Usable symbols','Duplicate symbols','Invalid rows']){const cell=el('th',label);cell.scope='col';headRow.append(cell);}
  head.append(headRow);const body=el('tbody');table.append(head,body);tableRegion.append(table);
  const diagnosticText=el('pre');diagnosticText.tabIndex=0;diagnosticText.setAttribute('aria-label','Complete reported join diagnostics and row references');
  const showReferences=button('Show full reported row references',()=>{
   if(!current?.available)return;
   const references={};for(const name of ['positions','watchlist'])references[name]=Array.isArray(frame[name])?frame[name].map((row,index)=>({index,symbol:typeof row?.symbol==='string'?row.symbol:null,research_evidence:row?.research_evidence??null})):null;
   diagnosticText.textContent=JSON.stringify({joins:frame.research.joins??null,row_references:references},null,2);
   diagnosticText.hidden=false;showReferences.setAttribute('aria-expanded','true');
  });
  const diagnosticNote=el('p','These are reported diagnostics from the loaded snapshot, not a recomputation of its joins. Original source text is available separately above.');
  diagnostics.append(diagnosticTitle,joinNote,tableRegion,diagnosticNote,showReferences,diagnosticText);
  container.replaceChildren(title,notice,totals,cards,pager,panel,diagnostics);
  function closePanel(restoreFocus=false){
   ++generation;release();panel.hidden=true;text.hidden=true;text.textContent='';status.textContent='';download.hidden=true;download.removeAttribute('href');
   if(restoreFocus&&opener?.isConnected)opener.focus();opener=null;
  }
  async function inspect(row,trigger){
   closePanel();opener=trigger;const selected=++generation;
   panel.hidden=false;panelTitle.textContent=row.label+' · original source';status.textContent='Checking all retained bytes…';
   close.focus();
   try{
    const result=await model.original(row);if(selected!==generation)return;
    objectURL=urls.createObjectURL(makeBlob(result.bytes));download.href=objectURL;download.download=result.filename;download.hidden=false;
    status.textContent='Byte length and SHA-256 match the snapshot record. '+result.interpretation+(result.text===null?' This body is not valid UTF-8; download the exact bytes for inspection.':' Text is shown literally without parsing or rounding numbers.');
    text.textContent=result.text===null?'':result.text;text.hidden=result.text===null;
   }catch(error){
    if(selected!==generation)return;release();download.hidden=true;download.removeAttribute('href');text.textContent='';text.hidden=true;
    status.textContent='Source inspection unavailable: '+(error instanceof Error?error.message:'verification failed');
   }
  }
  function showSources(){
   cards.replaceChildren();const rows=current.sources,start=sourcePage*pageSize;
   for(const row of rows.slice(start,start+pageSize)){
    const card=el('article',undefined,'research-source');card.append(el('h4',row.label),el('p',row.key,'research-key'));
    card.append(el('p','Reported status: '+row.status+(row.reason?' · '+row.reason:'')));
    const dates=el('dl');for(const [label,value] of [['Source-declared date',row.declared],['Acquisition started',row.started],['Acquisition completed',row.completed]])dates.append(el('dt',label),el('dd',value||'Unavailable'));
    card.append(dates,el('p',row.detail));
    const meta=el('details');meta.append(el('summary','Retained-byte metadata'),el('p','Recorded bytes: '+(row.bytes===null?'unavailable':row.bytes)),el('p','SHA-256: '+(row.sha||'unavailable'),'research-hash'));card.append(meta);
    const inspectButton=button('Verify and inspect '+row.label,()=>inspect(row,inspectButton));inspectButton.disabled=!row.inspectable;card.append(inspectButton);cards.append(card);
   }
   previous.disabled=sourcePage===0;next.disabled=start+pageSize>=rows.length;
   pageStatus.textContent=rows.length?'Sources '+(start+1)+'–'+Math.min(start+pageSize,rows.length)+' of '+rows.length:'No source records';
   pager.hidden=rows.length<=pageSize;
  }
  function page(change){if(!current?.available)return;closePanel();sourcePage=Math.max(0,Math.min(Math.ceil(current.sources.length/pageSize)-1,sourcePage+change));showSources();}
  function render(snapshot){
   if(snapshot===frame)return;frame=snapshot;closePanel();sourcePage=0;current=model.view(snapshot);
   title.textContent=current.title;notice.textContent=current.detail;
   totals.textContent=current.available?(current.byteContractConsistent?'Recorded complete-body totals are consistent. ':'Recorded body totals are inconsistent or unavailable. ')+
    'These inputs do not authorize position sizing.':'';
   cards.replaceChildren();body.replaceChildren();diagnosticText.textContent='';diagnosticText.hidden=true;showReferences.setAttribute('aria-expanded','false');diagnostics.open=false;diagnostics.hidden=!current.available;pager.hidden=true;
   if(!current.available)return;
   showSources();
   for(const row of current.joins){const tr=el('tr');for(const value of [row.name,row.rows,row.unique,row.duplicates,row.invalid])tr.append(el('td',value===null?'Unavailable':String(value)));body.append(tr);}
  }
  function clear(){frame=Symbol('cleared');render(null);}
  function destroy(){clear();container.replaceChildren();}
  return {render,clear,destroy};
 }
 const api={mount};if(typeof module==='object'&&module.exports)module.exports=api;else root.JHPortfolioResearchPage=api;
})(typeof globalThis==='object'?globalThis:this);
