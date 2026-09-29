(function(){
  'use strict';
  const $=id=>document.getElementById(id), m=window.JHPortfolioScenario, io=window.JHScenarioIO;
  if(!m||!io){$('model-status').textContent='Reviewed scenario code is unavailable. Reload this page before calculating or replaying.';return;}
  let model=null, result=null, next=1, revision=0, pendingImport=null;
  const cell=(tag,value,parent)=>{const e=document.createElement(tag);e.textContent=value;parent.append(e);return e;};
  function invalidate(){revision++;pendingImport?.abort();pendingImport=null;result=null;$('download').disabled=true;$('results').replaceChildren();$('error').textContent='';}
  function addRow(values={}){
    const row=document.createElement('tr');
    const existing=new Set([...$('positions').children].map(e=>e.dataset.id));
    while(existing.has('position-'+next))next++;
    row.dataset.id=values.id||'position-'+next++;
    for(const [key,label] of [['label','Position label'],['currency','Currency label'],['weight_pct','Signed weight, % NAV'],['local_price_return_pct','Local price shock, %'],['fx_return_pct','FX shock, %']]){
      const td=document.createElement('td'),input=document.createElement('input');input.name=key;input.setAttribute('aria-label',label);input.required=true;
      if(key.endsWith('_pct'))input.inputMode='decimal';else input.maxLength=key==='currency'?3:120;
      input.value=values[key]??'';td.append(input);row.append(td);
    }
    const td=document.createElement('td'),button=cell('button','Remove',td);button.type='button';button.setAttribute('aria-label','Remove this position');
    button.onclick=()=>{row.remove();invalidate();};row.append(td);$('positions').append(row);invalidate();
  }
  function input(){
    return {contract:m.INPUT,label:$('label').value,horizon:$('horizon').value,base_currency:$('base').value,
      nav:$('nav-missing').checked?null:$('nav').value,cash_rate_pct:$('rate').value,cost_pct_nav:$('cost').value,
      positions:[...$('positions').children].map(row=>({id:row.dataset.id,position_type:'cash_security',
        ...Object.fromEntries([...row.querySelectorAll('input')].map(e=>[e.name,e.value]))}))};
  }
  function fill(v){
    m.calculate(v); // Validate the complete import before changing the form.
    $('label').value=v.label;$('horizon').value=v.horizon;$('base').value=v.base_currency;
    $('nav').value=v.nav??'';$('nav-missing').checked=v.nav===null;$('nav').disabled=v.nav===null;$('nav').required=v.nav!==null;
    $('rate').value=v.cash_rate_pct;$('cost').value=v.cost_pct_nav;$('positions').replaceChildren();
    v.positions.forEach(addRow);invalidate();
  }
  function show(output){
    const root=$('results');root.replaceChildren();cell('h2','Consequences under your assumptions',root);
    const metrics=document.createElement('div');metrics.className='metrics';root.append(metrics);
    for(const [label,key,suffix] of [['Portfolio impact','total_pnl_pct_nav','% of initial NAV'],['Currency P&L','total_pnl',output.base_currency],
      ['Ending NAV','ending_nav',output.base_currency],['Gross / net exposure','gross_weight_pct','% / '+output.net_weight_pct.decimal+'%'],['Residual cash / borrowing','cash_weight_pct','% of initial NAV']]){
      const block=document.createElement('div');cell('span',label,block);cell('strong',output[key]===null?'Unavailable':output[key].decimal+' '+suffix,block);metrics.append(block);
    }
    const detail=document.createElement('div');detail.className='table-wrap';root.append(detail);
    const table=document.createElement('table');detail.append(table);const head=document.createElement('thead'),hr=document.createElement('tr');head.append(hr);table.append(head);
    ['Position','Weight %','Base price shock %','Impact % NAV','P&L '+output.base_currency,'Ending signed value'].forEach(v=>cell('th',v,hr));
    const body=document.createElement('tbody');table.append(body);
    output.positions.forEach(p=>{const row=document.createElement('tr');[p.label,p.weight_pct.decimal,p.base_price_return_pct.decimal,p.pnl_pct_nav.decimal,p.pnl?.decimal??'Unavailable',p.ending_value?.decimal??'Unavailable'].forEach(v=>cell('td',v,row));body.append(row);});
    cell('p','Reconciliation: positions '+output.asset_pnl_pct_nav.decimal+'% + cash/financing '+output.cash_pnl_pct_nav.decimal+'% − explicit costs '+output.cost_pct_nav.decimal+'% = '+output.total_pnl_pct_nav.decimal+'% of initial NAV.',root);
    if(output.displayed_pnl_rounding_adjustment)cell('p','Displayed currency P&L rounding adjustment: '+output.displayed_pnl_rounding_adjustment.decimal+' '+output.base_currency+'. Exact numerator/denominator values are retained in the export; display uses ties-to-even rounding.',root);
    if(output.capital_exhausted_under_assumptions)cell('p','The assumed loss exhausts initial NAV. Negative NAV is shown without clamping; actual margin calls and liquidation paths are outside this model.',root).className='warning';
    cell('p',output.scope,root);
    $('download').disabled=!model;
  }
  $('scenario').addEventListener('input',invalidate);
  $('nav-missing').onchange=()=>{$('nav').disabled=$('nav-missing').checked;$('nav').required=!$('nav-missing').checked;invalidate();};
  $('add').onclick=()=>{if($('positions').children.length<100)addRow();else $('error').textContent='The model accepts at most 100 positions.';};
  $('example').onclick=()=>fill({contract:m.INPUT,label:'SYNTHETIC example — not your portfolio',horizon:'One hypothetical trading day',base_currency:'USD',nav:'100000',cash_rate_pct:'0',cost_pct_nav:'0.1',
    positions:[{id:'example-a',label:'Synthetic asset A',position_type:'cash_security',currency:'USD',weight_pct:'60',local_price_return_pct:'-10',fx_return_pct:'0'},
      {id:'example-b',label:'Synthetic asset B',position_type:'cash_security',currency:'EUR',weight_pct:'30',local_price_return_pct:'2',fx_return_pct:'-1'}]});
  $('scenario').onsubmit=event=>{event.preventDefault();invalidate();try{const v=input();result={input:v,output:m.calculate(v)};show(result.output);}catch(error){$('error').textContent=error.message;}};
  $('download').onclick=()=>{
    if(!model||!result)return;
    const body=JSON.stringify({contract:'portfolio-scenario-export.v1',model,input:result.input,output:result.output},null,2);
    const url=URL.createObjectURL(new Blob([body],{type:'application/json'})),a=document.createElement('a');a.href=url;a.download='justhodl-hypothetical-scenario.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
  };
  $('import').onchange=async()=>{
    const file=$('import').files[0];if(!file)return;
    invalidate();const generation=revision,controller=new AbortController();pendingImport=controller;
    $('error').textContent='Reading and verifying scenario…';
    try{
      const packet=await io.readFile(file,{signal:controller.signal});
      if(generation!==revision||controller.signal.aborted)return;
      if(!model)throw Error('The reviewed model is not verified yet. Local arithmetic remains available.');
      const out=io.verify(packet,model,m);
      pendingImport=null;fill(packet.input);result={input:packet.input,output:out};show(out);
      $('error').textContent='Imported and completely recalculated on this device.';
      $('import').value='';
    }catch(error){
      if(generation!==revision||controller.signal.aborted)return;
      pendingImport=null;invalidate();$('error').textContent=error.message;$('import').value='';
    }finally{if(pendingImport===controller)pendingImport=null;}
  };
  addRow();
  (async()=>{
    try{
      const script=$('scenario-model'),declared={contract:m.CONTRACT,sha256:script.dataset.sha256,bytes:Number(script.dataset.bytes)};
      io.identity(declared,m);
      if(String(declared.bytes)!==script.dataset.bytes)throw Error('Page model byte identity is invalid');
      const fetchBytes=url=>io.readComplete(signal=>fetch(url,{cache:'no-store',credentials:'omit',redirect:'error',signal}));
      const [publication,bytes]=await Promise.all([fetchBytes('/data/position-sizing.json'),fetchBytes('/jh-portfolio-scenario.js')]);
      const packet=io.decode(publication),sha=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(v=>v.toString(16).padStart(2,'0')).join('');
      if(sha!==declared.sha256||bytes.byteLength!==declared.bytes)throw Error('Fetched model differs from the page-bound reviewed source');
      if(packet.contract!=='portfolio-scenario-availability.v1'||packet.model_contract!==m.CONTRACT||packet.input_contract!==m.INPUT||packet.call!=='WAIT'||!io.same(packet.permissions,{calls_eligible:false,sizing_eligible:false,execution_eligible:false,may_recommend_trades:false})||!io.same(packet.scenario_model,{key:'data/scenario-model/models/'+sha+'.js',sha256:sha,bytes:bytes.byteLength}))throw Error('Page and published model versions or permissions do not match');
      model=declared;
      $('model-status').textContent='Model published '+packet.generated_at+' · model time, not market data. Source SHA-256 '+sha+'.';
      const link=cell('a','Inspect the retained model source',$('model-status'));link.href='/'+packet.scenario_model.key;link.rel='noopener';
      if(result)$('download').disabled=false;
    }catch(error){$('model-status').textContent=error.message+'. Local arithmetic is available; verified export is disabled.';}
  })();
})();
