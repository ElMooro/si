(function (root) {
  'use strict';
  async function boot(host) {
    const A=root.JHEtfDeskResearch,H=root.JHHoldings,panel=host.querySelector('[data-etf-desk]');
    if (!A || !H || !panel) return;
    const fetcher=root.fetch.bind(root),q=selector=>panel.querySelector(selector);
    let packet=null,selectedFund='SPY',controller=null,publication=0,epoch=0,rowEpoch=0,profileEpoch=0,holdingEpoch=0;
    const params=new URL(root.location.href).searchParams,requestedFund=params.get('fund'),pinned=params.has('run')?params.get('run'):null;
    if(A.ticker(requestedFund))selectedFund=requestedFund;
    let snap=null,selectedRow=null,rowCache=new Map(),holdPage=0,flowPage=0,flowDoc=null,lastMask='';
    const form=host.querySelector('[data-ed-scenario]'),result=host.querySelector('[data-ed-scenario-output]');
    const invalidate=()=>{result.textContent='Selection, source or assumptions changed; recalculate your hypothetical scenario.';};
    form.oninput=invalidate;
    form.onsubmit=e=>{
      e.preventDefault();
      try {
        const values=['position','shock','cost'].map(name=>{const raw=form.elements[name].value;if (raw.trim()==='') throw Error('Complete every scenario assumption');return Number(raw);});
        const out=A.scenario(packet,selectedFund,...values),cash=n=>n.toLocaleString('en-US',{style:'currency',currency:'USD'});
        result.innerHTML='<p>'+A.esc(out.fund)+' hypothetical gross change: <b>'+cash(out.gross_change_usd)+'</b>; after your costs: <b>'+cash(out.net_change_usd)+'</b>.</p><p>'+A.esc(out.scope)+'</p><p>Evidence run '+A.esc(out.run.manifest_key.split('/').pop())+'. Inputs stay in this browser.</p>';
      } catch (error) {result.textContent=error.message;}
    };
    const errorText=(selector,error)=>{const el=q(selector);if(el && error.name!=='AbortError')el.textContent=error.message;};
    const wrapped=()=>({funds:{[selectedFund]:packet.funds[selectedFund].holdings}});
    function drawInventory() {
      q('[data-ed-inventory]').innerHTML=A.inventory(packet,q('[data-ed-query]').value);
      q('[data-ed-inventory]').querySelectorAll('[data-ed-select]').forEach(button=>{button.onclick=()=>{
        q('[data-ed-fund]').value=button.dataset.edSelect;void inspect();q('[data-ed-selection]').scrollIntoView({block:'start',behavior:'smooth'});
      };});
    }
    function drawFlows() {
      const rows=flowDoc?.history || [],pages=Math.max(1,Math.ceil(rows.length/25));flowPage=Math.max(0,Math.min(flowPage,pages-1));
      q('[data-ed-flow-previous]').disabled=flowPage===0;q('[data-ed-flow-next]').disabled=flowPage+1>=pages;
      q('[data-ed-flow-rows]').innerHTML='<p>'+rows.length+' verified-history observations. Page '+(flowPage+1)+' of '+pages+'. Latest effective date first. This is the acquired history window, not a lifetime tape.</p>'+A.table(
        ['Effective date','Processed date','Reported creation / redemption · USD','NAV · USD','Shares outstanding','Original row evidence'],rows.slice().reverse().slice(flowPage*25,flowPage*25+25).map(r=>[
          r.date,r.processed_date,A.exact(r.flow_decimal),A.exact(r.nav_decimal),A.exact(r.shares_decimal),r.source_rows.map(A.provenance).join(' | ')]),'Verified source flow history');
    }
    async function inspectProfile() {
      const token=++profileEpoch,generation=epoch,expected=packet,t=selectedFund,role=q('[data-ed-profile-basis]').value;
      q('[data-ed-profile]').textContent='Verifying original profile fields…';
      try {const doc=await A.profile(expected,t,role,fetcher,controller.signal);
        if(token===profileEpoch && generation===epoch && expected===packet)q('[data-ed-profile]').innerHTML=A.profileView(doc,role);
      }catch(error){if(token===profileEpoch && generation===epoch)errorText('[data-ed-profile]',error);}
    }
    async function showHoldings() {
      const token=++rowEpoch,generation=epoch,holdingGeneration=holdingEpoch,current=snap,cache=rowCache;selectedRow=null;invalidate();
      q('[data-ed-row-detail]').textContent='Select any holding to inspect its exact source row.';q('[data-ed-comparison]').textContent='';
      if(!current)return;
      const found=H.searchRows(current,q('[data-ed-holding-query]').value),pages=Math.max(1,Math.ceil(found.length/30));
      holdPage=Math.max(0,Math.min(holdPage,pages-1));const visible=found.slice(holdPage*30,holdPage*30+30);
      q('[data-ed-hold-previous]').disabled=holdPage===0;q('[data-ed-hold-next]').disabled=holdPage+1>=pages;
      q('[data-ed-holdings]').textContent='Verifying the selected holding row pages…';
      try {
        const numbers=[...new Set(visible.map(r=>r.part))];
        const parts=await Promise.all(numbers.map(async number=>{
          if(!cache.has(number))cache.set(number,H.rowPart(current,number,fetcher,controller.signal));
          try{return await cache.get(number);}catch(error){cache.delete(number);throw error;}
        }));
        if(token!==rowEpoch || generation!==epoch || holdingGeneration!==holdingEpoch || current!==snap)return;
        const byId=new Map(parts.flat().map(r=>[r.row_id,r])),rows=visible.map(r=>byId.get(r.row_id));
        if(rows.some(r=>!r))throw Error('Selected holding row is absent from verified pages');
        q('[data-ed-holdings]').innerHTML='<p>'+found.length+' of '+current.indexed_rows+' rows match. Page '+(holdPage+1)+' of '+pages+'.</p>'+H.rowTable(rows);
        q('[data-ed-holdings]').querySelectorAll('[data-hd-row]').forEach(button=>{button.onclick=()=>{
          selectedRow=rows.find(r=>r.row_id===button.dataset.hdRow);q('[data-ed-row-detail]').innerHTML=H.rowDetail(selectedRow);q('[data-ed-comparison]').textContent='';
        };});
      }catch(error){if(token===rowEpoch && generation===epoch && holdingGeneration===holdingEpoch)errorText('[data-ed-holdings]',error);}
    }
    async function inspectHoldings() {
      const generation=epoch,holdingGeneration=++holdingEpoch;rowEpoch++;snap=null;selectedRow=null;rowCache=new Map();holdPage=0;invalidate();
      const t=selectedFund,role=q('[data-ed-holding-basis]').value,source=wrapped();
      q('[data-ed-holding-query]').value='';q('[data-ed-holdings]').textContent='';q('[data-ed-row-detail]').textContent='';q('[data-ed-comparison]').textContent='';
      q('[data-ed-snapshot]').textContent='Verifying the complete constituent index…';q('[data-ed-compare]').disabled=true;
      try {const doc=await H.snapshot(source,t,role,fetcher,controller.signal);
        if(generation!==epoch || holdingGeneration!==holdingEpoch)return;snap=doc;q('[data-ed-snapshot]').innerHTML=H.snapshotView(doc,role);q('[data-ed-compare]').disabled=false;await showHoldings();
      }catch(error){if(generation===epoch && holdingGeneration===holdingEpoch)errorText('[data-ed-snapshot]',error);}
    }
    async function compareHoldings() {
      const generation=epoch,holdingGeneration=holdingEpoch,row=selectedRow,source=wrapped(),t=selectedFund;
      q('[data-ed-comparison]').textContent='Verifying the current and earlier position comparison…';
      try {const result=await H.comparison(source,t,row?.identity_key,fetcher,controller.signal),s=result.summary;
        let html='<p>Effective dates: '+A.esc(s.prior_effective_dates.join(', '))+' → '+A.esc(s.current_effective_dates.join(', '))+'. '+A.esc(s.scope)+'</p>'+A.table(['Identity status','Count'],Object.entries(s.identity_status_counts),'Position comparison coverage');
        if(row?.identity_key)html+=result.row?A.table(['Identity','Status','Unadjusted position-unit change','Raw weight change'],[[row.constituent_ticker || row.constituent_name,result.row.status,A.exact(result.row.shares_held_change_raw_decimal),A.exact(result.row.weight_change_raw_decimal)]],'Selected position comparison'):'<p>No unambiguous comparison for the selected identity.</p>';
        if(generation===epoch && holdingGeneration===holdingEpoch && row===selectedRow)q('[data-ed-comparison]').innerHTML=html;
      }catch(error){if(generation===epoch && holdingGeneration===holdingEpoch && row===selectedRow)errorText('[data-ed-comparison]',error);}
    }
    async function inspect() {
      epoch++;rowEpoch++;profileEpoch++;invalidate();flowDoc=null;flowPage=0;
      for(const name of ['position','shock','cost'])form.elements[name].value='';
      selectedFund=q('[data-ed-fund]').value;const t=selectedFund,expected=packet;
      q('[data-ed-selection]').innerHTML=A.fundView(packet,t);
      host.querySelector('[data-ed-scenario-name]').textContent=t;q('[data-ed-profile-basis]').value='current';q('[data-ed-holding-basis]').value='current';
      q('[data-ed-flow-rows]').textContent='Verifying retained flow observations…';
      // All three panels share the selected fund; each has its own request epoch.
      const hold=inspectHoldings(),generation=epoch,prof=inspectProfile();
      const flows=(async()=>{try{const doc=await A.history(expected,t,fetcher,controller.signal);
        if(generation===epoch && expected===packet){flowDoc=doc;drawFlows();}
      }catch(error){if(generation===epoch)errorText('[data-ed-flow-rows]',error);}})();
      await Promise.all([hold,prof,flows]);
    }
    function render(p) {
      return '<p>'+(pinned!==null?'Recorded ETF desk':'Latest ETF desk')+' · '+A.esc(p.generated_at)+' · <a href="/etf.html">Open latest desk</a> · <a href="/market-evidence.html">Connected market evidence</a></p><div data-ed-summary>'+A.summary(p)+'</div><button type="button" data-ed-refresh>'+(pinned!==null?'Recheck this recorded desk':'Refresh and verify')+'</button><h2>All configured funds</h2>'+
        '<label>Find a fund, name or issuer<input data-ed-query type="search" autocomplete="off"></label><div data-ed-inventory></div>'+
        '<div class="xr-controls"><label>Inspect fund<select data-ed-fund>'+Object.keys(p.funds).sort().map(t=>'<option'+(t===selectedFund?' selected':'')+'>'+A.esc(t)+'</option>').join('')+'</select></label></div><div data-ed-selection></div>'+
        '<h2>Profile fields and exposures</h2><label>Profile basis<select data-ed-profile-basis><option value="current">Current collection</option><option value="prior">Earlier query cutoff</option><option value="retained_previous_current">Previous retained profile, if available</option></select></label><div data-ed-profile role="status"></div>'+
        '<h2>Complete reported constituents</h2><label>Holdings basis<select data-ed-holding-basis><option value="current">Current collection</option><option value="prior">Earlier query cutoff</option><option value="retained_previous_current">Previous retained holdings, if available</option></select></label><div data-ed-snapshot></div>'+
        '<label>Find a holding by name, ticker or identifier<input data-ed-holding-query type="search" autocomplete="off"></label><div data-ed-holdings></div><div class="ed-pager"><button type="button" data-ed-hold-previous>Previous holdings</button><button type="button" data-ed-hold-next>Next holdings</button></div>'+
        '<div data-ed-row-detail role="status"></div><button type="button" data-ed-compare>Compare reported positions</button><div data-ed-comparison role="status"></div>'+
        '<h2>Original-source flow history</h2><div data-ed-flow-rows></div><div class="ed-pager"><button type="button" data-ed-flow-previous>Newer observations</button><button type="button" data-ed-flow-next>Older observations</button></div>'+
        '<h2>Evidence and definitions</h2><p><a href="/'+A.esc(p.replay.manifest_key)+'">Retained calculation run</a> · <a href="/flows.html">Canonical flow evidence</a> · <a href="/etf-holdings.html">Canonical holding evidence</a></p>'+
        '<details><summary>Read the calculation scope and source limits</summary>'+Object.entries(p.methodology).map(([k,v])=>'<p><b>'+A.esc(k)+'</b>: '+A.esc(v)+'</p>').join('')+'</details>';
    }
    async function refresh() {
      const token=++publication;epoch++;rowEpoch++;profileEpoch++;controller?.abort();controller=new AbortController();
      packet=null;snap=null;flowDoc=null;invalidate();panel.textContent='Verifying retained ETF desk evidence…';
      try {const candidate=pinned!==null?await A.recordedRun(pinned,fetcher,controller.signal):await A.verifyPacket((await A.load(A.CURRENT,fetcher,controller.signal)).doc,fetcher,controller.signal);
        if(token!==publication)return;packet=candidate;if(!packet.funds[selectedFund])selectedFund=Object.keys(packet.funds).sort()[0];
        panel.innerHTML=render(packet);q('[data-ed-query]').oninput=drawInventory;q('[data-ed-fund]').onchange=inspect;
        q('[data-ed-profile-basis]').onchange=inspectProfile;q('[data-ed-holding-basis]').onchange=inspectHoldings;
        q('[data-ed-holding-query]').oninput=()=>{holdPage=0;void showHoldings();};
        q('[data-ed-hold-previous]').onclick=()=>{holdPage--;void showHoldings();};q('[data-ed-hold-next]').onclick=()=>{holdPage++;void showHoldings();};
        q('[data-ed-flow-previous]').onclick=()=>{flowPage--;drawFlows();};q('[data-ed-flow-next]').onclick=()=>{flowPage++;drawFlows();};
        q('[data-ed-compare]').onclick=compareHoldings;q('[data-ed-refresh]').onclick=refresh;
        drawInventory();await inspect();
      }catch(error){if(token===publication && error.name!=='AbortError'){packet=null;panel.textContent='ETF desk evidence unavailable: '+error.message;}}
    }
    root.setInterval(()=>{
      if(!packet)return;
      const mask=JSON.stringify(Object.values(packet.funds).map(f=>A.eligibility(f)));
      q('[data-ed-summary]').innerHTML=A.summary(packet);
      if(lastMask && mask!==lastMask){invalidate();drawInventory();q('[data-ed-selection]').innerHTML=A.fundView(packet,selectedFund);}
      lastMask=mask;
    },60000);
    await refresh();
  }
  const api={boot};if(typeof module!=='undefined' && module.exports)module.exports=api;root.JHEtfDeskPage=api;
  if(root.document)root.document.addEventListener('DOMContentLoaded',()=>{root.document.querySelectorAll('main[data-etf-desk-page]').forEach(host=>{void boot(host);});});
})(typeof globalThis!=='undefined'?globalThis:this);
