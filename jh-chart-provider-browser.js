/* Provider discovery inside the chart. Metadata is not proof of history or entitlement. */
(function (global) {
  "use strict";
  var PROXY="https://justhodl-data-proxy.raafouis.workers.dev", PAGE=50, MAX_BYTES=4000000;
  // Exact adapters implemented by justhodl-symdir. Unknown prefixes must not fall through to a market symbol.
  var SCALAR=new Set(["ustpar","official-yields","fred","te","eurostat","ecb","nyfed","ofr","ofr-fsi","ofr-hfm","ofr-bsrm","boj","statcan","worldbank","treasury","boe","census","bls"]);
  var model={provider:"",dataset:"",query:"",offset:0,destination:"chart",view:"directory",filePage:-1,filePages:0}, generation=0, controller=null, opener=null, dialog=null, response=null;
  function text(value){return typeof value==="string"?value:"";}
  function providerId(value){return typeof value==="string"&&/^[a-z0-9][a-z0-9-]*$/.test(value)?value:"";}
  function natural(value){return Number.isSafeInteger(value)&&value>=0?value:null;}
  function route(state){
    if(!state.provider)return "/data/provider-catalog.json";
    if(state.view==="files")return "/data/providers/"+state.provider+(state.filePage<0?".json":"/page-"+String(state.filePage).padStart(3,"0")+".json");
    var params=new URLSearchParams({limit:String(PAGE),offset:String(state.offset),q:state.query});
    if(state.dataset){params.set("ds",state.dataset);return PROXY+"/browse?"+params;}
    params.set("provider",state.provider);return PROXY+"/explorer?"+params;
  }
  function action(row){
    if(!row||typeof row!=="object"||Array.isArray(row))return "unavailable";
    var id=text(row.id),kind=text(row.kind),provider=providerId(text(row.provider))||id.split(":")[0].toLowerCase();
    if(/^provider:[a-z0-9-]+$/.test(id))return "provider";
    if((kind==="dataset"||kind==="indicator_ref")&&id.includes(":")&&!/^asset:/.test(id))return "dataset";
    if(kind==="series"&&row.chartable===true&&id.includes(":")&&SCALAR.has(provider)&&id.split(":")[0].toLowerCase()===provider)return "chart";
    return "inspect";
  }
  function pageInfo(packet,state){
    var rows=Array.isArray(packet.rows)?packet.rows:[],count=rows.length,total=natural(packet.total),matched=natural(packet.matched);
    var end=state.offset+count,limited=!!packet.truncated||!!packet.warehouse_more,searchCap=!state.dataset&&!!state.query;
    var next=count===PAGE&&(total===null||end<total);
    if(searchCap&&end>=200)next=false;
    return {count:count,total:total,matched:matched,end:end,next:next,limited:limited,searchCap:searchCap};
  }
  async function receive(url,signal){
    var res=await global.fetch(url,{cache:"no-store",signal:signal});
    if(!res.ok)throw new Error("HTTP "+res.status+"; this endpoint was not retried through another route.");
    var chunks=[],n=0,reader=res.body&&res.body.getReader?res.body.getReader():null;
    if(reader){try{while(true){var part=await reader.read();if(part.done)break;n+=part.value.byteLength;if(n>MAX_BYTES){await reader.cancel();throw new Error("Catalogue response exceeds the 4 MB limit.");}chunks.push(part.value);}}finally{reader.releaseLock();}}
    else {var single=new Uint8Array(await res.arrayBuffer());n=single.length;if(n>MAX_BYTES)throw new Error("Catalogue response exceeds the 4 MB limit.");chunks=[single];}
    var raw=new Uint8Array(n),at=0;chunks.forEach(function(c){raw.set(c,at);at+=c.byteLength;});
    var packet=JSON.parse(new TextDecoder("utf-8",{fatal:true}).decode(raw));
    if(!packet||typeof packet!=="object"||Array.isArray(packet))throw new Error("Invalid catalogue document.");
    var hash=null;if(global.crypto&&global.crypto.subtle){var digest=await global.crypto.subtle.digest("SHA-256",raw);hash=Array.from(new Uint8Array(digest),function(x){return x.toString(16).padStart(2,"0");}).join("");}
    return {packet:packet,raw:raw,receipt:{url:url,received_at:new Date().toISOString(),http_status:res.status,bytes:n,sha256:hash,observation_freshness_verified:false}};
  }
  function node(tag,value,cls){var e=document.createElement(tag);if(value!==undefined)e.textContent=value;if(cls)e.className=cls;return e;}
  function button(label,handler){var b=node("button",label);b.type="button";b.onclick=handler;return b;}
  function status(message){dialog.querySelector(".jhp-status").textContent=message;}
  function close(){generation++;if(controller)controller.abort();controller=null;if(dialog){if(dialog.open)dialog.close();dialog.hidden=true;}var target=opener&&opener.isConnected&&opener.getClientRects().length?opener:document.getElementById("symchip");if(target)target.focus();}
  function capture(){if(!response)return;var a=node("a"),u=URL.createObjectURL(new Blob([response.raw],{type:"application/json"}));a.href=u;a.download="justhodl-provider-catalogue.json";a.click();global.setTimeout(function(){URL.revokeObjectURL(u);},1000);}
  function footer(){
    var foot=dialog.querySelector(".jhp-foot");foot.replaceChildren();
    foot.appendChild(node("p","Directory metadata describes what is indexed. History, licence, units and observation dates must still be checked when a series loads."));
    if(!response)return;
    var details=node("details"),summary=node("summary","Source receipt and original metadata");details.appendChild(summary);
    details.appendChild(node("pre",JSON.stringify(response.receipt,null,2)));details.appendChild(button("Download original metadata",capture));foot.appendChild(details);
  }
  function render(packet,state){
    var list=dialog.querySelector(".jhp-rows"),nav=dialog.querySelector(".jhp-pages"),crumb=dialog.querySelector(".jhp-crumb");list.replaceChildren();nav.replaceChildren();crumb.replaceChildren();
    crumb.appendChild(button("All providers",function(){navigate({provider:"",dataset:"",offset:0,query:"",view:"directory"});}));
    if(state.provider){
      crumb.appendChild(button(state.provider,function(){navigate({dataset:"",offset:0,query:"",view:"directory"});}));
      crumb.appendChild(button("Indexed series",function(){navigate({dataset:"",offset:0,query:"",view:"directory"});}));
      crumb.appendChild(button("Stored files",function(){navigate({dataset:"",offset:0,query:"",view:"files",filePage:-1,filePages:0});}));
    }
    if(state.dataset)crumb.appendChild(node("span",state.dataset));
    if(!state.provider){
      if(!Array.isArray(packet.providers))throw new Error("Provider catalogue is missing its provider list.");
      var rows=packet.providers.filter(function(p){return providerId(p.slug)&&(!state.query||(String(p.name)+" "+p.slug).toLowerCase().includes(state.query.toLowerCase()));});
      status(rows.length+" provider entries · catalogue as of "+(text(packet.as_of)||"unreported")+". Select a provider to browse its indexed datasets.");
      rows.forEach(function(p){var b=button(text(p.name)||p.slug,function(){navigate({provider:p.slug,dataset:"",offset:0,query:""});});b.className="jhp-row";b.appendChild(node("small",p.slug+" · "+(text(p.api)||"source not reported")));list.appendChild(b);});
    }else if(state.view==="files"){
      if(!Array.isArray(packet.keys))throw new Error("Stored-file catalogue has no key list.");
      if(state.filePage<0){if(packet.slug!==state.provider||natural(packet.n_pages)===null)throw new Error("Stored-file catalogue identity or page count is invalid.");model.filePages=packet.n_pages;state.filePages=packet.n_pages;}
      else if(packet.page!==state.filePage)throw new Error("Stored-file page does not match the request.");
      var files=packet.keys.filter(function(row){return !state.query||JSON.stringify(row).toLowerCase().includes(state.query.toLowerCase());});
      status("Stored-file metadata · "+(state.filePage<0?"overview":("continuation "+(state.filePage+1)+" of "+state.filePages))+" · "+files.length+" entries on this page. Search filters this page only; file presence does not establish chartable history.");
      files.forEach(function(row){var box=node("div",undefined,"jhp-row");box.appendChild(node("strong",text(row.key)||"Unnamed object"));box.appendChild(node("small","Reported status: "+(text(row.status)||"unreported")+" · stored object, no inferred scalar adapter"));var details=node("details");details.appendChild(node("summary","Full file metadata"));details.appendChild(node("pre",JSON.stringify(row,null,2)));box.appendChild(details);list.appendChild(box);});
      var back=button("Previous",function(){navigate({filePage:Math.max(-1,state.filePage-1)});});back.disabled=state.filePage<0;nav.appendChild(back);
      var forward=button("Next",function(){navigate({filePage:state.filePage+1});});forward.disabled=state.filePage+1>=state.filePages;nav.appendChild(forward);
    }else{
      if(packet.error||packet.warehouse_error)throw new Error(text(packet.error)||text(packet.warehouse_error)||"Provider directory reported an error.");
      if(!Array.isArray(packet.rows))throw new Error("Provider response has no row list.");
      if(state.dataset&&packet.ds!==state.dataset)throw new Error("Dataset identity does not match the requested dataset.");
      if(!state.dataset&&packet.provider!==state.provider)throw new Error("Provider identity does not match the requested provider.");
      if(packet.offset!==undefined&&packet.offset!==state.offset)throw new Error("Directory page offset does not match the request.");
      var info=pageInfo(packet,state),message=info.count?"Rows "+(state.offset+1)+"–"+info.end:"No rows returned for this selection";
      if(info.total!==null)message+=" · directory reports "+info.total+" entries";
      if(info.limited)message+=" · partial scan; absence here does not prove the series is unavailable";
      if(info.searchCap)message+=" · text search returns at most 200 entries; refine the query for more specific matches";
      if(packet.hint)message+=" · "+text(packet.hint);
      status(message);
      if(packet.facets&&typeof packet.facets==="object"&&!Array.isArray(packet.facets)){
        var facets=node("details"),heading=node("summary","Dimension codes in the scanned sample");facets.appendChild(heading);
        Object.keys(packet.facets).forEach(function(dimension){var values=packet.facets[dimension];if(!Array.isArray(values))return;var line=node("p");line.appendChild(node("strong",dimension+": "));values.forEach(function(item){if(!Array.isArray(item)||typeof item[0]!=="string")return;var label=item[0]+(text(item[2])?" — "+item[2]:"");line.appendChild(button(label,function(){navigate({query:[state.query,item[0]].filter(Boolean).join(" "),offset:0});}));});facets.appendChild(line);});
        facets.appendChild(node("p","Codes narrow the text search within the current scan. Select the full series ID to fix all dimensions."));list.appendChild(facets);
      }
      packet.rows.forEach(function(row){
        var kind=action(row),id=text(row.id),box=node("div",undefined,"jhp-row"),title=node("strong",text(row.name)||id||"Unidentified entry");
        if(kind!=="provider"&&id.split(":")[0].toLowerCase()!==state.provider)kind="inspect";
        box.appendChild(title);box.appendChild(node("code",id));
        var qualifiers=["Kind: "+(text(row.kind)||"unreported"),"Unit: "+(text(row.unit)||"unreported"),"Frequency: "+(text(row.freq)||"unreported")];
        if(row.first||row.last)qualifiers.push("Reported coverage: "+(text(row.first)||"?")+" → "+(text(row.last)||"?"));
        box.appendChild(node("small",qualifiers.join(" · ")));
        if(kind==="provider"&&id==="provider:"+state.provider)box.appendChild(node("small","Provider overview. Use Stored files for its published object catalogue; indexed series may be incomplete."));
        else if(kind==="provider")box.appendChild(button("Browse provider",function(){navigate({provider:id.split(":")[1],dataset:"",offset:0,query:"",view:"directory"});}));
        else if(kind==="dataset")box.appendChild(button("Choose series / dimensions",function(){navigate({dataset:id,offset:0,query:""});}));
        else if(kind==="chart")box.appendChild(button(state.destination==="add"?"Add exact series":state.destination==="compare"?"Compare exact series":"Chart exact series",function(){
          if(typeof global.jhGoSymbol!=="function"){status("Chart engine is not ready; selection retained.");return;}
          close();global.jhGoSymbol(id,state.destination);
        }));
        else box.appendChild(node("small",row.chartable===true?"Directory marks this chartable, but no exact scalar adapter is verified here. No substitute will be plotted.":"Stored object or reference metadata; select an exact scalar series before plotting."));
        var details=node("details"),summary=node("summary","Full directory entry");details.appendChild(summary);details.appendChild(node("pre",JSON.stringify(row,null,2)));box.appendChild(details);list.appendChild(box);
      });
      var prev=button("Previous",function(){navigate({offset:Math.max(0,state.offset-PAGE)});});prev.disabled=state.offset===0;nav.appendChild(prev);
      var next=button("Next",function(){navigate({offset:state.offset+PAGE});});next.disabled=!info.next;nav.appendChild(next);
    }
    footer();
  }
  async function load(){
    var mine=++generation;if(controller)controller.abort();controller=new AbortController();var own=controller,state=Object.assign({},model);response=null;
    status("Loading catalogue metadata…");dialog.querySelector(".jhp-rows").replaceChildren();dialog.querySelector(".jhp-pages").replaceChildren();footer();dialog.setAttribute("aria-busy","true");
    var timeout,job=receive(route(state),own.signal),deadline=new Promise(function(_,reject){timeout=global.setTimeout(function(){own.abort();reject(new Error("Catalogue request timed out. Use Retry to try the same endpoint."));},12000);});
    try{var value=await Promise.race([job,deadline]);if(mine!==generation)return;response=value;render(value.packet,state);}
    catch(error){if(mine!==generation)return;status("Catalogue unavailable: "+error.message);dialog.querySelector(".jhp-rows").replaceChildren();footer();}
    finally{global.clearTimeout(timeout);if(mine===generation)dialog.removeAttribute("aria-busy");}
  }
  function navigate(delta){Object.assign(model,delta);dialog.querySelector("input").value=model.query;dialog.querySelector("input").placeholder=model.view==="files"?"Filter metadata on this file page":"Series, country or dimension code";load();}
  function ensure(){
    if(dialog)return;
    var style=node("style");style.textContent="#jh-provider-browser[hidden]{display:none}#jh-provider-browser{position:fixed;inset:4vh max(12px,calc((100vw - 960px)/2));z-index:200;background:var(--bg,#131722);color:var(--fg,#d1d4dc);border:1px solid var(--line,#434651);border-radius:12px;box-shadow:0 20px 80px #0008;display:flex;flex-direction:column;padding:16px;max-height:92vh;gap:10px;font:13px/1.45 Arial,sans-serif}#jh-provider-browser *{box-sizing:border-box}#jh-provider-browser header,#jh-provider-browser form,.jhp-crumb,.jhp-pages{display:flex;align-items:center;gap:8px;flex-wrap:wrap}#jh-provider-browser h2{font-size:18px;margin:0;flex:1}.jhp-rows{overflow:auto;min-height:80px;flex:1;overscroll-behavior:contain}.jhp-row{display:block;width:100%;text-align:left;padding:12px;margin:0 0 8px;border:1px solid var(--line,#434651);border-radius:6px;background:var(--raised,#1e222d);color:inherit;overflow-wrap:anywhere}.jhp-row>strong,.jhp-row>code,.jhp-row>small{display:block;margin-bottom:5px}#jh-provider-browser button{cursor:pointer;padding:6px 10px;border:1px solid var(--line,#434651);border-radius:5px;background:var(--raised,#1e222d);color:inherit}#jh-provider-browser button:disabled{opacity:.45;cursor:default}#jh-provider-browser button:focus-visible,#jh-provider-browser input:focus-visible{outline:2px solid #2962ff;outline-offset:2px}#jh-provider-browser input{min-width:0;flex:1;width:180px;padding:8px;background:var(--raised,#1e222d);color:inherit;border:1px solid var(--line,#434651);border-radius:5px}#jh-provider-browser pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:180px;overflow:auto;font-size:11px}.jhp-foot p{margin:0}.jhp-status{color:var(--mut,#a9acb6)}.jhp-crumb span{overflow-wrap:anywhere;max-width:100%}@media(max-width:500px){#jh-provider-browser{inset:8px;padding:12px;max-height:calc(100dvh - 16px)}.jhp-foot{font-size:11px}}";document.head.appendChild(style);
    style.textContent+="#jh-provider-browser{margin:0;width:auto;height:auto}#jh-provider-browser::backdrop{background:#0007}";
    dialog=node("dialog");dialog.id="jh-provider-browser";dialog.hidden=true;dialog.setAttribute("aria-modal","true");dialog.setAttribute("aria-labelledby","jhp-title");dialog.addEventListener("cancel",function(e){e.preventDefault();close();});
    var head=node("header"),title=node("h2","Provider datasets & series");title.id="jhp-title";head.appendChild(title);head.appendChild(button("Close",close));dialog.appendChild(head);
    dialog.appendChild(node("nav",undefined,"jhp-crumb"));var form=node("form"),input=node("input");input.type="search";input.placeholder="Series, country or dimension code";input.setAttribute("aria-label","Filter provider metadata");form.appendChild(input);var submit=node("button","Search");submit.type="submit";form.appendChild(submit);form.onsubmit=function(e){e.preventDefault();navigate({query:input.value.trim(),offset:0});};form.appendChild(button("Retry",load));dialog.appendChild(form);
    var message=node("p",undefined,"jhp-status");message.setAttribute("role","status");dialog.appendChild(message);dialog.appendChild(node("div",undefined,"jhp-rows"));dialog.appendChild(node("nav",undefined,"jhp-pages"));dialog.appendChild(node("footer",undefined,"jhp-foot"));document.body.appendChild(dialog);
    dialog.addEventListener("keydown",function(e){e.stopPropagation();if(e.key==="Escape"){e.preventDefault();close();}if(e.key==="Tab"){var nodes=Array.from(dialog.querySelectorAll("button:not(:disabled),input,summary,a[href]")).filter(function(n){return n.getClientRects().length;});var first=nodes[0],last=nodes[nodes.length-1];if(e.shiftKey&&document.activeElement===first){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}});
  }
  function open(options){
    options=options||{};var id=text(options.id),p=providerId(options.provider)||(/^DATA:|^provider:/i.test(id)?providerId(id.split(":")[1]):providerId(id.split(":")[0].toLowerCase()));
    if(p==="search")p="";
    ensure();if(!dialog.open)opener=document.activeElement;model={provider:p,dataset:p&&id&&!/^DATA:|^provider:/i.test(id)?id:"",query:"",offset:0,destination:["chart","add","compare"].includes(options.destination)?options.destination:"chart",view:"directory",filePage:-1,filePages:0};dialog.hidden=false;if(!dialog.open)dialog.showModal();dialog.querySelector("input").value="";dialog.querySelector("input").placeholder="Series, country or dimension code";dialog.querySelector("input").focus();load();return true;
  }
  function install(){var parent=document.querySelector("#symsearch .shd");if(parent&&!document.getElementById("jh-provider-open")){var b=button("Providers",function(){var box=document.getElementById("symsearch");open({destination:box&&box.dataset.dest});});b.id="jh-provider-open";parent.insertBefore(b,document.getElementById("ssx"));}}
  global.JHChartProviderBrowser={open:open,close:close,route:route,action:action,pageInfo:pageInfo,receive:receive};
  if(typeof document!=="undefined"){if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",install,{once:true});else install();}
})(typeof window!=="undefined"?window:globalThis);
