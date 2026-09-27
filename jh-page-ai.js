/* jh-page-ai.js — universal per-page AI panel (explain + analyze + grounded outlook). */
(function () {
  if (typeof location==="undefined" || !/chart-pro\.html/i.test(location.pathname || "")) return;
  ["jh-chart-pro-dock", "jh-tv-lists-bridge", "jh-chart-tf-fix", "jh-chart-audit-fix"].forEach(function (name) {
    if (document.querySelector('script[src*="' + name + '"]')) return;
    var s = document.createElement("script");
    s.src = "/" + name + ".js?v=20260912c";
    (document.head || document.documentElement).appendChild(s);
  });
  function hide() {
    document.querySelectorAll(".ai-index-strip").forEach(function (el) { el.style.display = "none"; });
    var rows = [];
    document.querySelectorAll("div, nav, span").forEach(function (el) {
      var t = (el.textContent || "").replace(/\s+/g, " ").trim();
      if (el.children && el.children.length > 8) return;
      if (/^1D 5D 1M 3M 6M YTD 1Y 5Y All$/.test(t)) rows.push(el);
    });
    if (rows.length) rows[0].style.display = "none";
  }
  hide();
  document.addEventListener("DOMContentLoaded", hide);
  setTimeout(hide, 1200);
})();
(function(root){
 'use strict';
 const LIMIT=64*1024*1024;
 function pageName(path){if(path==='/'||path==='')return 'index';const match=/^\/([a-z0-9_-]+)\.html?$/i.exec(path);return match?match[1].toLowerCase():null;}
 function clock(value){
  if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(value))return null;
  const day=Date.parse(value.slice(0,10)+'T00:00:00Z'),at=Date.parse(value);return Number.isFinite(day)&&new Date(day).toISOString().slice(0,10)===value.slice(0,10)&&Number.isFinite(at)?at:null;
 }
 function strictJSON(source){
  let i=0;const number=/-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function ws(){while(/[\x20\t\r\n]/.test(source[i]||'x'))i++;}
  function string(){const start=i++;for(;i<source.length;i++){if(source[i]==='\\'){i++;continue;}if(source[i]==='"')return JSON.parse(source.slice(start,++i));}throw Error('Incomplete JSON string');}
  function value(depth){
   if(depth>128)throw Error('JSON nesting exceeds bound');ws();const c=source[i];
   if(c==='"')return string();
   if(c==='{'||c==='['){const object=c==='{',out=object?{}:[],seen=new Set(),end=object?'}':']';i++;ws();if(source[i]===end){i++;return out;}
    for(;;){ws();let key;if(object){if(source[i]!=='"')throw Error('JSON key required');key=string();if(seen.has(key))throw Error('Duplicate JSON key');seen.add(key);ws();if(source[i++]!==':')throw Error('JSON colon required');}
     const item=value(depth+1);if(object)Object.defineProperty(out,key,{value:item,enumerable:true,writable:true,configurable:true});else out.push(item);
     ws();if(source[i]===end){i++;return out;}if(source[i++]!==',')throw Error('Incomplete JSON structure');}
   }
   for(const [token,v]of [['true',true],['false',false],['null',null]])if(source.startsWith(token,i)){i+=token.length;return v;}
   number.lastIndex=i;const m=number.exec(source);if(!m)throw Error('Invalid JSON value');i=number.lastIndex;const n=Number(m[0]);if(!Number.isFinite(n))throw Error('Nonfinite JSON number');return n;
  }
  const result=value(0);ws();if(i!==source.length)throw Error('Trailing JSON content');return result;
 }
 function view(p,expected,now=Date.now()){
  if(!expected||!p||typeof p!=='object'||Array.isArray(p)||p.page!==expected)throw Error('Published page identity differs');
  const at=clock(p.generated_at);if(at===null||at>now)throw Error('Valid nonfuture publication required');
  return{page:expected,at:p.generated_at,ageHours:(now-at)/3600000,authority:false,
   fields:[['what_it_is','Purpose'],['what_it_does','Method described by the publisher'],['analysis','Published commentary'],['pick_read','Published instrument commentary']].filter(([k])=>typeof p[k]==='string'&&p[k].trim()).map(([k,label])=>({key:k,label,text:p[k]}))};
 }
 async function load(page,options={}){
  if(typeof page!=='string'||!/^[a-z0-9_-]+$/.test(page))throw Error('Exact page identity required');
  const controller=new AbortController();let timer,reader;
  const abort=()=>controller.abort();if(options.signal?.aborted)throw Error('Request cancelled');options.signal?.addEventListener('abort',abort,{once:true});
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Published explanation request timed out'));},options.timeout||15000);});
  try{return await Promise.race([deadline,(async()=>{const response=await(options.fetcher||root.fetch.bind(root))('/data/page-ai/'+page+'.json?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Published explanation unavailable'+(Number.isInteger(response.status)?' (HTTP '+response.status+')':''));
   reader=response.body.getReader();const parts=[];let size=0;for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>LIMIT)throw Error('Complete explanation exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.byteLength;}const raw=new TextDecoder('utf-8',{fatal:true}).decode(bytes),packet=strictJSON(raw);view(packet,page,options.now??Date.now());return{packet,raw};})()]);
  }finally{clearTimeout(timer);controller.abort();options.signal?.removeEventListener('abort',abort);if(reader)reader.cancel().catch(()=>{});}
 }
 function element(doc,tag,text,parent){const el=doc.createElement(tag);if(text!==undefined)el.textContent=text;if(parent)parent.appendChild(el);return el;}
 function css(doc){
  if(doc.getElementById('jhpai-css'))return;const s=element(doc,'style');s.id='jhpai-css';s.textContent=
   '#jhpai-fab{position:fixed;right:16px;bottom:16px;z-index:99998;background:#26324a;color:#e6edf3;border:1px solid #667898;border-radius:18px;padding:9px 14px;font:600 13px system-ui;cursor:pointer}'+
   '#jhpai-panel{position:fixed;right:16px;bottom:62px;z-index:99999;width:min(460px,calc(100vw - 32px));max-height:76vh;overflow:auto;background:#0f1726;color:#e6edf3;border:1px solid #53627a;border-radius:12px;box-shadow:0 12px 44px #0007;padding:18px;font:14px/1.55 system-ui}'+
   '#jhpai-panel[hidden]{display:none}#jhpai-panel h2{font-size:19px;margin:8px 0}#jhpai-panel h3{font-size:13px;color:#b9c9e4;margin:18px 0 5px}#jhpai-panel p{margin:9px 0;overflow-wrap:anywhere}#jhpai-panel .jhpai-muted{color:#b5c0d2;font-size:12px}'+
   '#jhpai-panel .jhpai-actions{display:flex;gap:10px;flex-wrap:wrap}#jhpai-panel button{background:#21314d;color:#e6edf3;border:1px solid #75869f;border-radius:6px;font:inherit;padding:6px 10px;cursor:pointer}#jhpai-panel button:disabled{opacity:.6}'+
   '#jhpai-panel a,#jhpai-panel summary{color:#b4c9fa}#jhpai-panel summary{cursor:pointer;margin:18px 0 8px}#jhpai-panel pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:320px;overflow:auto;background:#0a1020;padding:12px;font:12px/1.5 ui-monospace,monospace}'+
   '#jhpai-panel button:focus-visible,#jhpai-fab:focus-visible,#jhpai-panel summary:focus-visible,#jhpai-panel a:focus-visible{outline:2px solid #c5d8ff;outline-offset:3px}';doc.head.appendChild(s);
 }
 function init(doc,options={}){
  if(doc.getElementById('jhpai-fab'))return null;css(doc);const path=options.path||(()=>root.location.pathname),now=options.now||(()=>Date.now()),loader=options.loader||load;
  const button=element(doc,'button','Page explanation',doc.body);button.id='jhpai-fab';button.type='button';button.setAttribute('aria-controls','jhpai-panel');button.setAttribute('aria-expanded','false');
  const panel=element(doc,'section',undefined,doc.body);panel.id='jhpai-panel';panel.hidden=true;panel.setAttribute('aria-label','Published page explanation');
  const actions=element(doc,'div',undefined,panel);actions.className='jhpai-actions';const refresh=element(doc,'button','Refresh published explanation',actions),close=element(doc,'button','Close',actions);refresh.type=close.type='button';
  element(doc,'h2','Published explanation',panel);const status=element(doc,'p','Open to read the stored publication.',panel);status.className='jhpai-muted';status.setAttribute('role','status');
  element(doc,'p','Generated text is unqualified research. This panel does not verify the original inputs, model skill or portfolio consequences. A published timestamp is not the age of the underlying evidence.',panel).className='jhpai-muted';
  const content=element(doc,'div',undefined,panel),source=element(doc,'a','Complete published JSON',panel);source.hidden=true;
  const details=element(doc,'details',undefined,panel);element(doc,'summary','Inspect all original fields and legacy outcome claims',details);element(doc,'p','Historical return, confidence and alpha-status fields remain in the original packet; they confer no Calls, sizing or execution permission here.',details).className='jhpai-muted';const original=element(doc,'pre','Unavailable',details);
  const timers=options.timers||root;let generation=0,controller=null,loaded=null,ageTimer=null,activePage=null;
  function clear(){loaded=null;content.replaceChildren();original.textContent='Unavailable';source.hidden=true;details.ontoggle=null;}
  function updateAge(){
   if(activePage!==null&&pageName(path())!==activePage){hide();return;}
   if(loaded)try{const v=view(loaded.packet,loaded.page,now());status.textContent=v.at+' · '+v.ageHours.toFixed(1)+' hours since publication · underlying observation age unverified.';}catch(e){clear();status.textContent='Publication clock is no longer valid. No previous explanation remains displayed.';}
  }
  async function read(){
   if(panel.hidden)return;
   const ticket=++generation;if(controller)controller.abort();controller=new AbortController();clear();refresh.disabled=true;status.textContent='Reading the complete stored publication…';
   const page=pageName(path());activePage=page;if(!page){status.textContent='No unambiguous published-brief identity is declared for this route. No other page’s explanation is substituted.';refresh.disabled=false;return;}
   try{const result=await loader(page,{signal:controller.signal,now:now()});if(ticket!==generation||panel.hidden||pageName(path())!==page)return;
    const v=view(result.packet,page,now());if(typeof result.raw!=='string'||JSON.stringify(strictJSON(result.raw))!==JSON.stringify(result.packet))throw Error('Complete original packet differs');
    loaded={...result,page};updateAge();for(const field of v.fields){element(doc,'h3',field.label,content);element(doc,'p',field.text,content);}if(!v.fields.length)element(doc,'p','No narrative fields are present. Inspect the complete packet below.',content);
    source.href='/data/page-ai/'+page+'.json?exact=1&nogen=1';source.hidden=false;let shown=false;function showOriginal(){if(ticket!==generation||!details.open||shown||panel.hidden)return;shown=true;original.textContent=result.raw;}details.ontoggle=showOriginal;showOriginal();
   }catch(e){if(ticket!==generation||panel.hidden)return;clear();status.textContent='Unavailable: '+e.message+'. No old explanation is substituted; source generation was not requested.';}
   finally{if(ticket===generation)refresh.disabled=false;}
  }
  function hide(){generation++;controller?.abort();if(ageTimer!==null){timers.clearInterval(ageTimer);ageTimer=null;}panel.hidden=true;button.setAttribute('aria-expanded','false');activePage=null;clear();button.focus();}
  button.onclick=()=>{if(!panel.hidden){hide();return;}panel.hidden=false;button.setAttribute('aria-expanded','true');ageTimer=timers.setInterval(updateAge,60000);read();close.focus();};close.onclick=hide;refresh.onclick=read;panel.onkeydown=e=>{if(e.key==='Escape'){e.preventDefault();hide();}};
  root.addEventListener?.('pagehide',hide);root.addEventListener?.('popstate',updateAge);
  return{button,panel,status,content,details,original,refresh,close,read,hide,updateAge};
 }
 const api={pageName,clock,strictJSON,view,load,init};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 else{root.JHPageExplanation=api;if(!root.__jhPageAI){root.__jhPageAI=true;if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',()=>init(root.document),{once:true});else init(root.document);}}
})(typeof globalThis!=='undefined'?globalThis:this);
