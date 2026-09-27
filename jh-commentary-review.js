/* Whole published commentary; no account reads, model calls or score authority. */
(function(root){
 'use strict';
 const LIMIT=64*1024*1024;
 const ROUTES={'/risk-desk.html':'risk-desk','/fundamentals.html':'fundamentals'};
 const FIELDS={'risk-desk':['primary_risks','hedge_recommendation','leading_indicators'],'fundamentals':['best_value','warning_flags','watch_list']};
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
 async function load(page,options={}){
  if(!Object.values(ROUTES).includes(page))throw Error('Exact page identity required');
  const controller=new AbortController();let timer,reader;
  const abort=()=>controller.abort();if(options.signal?.aborted)throw Error('Request cancelled');options.signal?.addEventListener('abort',abort,{once:true});
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('Published commentary request timed out'));},options.timeout||15000);});
  try{return await Promise.race([deadline,(async()=>{const response=await(options.fetcher||root.fetch.bind(root))('/data/ai-commentary/'+page+'.json?exact=1&nogen=1',{cache:'no-store',redirect:'error',signal:controller.signal});
   if(!response.ok||!response.body?.getReader)throw Error('Published commentary unavailable'+(Number.isInteger(response.status)?' (HTTP '+response.status+')':''));
   reader=response.body.getReader();const parts=[];let size=0;for(;;){const {value,done}=await reader.read();if(done)break;size+=value.byteLength;if(size>LIMIT)throw Error('Complete commentary exceeds bound');parts.push(value);}
   const bytes=new Uint8Array(size);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.byteLength;}const raw=new TextDecoder('utf-8',{fatal:true}).decode(bytes),packet=strictJSON(raw);view(packet,page,options.now??Date.now());return{packet,raw};})()]);
  }finally{clearTimeout(timer);controller.abort();options.signal?.removeEventListener('abort',abort);if(reader)reader.cancel().catch(()=>{});}
 }
 function view(p,page,now=Date.now()){
  if(!Object.values(ROUTES).includes(page)||!p||typeof p!=='object'||Array.isArray(p)||p.page!==page)throw Error('Published commentary page identity differs');
  const at=clock(p.generated_at);if(at===null||at>now)throw Error('Valid nonfuture publication required');
  const research=p.contract==='commentary-research.v1',c=p.commentary;
  if(!c||typeof c!=='object'||Array.isArray(c))throw Error('Commentary object required');
  if(research){
   const flags=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
   if(p.model_api_calls!==0||c.model_api_calls!==0||p.model!=='deterministic-source-inventory'||c.mode!=='research_inventory'||p.portfolio_action!=='WAIT'||c.posture!=='WAIT'||flags.some(k=>p[k]!==false||c[k]!==false))throw Error('Research-only commentary contract differs');
   if(typeof c.headline!=='string'||!c.headline.trim()||FIELDS[page].some(k=>typeof c[k]!=='string'||!c[k].trim()))throw Error('Complete commentary text required');
  }
  return{page,at:p.generated_at,ageHours:(now-at)/3600000,research,authority:false,
   headline:research?c.headline:'Earlier commentary — research contract not yet published',
   fields:research?FIELDS[page].map((k,i)=>({label:['Declared input inventory','Evidence limitations','Decision eligibility'][i],text:c[k]})):[]};
 }
 function el(doc,tag,text,parent){const n=doc.createElement(tag);if(text!==undefined)n.textContent=text;if(parent)parent.appendChild(n);return n;}
 function init(doc,options={}){
  const panel=doc.getElementById('ai-brief-panel');if(!panel||panel.__commentaryReader)return null;panel.__commentaryReader=true;panel.replaceChildren();
  const style=el(doc,'style',undefined,doc.head);style.textContent='#ai-brief-panel{min-width:0;max-width:100%;overflow-wrap:anywhere}#ai-brief-panel h2{font-size:18px;margin:0 0 8px}#ai-brief-panel h3{font-size:13px;margin:16px 0 6px}#ai-brief-panel p{margin:8px 0;line-height:1.6}#ai-brief-panel pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:300px;overflow:auto;font:12px/1.5 ui-monospace,monospace}#ai-brief-panel button{background:#172238;color:#dce7f7;border:1px solid #70809b;padding:7px 12px;border-radius:6px;cursor:pointer}#ai-brief-panel button:focus-visible,#ai-brief-panel summary:focus-visible{outline:2px solid #b8ccff;outline-offset:3px}#ai-brief-panel summary{cursor:pointer}';
  el(doc,'h2','Published source commentary',panel);
  const status=el(doc,'p','Checking publication.',panel);status.setAttribute('role','status');
  el(doc,'p','Publication time is not the age of the underlying observations. This panel grants no Calls, sizing or execution permission.',panel);
  const content=el(doc,'div',undefined,panel),refresh=el(doc,'button','Refresh stored commentary',panel);refresh.type='button';
  const details=el(doc,'details',undefined,panel);el(doc,'summary','Inspect every original field and legacy claim',details);
  const original=el(doc,'pre','Unavailable',details);original.tabIndex=0;original.setAttribute('aria-label','Complete original commentary JSON');
  const path=options.path||(()=>root.location.pathname),now=options.now||(()=>Date.now()),loader=options.loader||load,timers=options.timers||root;
  let page=ROUTES[path()],generation=0,controller=null,loaded=null,interval=null,disposed=false;
  function clear(){loaded=null;content.replaceChildren();original.textContent='Unavailable';details.ontoggle=null;}
  function stop(){disposed=true;generation++;controller?.abort();if(interval!==null)timers.clearInterval(interval);interval=null;clear();refresh.disabled=true;}
  function age(){
   if(ROUTES[path()]!==page){stop();status.textContent='Route changed; commentary cleared.';return;}
   if(loaded){try{const v=view(loaded.packet,page,now());status.textContent='Published '+v.at+' · '+v.ageHours.toFixed(1)+' hours old · '+(v.research?'source inventory, not a forecast':'legacy publication; qualification unverified');}catch(e){clear();status.textContent='Unavailable: '+e.message;}}
  }
  async function read(){
   if(disposed||!page||ROUTES[path()]!==page)return;
   const ticket=++generation;controller?.abort();controller=new AbortController();clear();refresh.disabled=true;status.textContent='Reading the complete stored publication…';
   try{
    const result=await loader(page,{signal:controller.signal,now:now()});if(disposed||ticket!==generation||ROUTES[path()]!==page)return;
    const v=view(result.packet,page,now());if(typeof result.raw!=='string'||JSON.stringify(strictJSON(result.raw))!==JSON.stringify(result.packet))throw Error('Complete original packet differs');
    loaded=result;age();el(doc,'h3',v.headline,content);
    if(!v.research)el(doc,'p','The earlier note remains inspectable below. Its forecasts, confidence scores and directions have not passed the current source and decision contract.',content);
    for(const field of v.fields){el(doc,'h3',field.label,content);el(doc,'p',field.text,content);}
    function show(){if(!disposed&&ticket===generation&&details.open)original.textContent=result.raw;}details.ontoggle=show;show();
   }catch(e){if(disposed||ticket!==generation)return;clear();status.textContent='Unavailable: '+e.message+'. Previous text was not substituted and generation was not requested.';}
   finally{if(!disposed&&ticket===generation)refresh.disabled=false;}
  }
  refresh.onclick=read;
  if(!page){refresh.hidden=true;details.hidden=true;status.textContent='This public reader does not access account or learning-dependent commentary. Its existing publication and history remain unchanged; a reviewed source binding is required.';}
  else{interval=timers.setInterval(age,60000);read();}
  root.addEventListener?.('pagehide',stop);root.addEventListener?.('popstate',age);
  return{panel,status,content,details,original,refresh,read,stop,age};
 }
 const api={ROUTES,clock,strictJSON,view,load,init};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 else{root.JHCommentaryReview=api;if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',()=>init(root.document),{once:true});else init(root.document);}
})(typeof globalThis!=='undefined'?globalThis:this);
