(function(root){
 'use strict';
 const CONTRACT='cb-research.v1',PREFIX='data/cb-research/';
 const esc=value=>String(value??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const number=value=>typeof value==='number'&&Number.isFinite(value)?value:null;
 const num=value=>number(value)===null?'Unavailable':number(value).toLocaleString('en-US',{maximumFractionDigits:4});
 const unit=value=>({'USD_bn':'USD billions','EUR_bn':'EUR billions','JPY_bn':'JPY billions','CHF_bn':'CHF billions','JPY_per_USD':'JPY per USD','CHF_per_USD':'CHF per USD','USD_per_EUR':'USD per EUR','percent_per_annum':'percent per year'}[value]||value||'—');
 const path=key=>typeof key==='string'&&/^data\/[A-Za-z0-9_./-]+$/.test(key)&&!key.includes('..')?'/'+key:null;
 const link=(key,label)=>path(key)?`<a href="${path(key)}">${esc(label)}</a>`:esc(label+' unavailable');
 function day(s){if(typeof s!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(s)||s.startsWith('0000-'))return null;const n=Date.parse(s+'T00:00:00Z');return Number.isFinite(n)&&new Date(n).toISOString().slice(0,10)===s?n:null;}
 function clock(s){if(typeof s!=='string')return null;const m=/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|([+-])(\d{2}):(\d{2}))$/.exec(s);
  if(!m||day(m[1])===null||Number(m[2])>23||Number(m[3])>59||Number(m[4])>59||(m[5]!=='Z'&&(Number(m[7])>23||Number(m[8])>59)))return null;const n=Date.parse(s);return Number.isFinite(n)?n:null;
 }
 function strictJSON(source){
  // Reject duplicate identities and overflow rather than accepting the last key.
  if(typeof source!=='string')throw Error('JSON text required');
  let i=0;const number=/-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function ws(){while(/[\x20\t\r\n]/.test(source[i]||'x'))i++;}
  function string(){const start=i++;for(;i<source.length;i++){if(source[i]==='\\'){i++;continue;}if(source[i]==='"'){const text=JSON.parse(source.slice(start,++i));for(let n=0;n<text.length;n++){const c=text.charCodeAt(n);if(c>=0xD800&&c<=0xDBFF){const next=text.charCodeAt(++n);if(!(next>=0xDC00&&next<=0xDFFF))throw Error('Invalid JSON Unicode');}else if(c>=0xDC00&&c<=0xDFFF)throw Error('Invalid JSON Unicode');}return text;}}throw Error('Incomplete JSON string');}
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
  const out=value(0);ws();if(i!==source.length)throw Error('Trailing JSON content');return out;
 }
 function recent(stamp,now,hours=26){const parsed=clock(stamp),age=now-parsed;return Number.isFinite(now)&&parsed!==null&&age>=0&&age<=hours*3600000;}
 function observed(value,now,days){const parsed=day(value);if(parsed===null||!Number.isFinite(now)||!Number.isSafeInteger(days)||days<0)return false;const age=Math.floor(now/86400000)-Math.floor(parsed/86400000);return age>=0&&age<=days;}
 function fresh(m,p,now){const q=m?.quality||{};return number(m?.latest)!==null&&q.status==='fresh'&&recent(p.generated_at,now)&&recent(q.source_generated_at,now)&&recent(q.acquired_at,now)&&observed(q.observation_date,now,q.max_age_days);}
 function table(headers,rows){return `<div class="table"><table><thead><tr>${headers.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>'<tr>'+row.map(v=>'<td>'+v+'</td>').join('')+'</tr>').join('')}</tbody></table></div>`;}
 function rowEvidence(row,label){if(!row)return `<p>${esc(label)} unavailable.</p>`;
  return `<p><b>${esc(label)}</b> · ${esc(row.date)} (provider period ${esc(row.period)})<br>Native value ${esc(row.native_decimal)} · source status ${esc(row.source_status)} · original row ${esc(row.original_row_index)}<br>${link(row.original?.key,'Original response')}</p>`;
 }
 function evidence(m,horizon){const c=m.changes?.[horizon]||{};
  return `<details><summary>Measurement and comparison evidence</summary><p>${esc(m.source_id)} · original unit ${esc(m.source_unit)} · multiplier ${esc(m.scale_decimal??m.selected?.scale_decimal)} to ${esc(unit(m.unit))}<br>${esc(m.period_basis)}</p>`+
   rowEvidence(m.selected,'Latest observation')+rowEvidence(c.baseline,'Calendar baseline')+
   `<p>Target ${esc(c.target_date)} · ${esc(c.start_date)} → ${esc(c.end_date)}<br>${link(m.definition?.key,'Original definition')}<br>Acquired ${esc(m.quality?.acquired_at)}<br>Source packet ${esc(m.quality?.source_generated_at)}</p></details>`;
 }
 function bank(b,p,now,pinned,horizon){
  const m=b.balance_sheet||{},r=b.rate||{},ma=pinned||fresh(m,p,now),ra=pinned||fresh(r,p,now),change=r.changes?.[horizon];
  return `<article class="cb"><h3>${esc(b.cb)}</h3><div class="stance">${num(ma?m.latest:null)} ${esc(unit(m.unit))}</div>`+
   `<p>${esc(m.quality?.observation_date)} · ${esc(m.source_id)}<br>${esc(ma?'As measured: '+m.quality?.status:'Unavailable or expired')}</p><p>${esc(horizon)} calendar-month stock change: ${num(ma?m.changes?.[horizon]?.level_change:null)} ${esc(unit(m.unit))}.</p>${evidence(m,horizon)}`+
   `<hr><p>${esc(b.rate_definition)}</p><strong>${num(ra?r.latest:null)} ${ra&&number(r.latest)!==null?'%':''}</strong>`+
   `<p>${esc(r.quality?.observation_date)} · ${esc(r.source_id)}<br>${esc(horizon)} calendar-month change: ${num(ra?change?.level_change:null)} percentage points.</p>${evidence(r,horizon)}</article>`;
 }
 function decomposition(b,p,now,pinned){
  const d=b.decomposition||{},valid=d.status==='partial_attribution'&&(pinned||(recent(p.generated_at,now)&&observed(d.observation_date,now,21)&&Object.values(d.components||{}).every(m=>fresh(m,p,now))));
  if(!Object.keys(d.components||{}).length)return `<section class="panel"><h3>${esc(b.cb)} · component history unavailable</h3><p>Missing accounting inputs: ${esc((d.missing||[]).join('; '))}.</p><p>Net policy injection remains unidentified.</p></section>`;
  const rows=Object.entries(d.components||{}).map(([name,m])=>[`${esc(name.replaceAll('_',' '))}<br><small>${esc(m.source_id)}</small>`,num(valid?m.aligned_value:null),num(valid?m.aligned_change_1m:null),
    `<details><summary>Matched source rows</summary>${rowEvidence(m.aligned_current,'Ending component')}${rowEvidence(m.aligned_baseline,'Baseline component')}</details>`]);
  if(rows.length)rows.push(['Other assets and adjustments',num(valid?d.other_assets_and_adjustments_level:null),num(valid?d.other_assets_and_adjustments_change_1m:null),'Accounting remainder; transaction and valuation attribution unavailable.']);
  return `<section class="panel"><h3>${esc(b.cb)} · component stocks</h3><p>${esc(String(d.status).replaceAll('_',' '))} · ${esc(unit(d.unit))}<br>${esc(d.start_date)} → ${esc(d.observation_date)} · calendar-month target ${esc(d.calendar_target_date)}</p>`+
   (rows.length?table(['Component','Aligned level','One-month stock change','Evidence'],rows):`<p>${esc((d.missing||[]).join('; '))}</p>`)+
   `<p>Total stock change: ${num(valid?d.stock_change_1m:null)} ${esc(unit(d.unit))}. Arithmetic residual: ${num(valid?d.reconciliation_residual:null)}.</p><p>Net policy injection: <b>not identified</b>. Purchases, maturities and valuation require separate accounting.</p></section>`;
 }
 function settlement(p,now,pinned){const f=p.pd_settlement_fails||{};
  const rows=[f,f.ust_ex_tips||{}].map(r=>{const usable=r.quality?.status==='fresh'&&r.reconciliation?.consistent===true&&(pinned||(recent(p.generated_at,now)&&recent(f.source_generated_at,now)&&observed(r.as_of,now,14)));
   return [esc(r.label||r.scope_id),esc(r.as_of),num(usable?r.ftd_bn:null),num(usable?r.ftr_bn:null),num(usable?r.combined_bn:null),esc(r.quality?.status)];});
  return `<p>Retained engine context. Original FR2004 provider responses have not been verified in this research run. Two-sided gross is not unique securities, defaults, cash inflow or central-bank injection.</p>`+
   table(['Separate scope','Observation','FTD, USD bn','FTR, USD bn','Combined, USD bn','Status'],rows)+`<p>${esc(f.scope_note)}</p>`;
 }
 function render(p,now=Date.now(),pinned=false,horizon='1'){
  if(!p||p.contract!==CONTRACT||clock(p.generated_at)===null||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false)throw Error('Supported descriptive research contract required');
  if(!['1','6','12'].includes(horizon))throw Error('Unsupported calendar horizon');
  const available=Object.values(p.measurements||{}).filter(m=>fresh(m,p,now)).length;
  const ecbAvailable=Object.values(p.ecb_components||{}).filter(m=>fresh(m,p,now)).length;
  const id=p.replay?.manifest_key?.match(/^data\/cb-research\/runs\/([a-f0-9]{64})\.json$/)?.[1];
  return {hero:`<div class="hero"><div class="lab">Source-backed research · Calls: abstain</div><div class="big">Stocks &amp; rates</div><p>${esc(pinned?'Pinned snapshot — values are dated as published':available+' of '+p.quality?.expected_native_series+' native measurements and '+ecbAvailable+' of '+(p.quality?.expected_ecb_series??0)+' original ECB component series currently usable')}</p><p>${esc(p.decision?.reason)}</p></div>`,
   cbs:(p.central_banks||[]).map(b=>bank(b,p,now,pinned,horizon)).join(''),
   carry:(p.central_banks||[]).map(b=>decomposition(b,p,now,pinned)).join(''),
   edollar:table(['FX quotation','Value','Observation','Evidence'],Object.entries(p.fx_context||{}).map(([name,m])=>[esc(unit(m.unit||name)),num(pinned||fresh(m,p,now)?m.latest:null),esc(m.quality?.observation_date),evidence(m,horizon)])),
   fails:settlement(p,now,pinned),
   evidence:link(p.replay?.manifest_key,'Immutable run manifest')+(id?` · <a href="/cb-injection.html?run=${id}">Permanent snapshot link</a>`:'')+' · '+link(p.source_replay?.manifest_key,'Canonical FRED source run')+
     `<details><summary>Duplicate sources and dependencies</summary><pre>${esc(JSON.stringify({source_comparisons:p.source_comparisons,dependencies:p.dependency_groups},null,2))}</pre></details>`,
   note:p.note,ts:`Compiled ${p.generated_at} · source packet ${p.source_generated_at} · ${p.methodology_version}`};
 }
 function artifact(key){return key==='data/cb-injection.json'||typeof key==='string'&&/^data\/cb-research\/(?:runs|outputs)\/[a-f0-9]{64}\.json$/.test(key);}
 function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
 async function bytes(fetcher,key,limit=8*1024*1024,options={}){
  if(!artifact(key))throw Error('Unsupported research artifact');
  if(!Number.isSafeInteger(limit)||limit<1||limit>32*1024*1024)throw Error('Invalid research byte bound');
  const timeout=options.timeoutMs??12000,signal=options.signal;if(!Number.isSafeInteger(timeout)||timeout<1||timeout>60000)throw Error('Invalid research deadline');
  const aborted=()=>{const error=Error('Research request aborted');error.name='AbortError';return error;};if(signal?.aborted)throw aborted();
  const controller=new AbortController();let timer,reader,response,finished=false,stop;
  const interrupted=new Promise((_,reject)=>{stop=reject;timer=setTimeout(()=>{reject(Error('Research request timed out'));controller.abort();},timeout);});
  const onAbort=()=>{stop(aborted());controller.abort();};signal?.addEventListener('abort',onAbort,{once:true});
  try{return await Promise.race([interrupted,(async()=>{
   response=await fetcher('/'+key,{cache:'no-store',redirect:'error',signal:controller.signal});
   if(finished||signal?.aborted){try{Promise.resolve(response.body?.cancel?.()).catch(()=>{});}catch{}throw aborted();}
   if(!response.ok)throw Error('Research artifact unavailable (HTTP '+response.status+')');
   if(!response.body?.getReader)throw Error('Complete readable research body required');
   reader=response.body.getReader();const parts=[];let size=0;
   for(;;){const {value,done}=await reader.read();if(finished||signal?.aborted)throw aborted();if(done)break;
    if(!(value instanceof Uint8Array))throw Error('Invalid research body chunk');
    size+=value.byteLength;if(size>limit)throw Error('Research artifact exceeds size bound');parts.push(value);
   }
   const raw=new Uint8Array(size);let offset=0;for(const part of parts){raw.set(part,offset);offset+=part.byteLength;}return raw;
  })()]);}finally{finished=true;clearTimeout(timer);signal?.removeEventListener('abort',onAbort);controller.abort();
   if(reader){try{Promise.resolve(reader.cancel()).catch(()=>{});}catch{}try{reader.releaseLock?.();}catch{}}else{try{Promise.resolve(response?.body?.cancel?.()).catch(()=>{});}catch{}}
  }
 }
 function decoded(raw){return strictJSON(new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(raw));}
 async function sha(raw){return Array.from(new Uint8Array(await root.crypto.subtle.digest('SHA-256',raw))).map(b=>b.toString(16).padStart(2,'0')).join('');}
 async function loadSnapshot(id,fetcher=root.fetch.bind(root),options={}){
  if(typeof id!=='string'||!/^[a-f0-9]{64}$/.test(id))throw Error('Invalid snapshot identifier');
  const key=PREFIX+'runs/'+id+'.json',raw=await bytes(fetcher,key,256*1024,options);
  if(await sha(raw)!==id)throw Error('Run manifest hash differs');const m=decoded(raw),ref=m?.output;
  if(m?.contract!=='cb-replay.v1'||clock(m.generated_at)===null||!ref||typeof ref.sha256!=='string'||!/^[a-f0-9]{64}$/.test(ref.sha256)||ref.sha256!==m.output_sha256||ref.key!==PREFIX+'outputs/'+ref.sha256+'.json'||!Number.isSafeInteger(ref.bytes)||ref.bytes<0||ref.bytes>8*1024*1024)throw Error('Unsupported output identity');
  const body=await bytes(fetcher,ref.key,8*1024*1024,options);if(body.length!==ref.bytes||await sha(body)!==ref.sha256)throw Error('Snapshot output differs');
  const p=decoded(body);if(!p||p.contract!==CONTRACT||p.generated_at!==m.generated_at||p.call!==null||p.calls_eligible!==false||p.sizing_eligible!==false)throw Error('Snapshot contract differs');
  p.replay={manifest_key:key,output_sha256:ref.sha256,compilers:m.compilers};return p;
 }
 async function loadCurrent(fetcher=root.fetch.bind(root),options={}){
  const p=decoded(await bytes(fetcher,'data/cb-injection.json',32*1024*1024,options));
  if(!p||p.contract!==CONTRACT||clock(p.generated_at)===null)throw Error('Original-bound current research unavailable');
  const id=typeof p.replay?.manifest_key==='string'?p.replay.manifest_key.match(/^data\/cb-research\/runs\/([a-f0-9]{64})\.json$/)?.[1]:null;
  const packet=await loadSnapshot(id,fetcher,options);
  if(JSON.stringify(stable(p))!==JSON.stringify(stable(packet)))throw Error('Current pointer differs from immutable output');return packet;
 }
 const api={CONTRACT,PREFIX,esc,num,path,day,clock,strictJSON,bytes,fresh,render,loadSnapshot,loadCurrent,sha};
 if(typeof module==='object'&&module.exports)module.exports=api;else root.CBResearch=api;
})(typeof globalThis==='object'?globalThis:this);
