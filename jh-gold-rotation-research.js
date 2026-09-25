(function(root){
 'use strict';const A=typeof module==='object'?require('./jh-option-research.js'):root.JHOptionResearch;
 const CURRENT='data/gold-equity-rotation.json',PREFIX='data/gold-rotation-research/',CONTRACT='gold-rotation-original-research.v1',MAX=8*1024*1024;
 const symbols=['GLD','SPY','GDX','SLV','UUP','TLT','VNQ','REM'],kinds=['light','full','dividend-adjusted'];
 const compilers={gold_rotation_model:'6c4ef59e4c96d57c26a703f9edf8db537b54f663fab90c1e3b6021eea9c748db',gold_rotation_store:'1fdd26df503e27afaa3c65fdcbe400f6d771d5831cfd5bf1e62bb02434b97cea'};
 const owners=new WeakMap(),flags=['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];
 const digest=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v),authority=v=>v&&flags.every(k=>v[k]===false);
 const safe=k=>typeof k==='string'&&k.startsWith(PREFIX)&&/^(?:(runs|inputs|outputs)\/[a-f0-9]{64}\.json|compilers\/[a-f0-9]{64}\.py)$/.test(k.slice(PREFIX.length));
 const freeze=v=>{if(v&&typeof v==='object'&&!Object.isFrozen(v)){Object.values(v).forEach(freeze);Object.freeze(v);}return v;};
 async function load(key,fetcher,signal){
  if(key!==CURRENT&&!safe(key))throw Error('Unapproved Gold research path');
  const r=await fetcher('/'+key,{cache:key===CURRENT?'no-store':'default',signal});if(!r.ok)throw Error('Recorded Gold evidence is unavailable');
  if(Number(r.headers?.get('content-length'))>MAX)throw Error('Evidence byte bound');
  let raw;if(r.body?.getReader){const reader=r.body.getReader(),parts=[];let length=0;
   try{for(;;){const v=await reader.read();if(v.done)break;length+=v.value.byteLength;if(length>MAX)throw Error('Evidence byte bound');parts.push(v.value);}}
   catch(e){await reader.cancel();throw e;}
   raw=new Uint8Array(length);let offset=0;for(const p of parts){raw.set(p,offset);offset+=p.byteLength;}
  }else raw=new Uint8Array(await r.arrayBuffer());
  if(!raw.byteLength||raw.byteLength>MAX)throw Error('Evidence byte bound');return {raw,text:new TextDecoder('utf-8',{fatal:true}).decode(raw)};
 }
 async function artifact(ref,kind,fetcher,signal){
  if(!digest(ref?.sha256)||ref.key!==PREFIX+kind+'/'+ref.sha256+(kind==='compilers'?'.py':'.json')||!Number.isSafeInteger(ref.bytes)||ref.bytes<=0||ref.bytes>MAX)throw Error('Exact artifact identity required');
  const item=await load(ref.key,fetcher,signal);if(item.raw.byteLength!==ref.bytes||await A.sha(item.raw)!==ref.sha256)throw Error('Recorded artifact bytes differ');
  return kind==='compilers'?item.text:JSON.parse(item.text);
 }
 function typed(p){return p?.contract===CONTRACT&&authority(p)&&p.call===null&&p.state===null&&p.score===null&&p.signal_strength===null&&
  p.independent_investment_votes===0&&p.n_tickets===0&&Array.isArray(p.trade_tickets)&&!p.trade_tickets.length&&p.decision?.verb==='WAIT'&&p.decision?.meaning==='abstain'&&
  A.same(Object.keys(p.instruments||{}).sort(),symbols.slice().sort())&&symbols.every(s=>authority(p.instruments[s])&&Array.isArray(p.instruments[s].history))&&
  ['full','dividend-adjusted'].every(k=>authority(p.ratios?.[k])&&Array.isArray(p.ratios[k].history));}
 async function verified(p,fetcher,signal){
  if(!typed(p))throw Error('Descriptive native Gold research required');const key=p.replay?.manifest_key;
  if(!safe(key)||!key.startsWith(PREFIX+'runs/')||!digest(p.replay.output_sha256))throw Error('Exact recorded Gold run required');
  const item=await load(key,fetcher,signal),run=JSON.parse(item.text);
  if(key!==PREFIX+'runs/'+await A.sha(item.raw)+'.json'||run.contract!=='gold-rotation-replay.v1'||run.generated_at!==p.generated_at||run.output_sha256!==p.replay.output_sha256||run.output?.sha256!==run.output_sha256||!A.same(Object.keys(run.compilers||{}).sort(),Object.keys(compilers).sort()))throw Error('Recorded run differs');
  const [output,input]=await Promise.all([artifact(run.output,'outputs',fetcher,signal),artifact(run.input,'inputs',fetcher,signal)]);
  for(const [name,hash] of Object.entries(compilers)){if(run.compilers[name]?.sha256!==hash)throw Error('Qualified compiler differs');await artifact(run.compilers[name],'compilers',fetcher,signal);}
  const {replay,...body}=p;if(!A.same(body,output)||input.contract!=='gold-rotation-inputs.v1'||input.generated_at!==p.generated_at)throw Error('Publication differs from recorded evidence');
  owners.set(p,freeze({publication:p,run,input,browser_checks:'Artifact hashes, compiler identity and publication equality. Provider-original reconstruction and independent arithmetic are runner checks.'}));return freeze(p);
 }
 async function recordedRun(id,fetcher,signal){
  if(!digest(id))throw Error('Exact recorded Gold run required');const key=PREFIX+'runs/'+id+'.json',item=await load(key,fetcher,signal),run=JSON.parse(item.text);
  if(await A.sha(item.raw)!==id||run.contract!=='gold-rotation-replay.v1')throw Error('Recorded run differs');
  const output=await artifact(run.output,'outputs',fetcher,signal);return verified({...output,replay:{manifest_key:key,output_sha256:run.output_sha256}},fetcher,signal);
 }
 function evidence(p){if(!owners.has(p))throw Error('Verify the recorded publication first');return owners.get(p);}
 function selection(p,symbol,kind){evidence(p);if(!symbols.includes(symbol)||!kinds.includes(kind))throw Error('Choose a declared instrument and basis');return p.instruments[symbol];}
 function recordedUrl(p,symbol,kind){selection(p,symbol,kind);return '/gold-rotation.html?symbol='+symbol+'&basis='+kind+'&run='+p.replay.manifest_key.slice((PREFIX+'runs/').length,-5);}
 function decimal(v){if(typeof v!=='string'||v.length>80||!/^[-+]?\d+(?:\.\d+)?$/.test(v))throw Error('Finite exact decimal required');
  const [whole,fraction='']=v.replace(/^[-+]/,'').split('.');return {n:(v.startsWith('-')?-1n:1n)*BigInt(whole+fraction),p:fraction.length};}
 function format(v){if(v===null||v===undefined)return 'Unavailable';decimal(v);const [w,f='']=v.split('.'),tail=f.replace(/0+$/,'');return w.replace(/\B(?=(\d{3})+(?!\d))/g,',')+(tail?'.'+tail:'');}
 function exact(n,p){const negative=n<0n;n=negative?-n:n;const text=n.toString().padStart(p+1,'0');return (negative?'-':'')+(p?text.slice(0,-p)+'.'+text.slice(-p):text);}
 function scenario(p,symbol,kind,shares,change,cost){selection(p,symbol,kind);
  if(kind!=='full')throw Error('The exposure assumption uses the reported full close; adjusted history is not an executable share price');
  const price=p.instruments[symbol].latest?.full;if(price===null||price===undefined)throw Error('A dated reported close is required');
  const v=decimal(price),q=decimal(shares),r=decimal(change),c=decimal(cost);
  if(q.p>8||r.p>8||c.p>8||q.n<0n||c.n<0n||q.n>1000000000000n*10n**BigInt(q.p)||r.n < -100n*10n**BigInt(r.p)||r.n>1000n*10n**BigInt(r.p))throw Error('Use nonnegative shares and costs, and a price change from -100% to 1000%, with at most eight decimal places');
  const exposure=v.n*q.n,ep=v.p+q.p,gain=exposure*r.n,gp=ep+r.p+2,places=Math.max(gp,c.p);
  return {symbol,observation_date:p.instruments[symbol].observation_date,reported_close:price,shares,assumed_price_change_percent:change,assumed_total_cost_usd:cost,
   exposure_usd:exact(exposure,ep),price_effect_usd:exact(gain,gp),net_effect_usd:exact(gain*10n**BigInt(places-gp)-c.n*10n**BigInt(places-c.p),places),
   formula:'shares × dated full close × assumed price change / 100 − assumed total costs',dividends_taxes_and_fx_included:false,forecast:false,order:false,record:recordedUrl(p,symbol,kind)};
 }
 const api={CURRENT,PREFIX,CONTRACT,symbols,kinds,load,verified,recordedRun,evidence,selection,recordedUrl,format,scenario};
 if(typeof module==='object')module.exports=api;else root.JHGoldRotationResearch=api;
})(typeof window==='object'?window:globalThis);
