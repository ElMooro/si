const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs');
const model=require('../jh-portfolio-research.js'),page=require('../jh-portfolio-research-page.js');
const KEY='screener/alpha-score.json';
function retained(raw=Buffer.from('{"stocks":[]}'),key=KEY){return {schema_version:'snapshot-research-source.v1',key,status:'AVAILABLE',body_complete:true,body_bytes:raw.length,body_sha256:crypto.createHash('sha256').update(raw).digest('hex'),body_encoding:'base64',body:raw.toString('base64'),declared_generated_at:'2020-01-01T00:00:00Z',started_at:'2026-09-30T08:35:00Z',completed_at:'2026-09-30T08:35:01Z'};}
function snapshot(record=retained()){return {research:{schema_version:model.SCHEMA,source_documents:{[KEY]:record},source_byte_bound:model.LIMIT,retained_complete_body_bytes:record.body_bytes,joins:{}},positions:[],watchlist:[]};}
const row=record=>model.source(record.key,record);

test('original verification preserves literal unsafe numbers, whitespace, Unicode and markup',async()=>{
 const raw=Buffer.from('\ufeff{\n"number":900719925474099312345,"text":"π </script><img src=x>"\n}');
 const result=await model.original(row(retained(raw)));assert.deepEqual(Buffer.from(result.bytes),raw);assert.equal(result.text,raw.toString('utf8'));
 assert.equal(result.filename,'alpha-score.original');assert.match(result.text,/900719925474099312345/);
});
test('empty bodies and invalid UTF-8 preserve exact bytes without replacement text',async()=>{
 assert.equal((await model.original(row(retained(Buffer.alloc(0))))).text,'');
 const bad=Buffer.from([0xff,0xfe,0,0x80]);const result=await model.original(row(retained(bad)));assert.equal(result.text,null);assert.deepEqual(Buffer.from(result.bytes),bad);
});
test('complete semantically invalid source can be inspected without becoming valid research',async()=>{
 const value={...retained(Buffer.from('{"stocks":[],"stocks":[]}')),status:'UNAVAILABLE',reason_code:'INVALID_SOURCE'};
 assert.equal(model.view(snapshot(value)).sources[0].status,'UNAVAILABLE');assert.match((await model.original(row(value))).text,/"stocks"/);assert.equal(model.view(snapshot(value)).allowsSizing,false);
});
test('wrong hash or length rejects an otherwise complete body',async()=>{
 for(const change of [{body_sha256:'0'.repeat(64)},{body_bytes:1}])await assert.rejects(model.original(row({...retained(),...change})),/does not match|do not match/);
});
test('malformed metadata and forged inspectable flag cannot bypass bounds',async()=>{
 for(const change of [{schema_version:'other'},{key:'wrong'},{body_complete:false},{body_bytes:true},{body_bytes:-1},{body_bytes:model.LIMIT+1},{body_encoding:'utf-8'},{body_sha256:'g'.repeat(64)},{body:123}]){
  const source=model.source(KEY,{...retained(),...change});assert.equal(source.inspectable,false);await assert.rejects(model.original(source));
 }
 for(const change of [{bytes:model.LIMIT+1},{bytes:true},{sha:'bad'},{body:'a==='}])await assert.rejects(model.original({...row(retained()),...change,inspectable:true}));
});
test('base64 whitespace, partial padding, noncanonical padding bits and invalid alphabet are rejected',async()=>{
 for(const body of ['e30=\n','e30','e===','====','e30_','e31='])await assert.rejects(model.original(row({...retained(Buffer.from('{}')),body})));
});
test('whole eight-MiB boundary is accepted without a prefix-only verification',async()=>{
 const raw=Buffer.alloc(model.LIMIT,97);raw[raw.length-1]=90;const result=await model.original(row(retained(raw)));assert.deepEqual(Buffer.from(result.bytes),raw);assert.equal(result.text.length,model.LIMIT);
});
test('legacy or malformed evidence is unavailable instead of an empty successful result',()=>{
 for(const s of [null,{},[],{research:{}},{research:{schema_version:model.SCHEMA,source_documents:[]}}])assert.equal(model.view(s).available,false);
 const v=model.view(snapshot());assert.equal(v.sources.length,4);assert.equal(v.sources[1].inspectable,false);assert.equal(v.joins[0].rows,null);
});
test('source dates remain distinct from acquisition times and confer no freshness',()=>{
 const v=model.view(snapshot()),s=v.sources[0];assert.equal(s.declared,'2020-01-01T00:00:00Z');assert.equal(s.started,'2026-09-30T08:35:00Z');assert.equal(v.allowsSizing,false);assert.match(v.detail,/freshness, independence and investment validity remain unverified/);
});
test('unknown and prototype-named source keys are retained without inherited labels',async()=>{
 const s=snapshot();s.research.source_documents=JSON.parse('{"__proto__":{},"constructor":{}}');const v=model.view(s);assert.equal(v.sources.length,6);assert.equal(v.sources[4].label,'__proto__');assert.equal(v.sources[5].label,'constructor');
 const result=await model.original(row(retained(Buffer.from('{}'),'../../unsafe.html')));assert.equal(result.filename,'retained-source.original');
});
test('aggregate byte inconsistency stays visible independently of individual byte verification',()=>{
 const s=snapshot();assert.equal(model.view(s).byteContractConsistent,true);
 for(const value of [0,true,-1,model.LIMIT+1]){s.research.retained_complete_body_bytes=value;assert.equal(model.view(s).byteContractConsistent,false);}
});
test('missing arrays, genuine empty arrays, duplicates and invalid indices stay distinct',()=>{
 const s=snapshot();s.research.joins={alpha:{source_shape:'ARRAY',source_row_count:3,unique_usable_symbols:0,duplicate_symbol_occurrences:{AAA:[0,1]},invalid_zero_based_occurrences:[2]},confluence_s:{source_shape:'ARRAY',source_row_count:0,unique_usable_symbols:0,duplicate_symbol_occurrences:{},invalid_zero_based_occurrences:[]}};
 const v=model.view(s);assert.deepEqual(v.joins.slice(0,2).map(r=>[r.rows,r.unique,r.duplicates,r.invalid]),[[3,0,1,1],[0,0,0,0]]);assert.equal(v.joins[2].rows,null);
 s.research.joins.alpha.invalid_zero_based_occurrences=[true];assert.equal(model.view(s).joins[0].invalid,null);
});

// A small DOM implementation exercises the actual view's events and lifecycle.
function dom(){
 const document={activeElement:null,createElement:tag=>new Element(tag)};
 class Element{
  constructor(tag){this.tagName=tag;this.ownerDocument=document;this.children=[];this.attributes={};this.listeners={};this.hidden=false;this.parent=null;this.ownText='';}
  set textContent(value){this.ownText=String(value);this.replaceChildren();}get textContent(){return this.ownText+this.children.map(c=>c.textContent).join('');}
  append(...nodes){for(const n of nodes){n.parent=this;this.children.push(n);}}
  replaceChildren(...nodes){for(const n of this.children)n.parent=null;this.children=[];this.append(...nodes);}
  setAttribute(k,v){this.attributes[k]=String(v);}removeAttribute(k){delete this.attributes[k];if(k==='href')delete this.href;}
  addEventListener(k,fn){this.listeners[k]=fn;}click(){return this.listeners.click?.();}focus(){document.activeElement=this;}
  get isConnected(){return this===container||!!this.parent?.isConnected;}
 }
 const container=new Element('main');const all=()=>{const rows=[];function visit(node){rows.push(node);node.children.forEach(visit);}visit(container);return rows;};
 return {container,document,all,find:(tag,text)=>all().find(n=>n.tagName===tag&&n.textContent===text)};
}
function mounted(custom={}){
 const d=dom(),created=[],revoked=[],urls={createObjectURL(blob){created.push(blob);return 'blob:invented-'+created.length;},revokeObjectURL(url){revoked.push(url);}};
 return {...d,created,revoked,view:page.mount(d.container,{model,urls,makeBlob:bytes=>Buffer.from(bytes),...custom})};
}
const inspect=p=>p.find('button','Verify and inspect Alpha scores');
const originalText=p=>p.all().find(n=>n.attributes['aria-label']==='Original source text');
test('view displays distinct dates, disabled missing sources and all original bytes on demand',async()=>{
 const p=mounted(),s=snapshot(retained(Buffer.from('<img src=x onerror=bad()>900719925474099312345')));p.view.render(s);
 assert.match(p.container.textContent,/2020-01-01/);assert.match(p.container.textContent,/2026-09-30/);assert.equal(p.find('button','Verify and inspect Confluence').disabled,true);
 assert.equal(p.created.length,0);await inspect(p).click();assert.equal(p.created.length,1);assert.equal(originalText(p).textContent,'<img src=x onerror=bad()>900719925474099312345');assert.equal(p.all().filter(n=>n.tagName==='img').length,0);
 assert.equal(p.document.activeElement.textContent,'Close original source');await p.find('button','Close original source').click();assert.equal(p.revoked.length,1);assert.equal(p.document.activeElement,inspect(p));
});
test('unchanged frame rerender preserves the open source; new unavailable frame clears and revokes it',async()=>{
 const p=mounted(),s=snapshot();p.view.render(s);await inspect(p).click();p.view.render(s);assert.equal(p.revoked.length,0);assert.equal(originalText(p).hidden,false);
 p.view.render(null);assert.equal(p.revoked.length,1);assert.equal(originalText(p).hidden,true);assert.equal(originalText(p).textContent,'');assert.equal(inspect(p),undefined);assert.match(p.container.textContent,/Research evidence unavailable/);
 p.view.render(snapshot());await inspect(p).click();assert.equal(p.created.length,2);
});
test('late hash completion cannot restore a previous frame or a closed source',async()=>{
 for(const operation of ['replace','close','destroy']){
  let resolve;const p=mounted({model:{...model,original:()=>new Promise(r=>resolve=r)}});p.view.render(snapshot());const pending=inspect(p).click();
  if(operation==='replace')p.view.render(null);else if(operation==='close')p.find('button','Close original source').click();else p.view.destroy();
  resolve({bytes:new Uint8Array([1]),text:'old',filename:'old.original',interpretation:'old'});await pending;assert.equal(p.created.length,0);assert.doesNotMatch(p.container.textContent,/old/);
 }
});
test('source switching revokes the earlier download and failed verification exposes no bytes',async()=>{
 const s=snapshot(),p=mounted();s.research.source_documents['signals/confluence.json']={...retained(Buffer.from('{}'),'signals/confluence.json'),body_sha256:'0'.repeat(64)};p.view.render(s);await inspect(p).click();await p.find('button','Verify and inspect Confluence').click();assert.equal(p.revoked.length,1);assert.equal(p.created.length,1);assert.equal(originalText(p).hidden,true);assert.match(p.container.textContent,/do not match the recorded hash/);
});
test('invalid UTF-8 enables exact download but no fabricated original text',async()=>{
 const p=mounted();p.view.render(snapshot(retained(Buffer.from([255]))));await inspect(p).click();assert.equal(originalText(p).hidden,true);assert.deepEqual(p.created[0],Buffer.from([255]));assert.match(p.container.textContent,/not valid UTF-8/);
});
test('full row references and duplicate occurrences are preserved in lazy diagnostics',()=>{
 const p=mounted(),s=snapshot();s.positions=Array.from({length:105},(_,i)=>({symbol:'P'+i,research_evidence:{alpha:{source_key:KEY,zero_based_occurrences:[i]}}}));s.watchlist=[{symbol:'AAA',research_evidence:{invalid_fields:['alpha_score']}}];s.research.joins={alpha:{duplicate_symbol_occurrences:{AAA:[0,1]}}};p.view.render(s);
 const pre=p.all().find(n=>n.attributes['aria-label']==='Complete reported join diagnostics and row references');assert.equal(pre.textContent,'');p.find('button','Show full reported row references').click();const parsed=JSON.parse(pre.textContent);assert.equal(parsed.row_references.positions.length,105);assert.equal(parsed.row_references.positions[104].symbol,'P104');assert.deepEqual(parsed.joins,s.research.joins);assert.deepEqual(parsed.row_references.watchlist[0].research_evidence,s.watchlist[0].research_evidence);
});
test('additional source records are reachable across pages and never silently truncated',()=>{
 const s=snapshot(),p=mounted();for(let i=0;i<20;i++){const key='extra-'+String(i).padStart(2,'0');s.research.source_documents[key]=retained(Buffer.from('{}'),key);}p.view.render(s);
 const seen=new Set();for(let i=0;i<3;i++){for(const n of p.all().filter(n=>n.tagName==='button'&&n.textContent.startsWith('Verify and inspect ')))seen.add(n.textContent);if(i<2)p.find('button','Next sources').click();}assert.equal(seen.size,24);assert.equal(p.find('button','Next sources').disabled,true);p.find('button','Previous sources').click();assert.equal(p.find('button','Next sources').disabled,false);
});
test('clear releases the byte download and permits re-rendering the same frame after page restore',async()=>{
 const p=mounted(),s=snapshot();p.view.render(s);await inspect(p).click();p.view.clear();assert.equal(p.revoked.length,1);p.view.render(s);assert.ok(inspect(p));p.view.destroy();assert.equal(p.container.children.length,0);
});
test('production Blob construction preserves the exact binary artifact',async()=>{
 const d=dom(),blobs=[],raw=Buffer.from([0,255,128,13,10,42]);
 const v=page.mount(d.container,{model,urls:{createObjectURL(blob){blobs.push(blob);return 'blob:invented';},revokeObjectURL(){}}});
 v.render(snapshot(retained(raw)));await d.find('button','Verify and inspect Alpha scores').click();
 assert.equal(blobs[0].type,'application/octet-stream');assert.deepEqual(Buffer.from(await blobs[0].arrayBuffer()),raw);v.destroy();
});
test('page hooks evidence into complete snapshot render and lifecycle without a new data route',()=>{
 const html=fs.readFileSync('portfolio/index.html','utf8');assert.match(html,/renderWatchlist\(\);renderResearchEvidence\(\)/);assert.match(html,/researchView\?\.clear\(\)/);assert.match(html,/jh-portfolio-research-page\.js/);
 for(const path of ['jh-portfolio-research.js','jh-portfolio-research-page.js'])assert.doesNotMatch(fs.readFileSync(path,'utf8'),/\bfetch\s*\(|localStorage|sessionStorage|innerHTML\s*=/);
});
