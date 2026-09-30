const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),zlib=require('node:zlib');
const model=require('../jh-portfolio-quotes.js'),page=require('../jh-portfolio-quotes-page.js');
function retained(symbol='AAA',raw=Buffer.from('{"ticker":"AAA","extra":900719925474099312345}')){
 return {price:110,price_basis:'SPLIT_ADJUSTED_PREVIOUS_DAY_CLOSE',price_timestamp_basis:'AGGREGATE_WINDOW_START_UTC_MS',as_of_unix_ms:Date.parse('2026-09-29T04:00:00Z'),currency:null,
  source_evidence:{schema_version:'previous-close-source.v1',requested_symbol:symbol,status:'MEASURED_PREVIOUS_CLOSE',reason_code:null,collection_task_started:true,request_attempted:true,
   body_complete:true,body_bytes:raw.length,body_encoding:'base64',body:raw.toString('base64'),body_sha256:crypto.createHash('sha256').update(raw).digest('hex'),
   started_at:'2026-09-30T10:00:00Z',completed_at:'2026-09-30T10:00:01Z'}};
}
function snapshot(records={AAA:retained()}){
 const rows=Object.values(records),symbols=Object.keys(records).sort();
 return {accounting:{source_prices:records,quote_collection:{schema_version:model.SCHEMA,status:'COMPLETE_ATTEMPT_COVERAGE',requested_symbols:symbols,unique_requested_count:symbols.length,
  tasks_started:symbols.length,unattempted_count:0,measured_previous_close_count:symbols.length,reason_counts:{},source_body_byte_bound:4*1024*1024,retained_complete_body_bytes:rows.reduce((n,r)=>n+r.source_evidence.body_bytes,0)}}};
}
const source=record=>model.source('AAA',record);

test('complete current Python collector fixtures retain every identity and verify every acquired body',async()=>{
 const fixture=JSON.parse(zlib.gunzipSync(fs.readFileSync('tests/fixtures/portfolio-quote-collection-synthetic.json.gz')));let identities=0,bodies=0;
 for(const row of fixture.cases.slice(0,5)){
  const view=model.view({accounting:{source_prices:row.output,quote_collection:row.collection}});assert.equal(view.coverageConsistent,true,row.name);assert.equal(view.rows.length,row.complete_invented_inputs.symbols.length);
  identities+=view.rows.length;for(const record of view.rows)if(record.inspectable){const body=await model.original(record);assert.equal(body.bytes.length,record.bytes);bodies++;}
 }
 assert.equal(identities,89);assert.equal(bodies,47);
});
test('source bar time, acquisition time and unverified currency are kept distinct',()=>{
 const row=model.view(snapshot()).rows[0];assert.equal(row.windowStart,'2026-09-29T04:00:00.000Z');assert.equal(row.started,'2026-09-30T10:00:00Z');assert.equal(row.price,110);assert.equal(row.allowsSizing,false);
 assert.match(model.view(snapshot()).detail,/not live execution quotes/);assert.match(model.view(snapshot()).detail,/Currency and held-instrument identity remain unverified/);
});
test('legacy evidence differs from genuine empty collection and absent records',()=>{
 for(const value of [null,{},[],{accounting:{source_prices:[]}}])assert.equal(model.view(value).available,false);
 const legacy=snapshot();delete legacy.accounting.quote_collection;assert.equal(model.view(legacy).rows.length,1);assert.equal(model.view(legacy).coverageConsistent,false);assert.match(model.view(legacy).coverageText,/unavailable/);
 const empty=model.view(snapshot({}));assert.equal(empty.available,true);assert.equal(empty.coverageConsistent,true);assert.match(empty.coverageText,/0 of 0/);
});
test('missing and unexpected identities remain visible and coverage fails',()=>{
 const s=snapshot();s.accounting.quote_collection.requested_symbols=['AAA','BBB'];s.accounting.quote_collection.unique_requested_count=2;
 let v=model.view(s);assert.equal(v.coverageConsistent,false);assert.deepEqual(v.rows.map(r=>r.key),['AAA','BBB']);assert.equal(v.rows[1].record,null);
 s.accounting.source_prices.CCC=retained('CCC');v=model.view(s);assert.deepEqual(v.rows.map(r=>r.key),['AAA','BBB','CCC']);assert.equal(v.coverageConsistent,false);
});
test('duplicate unordered malformed and prototype request identities cannot certify coverage',()=>{
 for(const requested of [['AAA','AAA'],['BBB','AAA'],['AAA',true],['__proto__'],null]){
  const s=snapshot();s.accounting.quote_collection.requested_symbols=requested;assert.equal(model.view(s).coverageConsistent,false);
 }
 const records=JSON.parse('{"__proto__":{},"constructor":{},"AAA":{}}'),v=model.view({accounting:{source_prices:records}});
 assert.deepEqual(v.rows.map(r=>r.key),['AAA','__proto__','constructor']);assert.equal(v.rows[1].identified,false);
});
test('reported count byte and reason disagreements remain explicit',()=>{
 for(const change of [{tasks_started:true},{tasks_started:0},{unique_requested_count:2},{unattempted_count:1},{measured_previous_close_count:0},{retained_complete_body_bytes:0},{source_body_byte_bound:1},{status:'PARTIAL_ATTEMPT_COVERAGE'},{reason_counts:{FAKE:1}}]){
  const s=snapshot();Object.assign(s.accounting.quote_collection,change);assert.equal(model.view(s).coverageConsistent,false,JSON.stringify(change));assert.match(model.view(s).coverageText,/inconsistent/);
 }
});
test('malformed unsafe and contradictory quote prices never become a displayed close',()=>{
 for(const price of [true,'110',0,-1,null,Infinity,NaN,2**53])assert.equal(source({...retained(),price}).price,null);
 for(const change of [{requested_symbol:'OTHER'},{status:'UNAVAILABLE'},{body_complete:false},{reason_code:'ERROR'},{schema_version:'other'}]){
  const row=retained();Object.assign(row.source_evidence,change);assert.equal(source(row).price,null);
 }
 for(const change of [{price_basis:'LIVE'},{price_timestamp_basis:'ACQUISITION'},{as_of_unix_ms:true},{as_of_unix_ms:2**53}])assert.equal(source({...retained(),...change}).price,null);
});
test('unattempted state cannot coexist with a measured completed request',()=>{
 const s=snapshot();s.accounting.source_prices.AAA.source_evidence.collection_task_started=false;assert.equal(model.view(s).coverageConsistent,false);
});
test('whole original response preserves long integers whitespace markup and Unicode',async()=>{
 const raw=Buffer.from('\ufeff{\n"value":900719925474099312345,"note":"π <img src=x>"\n}');const result=await model.original(source(retained('AAA',raw)));
 assert.deepEqual(Buffer.from(result.bytes),raw);assert.equal(result.text,raw.toString('utf8'));assert.equal(result.filename,'quote-AAA.original');
});
test('whole 128 KiB quote boundary is verified and larger forged metadata is refused',async()=>{
 const raw=Buffer.alloc(model.LIMIT,97);raw[raw.length-1]=90;assert.deepEqual(Buffer.from((await model.original(source(retained('AAA',raw)))).bytes),raw);
 await assert.rejects(model.original({...source(retained()),inspectable:true,bytes:model.LIMIT+1}));
 const tooBig=retained('AAA',Buffer.alloc(model.LIMIT+1));assert.equal(source(tooBig).inspectable,false);
});
test('corrupted and noncanonical encodings do not produce a downloadable original',async()=>{
 for(const change of [{body_sha256:'0'.repeat(64)},{body_bytes:1},{body:'e31='},{body:'e30=\n'},{body_encoding:'utf-8'}]){
  const row=retained('AAA',Buffer.from('{}'));Object.assign(row.source_evidence,change);await assert.rejects(model.original(source(row)));
 }
});
test('binary originals retain exact bytes and malformed record values remain inspectable metadata',async()=>{
 const raw=Buffer.from([255,254,0,128]);const result=await model.original(source(retained('AAA',raw)));assert.equal(result.text,null);assert.deepEqual(Buffer.from(result.bytes),raw);
 for(const record of [null,true,[],42,'malformed']){const row=model.source('AAA',record);assert.equal(row.price,null);assert.deepEqual(row.record,record);assert.equal(row.inspectable,false);}
});

function dom(){
 const document={activeElement:null,createElement:tag=>new Element(tag)};
 class Element{
  constructor(tag){this.tagName=tag;this.ownerDocument=document;this.children=[];this.attributes={};this.listeners={};this.hidden=false;this.parent=null;this.ownText='';}
  set textContent(v){this.ownText=String(v);this.replaceChildren();}get textContent(){return this.ownText+this.children.map(c=>c.textContent).join('');}
  append(...nodes){for(const n of nodes){n.parent=this;this.children.push(n);}}
  replaceChildren(...nodes){for(const n of this.children)n.parent=null;this.children=[];this.append(...nodes);}
  setAttribute(k,v){this.attributes[k]=String(v);}removeAttribute(k){delete this.attributes[k];if(k==='href')delete this.href;}
  addEventListener(k,fn){this.listeners[k]=fn;}click(){return this.listeners.click?.();}focus(){document.activeElement=this;}
  get isConnected(){return this===container||!!this.parent?.isConnected;}
 }
 const container=new Element('main');const all=()=>{const rows=[];function walk(n){rows.push(n);n.children.forEach(walk);}walk(container);return rows;};
 return {container,document,all,find:(tag,text)=>all().find(n=>n.tagName===tag&&n.textContent===text)};
}
function mounted(custom={}){
 const d=dom(),created=[],revoked=[],urls={createObjectURL(blob){created.push(blob);return 'blob:quote-'+created.length;},revokeObjectURL(url){revoked.push(url);}};
 return {...d,created,revoked,view:page.mount(d.container,{model,urls,makeBlob:bytes=>Buffer.from(bytes),...custom})};
}
const originalText=p=>p.all().find(n=>n.attributes['aria-label']==='Original quote response text');
test('view verifies original bytes on demand and restores keyboard focus on close',async()=>{
 const p=mounted(),s=snapshot();p.view.render(s);assert.equal(p.created.length,0);const open=p.find('button','Inspect AAA');await open.click();
 assert.equal(p.document.activeElement,p.find('button','Close quote evidence'));assert.match(originalText(p).textContent,/900719925474099312345/);assert.equal(p.created.length,1);
 p.find('button','Close quote evidence').click();assert.equal(p.document.activeElement,open);assert.equal(originalText(p).textContent,'');assert.deepEqual(p.revoked,['blob:quote-1']);
});
test('all 105 quote records can be reached without changing the original population',()=>{
 const records=Object.fromEntries(Array.from({length:105},(_,i)=>{const key='S'+String(i).padStart(4,'0');return [key,retained(key)];}));const s=snapshot(records),before=JSON.stringify(s),p=mounted();p.view.render(s);
 const seen=[];for(let i=0;i<5;i++){seen.push(...p.all().filter(n=>n.tagName==='button'&&/^Inspect S/.test(n.textContent)).map(n=>n.textContent.slice(8)));if(i<4)p.find('button','Next quote page').click();}
 assert.equal(seen.length,105);assert.equal(new Set(seen).size,105);assert.deepEqual(seen,Object.keys(records));assert.equal(p.find('button','Next quote page').disabled,true);assert.equal(JSON.stringify(s),before);
});
test('unavailable rows retain complete failure details without an original download',async()=>{
 const s=snapshot();s.accounting.source_prices.AAA={price:null,source_evidence:{schema_version:'previous-close-source.v1',requested_symbol:'AAA',status:'NOT_ATTEMPTED',reason_code:'COLLECTION_ACCEPTANCE_DEADLINE',collection_task_started:false,request_attempted:false,body_complete:false},extra:{whole:'INVENTED_DETAIL'}};
 const p=mounted();p.view.render(s);await p.find('button','Inspect AAA').click();assert.equal(p.created.length,0);assert.match(p.container.textContent,/No complete response/);assert.match(p.container.textContent,/INVENTED_DETAIL/);
});
test('hash failure clears original text and withholds the download',async()=>{
 const s=snapshot();s.accounting.source_prices.AAA.source_evidence.body_sha256='0'.repeat(64);const p=mounted();p.view.render(s);await p.find('button','Inspect AAA').click();assert.equal(p.created.length,0);assert.match(p.container.textContent,/verification failed/);assert.equal(originalText(p).textContent,'');assert.equal(p.find('a','Download verified quote response').hidden,true);
});
test('a delayed previous verification cannot overwrite a replacement snapshot',async()=>{
 let finish;const p=mounted({model:{...model,original:()=>new Promise(resolve=>finish=resolve)}});p.view.render(snapshot());const opening=p.find('button','Inspect AAA').click();p.view.render(snapshot({BBB:retained('BBB')}));finish({bytes:Buffer.from('OLD'),text:'OLD',filename:'old.original'});await opening;
 assert.equal(p.created.length,0);assert.equal(originalText(p).textContent,'');assert.ok(p.find('button','Inspect BBB'));assert.doesNotMatch(p.container.textContent,/OLD/);
});
test('clear removes retained quote details revokes blobs and allows recovery',async()=>{
 const p=mounted(),s=snapshot();p.view.render(s);await p.find('button','Inspect AAA').click();p.view.clear();assert.equal(p.revoked.length,1);assert.doesNotMatch(p.container.textContent,/900719925474099312345/);assert.match(p.container.textContent,/Quote evidence unavailable/);p.view.render(s);assert.ok(p.find('button','Inspect AAA'));
});
test('unchanged snapshot preserves open evidence while page navigation closes it',async()=>{
 const s=snapshot(Object.fromEntries(Array.from({length:26},(_,i)=>{const k='S'+String(i).padStart(3,'0');return[k,retained(k)];}))),p=mounted();p.view.render(s);await p.find('button','Inspect S000').click();p.view.render(s);assert.equal(p.revoked.length,0);assert.equal(originalText(p).hidden,false);p.find('button','Next quote page').click();assert.equal(p.revoked.length,1);assert.equal(originalText(p).textContent,'');
});
test('production Blob contains the verified original bytes and never interprets markup',async()=>{
 const raw=Buffer.from('<script>INVENTED_MARKUP</script>900719925474099312345'),p=mounted({makeBlob:undefined});p.view.render(snapshot({AAA:retained('AAA',raw)}));await p.find('button','Inspect AAA').click();assert.equal(p.created[0].type,'application/octet-stream');assert.deepEqual(Buffer.from(await p.created[0].arrayBuffer()),raw);assert.equal(p.all().filter(n=>n.tagName==='script').length,0);assert.equal(originalText(p).textContent,raw.toString());
});
