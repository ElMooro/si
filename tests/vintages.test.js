const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),zlib=require('node:zlib');
const api=require('../jh-vintages.js');
const prefix='data/vintage-research/',hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function fixture(){
 const files=new Map(),params={series_id:'WTREGEN',file_type:'json',realtime_start:'1776-07-04',realtime_end:'9999-12-31'};
 const definitions=[{id:'WTREGEN',title:'Treasury General Account',units:'Billions of Dollars',frequency_short:'W',seasonal_adjustment:'Not Seasonally Adjusted',realtime_start:'2026-09-01',realtime_end:'2026-09-03'},
 {id:'WTREGEN',title:'Treasury General Account',units:'Millions of U.S. Dollars',frequency_short:'W',seasonal_adjustment:'Not Seasonally Adjusted',realtime_start:'2026-09-04',realtime_end:'9999-12-31'}];
 function original(request,body){const raw=Buffer.from(api.canonical(body)),sha=hash(raw),requestSha=hash(api.canonical(request)),key=prefix+'originals/'+requestSha+'/'+sha+'.json.gz';files.set('/'+key,zlib.gzipSync(raw));return {request,acquired_at:'2026-09-09T00:00:00Z',evidence:{key,sha256:sha,request_sha256:requestSha,bytes:raw.length}};}
 const definitionSource=original({endpoint:'series',params},{seriess:definitions,realtime_start:params.realtime_start,realtime_end:params.realtime_end});
 const observations=[{date:'2026-09-01',value:'1.250',realtime_start:'2026-09-01',realtime_end:'2026-09-03'},
 {date:'2026-09-01',value:'1250',realtime_start:'2026-09-04',realtime_end:'2026-09-08'}];
 const source=original({endpoint:'series/observations',params:{...params,realtime_start:'2026-09-01',realtime_end:'2026-09-08',units:'lin',output_type:1,sort_order:'asc',limit:10000,offset:0,observation_start:'1776-07-04',observation_end:'9999-12-31'}},{observations});
 const doc={contract:'fred-vintage-periods.v1',series:'WTREGEN',collection_id:'one',generated_at:'2026-09-09T00:00:00Z',acquired_at:'2026-09-09T00:00:00Z',definitions,definition_source:definitionSource,observation_sources:[source],n_vintages:2,
 coverage:{status:'complete_requested_windows',scope:'series_archive',archive_start:'2026-09-01',archive_end:'2026-09-08',observations:1,missing_periods:0,pages:1},replay:{manifest_key:prefix+'runs/'+'a'.repeat(64)+'.json'},
 vintages:observations.map((r,i)=>({date:r.date,value_decimal:r.value,value:Number(r.value),valid_from:r.realtime_start,valid_through:r.realtime_end,start_left_censored:i===0,source_page:0,source_row:i}))};
 function store(value){const raw=Buffer.from(api.canonical(value)),sha=hash(raw),key=prefix+'outputs/'+sha+'.json';files.set('/'+key,raw);return {status:'source_replayed',key,sha256:sha,...Object.fromEntries(['coverage','replay','generated_at','acquired_at','n_vintages'].map(k=>[k,value[k]]))};}
 const entry=store(doc),index={contract:'fred-vintage-index.v1',collection_id:'one',detail:{WTREGEN:entry}};
 const fetcher=async url=>new Response(files.get(url)||'not found',{status:files.has(url)?200:404});
 return {doc,index,entry,files,fetcher,store};
}
test('immutable output binds collection and bytes; unavailable is not an older fallback',async()=>{
 const f=fixture();assert.equal((await api.loadPacket(f.index,'WTREGEN',f.fetcher)).series,'WTREGEN');
 f.files.set('/'+f.entry.key,Buffer.from('{}'));await assert.rejects(api.loadPacket(f.index,'WTREGEN',f.fetcher),/hash differs/);
 await assert.rejects(api.loadPacket(f.index,'MISSING',f.fetcher),/unavailable/i);
 const g=fixture();g.index.collection_id='other';await assert.rejects(api.loadPacket(g.index,'WTREGEN',g.fetcher),/collection differs/);
});
test('historical units and exact decimal text follow the selected closed interval',()=>{
 const {doc}=fixture();assert.equal(api.select(doc,'2026-09-03').definition.units,'Billions of Dollars');
 assert.equal(api.select(doc,'2026-09-04').definition.units,'Millions of U.S. Dollars');
 const html=api.render(doc,'2026-09-03');assert.match(html,/1.250/);assert.match(html,/initial release date is unknown/);assert.match(html,/2026-09-04T12:00:00.000Z/);
 assert.equal(api.select(doc,'2026-09-09').status,'outside_retained_archive');
 assert.throws(()=>api.select(doc,'2026-02-30'));doc.vintages.push({...doc.vintages[1]});assert.throws(()=>api.select(doc,'2026-09-05'),/Ambiguous/);
});
test('latest missing remains missing; an observed zero renders as zero',()=>{
 const {doc}=fixture();doc.vintages.push({...doc.vintages[1],date:'2026-09-02',value_decimal:null,value:null});
 assert.equal(api.select(doc,'2026-09-05').status,'missing');assert.match(api.render(doc,'2026-09-05'),/<strong>Not reported<\/strong>/);
 doc.vintages[2].value_decimal='0';doc.vintages[2].value=0;assert.match(api.render(doc,'2026-09-05'),/<strong>0<\/strong>/);
});
test('original response hashes, full request identities and whole historical definition are verified',async()=>{
 const f=fixture();assert.match(await api.verify(f.doc,'2026-09-05',f.fetcher),/Verified both original response hashes/);
 f.doc.definitions[1].frequency_short='D';await assert.rejects(api.verify(f.doc,'2026-09-05',f.fetcher),/definition differs/);
 const g=fixture();g.doc.vintages[1].value_decimal='123';await assert.rejects(api.verify(g.doc,'2026-09-05',g.fetcher),/row differs/);
 const h=fixture();h.doc.observation_sources[0].request.params.offset=10;await assert.rejects(api.verify(h.doc,'2026-09-05',h.fetcher),/request identity differs/);
 const transformed=fixture();transformed.doc.observation_sources[0].request.params.units='pch';await assert.rejects(api.verify(transformed.doc,'2026-09-05',transformed.fetcher),/Native archive request/);
 const j=fixture();j.files.set('/'+j.doc.definition_source.evidence.key,zlib.gzipSync(Buffer.from('{}')));await assert.rejects(api.verify(j.doc,'2026-09-05',j.fetcher),/hash or length differs/);
});
test('archive segments load only a matching bound packet and reject gaps and corrupt descriptors',async()=>{
 const f=fixture();f.doc.coverage.scope='archive_segment';const segment=f.store(f.doc);
 const catalog={contract:'fred-vintage-segments.v1',series:'WTREGEN',coverage:{archive_start:'2026-09-01',archive_end:'2026-09-08'},segments:[segment]};
 assert.equal((await api.resolve(catalog,'2026-09-05',f.fetcher)).series,'WTREGEN');
 segment.n_vintages=999;await assert.rejects(api.resolve(catalog,'2026-09-05',f.fetcher),/descriptor differs/);
 segment.coverage.archive_start='2026-09-02';await assert.rejects(api.resolve(catalog,'2026-09-05',f.fetcher),/gap or overlap/);
});
test('unsafe source text and links never become executable page content',()=>{
 const {doc}=fixture();doc.definitions[1].title='<img src=x onerror=alert(1)>';doc.definition_source.evidence.key='javascript:bad';
 const html=api.render(doc,'2026-09-05');assert.ok(!html.includes('<img'));assert.match(html,/&lt;img/);assert.ok(!html.includes('href="javascript:'));
 assert.equal(api.path('data/vintage-research/../../private'),null);
});
test('response and decompression streams stop at the byte bound',async()=>{
 let cancelled=false;const stream=new ReadableStream({pull(c){c.enqueue(new Uint8Array(10));},cancel(){cancelled=true;}});
 await assert.rejects(api.bounded(stream,15),/exceeds bound/);assert.equal(cancelled,true);
});
