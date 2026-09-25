const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path');
globalThis.crypto=crypto.webcrypto;
const api=require('../jh-buyback-documents.js');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/buyback-document-evidence-synthetic.json')));
const base=fixture.base,catalog=fixture.catalog,now=Date.parse(catalog.capture_completed_at)+1;
const bytes=Buffer.from(JSON.stringify(catalog)),sha=crypto.createHash('sha256').update(bytes).digest('hex');

test('real Python compiler output verifies complete document and literal-coordinate conservation',async()=>{
 assert.deepEqual(await api.checked(bytes,sha,base,now),catalog);
 const html=api.table(catalog,base.rows[0]);
 for(const text of ['Inspect all 2 embedded documents','main.htm','ex991.htm','EX-99.1','start inclusive, end exclusive','not buyback classifications'])assert(html.includes(text),text);
 assert(html.includes(catalog.filings[0].documents[1].text_bytes.toLocaleString('en-US')+' text bytes'));
 for(const d of catalog.filings[0].documents){assert(html.includes(d.sha256));assert(html.includes(d.text_sha256));assert(html.includes(d.document_url));}
 assert.equal((html.match(/<li>/g)||[]).length,2);assert(!html.includes('NOT_FOR_PUBLIC'));
 assert(!html.includes('1000000'));assert(!html.includes('position size'));
});

test('catalog fetch is explicit, anonymous, bounded and exactly hash-pinned',async()=>{
 const requests=[],reference={path:'/assets/research/buyback-documents-20260925.json',sha256:sha};
 const fetcher=async(url,options)=>{requests.push({url,options});return new Response(bytes);};
 assert.deepEqual(await api.load(fetcher,reference,base,now),catalog);
 assert.deepEqual(requests,[{url:reference.path,options:{cache:'no-store',credentials:'omit'}}]);
 await assert.rejects(api.load(fetcher,{...reference,path:'/data/accounts.json'},base,now));assert.equal(requests.length,1);
 await assert.rejects(api.checked(Buffer.concat([bytes,Buffer.from(' ')]),sha,base,now));
 await assert.rejects(api.load(async()=>new Response('',{status:503}),reference,base,now));
 await assert.rejects(api.load(async()=>new Response(new Uint8Array(4*1024*1024+1)),reference,base,now));
});

test('changed capture, truncated document lists, overlapping ranges and invented authority are rejected',()=>{
 const mutations=[c=>c.source_packet.sha256='a'.repeat(64),c=>c.manifest_sha256='b'.repeat(64),
  c=>c.documents++,c=>c.rows.pop(),c=>c.filings[0].documents.pop(),c=>c.filings[0].source_rows.push(0),
  c=>c.filings[0].documents[1].document_url='javascript:alert(1)',c=>c.filings[0].documents[1].filename='../bad.htm',
  c=>c.filings[0].documents[1].byte_start=0,c=>c.filings[0].documents[1].text_byte_end++,
  c=>c.filings[0].documents[1].literal_keyword_occurrences[0].byte_start++,
  c=>c.filings[0].documents[1].document_url_current_bytes_verified=true,
  c=>c.rows[0].reported_authorization_amount_verified=true,...api.FLAGS.map(k=>c=>c[k]=true)];
 for(const change of mutations){const c=structuredClone(catalog);change(c);assert.throws(()=>api.validate(c,base,now));}
 assert.throws(()=>api.validate(catalog,base,now-2000));
});

test('document rendering escapes labels and refuses a different original-file join',()=>{
 const c=structuredClone(catalog);c.filings[0].documents[0].type='<img src=x onerror=bad>';
 const html=api.table(c,base.rows[0]);assert(html.includes('&lt;img'));assert(!html.includes('<img'));
 assert.equal(api.table(catalog,{...base.rows[0],original_sha256:'a'.repeat(64)}),'');
 assert.equal(api.table(null,base.rows[0]),'');
});
