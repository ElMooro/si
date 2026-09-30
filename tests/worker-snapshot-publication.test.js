/* Current reviewed modules, complete invented bodies, in-memory storage only. */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {createHash} = require('node:crypto');
const root = path.join(__dirname, '..');
const api = import(pathToFileURL(path.join(root, 'cloudflare/workers/justhodl-data-proxy/src/portfolio-snapshot.js')).href);
const binding = require('../jh-portfolio-binding.js');
const utf8 = new TextEncoder();
const sha = raw => createHash('sha256').update(raw).digest('hex');
function fixture() {
  const writes = [], stored = new Map([['private-artifact:portfolio-snapshot', '{"predecessor":"complete invented original"}']]);
  const env = {USER_DATA: {async put(key, value) { writes.push({key, value}); stored.set(key, value); }}};
  async function publish(body, headers = {}) {
    return (await api).publishSnapshot(new Request('https://local.test/private-artifact?kind=portfolio-snapshot', {method:'PUT', body, headers}), env, {'Access-Control-Allow-Origin':'https://justhodl.ai'});
  }
  return {writes,stored,env,publish};
}

test('snapshot publication preserves every original byte and negative zero with a complete acknowledgement', async () => {
  const f = fixture();
  const raw = ' \n{"positions":[],"unknown":{"negative":-0.0,"text":"literal π \\u2603","values":[false,null,0,1.25]},"watchlist":[]}\n';
  const response = await f.publish(raw, {'X-JH-Body-SHA256':sha(raw)});
  assert.equal(response.status,200);assert.equal(f.writes.length,1);assert.equal(f.writes[0].value,raw);
  assert.ok(Object.is(JSON.parse(f.writes[0].value).unknown.negative,-0));
  assert.deepEqual(await response.json(),{ok:true,protocol:'portfolio-snapshot-bytes.v1',body_bytes:Buffer.byteLength(raw),body_sha256:sha(raw),identity_bytes:binding.encode(JSON.parse(raw)).byteLength});
  assert.match(response.headers.get('Cache-Control'),/no-store/);assert.equal(response.headers.get('Vary'),'Authorization');
  // Full native-handler outputs, not shortened account-shaped examples. All
  // source bytes are bound, and every original mocked S3 write is preserved.
  const frames=JSON.parse(require('node:zlib').gunzipSync(fs.readFileSync(path.join(root,'tests/fixtures/portfolio-sector-browser-synthetic.json.gz'))));
  for(const [name,digest] of Object.entries(frames.source_files)){
    const retained=name==='aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py'?'tests/fixtures/pre-snapshot-byte-publication/lambda_function.py.txt':name;
    assert.equal(sha(fs.readFileSync(path.join(root,retained))),digest);
  }
  assert.equal(Object.keys(frames.cases).length,9);
  const nativeNames=['complete','partial','binary','empty','mixed','known'];
  assert.deepEqual(Object.keys(frames.cases).filter(name=>Array.isArray(frames.cases[name].writes)),nativeNames);
  assert.deepEqual(Object.keys(frames.cases).filter(name=>Object.hasOwn(frames.cases[name],'injected_browser_fault')),['corrupt','legacy','identity']);
  for(const name of nativeNames){
    const row=frames.cases[name];assert.equal(row.writes.length,2);
    const wire=row.writes.find(write=>write.sink==='s3').request.Body;
    const complete=Buffer.from(wire.body,'base64');assert.equal(complete.length,wire.bytes);assert.equal(sha(complete),wire.sha256);
    const native=fixture(),accepted=await native.publish(complete);assert.equal(accepted.status,200);
    assert.equal(native.writes[0].value,complete.toString('utf8'));assert.equal((await accepted.json()).body_sha256,wire.sha256);
  }
  const current=JSON.parse(require('node:zlib').gunzipSync(fs.readFileSync(path.join(root,'tests/fixtures/snapshot-byte-publication-synthetic.json.gz'))));
  for(const [name,digest] of Object.entries(current.source_files)){
    // These whole frames precede native ordering. Keep their generating source
    // inert and hash-bound; current ordered frames have their own integration.
    const retained=name.startsWith('aws/lambdas/justhodl-portfolio-snapshot/source/')?'tests/fixtures/pre-snapshot-native-ordering/'+path.basename(name)+'.txt':name;
    assert.equal(sha(fs.readFileSync(path.join(root,retained))),digest);
  }
  assert.equal(Object.keys(current.cases).length,6);
  for(const [name,row] of Object.entries(current.cases)){
    assert.equal(row.measurement_values_unchanged,true);
    if(name==='partial'){
      assert.equal(row.candidate_only_not_published,true);assert.equal(row.writes.length,0);assert.equal(row.acknowledgements.length,0);assert.match(row.failure.reason,/runtime reserve/);continue;
    }
    const wire=row.candidate.complete_body,complete=Buffer.from(wire.body,'base64');assert.equal(sha(complete),wire.sha256);assert.equal(complete.length,wire.bytes);
    assert.deepEqual(row.writes.map(write=>write.sink),['private','s3']);assert.ok(row.writes.every(write=>write.body_sha256===wire.sha256));
    const receiver=fixture(),response=await receiver.publish(complete);assert.equal(response.status,200);assert.equal(receiver.writes[0].value,complete.toString('utf8'));
    assert.deepEqual(await response.json(),JSON.parse(Buffer.from(row.acknowledgements[0].complete_acknowledgement.body,'base64')));
  }
});
test('snapshot value-size accounting matches the real browser for complete cross-runtime vectors', async () => {
  const a = await api;
  const vectors = JSON.parse(fs.readFileSync(path.join(root,'tests/fixtures/portfolio-value-identity-vectors.json')));
  for (const row of vectors) {
    const doc = {value:row.value};
    assert.equal(a.snapshotIdentityBytes(utf8.encode(JSON.stringify(doc))),binding.encode(doc).byteLength);
  }
  for (const n of [4095,4096,4097,8191]) {
    const doc = {['a'.repeat(n)+'😀']:'a'.repeat(n)+'😀 雪',all:[-0,0,false,null]};
    assert.equal(a.snapshotIdentityBytes(utf8.encode(JSON.stringify(doc))),binding.encode(doc).byteLength);
  }
});
test('legacy publisher without a digest receives the same additive acknowledgement', async () => {
  const f=fixture(),raw='{"positions":[],"watchlist":[]}';
  const r=await f.publish(raw);assert.equal(r.status,200);assert.equal((await r.json()).body_sha256,sha(raw));assert.equal(f.writes[0].value,raw);
});
test('duplicate keys, malformed JSON, unsupported Unicode, unsafe integers and nonfinite values never replace the predecessor', async () => {
  for (const raw of ['{"a":1,"a":2}','{"x":1e999}','{"x":9007199254740992}','{"x":-9007199254740992}','{"x":"\\ud800"}','{"x":"\\udfff"}','[1]','null','{}{}','\ufeff{}','{"a":true,}']) {
    const f=fixture(),r=await f.publish(raw);assert.equal(r.status,400,raw);assert.deepEqual(f.writes,[]);assert.equal(f.stored.size,1);
  }
});
test('invalid UTF-8 bytes are never replaced by the decoder and stored', async () => {
  const f=fixture();const r=await f.publish(new Uint8Array([123,34,120,34,58,34,0xff,34,125]));assert.equal(r.status,400);assert.equal(f.writes.length,0);
});
test('exact nesting depth is accepted and a deeper complete frame is refused', async () => {
  const f=fixture(),raw='{"x":'+'['.repeat(127)+'0'+']'.repeat(127)+'}';
  assert.equal((await f.publish(raw)).status,200);
  const over=fixture();assert.equal((await over.publish('{"x":'+'['.repeat(128)+'0'+']'.repeat(128)+'}')).status,400);assert.equal(over.writes.length,0);
});
test('body size, declared length and content encoding are enforced before storage', async () => {
  for (const [body,headers,status] of [['{}',{'Content-Length':'3'},400],['{}',{'Content-Length':'20000001'},413],['{}',{'Content-Encoding':'gzip'},400],['{}',{'Content-Length':'-1'},413],['{"x":"'+'x'.repeat(20000000)+'"}',{},413]]) {
    const f=fixture();assert.equal((await f.publish(body,headers)).status,status);assert.equal(f.writes.length,0);
  }
});
test('numeric expansion beyond the real 32 MiB typed identity limit cannot be published', async () => {
  const f=fixture(),raw='{"numbers":['+'0,'.repeat(3728270)+'0]}';
  assert.ok(Buffer.byteLength(raw)<20000000);
  assert.equal((await f.publish(raw)).status,400);assert.equal(f.writes.length,0);
});
test('a mismatched or malformed digest cannot write a different body', async () => {
  for (const expected of ['0'.repeat(64),'bad','A'.repeat(64)]) {
    const f=fixture();assert.equal((await f.publish('{"all":"invented"}',{'X-JH-Body-SHA256':expected})).status,400);assert.equal(f.writes.length,0);
  }
});
test('an incomplete stream reports no acknowledgement and never writes a prefix', async () => {
  const f=fixture();
  const request={headers:new Headers(),body:new ReadableStream({start(controller){controller.enqueue(utf8.encode('{"x":"complete prefix'));controller.error(Error('invented read failure'));}})};
  const r=await (await api).publishSnapshot(request,f.env,{});assert.equal(r.status,400);assert.equal(f.writes.length,0);
});
test('a failed store remains unconfirmed and a later successful request recovers', async () => {
  const f=fixture(),original=f.env.USER_DATA.put;f.env.USER_DATA.put=async()=>{throw Error('INVENTED_PRIVATE_DIAGNOSTIC');};
  const r=await f.publish('{"x":0}');assert.equal(r.status,503);assert.doesNotMatch(await r.text(),/INVENTED_PRIVATE/);assert.equal(f.writes.length,0);
  f.env.USER_DATA.put=original;assert.equal((await f.publish('{"x":0}')).status,200);assert.equal(f.writes.length,1);
});
test('complete inert predecessors and the reproduced loss remain hash-bound without archived execution', () => {
  const audit=JSON.parse(fs.readFileSync(path.join(root,'docs/audit/2026-09-30/portfolio-snapshot-mirror-bytes.json')));
  for(const row of Object.values(audit.fixtures)){const raw=fs.readFileSync(path.join(root,row.path));assert.equal(raw.length,row.bytes);assert.equal(sha(raw),row.sha256);}
  const before=JSON.parse(fs.readFileSync(path.join(root,audit.fixtures.complete_synthetic.path)));assert.equal(before.input_negative_zero,true);assert.equal(before.received_negative_zero,false);
});
