/* Pinned Wrangler's local workerd/SQLite only. Never uses real bindings/secrets.
 * Usage: node this-file <complete dry-run index.js> <installed wrangler directory>
 */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createRequire } = require('node:module');
const { createHash } = require('node:crypto');
const sha = raw => createHash('sha256').update(raw).digest('hex');

async function main() {
  const [bundlePath, wranglerRoot] = process.argv.slice(2);
  assert.ok(bundlePath && wranglerRoot, 'Complete build and pinned Wrangler installation required');
  const localRequire = createRequire(path.resolve(wranglerRoot, 'package.json'));
  assert.equal(localRequire('./package.json').version, '4.144.0');
  const { Miniflare, convertV4MiniflareOptions } = localRequire('miniflare');
  const code = fs.readFileSync(bundlePath, 'utf8'), expected = sha(Buffer.from(code));
  const persistence = fs.mkdtempSync(path.join(os.tmpdir(), 'jh-portfolio-ordering-'));
  const token = 'invented-local-runtime-token-446';
  let external = 0, checks = 0, mf;
  const options = convertV4MiniflareOptions({
    modules: true, script: code, compatibilityDate: '2025-10-01',
    kvNamespaces: ['USER_DATA'], durableObjects: { WORKSPACE_COORDINATOR: { className: 'WorkspaceCoordinator', useSQLite: true } },
    bindings: { ADMIN_TOKEN: token }, resourcePersistencePath: persistence,
    telemetry: { enabled: false }, cf: false,
    outboundService: async () => { external++; throw Error('External fetch is forbidden in runtime acceptance'); },
  });
  const call = (method, body, headers = {}, action = '') => mf.dispatchFetch('http://local.test/private-artifact?kind=portfolio-risk' + action,
    { method, headers: { 'X-JH-Service-Token': token, ...headers }, ...(body === undefined ? {} : { body }) });
  const snapshotCall = (method, body) => mf.dispatchFetch('http://local.test/private-artifact?kind=portfolio-snapshot',
    { method, headers: { 'X-JH-Service-Token': token }, ...(body === undefined ? {} : { body }) });
  const reserve = async () => {
    const r = await call('POST', '{}', {}, '&action=reserve'); assert.equal(r.status, 200, await r.clone().text()); return r.json();
  };
  const packet = ticket => ' \n' + JSON.stringify({ status: 'INCOMPLETE', generated_at: null,
    snapshot_binding: { encoding: 'typed-json-binary64.v1', value_sha256: 'a'.repeat(64), generated_at: null },
    unknown: '雪'.repeat(900000), zero: 0, null_value: null, boolean: false, permissions: { sizing_eligible: false },
    publication: { schema_version: 'portfolio-risk-publication.v1', revision: ticket.revision,
      source_etag: '"invented-source"', source_value_sha256: 'a'.repeat(64), started_at: '2026-09-18T17:00:00Z' } }) + '\n';
  const publish = (ticket, raw) => call('PUT', raw, { 'X-JH-Publication-Token': ticket.token, 'X-JH-Body-SHA256': sha(Buffer.from(raw)) });
  try {
    mf = new Miniflare(options); await mf.ready;
    const kv = await mf.getKVNamespace('USER_DATA'); const legacy = '{"invented_legacy":"whole 雪","zero":-0.0}\n';
    await kv.put('private-artifact:portfolio-risk', legacy);
    assert.equal(await (await call('GET')).text(), legacy); checks++;
    const tickets = await Promise.all(Array.from({ length: 8 }, reserve));
    assert.deepEqual(tickets.map(t => t.revision).sort((a, b) => a - b), [1, 2, 3, 4, 5, 6, 7, 8]); checks++;
    const newest = tickets.find(t => t.revision === 8), raw = packet(newest);
    assert.equal((await publish(newest, raw)).status, 200); checks++;
    const older = tickets.find(t => t.revision === 1);
    assert.equal((await publish(older, packet(older))).status, 409); checks++;
    const retry = await publish(newest, raw); assert.equal((await retry.json()).status, 'unchanged'); checks++;
    assert.equal((await publish(newest, raw + ' ')).status, 409); checks++;
    assert.equal((await call('PUT', '{"legacy":"late"}')).status, 409); checks++;
    assert.equal(await kv.get('private-artifact:portfolio-risk'), legacy); checks++;
    const get = await call('GET'); assert.equal(await get.text(), raw); assert.equal(get.headers.get('X-JH-Body-SHA256'), sha(Buffer.from(raw))); checks++;
    const snapshotRaw = ' {"positions":[],"watchlist":[],"zero":-0.0,"unknown":{"whole":"' + '雪'.repeat(900000) + '"}}\n';
    const snapshotAck = await (await snapshotCall('PUT', snapshotRaw)).json();
    assert.equal(snapshotAck.protocol, 'portfolio-snapshot-bytes.v1'); assert.equal(snapshotAck.body_sha256, sha(Buffer.from(snapshotRaw))); assert.equal(snapshotAck.body_bytes, Buffer.byteLength(snapshotRaw)); checks++;
    assert.equal(await (await snapshotCall('GET')).text(), snapshotRaw); checks++;
    assert.equal((await snapshotCall('PUT', '{"duplicate":1,"duplicate":2}')).status, 400);
    assert.equal(await (await snapshotCall('GET')).text(), snapshotRaw); checks++;
    await mf.dispose(); mf = new Miniflare(options); await mf.ready;
    const head = await call('HEAD'); assert.equal(head.status, 200); assert.equal(Number(head.headers.get('Content-Length')), Buffer.byteLength(raw)); checks++;
    assert.equal(await (await call('GET')).text(), raw); checks++;
    assert.equal((await reserve()).revision, 9); checks++;
    const restoredSnapshot = await (await snapshotCall('GET')).text();
    assert.equal(restoredSnapshot, snapshotRaw); assert.ok(Object.is(JSON.parse(restoredSnapshot).zero, -0)); checks++;
    assert.equal(external, 0); assert.equal(sha(fs.readFileSync(bundlePath)), expected);
    console.log(JSON.stringify({ status: 'passed', checks, compiled_sha256: expected, complete_synthetic_bytes: Buffer.byteLength(raw), complete_snapshot_synthetic_bytes: Buffer.byteLength(snapshotRaw),
      runtime: 'local workerd with SQLite Durable Object', external_requests: external, actual_private_reads: 0, actual_private_writes: 0 }));
  } finally { if (mf) await mf.dispose(); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
