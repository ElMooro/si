/* Owner-only portfolio mirror. No market requests, account discovery or messages.
 * Revisions are issued before acquisition, not inferred from completion time.
 * This is one store's transaction; S3 publication is a separate conditional write.
 */
export const RISK_PROTOCOL = 'portfolio-risk-publication.v1';
export const RISK_OBJECT = 'private-artifact:portfolio-risk:publication-v1';
export const MAX_RISK_BYTES = 20000000;
const CHUNK_BYTES = 256 * 1024, RESERVATIONS = 128;
const KEY = 'private-artifact:portfolio-risk';

class PublicationError extends Error {
  constructor(code, status = 503) { super(code); this.status = status; }
}
const fail = (code, status) => { throw new PublicationError(code, status); };
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const revision = value => Number.isSafeInteger(value) && value >= 1;
const digestPattern = /^[a-f0-9]{64}$/;
const tokenPattern = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const headers = { 'Content-Type': 'application/json', 'Cache-Control': 'private, no-store', Vary: 'Authorization' };
const json = (value, status = 200) => new Response(JSON.stringify(value), { status, headers });

// Same strict JSON grammar as the evidence reader, owned by this Worker build.
// Do not reserialize accepted bodies: formatting and unknown fields are evidence.
export function riskDocument(raw) {
  if (!(raw instanceof Uint8Array) || raw.byteLength > MAX_RISK_BYTES) fail('invalid_complete_body', 413);
  const source = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(raw);
  let i = 0; const number = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  function ws() { while (/[\x20\t\r\n]/.test(source[i] || 'x')) i++; }
  function string() {
    const start = i++;
    for (; i < source.length; i++) {
      if (source[i] === '\\') { i++; continue; }
      if (source[i] === '"') {
        const text = JSON.parse(source.slice(start, ++i));
        for (let n = 0; n < text.length; n++) {
          const c = text.charCodeAt(n);
          if (c >= 0xD800 && c <= 0xDBFF) {
            const next = text.charCodeAt(++n);
            if (!(next >= 0xDC00 && next <= 0xDFFF)) fail('invalid_json_unicode', 400);
          } else if (c >= 0xDC00 && c <= 0xDFFF) fail('invalid_json_unicode', 400);
        }
        return text;
      }
    }
    fail('incomplete_json_string', 400);
  }
  function value(depth) {
    if (depth > 128) fail('json_nesting_bound', 400);
    ws(); const c = source[i];
    if (c === '"') return string();
    if (c === '{' || c === '[') {
      const dict = c === '{', out = dict ? {} : [], seen = new Set(), end = dict ? '}' : ']';
      i++; ws(); if (source[i] === end) { i++; return out; }
      for (;;) {
        ws(); let key;
        if (dict) {
          if (source[i] !== '"') fail('invalid_json_key', 400);
          key = string(); if (seen.has(key)) fail('duplicate_json_key', 400); seen.add(key);
          ws(); if (source[i++] !== ':') fail('invalid_json_colon', 400);
        }
        const item = value(depth + 1);
        if (dict) Object.defineProperty(out, key, { value: item, enumerable: true, writable: true, configurable: true });
        else out.push(item);
        ws(); if (source[i] === end) { i++; return out; }
        if (source[i++] !== ',') fail('incomplete_json_structure', 400);
      }
    }
    for (const [token, v] of [['true', true], ['false', false], ['null', null]]) {
      if (source.startsWith(token, i)) { i += token.length; return v; }
    }
    number.lastIndex = i; const match = number.exec(source);
    if (!match) fail('invalid_json_value', 400);
    i = number.lastIndex; const n = Number(match[0]);
    if (!Number.isFinite(n)) fail('nonfinite_json_number', 400);
    return n;
  }
  const out = value(0); ws();
  if (i !== source.length || !object(out)) fail('invalid_artifact', 400);
  return out;
}

export async function riskBody(request, limit = MAX_RISK_BYTES, timeoutMs = 20000) {
  let reader, timer, finished = false;
  const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(new PublicationError('body_deadline', 408)), timeoutMs); });
  try {
    return await Promise.race([timeout, (async () => {
      const encoding = request.headers.get('Content-Encoding');
      if (encoding && encoding !== 'identity') fail('unsupported_body_encoding', 400);
      const length = request.headers.get('Content-Length');
      if (length !== null && (!/^(?:0|[1-9]\d*)$/.test(length) || !Number.isSafeInteger(Number(length)) || Number(length) > limit)) fail('invalid_body_length', 413);
      if (!request.body?.getReader) fail('complete_body_required', 400);
      reader = request.body.getReader(); const parts = []; let size = 0;
      for (;;) {
        const { value, done } = await reader.read();
        if (finished) fail('body_deadline', 408);
        if (done) break;
        if (!(value instanceof Uint8Array)) fail('invalid_body_chunk', 400);
        size += value.byteLength; if (size > limit) fail('artifact_too_large', 413);
        parts.push(value);
      }
      if (length !== null && Number(length) !== size) fail('incomplete_body', 400);
      const raw = new Uint8Array(size); let offset = 0;
      for (const part of parts) { raw.set(part, offset); offset += part.byteLength; }
      return raw;
    })()]);
  } finally {
    finished = true; clearTimeout(timer);
    if (reader) { try { Promise.resolve(reader.cancel()).catch(() => {}); } catch {} try { reader.releaseLock(); } catch {} }
    else { try { Promise.resolve(request.body?.cancel()).catch(() => {}); } catch {} }
  }
}

export async function riskDigest(raw) {
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256', raw))].map(n => n.toString(16).padStart(2, '0')).join('');
}
function manifestValid(m) {
  return object(m) && Number.isSafeInteger(m.bytes) && m.bytes > 0 && m.bytes <= MAX_RISK_BYTES &&
    digestPattern.test(m.sha256) && m.chunks === Math.ceil(m.bytes / CHUNK_BYTES) &&
    typeof m.prefix === 'string' && /^(?:risk:body:[1-9][0-9]*|risk:legacy):$/.test(m.prefix);
}
async function putBody(txn, prefix, raw, sha256) {
  const count = Math.ceil(raw.byteLength / CHUNK_BYTES);
  for (let i = 0; i < count; i++) await txn.put(prefix + i, raw.slice(i * CHUNK_BYTES, (i + 1) * CHUNK_BYTES));
  return { prefix, bytes: raw.byteLength, sha256, chunks: count };
}
async function readBody(storage, manifest) {
  if (!manifestValid(manifest)) fail('stored_manifest_invalid');
  const raw = new Uint8Array(manifest.bytes);
  for (let i = 0; i < manifest.chunks; i++) {
    const part = await storage.get(manifest.prefix + i);
    if (!(part instanceof Uint8Array) || part.byteLength !== Math.min(CHUNK_BYTES, raw.byteLength - i * CHUNK_BYTES)) fail('stored_body_incomplete');
    raw.set(part, i * CHUNK_BYTES);
  }
  if (await riskDigest(raw) !== manifest.sha256) fail('stored_body_mismatch');
  return raw;
}
async function legacyBody(env) {
  const value = await env.USER_DATA.get(KEY, { type: 'arrayBuffer' });
  if (value === null) return null;
  if (!(value instanceof ArrayBuffer)) fail('legacy_body_unavailable');
  const raw = new Uint8Array(value);
  try { riskDocument(raw); } catch { fail('legacy_body_invalid'); }
  return raw;
}
function currentValid(current) {
  if (current !== undefined && (!object(current) || !revision(current.revision) || !tokenPattern.test(current.token) || !manifestValid(current.body) || current.body.prefix !== 'risk:body:' + current.revision + ':')) fail('stored_publication_invalid');
}
async function currentState(storage) {
  const current = await storage.get('risk:current'); currentValid(current);
  const governed = await storage.get('risk:governed');
  if ((governed !== undefined && governed !== true) || (governed === true) !== (current !== undefined)) fail('stored_publication_incomplete');
  return current;
}
function parseBody(raw) {
  try { return riskDocument(raw); }
  catch (error) { if (error instanceof PublicationError) throw error; fail('invalid_artifact', 400); }
}

// Invoked only inside the dedicated, serialized WorkspaceCoordinator object.
export async function handlePortfolioPublication(storage, env, request) {
  try {
    const url = new URL(request.url);
    if (request.method === 'POST' && url.searchParams.get('action') === 'reserve') {
      const doc = parseBody(await riskBody(request, 4096));
      const minimum = Object.hasOwn(doc, 'minimum_revision') ? doc.minimum_revision : 0;
      if (!Number.isSafeInteger(minimum) || minimum < 0 || minimum >= Number.MAX_SAFE_INTEGER) fail('invalid_minimum_revision', 400);
      const token = crypto.randomUUID();
      return await storage.transaction(async txn => {
        const current = await currentState(txn);
        const previous = await txn.get('risk:counter');
        if ((current && previous === undefined) || (previous !== undefined && (!Number.isSafeInteger(previous) || previous < (current?.revision ?? 0)))) fail('stored_counter_invalid');
        const issued = Math.max(previous ?? 0, current?.revision ?? 0, minimum) + 1;
        if (!revision(issued)) fail('revision_exhausted');
        await txn.put('risk:counter', issued);
        await txn.put('risk:reservation:' + issued, token);
        // Bounded rolling journal: old candidates fail explicitly, never regain rank.
        const old = await txn.list({ prefix: 'risk:reservation:', limit: RESERVATIONS + 1 });
        for (const key of old.keys()) if (Number(key.slice('risk:reservation:'.length)) <= issued - RESERVATIONS) await txn.delete(key);
        return json({ ok: true, protocol: RISK_PROTOCOL, revision: issued, token });
      });
    }
    const current = await currentState(storage);
    if (request.method === 'GET' || request.method === 'HEAD') {
      const raw = current ? await readBody(storage, current.body) : await legacyBody(env);
      if (raw === null) return json({ error: 'private artifact awaiting sync' }, 503);
      return new Response(request.method === 'HEAD' ? null : raw, { headers: { ...headers,
        'Content-Length': String(raw.byteLength), ...(current ? { 'X-JH-Publication-Revision': String(current.revision), 'X-JH-Body-SHA256': current.body.sha256 } : {}) } });
    }
    if (request.method !== 'PUT') return json({ error: 'method not allowed' }, 405);
    const raw = await riskBody(request), doc = parseBody(raw), pub = doc.publication;
    if (pub === undefined) {
      if (current) fail('ordered_publication_required', 409);
      // Compatibility only until the native publisher is upgraded. This path
      // still has legacy KV semantics and is never advertised as ordered.
      await env.USER_DATA.put(KEY, new TextDecoder('utf-8', { fatal: true }).decode(raw));
      return json({ ok: true, publication_ordering: 'legacy_unverified' });
    }
    if (!object(pub) || pub.schema_version !== RISK_PROTOCOL || !revision(pub.revision) ||
        typeof pub.source_etag !== 'string' || !/^"[^"\r\n]{1,200}"$/.test(pub.source_etag) ||
        typeof pub.source_value_sha256 !== 'string' || !digestPattern.test(pub.source_value_sha256) ||
        !object(doc.snapshot_binding) || doc.snapshot_binding.encoding !== 'typed-json-binary64.v1' || doc.snapshot_binding.value_sha256 !== pub.source_value_sha256 ||
        typeof pub.started_at !== 'string' ||
        !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)$/.test(pub.started_at) || !Number.isFinite(Date.parse(pub.started_at))) fail('invalid_publication', 400);
    const token = request.headers.get('X-JH-Publication-Token'), declared = request.headers.get('X-JH-Body-SHA256');
    if (!tokenPattern.test(token ?? '') || !digestPattern.test(declared ?? '')) fail('publication_proof_required', 400);
    const sha256 = await riskDigest(raw);
    if (sha256 !== declared) fail('body_digest_mismatch', 400);
    if (current && pub.revision < current.revision) fail('superseded_publication', 409);
    if (current && pub.revision === current.revision) {
      if (token !== current.token || sha256 !== current.body.sha256) fail('revision_body_conflict', 409);
      await readBody(storage, current.body); // Retries prove retained bytes too.
      return json({ ok: true, protocol: RISK_PROTOCOL, revision: current.revision, body_sha256: sha256, status: 'unchanged' });
    }
    if (await storage.get('risk:reservation:' + pub.revision) !== token) fail('unknown_publication_reservation', 409);
    // Preserve the complete pre-migration KV body once, even if an in-flight
    // legacy Worker later writes to KV. No fallback if that body is malformed.
    const legacy = current ? null : await legacyBody(env);
    const legacySha = legacy ? await riskDigest(legacy) : null;
    if (current) await readBody(storage, current.body);
    return await storage.transaction(async txn => {
      const actual = await currentState(txn);
      if ((actual?.revision ?? 0) !== (current?.revision ?? 0)) fail('publication_changed', 409);
      if (await txn.get('risk:reservation:' + pub.revision) !== token) fail('unknown_publication_reservation', 409);
      if (!actual && legacy) {
        if (await txn.get('risk:legacy')) fail('legacy_retention_conflict');
        await txn.put('risk:legacy', await putBody(txn, 'risk:legacy:', legacy, legacySha));
      }
      const body = await putBody(txn, 'risk:body:' + pub.revision + ':', raw, sha256);
      await txn.put('risk:current', { revision: pub.revision, token, body });
      await txn.put('risk:governed', true);
      // Complete prior packets are retained by the native S3 publisher. The DO
      // stores one current packet and its immutable migration predecessor.
      if (actual) for (let i = 0; i < actual.body.chunks; i++) await txn.delete(actual.body.prefix + i);
      return json({ ok: true, protocol: RISK_PROTOCOL, revision: pub.revision, body_sha256: sha256, status: 'published' });
    });
  } catch (error) {
    if (error instanceof PublicationError) return json({ ok: false, error: error.message }, error.status);
    // Storage/provider exception bodies can contain private data. Never echo.
    return json({ ok: false, error: 'private_publication_unavailable' }, 503);
  }
}

// Called after the existing owner/service authorization boundary in index.js.
export async function routePortfolioPublication(request, env, cors) {
  if (!env.WORKSPACE_COORDINATOR) return new Response(JSON.stringify({ error: 'durable private storage unavailable' }), { status: 503, headers: { ...cors, ...headers } });
  const stub = env.WORKSPACE_COORDINATOR.get(env.WORKSPACE_COORDINATOR.idFromName(RISK_OBJECT));
  try {
    const response = await stub.fetch(new Request('https://portfolio-risk.internal/risk-publication' + new URL(request.url).search, request));
    const combined = new Headers(cors);
    for (const [key, value] of response.headers) combined.set(key, value);
    for (const [key, value] of Object.entries(headers)) combined.set(key, value);
    return new Response(response.body, { status: response.status, headers: combined });
  } catch { return new Response(JSON.stringify({ error: 'durable private storage unavailable' }), { status: 503, headers: { ...cors, ...headers } }); }
}
