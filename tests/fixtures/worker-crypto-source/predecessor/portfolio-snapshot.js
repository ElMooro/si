/* Snapshot representation preservation, not ordering or portfolio qualification.
 * Uses the reviewed bounded reader/strict parser; retains the complete UTF-8
 * representation in existing private KV. No new store, binding or schedule.
 */
import { riskBody, riskDocument, riskDigest } from './portfolio-publication.js';

export const SNAPSHOT_PROTOCOL = 'portfolio-snapshot-bytes.v1';
const KEY = 'private-artifact:portfolio-snapshot';
const MAX_IDENTITY_BYTES = 32 * 1024 * 1024;
const utf8 = new TextEncoder();

export function snapshotFrame(raw) {
  const document = riskDocument(raw);
  let size = 0;
  const add = count => {
    size += count;
    if (size > MAX_IDENTITY_BYTES) throw Error('snapshot_identity_bound');
  };
  const text = value => {
    // Count UTF-8 without constructing another whole encoded string. Do not
    // split an astral code point across chunks.
    let bytes = 0;
    for (let start = 0; start < value.length;) {
      let end = Math.min(start + 4096, value.length);
      const last = value.charCodeAt(end - 1);
      if (end < value.length && last >= 0xD800 && last <= 0xDBFF) end--;
      bytes += utf8.encode(value.slice(start, end)).byteLength;
      start = end;
    }
    add(2 + String(bytes).length + bytes);
  };
  const visit = value => {
    if (value === null || typeof value === 'boolean') add(1);
    else if (typeof value === 'number') {
      if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) throw Error('snapshot_number_unsafe');
      add(9);
    } else if (typeof value === 'string') text(value);
    else if (Array.isArray(value)) {
      add(2 + String(value.length).length);
      for (const item of value) visit(item);
    } else {
      const keys = Object.keys(value);
      add(2 + String(keys.length).length);
      for (const key of keys) { text(key); visit(value[key]); }
    }
  };
  visit(document);
  return { document, identityBytes: size };
}

export function snapshotIdentityBytes(raw) {
  return snapshotFrame(raw).identityBytes;
}

export async function publishSnapshot(request, env, cors) {
  const headers = { ...cors, 'Content-Type': 'application/json', 'Cache-Control': 'private, no-store', Vary: 'Authorization' };
  const reply = (document, status = 200) => new Response(JSON.stringify(document), { status, headers });
  let raw, identityBytes, sha256;
  try {
    raw = await riskBody(request);
    identityBytes = snapshotIdentityBytes(raw);
    sha256 = await riskDigest(raw);
    const expected = request.headers.get('X-JH-Body-SHA256');
    if (expected !== null && (!/^[a-f0-9]{64}$/.test(expected) || expected !== sha256)) return reply({ error: 'snapshot body digest mismatch' }, 400);
  } catch (error) {
    const status = [400, 408, 413].includes(error?.status) ? error.status : 400;
    return reply({ error: 'complete compatible snapshot required' }, status);
  }
  try {
    // Strict parsing above validates the complete original. Reserializing it
    // would erase negative zero and alter evidence retained by the producer.
    await env.USER_DATA.put(KEY, new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(raw));
  } catch {
    return reply({ error: 'snapshot publication unconfirmed' }, 503);
  }
  return reply({ ok: true, protocol: SNAPSHOT_PROTOCOL, body_bytes: raw.byteLength, body_sha256: sha256, identity_bytes: identityBytes });
}
