// Private Durable Object operations. Called only through a Worker binding after
// authentication; none of these paths is exposed by the public Worker router.
const json = (body, status = 200) => new Response(JSON.stringify(body), {
  status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'private, no-store' },
});
const canonical = value => JSON.stringify(value && typeof value === 'object'
  ? Array.isArray(value) ? value.map(v => JSON.parse(canonical(v)))
    : Object.fromEntries(Object.keys(value).sort().map(k => [k, JSON.parse(canonical(value[k]))]))
  : value);

async function fetchJson(url, init = {}) {
  const response = await fetch(url, { ...init, signal: AbortSignal.timeout(15000) });
  let body; try { body = await response.json(); } catch (_) { body = null; }
  if (!response.ok || !body) throw new Error('upstream request failed (' + response.status + ')');
  return body;
}
function profileHeaders(env) {
  return { apikey: env.SUPABASE_SERVICE_KEY, Authorization: 'Bearer ' + env.SUPABASE_SERVICE_KEY,
    'Content-Type': 'application/json', Prefer: 'resolution=merge-duplicates,return=minimal' };
}
async function profile(env, uid) {
  const rows = await fetchJson(env.SUPABASE_URL + '/rest/v1/profiles?id=eq.' + encodeURIComponent(uid) + '&select=id,stripe_customer_id',
    { headers: profileHeaders(env) });
  if (!Array.isArray(rows) || rows.length > 1) throw new Error('invalid billing profile');
  return rows[0] || null;
}
async function persistProfile(env, body) {
  const r = await fetch(env.SUPABASE_URL + '/rest/v1/profiles?on_conflict=id', {
    method: 'POST', headers: profileHeaders(env), body: JSON.stringify(body), signal: AbortSignal.timeout(15000),
  });
  if (!r.ok) throw new Error('profile write failed (' + r.status + ')');
}
const stripe = (env, path) => fetchJson('https://api.stripe.com/v1' + path,
  { headers: { Authorization: 'Bearer ' + env.STRIPE_SECRET } });

// Bound each stored value, even when a migrated journal has thousands of
// entries. SQLite-backed DO values have a finite limit; the 10 MB request cap
// must never become one oversized storage.put. Called inside a transaction.
async function readDocument(storage, key) {
  const meta = await storage.get(key);
  if (meta?.storage_schema !== 'json_chunks_v1') return meta;
  const pieces = await Promise.all(Array.from({ length: meta.chunks }, (_, i) => storage.get(key + ':chunk:' + i)));
  if (pieces.some(s => typeof s !== 'string')) throw new Error('incomplete durable document');
  return JSON.parse(pieces.join(''));
}
async function putDocument(storage, key, document) {
  const encoded = JSON.stringify(document), chunks = Math.ceil(encoded.length / 16000);
  for (let i = 0; i < chunks; i++) await storage.put(key + ':chunk:' + i, encoded.slice(i * 16000, (i + 1) * 16000));
  await storage.put(key, { storage_schema: 'json_chunks_v1', chunks });
}

async function journal(storage, env, input) {
  if (!input.actor || !input.store) return json({ error: 'identity required' }, 400);
  let existing = await readDocument(storage, 'journal');
  if (!existing) {
    // Preserve legacy stores. Never delete the only copy while migrating.
    const raw = await env.USER_DATA.get('journal:' + input.store);
    const old = raw ? JSON.parse(raw) : { entries: [] };
    if (!Array.isArray(old.entries)) throw new Error('invalid legacy journal');
    let entries = old.entries;
    if (input.migrateOwnerLegacy && input.store !== 'khalid') {
      const legacy = await env.USER_DATA.get('journal:khalid');
      if (legacy) {
        const d = JSON.parse(legacy);
        if (!Array.isArray(d.entries)) throw new Error('invalid legacy owner journal');
        const ids = new Set(entries.map(e => e.id));
        entries = entries.concat(d.entries.filter(e => !ids.has(e.id)));
      }
    }
    existing = { entries: entries.map(e => ({ ...e, locked: true })), revision: 0,
      schema: 'journal.v2', migrated_at: new Date().toISOString() };
    await storage.transaction(async tx => {
      if (!await tx.get('journal')) {
        await putDocument(tx, 'journal:legacy:baseline', existing);
        await putDocument(tx, 'journal', existing);
      }
    });
  }
  if (input.method === 'GET') return json(existing);
  const b = input.body || {};
  if (!Array.isArray(b.entries)) return json({ error: 'entries must be an array' }, 400);
  return storage.transaction(async tx => {
    const current = await readDocument(tx, 'journal');
    if (!Number.isSafeInteger(b.baseRevision) || b.baseRevision !== current.revision)
      return json({ error: 'revision conflict', current }, 409);
    const previous = new Map(current.entries.map(e => [e.id, e]));
    const seen = new Set(); const additions = [];
    for (const e of b.entries) {
      if (!e || typeof e.id !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(e.id) || seen.has(e.id))
        return json({ error: 'invalid or duplicate entry id' }, 400);
      seen.add(e.id);
      if (previous.has(e.id)) {
        if (canonical(e) !== canonical(previous.get(e.id)))
          return json({ error: 'locked decisions cannot be edited; append a correction', entry_id: e.id }, 409);
        continue;
      }
      if (typeof e.thesis !== 'string' || !e.thesis.trim() || e.thesis.length > 20000)
        return json({ error: 'a decision or correction needs a thesis' }, 400);
      if (e.correction_of && !previous.has(e.correction_of)) return json({ error: 'unknown correction target' }, 400);
      if (!e.correction_of && (typeof e.ticker !== 'string' || !/^[A-Z0-9.^:_/-]{1,40}$/.test(e.ticker)
          || !['bullish', 'bearish', 'watch'].includes(e.direction)
          || !Number.isInteger(e.horizon_days) || e.horizon_days < 1 || e.horizon_days > 3650
          || !(e.entry_price === null || Number.isFinite(e.entry_price) && e.entry_price > 0)))
        return json({ error: 'invalid decision fields' }, 400);
      const now = Date.now();
      additions.push({ ...e, created: now, locked: true, entry_type: e.correction_of ? 'correction' : 'decision',
        price_verification: e.correction_of ? null : 'client_reported',
        _server: { actor_uid: input.actor.uid, actor_role: input.actor.role, received_at: new Date(now).toISOString(), revision: current.revision + 1 } });
    }
    if ([...previous.keys()].some(id => !seen.has(id))) return json({ error: 'locked decisions cannot be deleted; append a correction' }, 409);
    if (!additions.length) return json({ ok: true, ...current });
    const next = { ...current, entries: current.entries.concat(additions), revision: current.revision + 1, updated_at: new Date().toISOString() };
    await putDocument(tx, 'journal:event:' + String(next.revision).padStart(12, '0'), {
      revision: next.revision, prior_revision: current.revision, at: next.updated_at, actor: input.actor, additions,
    });
    await putDocument(tx, 'journal', next);
    return json({ ok: true, ...next });
  });
}

async function verifyCustomerOwner(env, customer, uid) {
  const record = await stripe(env, '/customers/' + encodeURIComponent(customer));
  if (record.id !== customer || record.deleted) throw new Error('invalid billing customer');
  const owner = record.metadata?.user_id;
  if (owner) {
    if (owner !== uid) throw new Error('billing customer owner mismatch');
    return;
  }
  // Legacy customers predate customer-level metadata. Require positive Stripe
  // subscription evidence, and reject ambiguous multi-account customer history.
  let cursor = null, proven = false;
  for (let page = 0; page < 20; page++) {
    const query = new URLSearchParams({ customer, status: 'all', limit: '100' });
    if (cursor) query.set('starting_after', cursor);
    const records = await stripe(env, '/subscriptions?' + query);
    if (!Array.isArray(records.data) || typeof records.has_more !== 'boolean') throw new Error('invalid customer ownership history');
    for (const sub of records.data) {
      if (sub.customer !== customer) throw new Error('invalid customer ownership history');
      const subOwner = sub.metadata?.user_id;
      if (subOwner && subOwner !== uid) throw new Error('ambiguous customer ownership');
      if (subOwner === uid) proven = true;
    }
    if (!records.has_more) {
      if (!proven) throw new Error('billing ownership unproven');
      return;
    }
    cursor = records.data.at(-1)?.id;
    if (!cursor) throw new Error('invalid customer ownership pagination');
  }
  throw new Error('customer ownership pagination incomplete');
}

async function customerFor(storage, env, input, create, allowMissing = false) {
  let binding = await storage.get('billing:customer');
  if (binding && binding.uid !== input.uid) throw new Error('billing owner mismatch');
  if (!binding) {
    const p = await profile(env, input.uid);
    if (p && p.stripe_customer_id) {
      if (!/^cus_[A-Za-z0-9]+$/.test(p.stripe_customer_id)) throw new Error('invalid customer id');
      await verifyCustomerOwner(env, p.stripe_customer_id, input.uid);
      binding = { uid: input.uid, customer: p.stripe_customer_id };
    }
  }
  if (!binding && create) {
    const form = new URLSearchParams({ 'metadata[user_id]': input.uid });
    if (input.email) form.set('email', input.email);
    const c = await fetchJson('https://api.stripe.com/v1/customers', { method: 'POST',
      headers: { Authorization: 'Bearer ' + env.STRIPE_SECRET, 'Content-Type': 'application/x-www-form-urlencoded',
        'Idempotency-Key': 'justhodl-customer-v1-' + input.uid }, body: form.toString() });
    if (!c.id || !String(c.id).startsWith('cus_')) throw new Error('invalid Stripe customer');
    binding = { uid: input.uid, customer: c.id };
  }
  if (!binding && allowMissing) return null;
  if (!binding || !String(binding.customer).startsWith('cus_')) throw new Error('unbound billing customer');
  if (input.customer && input.customer !== binding.customer) throw new Error('billing customer mismatch');
  await storage.put('billing:customer', binding);
  if (create) await persistProfile(env, { id: input.uid, stripe_customer_id: binding.customer });
  return binding.customer;
}

async function currentPlan(env, customer) {
  const map = JSON.parse(env.PRICE_PLAN_MAP || '{}');
  const rank = { free: 0, pro: 1, elite: 2, enterprise: 3 };
  let plan = 'free', cursor = null, subscriptions = [];
  for (let page = 0; page < 20; page++) {
    const q = new URLSearchParams({ customer, status: 'all', limit: '100' });
    if (cursor) q.set('starting_after', cursor);
    const d = await stripe(env, '/subscriptions?' + q);
    if (!Array.isArray(d.data) || typeof d.has_more !== 'boolean') throw new Error('invalid subscription list');
    for (const sub of d.data) {
      if (!sub || sub.customer !== customer || !['incomplete', 'incomplete_expired', 'trialing', 'active', 'past_due', 'canceled', 'unpaid', 'paused'].includes(sub.status)) throw new Error('invalid subscription owner/status');
      if (!['active', 'trialing'].includes(sub.status)) continue;
      if (!sub.items || !Array.isArray(sub.items.data) || sub.items.has_more) throw new Error('incomplete subscription prices');
      if (!sub.items.data.length) throw new Error('empty subscription prices');
      for (const item of sub.items.data) {
        const pid = item.price && item.price.id;
        if (!Object.hasOwn(map, pid) || !Object.hasOwn(rank, map[pid])) throw new Error('unmapped active subscription price');
        if (rank[map[pid]] > rank[plan]) plan = map[pid];
      }
      subscriptions.push(sub.id);
    }
    if (!d.has_more) return { plan, subscriptions };
    cursor = d.data.at(-1)?.id;
    if (!cursor) throw new Error('invalid subscription pagination');
  }
  throw new Error('subscription pagination incomplete');
}

async function billing(storage, env, input) {
  if (!input.uid || !env.STRIPE_SECRET || !env.SUPABASE_URL || !env.SUPABASE_SERVICE_KEY) throw new Error('billing configuration/identity missing');
  if (input.action === 'customer') return json({ customer: await customerFor(storage, env, input, true) });
  if (input.action === 'portal') return json({ customer: await customerFor(storage, env, input, false, true) });
  if (input.action === 'plan') {
    const cached = await storage.get('billing:projection');
    if (cached && Number.isFinite(cached.valid_until_ms) && cached.valid_until_ms > Date.now())
      return json({ plan: cached.plan, version: cached.version, valid_until_ms: cached.valid_until_ms, src: 'durable_stripe_projection' });
    // Lazy migration of existing subscribers uses their bound customer and
    // CURRENT Stripe state, never a profile/KV plan or editable auth metadata.
    const customer = await customerFor(storage, env, input, false, true);
    const latest = customer ? await currentPlan(env, customer) : { plan: 'free', subscriptions: [] };
    const projection = { uid: input.uid, customer, ...latest,
      version: ((await storage.get('billing:version')) || 0) + 1,
      at: new Date().toISOString(), valid_until_ms: Date.now() + 300000 };
    await storage.transaction(async tx => {
      await tx.put('billing:version', projection.version);
      await tx.put('billing:projection', projection);
    });
    return json({ plan: projection.plan, version: projection.version, valid_until_ms: projection.valid_until_ms, src: 'durable_stripe_projection' });
  }
  const id = input.event_id;
  if (typeof id !== 'string' || !/^evt_[A-Za-z0-9_-]{1,200}$/.test(id)) return json({ error: 'invalid event id' }, 400);
  if (await storage.get('billing:event:' + id)) return json({ ok: true, duplicate: true });
  const customer = await customerFor(storage, env, input, false);
  // Every delivery reconciles the current customer-wide subscription state.
  // Historical webhook status/plan/items are NEVER a fallback for Stripe errors.
  const latest = await currentPlan(env, customer);
  const prior = await storage.get('billing:outbox');
  const version = ((await storage.get('billing:version')) || 0) + 1;
  const projection = { uid: input.uid, customer, ...latest, version, at: new Date().toISOString(), valid_until_ms: Date.now() + 300000,
    event_ids: [...new Set([...(prior?.event_ids || []), id])] };
  await storage.transaction(async tx => {
    await tx.put('billing:version', version);
    await tx.put('billing:outbox', projection);
    await tx.put('billing:projection', projection);
  });
  // Supabase is an ADVISORY billing mirror. Authorization reads only the durable
  // projection above. A crash can leave an old external write in flight; it
  // cannot elevate access because no entitlement reader trusts this mirror.
  // Delivery is retried until the mirror catches up, without creating charges.
  await persistProfile(env, { id: input.uid, plan: latest.plan, stripe_customer_id: customer });
  if (env.USER_DATA) await env.USER_DATA.put('plan:' + input.uid, latest.plan, { expirationTtl: 3600 });
  await storage.transaction(async tx => {
    for (const eventId of projection.event_ids) await tx.put('billing:event:' + eventId, { version, at: projection.at });
    await tx.delete('billing:outbox');
  });
  return json({ ok: true, plan: latest.plan, version });
}

export async function handleAccountState(storage, env, request) {
  try {
    if (request.method !== 'POST') return json({ error: 'method not allowed' }, 405);
    const input = await request.json();
    const path = new URL(request.url).pathname;
    if (path === '/journal') return await journal(storage, env, input);
    if (path === '/billing') return await billing(storage, env, input);
    return json({ error: 'unknown account operation' }, 404);
  } catch (_) {
    // Do not echo upstream payloads, user content or secrets into public errors.
    return json({ error: 'account persistence or upstream unavailable; retry' }, 503);
  }
}
