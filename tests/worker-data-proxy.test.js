// audit 2026-09-08 Release A -- justhodl-data-proxy authorization/entitlement tests.
// Runs the REAL worker module in Node (>=22) with in-memory KV and stubbed
// Supabase/Stripe endpoints. No network. These are the audit's reproduced
// attacks (INST-01/02/04/05/06) asserted CLOSED, plus the legitimate paths.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const nodeCrypto = require("node:crypto");

const WORKER = path.join(__dirname, "..", "cloudflare", "workers", "justhodl-data-proxy", "src", "index.js");
const OWNER_STORE = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9";
const OWNER_UID = "11111111-2222-4333-8444-555555555555";
const OTHER_UID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const ADMIN = "svc_token_for_tests_0123456789";

function kvStore(seed) {
  const m = new Map(Object.entries(seed || {}));
  return {
    _m: m,
    async get(k) { return m.has(k) ? m.get(k) : null; },
    async put(k, v) { m.set(k, String(v)); },
    async delete(k) { m.delete(k); },
    async list(opts) {
      const prefix = (opts && opts.prefix) || "";
      const keys = [...m.keys()].filter(k => k.startsWith(prefix)).sort().map(name => ({ name }));
      return { keys, list_complete: true, cursor: undefined };
    },
  };
}

// Supabase + Stripe stub. `state` lets each test script the responses and inspect calls.
function installFetch(state) {
  globalThis.fetch = async (input, init) => {
    const url = typeof input === "string" ? input : input.url;
    const method = (init && init.method) || "GET";
    state.calls.push({ url, method, headers: (init && init.headers) || {}, body: init && init.body });
    if (url.endsWith("/auth/v1/user")) {
      const tok = String((init.headers || {}).Authorization || "").replace("Bearer ", "");
      const u = state.tokens[tok];
      return new Response(JSON.stringify(u || { error: "bad token" }), { status: u ? 200 : 401 });
    }
    if (url.includes("/auth/v1/admin/users/")) {
      const id = url.split("/auth/v1/admin/users/")[1];
      return new Response(JSON.stringify(state.accounts.has(id) ? { id } : { message: "not found" }), { status: state.accounts.has(id) ? 200 : 404 });
    }
    if (url.includes("/auth/v1/admin/users")) {
      return new Response(JSON.stringify({ users: [...state.accounts].map(id => ({ id, email: id === OWNER_UID ? "raafouis@gmail.com" : "x@example.com" })) }), { status: 200 });
    }
    if (url.includes("/rest/v1/profiles")) {
      state.profileWrites.push(JSON.parse(init.body));
      return new Response("", { status: state.profileStatus || 201 });
    }
    if (url.startsWith("https://api.stripe.com/v1/checkout/sessions") && method === "POST") {
      state.stripeForm = new URLSearchParams(init.body);
      return new Response(JSON.stringify({ id: "cs_test_1", url: "https://checkout.stripe.com/x" }), { status: 200 });
    }
    if (url.startsWith("https://api.stripe.com/v1/subscriptions/")) {
      return new Response(JSON.stringify(state.subscription || { error: "none" }), { status: state.subscription ? 200 : 404 });
    }
    if (url.includes("/line_items")) {
      return new Response(JSON.stringify(state.lineItems || { data: [] }), { status: 200 });
    }
    throw new Error("unexpected fetch in test: " + url);
  };
}

function baseEnv(kv) {
  return {
    USER_DATA: kv,
    SUPABASE_URL: "https://sb.test",
    SUPABASE_SERVICE_KEY: "service-role-key",
    ADMIN_TOKEN: ADMIN,
    OWNER_EMAILS: "raafouis@gmail.com",
    BRAIN_OWNER_STORE: OWNER_STORE,
    PRICE_PLAN_MAP: JSON.stringify({ price_pro_123: "pro" }),
    STRIPE_SECRET: "sk_test_x",
    STRIPE_WEBHOOK_SECRET: "whsec_test",
  };
}

async function worker() { return (await import(pathToFileURL(WORKER).href)).default; }

function req(pathq, opts) {
  opts = opts || {};
  return new Request("https://justhodl-data-proxy.raafouis.workers.dev" + pathq, {
    method: opts.method || "GET", headers: opts.headers || {}, body: opts.body,
  });
}

function fresh(seed) {
  const state = { calls: [], tokens: { owner_tok_000000000000: { id: OWNER_UID, email: "raafouis@gmail.com" }, other_tok_000000000000: { id: OTHER_UID, email: "someone@example.com" } },
                  accounts: new Set([OWNER_UID, OTHER_UID]), profileWrites: [], profileStatus: 201 };
  installFetch(state);
  globalThis.__jhTokCache = new Map();
  const kv = kvStore(seed);
  return { state, kv, env: baseEnv(kv) };
}

// ── INST-01: Brain / Journal / debug / purge ───────────────────────────────

test("INST-01: anonymous Brain and Journal reads/writes are refused (401), including the legacy fixed owner store id", async () => {
  const { kv, env } = fresh({ ["bcache:" + OWNER_STORE]: JSON.stringify([{ id: "n1", text: "private note about my portfolio" }]) });
  const w = await worker();
  for (const p of ["/brain?uid=" + OWNER_STORE, "/brain?uid=" + OWNER_UID, "/journal?uid=" + OWNER_UID, "/journal"]) {
    const g = await w.fetch(req(p), env, {});
    assert.equal(g.status, 401, "GET " + p);
    const pu = await w.fetch(req(p, { method: "PUT", body: JSON.stringify({ note: { id: "evil", text: "x".repeat(40) } }) }), env, {});
    assert.equal(pu.status, 401, "PUT " + p);
  }
  assert.ok(!kv._m.has("bnote:" + OWNER_STORE + ":evil"), "no write landed");
});

test("INST-01: /brain-debug and /brain-purge are role-gated; the retired literal token grants nothing", async () => {
  const { env } = fresh({ ["bidx:" + OWNER_STORE]: "[]" });
  const w = await worker();
  assert.equal((await w.fetch(req("/brain-debug"), env, {})).status, 401);
  assert.equal((await w.fetch(req("/brain-purge?uid=" + OWNER_STORE + "&token=jhpurge_9f48_2026"), env, {})).status, 401);
  assert.equal((await w.fetch(req("/brain?uid=" + OWNER_STORE + "&build=1&token=jhpurge_9f48_2026"), env, {})).status, 401);
  // a verified NON-owner user is forbidden from maintenance, not merely unauthenticated
  const r = await w.fetch(req("/brain-debug", { headers: { Authorization: "Bearer other_tok_000000000000" } }), env, {});
  assert.equal(r.status, 403);
});

test("INST-01: the verified owner reads and writes the legacy owner store regardless of ?uid=", async () => {
  const { kv, env } = fresh({ ["bcache:" + OWNER_STORE]: JSON.stringify([{ id: "n1", text: "private note about my portfolio" }]), ["bidx:" + OWNER_STORE]: JSON.stringify(["n1"]) });
  const w = await worker();
  const H = { Authorization: "Bearer owner_tok_000000000000" };
  const g = await w.fetch(req("/brain?uid=someone-elses-uid-000000", { headers: H }), env, {});
  assert.equal(g.status, 200);
  const d = await g.json();
  assert.equal(d.scope, "owner"); assert.equal(d.store, OWNER_STORE); assert.equal(d.notes.length, 1);
  const note = { id: "n2", text: "The long-term rule is to buy confirmed weekly double bottoms only after the war room clears risk." };
  const pu = await w.fetch(req("/brain", { method: "PUT", headers: H, body: JSON.stringify({ note }) }), env, {});
  assert.equal(pu.status, 200);
  assert.ok(kv._m.has("bnote:" + OWNER_STORE + ":n2"), "note landed in the owner store");
  // debug + purge now work for the owner without any query token
  assert.equal((await w.fetch(req("/brain-debug", { headers: H }), env, {})).status, 200);
});

test("INST-01: a verified non-owner user is scoped to their own uid store and cannot reach the owner's", async () => {
  const { kv, env } = fresh({ ["bcache:" + OWNER_STORE]: JSON.stringify([{ id: "n1", text: "owner secret" }]) });
  const w = await worker();
  const H = { Authorization: "Bearer other_tok_000000000000" };
  const g = await w.fetch(req("/brain?uid=" + OWNER_STORE, { headers: H }), env, {});
  assert.equal(g.status, 200);
  const d = await g.json();
  assert.equal(d.store, OTHER_UID); assert.equal(d.scope, "user"); assert.equal(d.notes.length, 0);
  const note = { id: "u1", text: "A perfectly ordinary personal note that is long enough to pass the junk guard." };
  await w.fetch(req("/brain?uid=" + OWNER_STORE, { method: "PUT", headers: H, body: JSON.stringify({ note }) }), env, {});
  assert.ok(kv._m.has("bnote:" + OTHER_UID + ":u1"));
  assert.ok(!kv._m.has("bnote:" + OWNER_STORE + ":u1"), "owner store untouched");
  // journal: same scoping, no PIN, no uid override
  const pj = await w.fetch(req("/journal?uid=" + OWNER_UID, { method: "PUT", headers: H, body: JSON.stringify({ entries: [{ id: "j1" }] }) }), env, {});
  assert.equal(pj.status, 200);
  assert.ok(kv._m.has("journal:" + OTHER_UID)); assert.ok(!kv._m.has("journal:" + OWNER_UID));
});

test("INST-01: service role (X-JH-Service-Token) may select a store explicitly; a wrong token is anonymous", async () => {
  const { env } = fresh({ ["bcache:" + OWNER_STORE]: JSON.stringify([{ id: "n1", text: "owner note" }]) });
  const w = await worker();
  const ok = await w.fetch(req("/brain?uid=" + OWNER_STORE, { headers: { "X-JH-Service-Token": ADMIN } }), env, {});
  assert.equal(ok.status, 200); assert.equal((await ok.json()).scope, "service");
  const bad = await w.fetch(req("/brain?uid=" + OWNER_STORE, { headers: { "X-JH-Service-Token": "svc_token_for_tests_0123456780" } }), env, {});
  assert.equal(bad.status, 401);
  const noTok = await w.fetch(req("/brain?uid=" + OWNER_STORE, { headers: { "X-JH-Service-Token": ADMIN } }), Object.assign({}, env, { ADMIN_TOKEN: "" }), {});
  assert.equal(noTok.status, 401, "unset ADMIN_TOKEN never matches");
});

test("INST-01: the owner's legacy 'khalid' journal is merged into the owner's uid store once", async () => {
  const { kv, env } = fresh({ "journal:khalid": JSON.stringify({ entries: [{ id: "old1" }, { id: "old2" }] }), ["journal:" + OWNER_UID]: JSON.stringify({ entries: [{ id: "old2" }, { id: "new1" }] }) });
  const w = await worker();
  const r = await w.fetch(req("/journal", { headers: { Authorization: "Bearer owner_tok_000000000000" } }), env, {});
  const d = await r.json();
  assert.deepEqual(d.entries.map(e => e.id).sort(), ["new1", "old1", "old2"]);
  assert.ok(!kv._m.has("journal:khalid"), "legacy key removed after merge");
});

// ── INST-02: userdata namespace ────────────────────────────────────────────

test("INST-02: an anonymous /userdata read never falls back to the authenticated u:<uid> namespace", async () => {
  const { env } = fresh({ ["u:" + OWNER_UID]: JSON.stringify({ favorites: ["NVDA", "TSM"] }), ["anon:dev-guest-1234"]: JSON.stringify({ favorites: ["SPY"] }) });
  const w = await worker();
  const leak = await w.fetch(req("/userdata/" + OWNER_UID), env, {});
  assert.equal(leak.status, 200);
  assert.deepEqual(await leak.json(), { empty: true });
  const guest = await w.fetch(req("/userdata/dev-guest-1234"), env, {});
  assert.deepEqual(await guest.json(), { favorites: ["SPY"] });
  const own = await w.fetch(req("/userdata/self", { headers: { Authorization: "Bearer owner_tok_000000000000" } }), env, {});
  assert.deepEqual(await own.json(), { favorites: ["NVDA", "TSM"] });
  const bad = await w.fetch(req("/userdata/" + OWNER_UID, { headers: { Authorization: "Bearer not_a_real_token_00000" } }), env, {});
  assert.equal(bad.status, 401);
});

test("INST-02: /admin/userdata-migrate moves only non-account legacy blobs and only for the service role", async () => {
  const { kv, env } = fresh({ ["u:" + OWNER_UID]: "{\"a\":1}", "u:legacy-guest-9999": "{\"b\":2}" });
  const w = await worker();
  assert.equal((await w.fetch(req("/admin/userdata-migrate", { method: "POST" }), env, {})).status, 401);
  const r = await w.fetch(req("/admin/userdata-migrate", { method: "POST", headers: { "X-JH-Service-Token": ADMIN } }), env, {});
  const d = await r.json();
  assert.equal(d.accounts, 1); assert.equal(d.migrated, 1);
  assert.equal(kv._m.get("anon:legacy-guest-9999"), "{\"b\":2}");
  assert.ok(!kv._m.has("anon:" + OWNER_UID), "real account blob never copied to the anonymous namespace");
});

// ── INST-04: checkout ──────────────────────────────────────────────────────

test("INST-04: checkout requires a verified user, an allowlisted price, and pins the return host", async () => {
  const { state, env } = fresh();
  const w = await worker();
  const anon = await w.fetch(req("/create-checkout", { method: "POST", body: JSON.stringify({ priceId: "price_pro_123", userId: OWNER_UID, plan: "enterprise" }) }), env, {});
  assert.equal(anon.status, 401);
  const H = { Authorization: "Bearer other_tok_000000000000" };
  const unknown = await w.fetch(req("/create-checkout", { method: "POST", headers: H, body: JSON.stringify({ priceId: "price_cheap_999", plan: "enterprise" }) }), env, {});
  assert.equal(unknown.status, 400);
  const ok = await w.fetch(req("/create-checkout", { method: "POST", headers: H, body: JSON.stringify({ priceId: "price_pro_123", plan: "enterprise", userId: OWNER_UID, email: "victim@x.com", returnUrl: "https://evil.example" }) }), env, {});
  assert.equal(ok.status, 200);
  const f = state.stripeForm;
  assert.equal(f.get("client_reference_id"), OTHER_UID, "buyer is the verified caller, not the body userId");
  assert.equal(f.get("metadata[user_id]"), OTHER_UID);
  assert.equal(f.get("metadata[plan]"), "pro", "plan comes from the server price map, not the body");
  assert.equal(f.get("customer_email"), "someone@example.com");
  assert.ok(f.get("success_url").startsWith("https://justhodl.ai/"), "return host pinned: " + f.get("success_url"));
});

// ── INST-05: webhook ───────────────────────────────────────────────────────

function signed(payload, secret) {
  const t = Math.floor(Date.now() / 1000);
  const v1 = nodeCrypto.createHmac("sha256", secret).update(`${t}.${payload}`).digest("hex");
  return `t=${t},v1=${v1}`;
}

test("INST-05: a failed profile persistence is NOT acknowledged (500 so Stripe retries) and no edge entitlement is cached", async () => {
  const { state, kv, env } = fresh();
  state.profileStatus = 503;
  state.subscription = { id: "sub_1", status: "active", items: { data: [{ price: { id: "price_pro_123" } }] } };
  const w = await worker();
  const payload = JSON.stringify({ id: "evt_1", type: "customer.subscription.updated", data: { object: { id: "sub_1", status: "active", metadata: { user_id: OTHER_UID }, items: { data: [{ price: { id: "price_pro_123" } }] } } } });
  const r = await w.fetch(req("/stripe-webhook", { method: "POST", headers: { "stripe-signature": signed(payload, "whsec_test") }, body: payload }), env, {});
  assert.equal(r.status, 500);
  assert.ok(!kv._m.has("plan:" + OTHER_UID), "no plan cached after a failed durable write");
  assert.ok(!kv._m.has("stripe-evt:evt_1"), "event not marked processed");
});

test("INST-05: entitlement derives from the live subscription items via the price map; duplicates are idempotent; unmapped prices never change plan", async () => {
  const { state, kv, env } = fresh();
  state.subscription = { id: "sub_2", status: "active", items: { data: [{ price: { id: "price_pro_123" } }] } };
  const w = await worker();
  // client-supplied metadata says enterprise; the subscription item says pro
  const payload = JSON.stringify({ id: "evt_2", type: "customer.subscription.updated", data: { object: { id: "sub_2", status: "active", customer: "cus_1", metadata: { user_id: OTHER_UID, plan: "enterprise" }, items: { data: [{ price: { id: "price_pro_123" } }] } } } });
  const sig = signed(payload, "whsec_test");
  const r1 = await w.fetch(req("/stripe-webhook", { method: "POST", headers: { "stripe-signature": sig }, body: payload }), env, {});
  assert.equal(r1.status, 200);
  assert.equal(kv._m.get("plan:" + OTHER_UID), "pro");
  assert.equal(state.profileWrites.length, 1);
  const r2 = await w.fetch(req("/stripe-webhook", { method: "POST", headers: { "stripe-signature": sig }, body: payload }), env, {});
  assert.equal(r2.status, 200); assert.equal(await r2.text(), "ok duplicate");
  assert.equal(state.profileWrites.length, 1, "duplicate event performed no second write");
  // unmapped price -> refuse to guess, 500, no plan change
  state.subscription = { id: "sub_3", status: "active", items: { data: [{ price: { id: "price_unknown_777" } }] } };
  const p3 = JSON.stringify({ id: "evt_3", type: "customer.subscription.updated", data: { object: { id: "sub_3", status: "active", metadata: { user_id: OWNER_UID, plan: "enterprise" }, items: { data: [{ price: { id: "price_unknown_777" } }] } } } });
  const r3 = await w.fetch(req("/stripe-webhook", { method: "POST", headers: { "stripe-signature": signed(p3, "whsec_test") }, body: p3 }), env, {});
  assert.equal(r3.status, 500);
  assert.ok(!kv._m.has("plan:" + OWNER_UID));
  assert.ok([...kv._m.keys()].some(k => k.startsWith("stripe-unmapped:")), "unmapped price recorded for the operator");
  // a bad signature is still rejected
  const r4 = await w.fetch(req("/stripe-webhook", { method: "POST", headers: { "stripe-signature": "t=1,v1=00" }, body: payload }), env, {});
  assert.equal(r4.status, 400);
});

// ── INST-06: no literal secrets ────────────────────────────────────────────

test("INST-06: no maintenance literal or provider key literal remains in the worker sources", () => {
  const fs = require("node:fs");
  const files = [WORKER, path.join(__dirname, "..", "cloudflare", "workers", "justhodl-data-proxy", "wrangler.toml")];
  for (const f of files) {
    const s = fs.readFileSync(f, "utf8");
    assert.ok(!/jhpurge_/.test(s), "purge literal in " + f);
    assert.ok(!/POLYGON_KEY\s*(=|\|\|)\s*"[A-Za-z0-9_]{20,}"/.test(s), "polygon literal (assignment or fallback) in " + f);
    assert.ok(!/FRED_KEY\s*(=|\|\|)\s*"[0-9a-f]{32}"/.test(s), "fred literal (assignment or fallback) in " + f);
    assert.ok(!/(apiKey|api_key|apikey)=[A-Za-z0-9_]{24,}/.test(s), "hard-coded provider key in a URL in " + f);
  }
});
