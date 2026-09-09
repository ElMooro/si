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
    if (url.includes("/rest/v1/profiles") && method === "GET") {
      const uid = new URL(url).searchParams.get('id').slice(3);
      return Response.json(state.noProfile ? [] : [{id: uid, stripe_customer_id: state.profileCustomer || (uid === OWNER_UID ? 'cus_owner' : 'cus_1'), plan:state.profilePlan||'free'}]);
    }
    if (url.includes("/rest/v1/profiles")) {
      state.profileWrites.push(JSON.parse(init.body));
      if(state.profileBarrier)await state.profileBarrier;
      return new Response("", { status: state.profileStatus || 201 });
    }
    if (url === 'https://api.stripe.com/v1/billing_portal/sessions') return Response.json({url:'https://billing.stripe.com/session-fixture'});
    if (url.startsWith("https://api.stripe.com/v1/checkout/sessions") && method === "POST") {
      state.stripeForm = new URLSearchParams(init.body);
      return new Response(JSON.stringify({ id: "cs_test_1", url: "https://checkout.stripe.com/x" }), { status: 200 });
    }
    if (url.startsWith("https://api.stripe.com/v1/subscriptions?")) {
      if(state.stripeFailure)return new Response('{}',{status:503});
      const customer=new URL(url).searchParams.get('customer');
      const data=state.subscriptions || (state.subscription?[state.subscription]:[]);
      return Response.json({data:data.map(s=>({...s,customer:s.customer||customer})),has_more:false});
    }
    if (url === 'https://api.stripe.com/v1/customers') return Response.json({id:'cus_new'});
    if (url.startsWith('https://api.stripe.com/v1/customers/')) { const id=url.split('/').at(-1);return Response.json({id,metadata:state.customerMetadata===undefined?{user_id:id==='cus_owner'?OWNER_UID:OTHER_UID}:state.customerMetadata}); }
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
  const env=baseEnv(kv), objects=new Map();
  env.WORKSPACE_COORDINATOR={idFromName(n){return n},get(name){return {async fetch(request){
    if(!objects.has(name)){
      const data=new Map();const storage={async get(k){return data.has(k)?structuredClone(data.get(k)):undefined},async put(k,v){data.set(k,structuredClone(v))},async delete(k){data.delete(k)},async transaction(fn){return fn(storage)}};
      const object={instance:null,data,storage};objects.set(name,object);
      object.ready=import(pathToFileURL(WORKER).href).then(({WorkspaceCoordinator})=>{object.instance=new WorkspaceCoordinator({storage},env)});
    }
    await objects.get(name).ready;
    return objects.get(name).instance.fetch(request);
  }}}};
  return { state, kv, env, objects };
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
  const pj = await w.fetch(req("/journal?uid=" + OWNER_UID, { method: "PUT", headers: H, body: JSON.stringify({ baseRevision:0, entries: [{ id: "j1",ticker:"SPY",direction:"watch",thesis:"Synthetic decision",horizon_days:30,entry_price:null }] }) }), env, {});
  assert.equal(pj.status, 200);
  const ownJournal=await w.fetch(req('/journal',{headers:H}),env,{});
  assert.equal((await ownJournal.json()).entries.length,1);
  assert.ok(!kv._m.has("journal:" + OWNER_UID));
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
  assert.ok(kv._m.has("journal:khalid"), "legacy source preserved after durable migration");
  assert.equal(d.revision,0);assert.ok(d.entries.every(e=>e.locked));
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
  assert.equal(f.get("customer"), "cus_1", "checkout uses the verified buyer customer binding");
  assert.ok(f.get("success_url").startsWith("https://justhodl.ai/"), "return host pinned: " + f.get("success_url"));
});

// ── INST-05: webhook ───────────────────────────────────────────────────────

function signed(payload, secret) {
  const t = Math.floor(Date.now() / 1000);
  const v1 = nodeCrypto.createHmac("sha256", secret).update(`${t}.${payload}`).digest("hex");
  return `t=${t},v1=${v1}`;
}

test("INST-05: failed profile persistence is retryable and no edge entitlement is cached", async () => {
  const { state, kv, env } = fresh();
  state.profileStatus = 503;
  state.subscription = { id: "sub_1", status: "active", items: { data: [{ price: { id: "price_pro_123" } }] } };
  const w = await worker();
  const payload = JSON.stringify({ id: "evt_1", type: "customer.subscription.updated", data: { object: { id: "sub_1", status: "active", customer:"cus_1", metadata: { user_id: OTHER_UID }, items: { data: [{ price: { id: "price_pro_123" } }] } } } });
  const r = await w.fetch(req("/stripe-webhook", { method: "POST", headers: { "stripe-signature": signed(payload, "whsec_test") }, body: payload }), env, {});
  assert.equal(r.status, 503);
  assert.ok(!kv._m.has("plan:" + OTHER_UID), "no plan cached after a failed durable write");
  assert.ok(!kv._m.has("stripe-evt:evt_1"), "event not marked processed");
});

test('private artifact aliases reject anonymous and unrelated users before consulting cache', async () => {
  const {env,kv}=fresh({'private-artifact:brain':JSON.stringify({notes:[{id:'fixture',text:'synthetic private text'}]})});
  const w=await worker();let cacheReads=0;
  globalThis.caches={default:{async match(){cacheReads++;throw new Error('private route consulted cache')}}};
  for(const p of ['/brain.json','/data/brain.json','/brain-history.json','/data/brain-history.json','/data/journal-graded.json','/private-artifact?kind=brain']){
    assert.equal((await w.fetch(req(p),env,{})).status,401,p);
    assert.equal((await w.fetch(req(p,{headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{})).status,403,p);
  }
  assert.equal(cacheReads,0);
  const r=await w.fetch(req('/data/brain.json',{headers:{Authorization:'Bearer owner_tok_000000000000'}}),env,{});
  assert.equal(r.status,200);assert.equal((await r.json()).notes.length,1);assert.match(r.headers.get('Cache-Control'),/no-store/);
  const denied=await w.fetch(req('/private-artifact?kind=brain',{method:'PUT',headers:{Authorization:'Bearer owner_tok_000000000000'},body:'{}'}),env,{});
  assert.equal(denied.status,403);
  const published=await w.fetch(req('/private-artifact?kind=brain-history',{method:'PUT',headers:{'X-JH-Service-Token':ADMIN},body:'{"history":[]}'}),env,{});
  assert.equal(published.status,200);assert.ok(kv._m.has('private-artifact:brain-history'));
});

test('Journal decisions survive delete/edit attempts, concurrent writes and durable object restart; corrections append', async () => {
  const {env,objects}=fresh();const w=await worker(),headers={Authorization:'Bearer other_tok_000000000000'};
  const entry=id=>({id,ticker:'SPY',direction:'watch',thesis:'Synthetic decision',horizon_days:30,entry_price:null,created:1});
  const write=body=>w.fetch(req('/journal',{method:'PUT',headers,body:JSON.stringify(body)}),env,{});
  let r=await write({entries:[entry('first')],baseRevision:0});assert.equal(r.status,200);let d=await r.json();
  assert.equal(d.revision,1);assert.notEqual(d.entries[0].created,1);assert.equal(d.entries[0]._server.actor_uid,OTHER_UID);
  assert.equal((await write({entries:[],baseRevision:1})).status,409);
  assert.equal((await write({entries:[{...d.entries[0],thesis:'retroactive revision'}],baseRevision:1})).status,409);
  const race=await Promise.all([write({entries:d.entries.concat(entry('second')),baseRevision:1}),write({entries:d.entries.concat(entry('third')),baseRevision:1})]);
  assert.deepEqual(race.map(x=>x.status).sort(),[200,409]);
  const obj=objects.get('journal:'+OTHER_UID);const {WorkspaceCoordinator}=await import(pathToFileURL(WORKER).href);obj.instance=new WorkspaceCoordinator({storage:obj.storage},env);
  d=await (await w.fetch(req('/journal',{headers}),env,{})).json();assert.equal(d.revision,2);assert.equal(d.entries.length,2);
  r=await write({entries:d.entries.concat({id:'correction',thesis:'A later clarification',correction_of:'first'}),baseRevision:2});
  assert.equal(r.status,200);d=await r.json();assert.equal(d.entries[0].thesis,'Synthetic decision');assert.equal(d.entries[2].entry_type,'correction');
  assert.ok(obj.data.has('journal:event:000000000003'));
});

function delivery(id='evt_residual',over={}){
  const payload=JSON.stringify({id,type:over.type||'customer.subscription.updated',data:{object:{id:'sub_historical',customer:'cus_1',status:'active',metadata:{user_id:OTHER_UID},items:{data:[{price:{id:'price_pro_123'}}]},...over.object}}});
  return req('/stripe-webhook',{method:'POST',headers:{'stripe-signature':signed(payload,'whsec_test')},body:payload});
}
test('billing does not consume historical status when Stripe is unavailable; later retry recovers', async()=>{
  const {env,state,kv,objects}=fresh();const w=await worker();state.stripeFailure=true;
  assert.equal((await w.fetch(delivery(),env,{})).status,503);assert.equal(state.profileWrites.length,0);assert.ok(!kv._m.has('plan:'+OTHER_UID));
  state.stripeFailure=false;state.subscription={id:'sub_current',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};
  assert.equal((await w.fetch(delivery(),env,{})).status,200);assert.equal(kv._m.get('plan:'+OTHER_UID),'pro');
  assert.ok(objects.get('billing:'+OTHER_UID).data.has('billing:event:evt_residual'));
});
test('billing duplicate deliveries are serialized across external awaits and durable restarts', async()=>{
  const {env,state,objects}=fresh();const w=await worker();state.subscription={id:'sub_current',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};
  let release;state.profileBarrier=new Promise(resolve=>release=resolve);
  const first=w.fetch(delivery(),env,{});
  for(let i=0;i<100&&state.profileWrites.length===0;i++)await new Promise(setImmediate);
  assert.equal(state.profileWrites.length,1);const second=w.fetch(delivery(),env,{});
  await new Promise(setImmediate);assert.equal(state.profileWrites.length,1,'second delivery cannot overtake a pending external write');
  release();const pair=await Promise.all([first,second]);assert.deepEqual(pair.map(r=>r.status),[200,200]);assert.equal(state.profileWrites.length,1);
  const obj=objects.get('billing:'+OTHER_UID);const {WorkspaceCoordinator}=await import(pathToFileURL(WORKER).href);obj.instance=new WorkspaceCoordinator({storage:obj.storage},env);
  const r=await w.fetch(delivery(),env,{});assert.equal((await r.json()).duplicate,true);assert.equal(state.profileWrites.length,1);
});
test('old subscription deletion retains active replacement; cancellation of final active subscription removes plan', async()=>{
  const {env,state,kv}=fresh();const w=await worker();state.subscriptions=[{id:'sub_historical',status:'canceled',items:{data:[]}},{id:'sub_replacement',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}}];
  assert.equal((await w.fetch(delivery('evt_delete',{type:'customer.subscription.deleted'}),env,{})).status,200);assert.equal(kv._m.get('plan:'+OTHER_UID),'pro');
  state.subscriptions[1].status='canceled';assert.equal((await w.fetch(delivery('evt_cancel'),env,{})).status,200);assert.equal(kv._m.get('plan:'+OTHER_UID),'free');
});
test('billing outbox survives persistence failure and retries current state; customer mismatch refuses writes', async()=>{
  const {env,state,objects,kv}=fresh();const w=await worker();state.subscription={id:'sub_current',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};state.profileStatus=503;
  assert.equal((await w.fetch(delivery(),env,{})).status,503);const obj=objects.get('billing:'+OTHER_UID);assert.ok(obj.data.has('billing:outbox'));assert.ok(!obj.data.has('billing:event:evt_residual'));
  state.subscription.status='canceled';state.profileStatus=201;assert.equal((await w.fetch(delivery(),env,{})).status,200);assert.equal(kv._m.get('plan:'+OTHER_UID),'free');assert.ok(!obj.data.has('billing:outbox'));
  const writes=state.profileWrites.length;assert.equal((await w.fetch(delivery('evt_wrong',{object:{customer:'cus_someone_else'}}),env,{})).status,503);assert.equal(state.profileWrites.length,writes);
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
  assert.equal(r2.status, 200); assert.equal((await r2.json()).duplicate, true);
  assert.equal(state.profileWrites.length, 1, "duplicate event performed no second write");
  // unmapped price -> refuse to guess, 500, no plan change
  state.subscription = { id: "sub_3", status: "active", items: { data: [{ price: { id: "price_unknown_777" } }] } };
  const p3 = JSON.stringify({ id: "evt_3", type: "customer.subscription.updated", data: { object: { id: "sub_3", status: "active", customer:"cus_owner", metadata: { user_id: OWNER_UID, plan: "enterprise" }, items: { data: [{ price: { id: "price_unknown_777" } }] } } } });
  const r3 = await w.fetch(req("/stripe-webhook", { method: "POST", headers: { "stripe-signature": signed(p3, "whsec_test") }, body: p3 }), env, {});
  assert.equal(r3.status, 503);
  assert.ok(!kv._m.has("plan:" + OWNER_UID));
  assert.ok(!kv._m.has("plan:" + OWNER_UID), "unmapped price leaves prior entitlement unchanged");
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
    assert.ok(!/const \w*[Kk]ey\s*=\s*"[A-Za-z0-9_-]{24,}"/.test(s), "hard-coded key constant in " + f);
  }
});

test('durable entitlement routes ignore forged KV/profile tiers and require user/service identity',async()=>{
  const {env,state,objects}=fresh({['plan:'+OTHER_UID]:'enterprise'});state.profilePlan='enterprise';const w=await worker();
  const self=()=>w.fetch(req('/plan/self',{headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{});
  assert.equal((await w.fetch(req('/plan/self'),env,{})).status,401);
  assert.equal((await w.fetch(req('/plan/service?uid='+OTHER_UID,{headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{})).status,401);
  let r=await self();assert.equal(r.status,200);let p=await r.json();assert.equal(p.plan,'free');assert.equal(p.src,'durable_stripe_projection');assert.equal(p.version,1);assert.match(r.headers.get('Cache-Control'),/no-store/);
  state.subscription={id:'sub_current',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};
  const obj=objects.get('billing:'+OTHER_UID);obj.data.get('billing:projection').valid_until_ms=Date.now()-1;
  r=await w.fetch(req('/plan/service?uid='+OTHER_UID,{headers:{'X-JH-Service-Token':ADMIN}}),env,{});p=await r.json();assert.equal(p.plan,'pro');assert.equal(p.version,2);
  obj.data.get('billing:projection').valid_until_ms=Date.now()-1;state.stripeFailure=true;assert.equal((await self()).status,503,'expired authority never falls back to prior pro/forged enterprise');
});
test('a valid durable entitlement survives advisory mirror failure; late stale mirror cannot elevate cancellation',async()=>{
  const {env,state,kv}=fresh();const w=await worker();state.subscription={id:'sub_current',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};state.profileStatus=503;
  assert.equal((await w.fetch(delivery('evt_mirror_failure'),env,{})).status,503);
  const self=()=>w.fetch(req('/plan/self',{headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{});
  let p=await (await self()).json();assert.equal(p.plan,'pro');assert.equal(p.version,1);
  state.subscription.status='canceled';state.profileStatus=201;assert.equal((await w.fetch(delivery('evt_cancellation'),env,{})).status,200);
  await kv.put('plan:'+OTHER_UID,'enterprise');state.profilePlan='enterprise';p=await (await self()).json();assert.equal(p.plan,'free');assert.equal(p.version,2);
});
test('existing subscribers migrate through current Stripe state; accounts without a customer start free',async()=>{
  let {env,state}=fresh();const w=await worker();state.subscription={id:'sub_existing',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};
  let r=await w.fetch(req('/plan/self',{headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{});assert.equal((await r.json()).plan,'pro');
  ({env,state}=fresh());state.noProfile=true;r=await w.fetch(req('/plan/self',{headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{});assert.equal((await r.json()).plan,'free');assert.equal(state.calls.filter(c=>c.url.startsWith('https://api.stripe.com')).length,0);
});
test('large legacy journal migrates losslessly in bounded durable chunks, including a service-first read',async()=>{
  const entries=Array.from({length:120},(_,i)=>({id:'legacy_'+i,thesis:'Synthetic '.repeat(2000)}));const raw=JSON.stringify({entries});assert.ok(raw.length>2000000);
  const {env,kv,objects}=fresh({'journal:khalid':raw,'owner:uids':JSON.stringify([OWNER_UID])});const w=await worker();
  const r=await w.fetch(req('/journal',{headers:{'X-JH-Service-Token':ADMIN}}),env,{});assert.equal(r.status,200);const d=await r.json();assert.equal(d.entries.length,120);assert.equal(d.entries[119].thesis,entries[119].thesis);assert.equal(d.revision,0);assert.equal(kv._m.get('journal:khalid'),raw);
  const obj=objects.get('journal:'+OWNER_UID);assert.ok(obj.data.has('journal:legacy:baseline'));for(const v of obj.data.values())assert.ok(Buffer.byteLength(JSON.stringify(v))<128000,'no oversized stored value');
});


test('legacy profile customer binding requires independent Stripe owner proof, including ambiguous-history rejection',async()=>{
  const w=await worker();const headers={Authorization:'Bearer other_tok_000000000000'};
  let {env,state,objects}=fresh();state.profileCustomer='cus_owner';state.subscription={id:'sub_owner',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};
  assert.equal((await w.fetch(req('/plan/self',{headers}),env,{})).status,503);assert.ok(!objects.get('billing:'+OTHER_UID).data.has('billing:customer'));
  ({env,state}=fresh());state.customerMetadata={};state.subscription={id:'sub_legacy',metadata:{user_id:OTHER_UID},status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};
  let r=await w.fetch(req('/plan/self',{headers}),env,{});assert.equal(r.status,200);assert.equal((await r.json()).plan,'pro');
  ({env,state}=fresh());state.customerMetadata={};state.subscription={id:'sub_unknown',status:'active',items:{data:[{price:{id:'price_pro_123'}}]}};
  assert.equal((await w.fetch(req('/plan/self',{headers}),env,{})).status,503);
  ({env,state}=fresh());state.customerMetadata={};state.subscriptions=[{id:'sub_self',metadata:{user_id:OTHER_UID}},{id:'sub_other',metadata:{user_id:OWNER_UID}}];
  assert.equal((await w.fetch(req('/plan/self',{headers}),env,{})).status,503);
});

test('every private corpus and archive alias is blocked before stale cache, including encoded/range/version attempts',async()=>{
  const {env}=fresh();const w=await worker();let cacheReads=0;globalThis.caches={default:{async match(){cacheReads++;return Response.json({private:'stale'})}}};
  const keys=['brain.json','brain-history.json','journal-graded.json','my-brief.json','devils-advocate.json','notes-index.json','notes-themes.json','playbook-rules.json','tradingview-notes.json','tv-sources.json','_telegram-chat.json','_askdesk/old.json','search/index/provider-search-old.sqlite.gz','equity-research-history/SPY/old.json',
    'portfolio/snapshot.json','portfolio/risk.json','portfolio/sizing.json','portfolio/catalysts.json','portfolio/holdings.json','portfolio/pm-history.json','portfolio-manager-brief.json','history/_fleet-monitor-history.jsonl',
    'risk-sizer.json','risk/recommendations.json','pm-decision.json','pm-decision-history.json','behavior-mirror.json','ai-brief.json','ai-brief.md',
    'history/behavior-mirror-history.json','portfolio/sizing-alert-history.json','portfolio/catalyst-alert-history.json','portfolio/risk-alert-history.json',
    'audit-private/20260909-originals/fixture.json','backtest/ledger/latest.json','backtest/ledger/versions/fixture.json','ai-commentary/history/portfolio/old.json',
    'history/archive/feed/data/ai-brief.json/old.json','history/archive/feed/ai-brief.json/old.json','history/archive/feed/portfolio/snapshot.json/old.json',
    'user-watchlist.json','vol-regime-private.json','user-trades.json','user-trades-stats.json','history/archive/feed/data/user-trades.json/old.json'];
  for(const key of keys)for(const prefix of ['/','/data/'])for(const method of ['GET','HEAD']){
    const r=await w.fetch(req(prefix+key+'?versionId=old',{method,headers:{Range:'bytes=0-12'}}),env,{});assert.ok([401,403].includes(r.status),prefix+key);assert.match(r.headers.get('Cache-Control'),/no-store/);
  }
  assert.equal((await w.fetch(req('/data/%62rain.json'),env,{})).status,400);assert.equal(cacheReads,0);
});
test('all dedicated owner account engines publish and read through authenticated no-store mirrors',async()=>{
  const {env}=fresh();const w=await worker();const keys={
    'portfolio/snapshot.json':'portfolio-snapshot','portfolio/risk.json':'portfolio-risk','portfolio/sizing.json':'portfolio-sizing','portfolio/catalysts.json':'portfolio-catalysts',
    'data/risk-sizer.json':'risk-sizer','risk/recommendations.json':'risk-sizer','data/pm-decision.json':'pm-decision','data/pm-decision-history.json':'pm-decision-history',
    'data/behavior-mirror.json':'behavior-mirror','data/ai-brief.json':'ai-brief','data/portfolio-manager-brief.json':'portfolio-manager-brief',
    'data/user-watchlist.json':'user-watchlist','data/vol-regime-private.json':'vol-regime-private','data/user-trades.json':'personal-trades','data/user-trades-stats.json':'personal-trades-stats',
    'portfolio/catalyst-alert-history.json':'portfolio-catalyst-history','portfolio/risk-alert-history.json':'portfolio-risk-history',
    'portfolio/sizing-alert-history.json':'portfolio-sizing-history','data/history/behavior-mirror-history.json':'behavior-mirror-history'};
  globalThis.caches={default:{async match(){throw new Error('private cache read')},async put(){throw new Error('private cache write')}}};
  for(const [key,kind] of Object.entries(keys)){
    const doc={engine:kind,account_fixture:{positions:[{ticker:'SYNTHETIC',qty:3}],nav:100}};
    assert.equal((await w.fetch(req('/private-artifact?kind='+kind,{method:'PUT',headers:{'X-JH-Service-Token':ADMIN},body:JSON.stringify(doc)}),env,{})).status,200);
    for(const suffix of ['?versionId=old','']){
      assert.equal((await w.fetch(req('/'+key+suffix),env,{})).status,401);
      assert.equal((await w.fetch(req('/'+key+suffix,{headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{})).status,403);
      const result=await w.fetch(req('/'+key+suffix,{headers:{Authorization:'Bearer owner_tok_000000000000'}}),env,{});
      assert.equal(result.status,200,key);assert.match(result.headers.get('Cache-Control'),/private, no-store/);assert.deepEqual(await result.json(),doc);
    }
    const head=await w.fetch(req('/'+key,{method:'HEAD',headers:{'X-JH-Service-Token':ADMIN}}),env,{});assert.equal(head.status,200);assert.equal(await head.text(),'');
  }
});
test('manual owner APIs authenticate all reads and mutations before fixed service forwarding',async()=>{
  const {env}=fresh();const w=await worker();const prior=globalThis.fetch,calls=[];
  globalThis.fetch=async(url,init)=>{
    if(String(url).includes('.lambda-url.')){calls.push({url,init});return Response.json({ok:true,watchlist:{version:1},trades:{version:1,trades:[]}});}
    return prior(url,init);
  };
  for(const endpoint of ['/owner-api/watchlist','/owner-api/trades'])for(const method of ['GET','POST']){
    const suffix=method==='POST'&&endpoint.endsWith('trades')?'/add':'';
    const body=method==='POST'?'{}':undefined;
    assert.equal((await w.fetch(req(endpoint+suffix,{method,body}),env,{})).status,401);
    assert.equal((await w.fetch(req(endpoint+suffix,{method,body,headers:{Authorization:'Bearer other_tok_000000000000'}}),env,{})).status,403);
  }
  assert.equal(calls.length,0);
  for(const endpoint of ['/owner-api/watchlist','/owner-api/watchlist/add','/owner-api/watchlist/remove','/owner-api/watchlist/replace','/owner-api/trades/add','/owner-api/trades/close','/owner-api/trades/update','/owner-api/trades/delete','/owner-api/trades/mtm']){
    const result=await w.fetch(req(endpoint,{method:'POST',body:'{"fixture":true}',headers:{Authorization:'Bearer owner_tok_000000000000','x-justhodl-token':'do-not-forward'}}),env,{});
    assert.equal(result.status,200,endpoint);assert.match(result.headers.get('Cache-Control'),/private, no-store/);
    const call=calls.at(-1);assert.equal(call.init.headers['X-JH-Service-Token'],ADMIN);assert.equal(call.init.headers.Authorization,undefined);assert.equal(call.init.headers['x-justhodl-token'],undefined);
    assert.equal(call.init.body,'{"fixture":true}');assert.equal(call.init.redirect,'error');assert.equal(call.init.cache,'no-store');
  }
  const before=calls.length;
  for(const endpoint of ['/owner-api/unknown','/owner-api/watchlist/delete','/owner-api/trades/replace'])assert.equal((await w.fetch(req(endpoint,{headers:{'X-JH-Service-Token':ADMIN}}),env,{})).status,404);
  assert.equal((await w.fetch(req('/owner-api/watchlist',{method:'POST',body:'x'.repeat(1000001),headers:{'X-JH-Service-Token':ADMIN}}),env,{})).status,413);assert.equal(calls.length,before);
  assert.equal((await w.fetch(req('/owner-api/trades',{headers:{'X-JH-Service-Token':ADMIN}}),env,{})).status,200);
});
test('sanitized public derivatives bypass old Worker and upstream cache generations',async()=>{
  const {env}=fresh();const w=await worker();let cacheReads=0,upstreamUrl,upstreamOptions;
  globalThis.caches={default:{async match(){cacheReads++;return Response.json({private:'stale'})},async put(){throw new Error('private-derived payload recached')}}};globalThis.fetch=async(url,opts)=>{upstreamUrl=String(url);upstreamOptions=opts;return Response.json({safe:true})};
  for(const key of ['brain-compiler.json','wealth-plan-snapshot.json','tax-plan-snapshot.json','source-map.json','etf-flows/daily.json','macro/regime.json','etf-flows/history/2026-09-09.json','macro/history/2026-09-09.json']){
    const r=await w.fetch(req('/data/'+key),env,{waitUntil(){}});assert.equal(r.status,200);assert.equal(cacheReads,0);assert.ok(upstreamUrl.endsWith('?audit_privacy=20260909'));assert.equal(upstreamOptions.cf.cacheTtl,0);assert.equal(upstreamOptions.cf.cacheEverything,false);assert.equal(r.headers.get('Cache-Control'),'no-store');
  }
});
test('AI proxy forwards authoritative entitlement and private-artifact auth without caching or synthesizing tiers',async()=>{
  const ai=(await import(pathToFileURL(path.join(__dirname,'..','cloudflare/workers/justhodl-ai-proxy/src/index.js')).href)).default;const calls=[];
  globalThis.fetch=async(url,init)=>{calls.push({url,init});return Response.json({error:'verified user required'},{status:401})};
  for(const route of ['/plan/self','/private-artifact?kind=brain','/owner-api/watchlist','/owner-api/trades']){
    const r=await ai.fetch(new Request('https://api.justhodl.ai'+route,{headers:{Origin:'https://justhodl.ai',Authorization:'Bearer fixture'}}),{},{});assert.equal(r.status,401);assert.match(r.headers.get('Cache-Control'),/no-store/);assert.equal(calls.at(-1).init.headers.Authorization,'Bearer fixture');assert.ok(calls.at(-1).url.endsWith(route));
  }
});


test('billing portal uses the durable verified customer binding and refuses cross-account profile claims',async()=>{
  const w=await worker();const headers={Authorization:'Bearer other_tok_000000000000'};
  let {env,state}=fresh();assert.equal((await w.fetch(req('/billing-portal',{method:'POST',headers}),env,{})).status,200);
  assert.equal(new URLSearchParams(state.calls.find(c=>c.url.includes('/billing_portal/')).body).get('customer'),'cus_1');
  ({env,state}=fresh());state.profileCustomer='cus_owner';assert.equal((await w.fetch(req('/billing-portal',{method:'POST',headers}),env,{})).status,503);assert.ok(!state.calls.some(c=>c.url.includes('/billing_portal/')));
  ({env,state}=fresh());state.noProfile=true;assert.equal((await w.fetch(req('/billing-portal',{method:'POST',headers}),env,{})).status,404);
});
test('personal Ask route authorizes owner before forwarding the service token; unrelated accounts are denied',async()=>{
  const {env}=fresh();const w=await worker();const priorFetch=globalThis.fetch;let upstream=0;
  globalThis.fetch=async(url,init)=>{if(String(url).includes('.lambda-url.')){upstream++;assert.equal(init.headers['X-JH-Service-Token'],ADMIN);return Response.json({answer:'synthetic private answer'})}return priorFetch(url,init)};
  for(const headers of [{},{Authorization:'Bearer other_tok_000000000000'}])assert.ok([401,403].includes((await w.fetch(req('/ask',{method:'POST',headers,body:'{"q":"fixture?"}'}),env,{})).status));assert.equal(upstream,0);
  const r=await w.fetch(req('/ask',{method:'POST',headers:{Authorization:'Bearer owner_tok_000000000000'},body:'{"q":"fixture?"}'}),env,{});assert.equal(r.status,200);assert.equal(upstream,1);assert.match(r.headers.get('Cache-Control'),/no-store/);
});
