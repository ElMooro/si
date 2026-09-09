// ops 5300 -- /ai/* bridge on justhodl-data-proxy: owner/service only, service token forwarded, control pointer resolved.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

const WORKER = path.join(__dirname, "..", "cloudflare", "workers", "justhodl-data-proxy", "src", "index.js");
const OWNER_UID = "11111111-2222-4333-8444-555555555555";
const OTHER_UID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const ADMIN = "svc_token_for_tests_0123456789";
const LAMBDA = "https://abcdefghijklmnopqrstuvwxyz012345.lambda-url.us-east-1.on.aws/";

function kvStore() {
  const m = new Map();
  return { async get(k) { return m.has(k) ? m.get(k) : null; }, async put(k, v) { m.set(k, String(v)); }, async delete(k) { m.delete(k); },
           async list() { return { keys: [], list_complete: true }; } };
}
function env(kv, extra) {
  return { USER_DATA: kv, SUPABASE_URL: "https://sb.test", SUPABASE_SERVICE_KEY: "service-role-key", ADMIN_TOKEN: ADMIN, OWNER_EMAILS: "raafouis@gmail.com",
           BRAIN_OWNER_STORE: "brain-930ffa48-60a1-4b11-8726-8848d1b827f9", ...(extra || {}) };
}
function stubFetch(state) {
  globalThis.fetch = async (input, init) => {
    const url = typeof input === "string" ? input : input.url;
    state.calls.push({ url, method: (init && init.method) || "GET", headers: (init && init.headers) || {}, body: init && init.body });
    if (url.endsWith("/auth/v1/user")) {
      const tok = String((init.headers || {}).Authorization || "").replace("Bearer ", "");
      const u = { owner_tok_000000000000: { id: OWNER_UID, email: "raafouis@gmail.com" }, other_tok_000000000000: { id: OTHER_UID, email: "x@example.com" } }[tok];
      return new Response(JSON.stringify(u || { error: "bad" }), { status: u ? 200 : 401 });
    }
    if (url.includes("/data/ai/control.json")) return new Response(JSON.stringify(state.control), { status: state.control ? 200 : 404 });
    if (url.startsWith(LAMBDA)) return new Response(JSON.stringify({ ok: true, echo: { path: new URL(url).pathname, body: init && init.body ? JSON.parse(init.body) : null } }), { status: 200 });
    throw new Error("unexpected fetch in test: " + url);
  };
}
async function worker() { return (await import(pathToFileURL(WORKER).href)).default; }
function req(p, opts) { opts = opts || {}; return new Request("https://justhodl-data-proxy.raafouis.workers.dev" + p, { method: opts.method || "GET", headers: opts.headers || {}, body: opts.body }); }
function fresh(control) { const state = { calls: [], control }; stubFetch(state); globalThis.__jhTokCache = new Map(); globalThis.__jhAiCtl = undefined; return state; }

test("/ai: anonymous is 401, a non-owner user is 403, nothing reaches the Lambda", async () => {
  const state = fresh({ function_url: LAMBDA });
  const w = await worker();
  const e = env(kvStore());
  let r = await w.fetch(req("/ai/inventory", { method: "POST", body: "{}" }), e, {});
  assert.equal(r.status, 401);
  r = await w.fetch(req("/ai/inventory", { method: "POST", body: "{}", headers: { Authorization: "Bearer other_tok_000000000000" } }), e, {});
  assert.equal(r.status, 403);
  assert.ok(!state.calls.some(c => c.url.startsWith(LAMBDA)), "lambda must not be called");
});

test("/ai: the verified owner is bridged to the Lambda with the service token and the action path", async () => {
  const state = fresh({ function_url: LAMBDA });
  const w = await worker();
  const r = await w.fetch(req("/ai/train/classifier?x=1", { method: "POST", body: JSON.stringify({ dataset_id: "d1" }), headers: { Authorization: "Bearer owner_tok_000000000000" } }), env(kvStore()), {});
  assert.equal(r.status, 200);
  const j = await r.json();
  assert.equal(j.echo.path, "/train/classifier");
  assert.equal(j.echo.body.dataset_id, "d1");
  const up = state.calls.find(c => c.url.startsWith(LAMBDA));
  assert.equal(up.headers["X-JH-Service-Token"], ADMIN);
  assert.ok(up.url.endsWith("/train/classifier?x=1"));
  assert.equal(r.headers.get("Cache-Control"), "private, no-store");
});

test("/ai: the service role may call it; an invalid control pointer or a bad action never leaks upstream", async () => {
  let state = fresh({ function_url: "https://evil.example/" });
  const w = await worker();
  let r = await w.fetch(req("/ai/status", { headers: { "X-JH-Service-Token": ADMIN } }), env(kvStore()), {});
  assert.equal(r.status, 503);
  assert.ok(!state.calls.some(c => c.url.startsWith("https://evil.example")));
  state = fresh({ function_url: LAMBDA });
  r = await w.fetch(req("/ai/Status.json", { headers: { "X-JH-Service-Token": ADMIN } }), env(kvStore()), {});
  assert.equal(r.status, 404);
  assert.ok(!state.calls.some(c => c.url.startsWith(LAMBDA)), "a malformed action never reaches the Lambda");
  r = await w.fetch(req("/ai/status", { headers: { "X-JH-Service-Token": ADMIN } }), env(kvStore()), {});
  assert.equal(r.status, 200);
  assert.equal((await r.json()).echo.path, "/status");
});
