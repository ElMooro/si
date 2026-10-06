// 2026-10-06 — per-account chart store (/userstore/*) and the allowlisted /fmp read-through.
// Runs the real data-proxy module with an in-memory KV and a stubbed Supabase /auth/v1/user.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

const WORKER = path.join(__dirname, "..", "cloudflare", "workers", "justhodl-data-proxy", "src", "index.js");
const A = "11111111-2222-4333-8444-555555555555", B = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

function kv() {
  const m = new Map();
  return { _m: m, async get(k) { return m.has(k) ? m.get(k) : null; }, async put(k, v) { m.set(k, String(v)); }, async delete(k) { m.delete(k); },
    async list(o) { const p = (o && o.prefix) || ""; return { keys: [...m.keys()].filter(k => k.startsWith(p)).map(name => ({ name })), list_complete: true }; } };
}
function env(store) {
  return { USER_DATA: store, SUPABASE_URL: "https://sb.test", SUPABASE_SERVICE_KEY: "service-role-key", FMP_KEY: "fmpkey", OWNER_EMAILS: "raafouis@gmail.com" };
}
function install(fmpCalls) {
  const tokens = { tok_a_00000000000000000: { id: A, email: "a@example.com" }, tok_b_00000000000000000: { id: B, email: "b@example.com" } };
  globalThis.__jhTokCache = new Map();
  globalThis.caches = { default: { async match() { return null; }, async put() {} } };
  globalThis.fetch = async (input, init) => {
    const url = typeof input === "string" ? input : input.url;
    if (url.endsWith("/auth/v1/user")) {
      const t = String(((init && init.headers) || {}).Authorization || "").replace("Bearer ", "");
      const u = tokens[t];
      return new Response(JSON.stringify(u || { error: "bad" }), { status: u ? 200 : 401 });
    }
    if (url.startsWith("https://financialmodelingprep.com/stable/")) {
      fmpCalls.push(url);
      if (url.includes("/etf/holdings")) return new Response(JSON.stringify({ "Error Message": "Premium endpoint apikey=fmpkey" }), { status: 402 });
      return Response.json([{ date: "2025-09-27", revenue: 416161000000 }]);
    }
    throw new Error("unexpected fetch " + url);
  };
}
const w = async () => (await import(pathToFileURL(WORKER).href)).default;
const R = (p, o) => new Request("https://justhodl-data-proxy.raafouis.workers.dev" + p, Object.assign({ method: "GET" }, o || {}));
const ctx = { waitUntil() {} };

test("userstore requires a verified account and isolates accounts", async () => {
  const store = kv(), e = env(store), W = await w(); install([]);
  let r = await W.fetch(R("/userstore/chart-watchlist"), e, ctx);
  assert.equal(r.status, 401);
  r = await W.fetch(R("/userstore/chart-watchlist", { method: "PUT", headers: { Authorization: "Bearer tok_a_00000000000000000", "Content-Type": "application/json" }, body: JSON.stringify({ doc: { lists: { x: 1 } }, updated_at: 5 }) }), e, ctx);
  assert.equal(r.status, 200);
  assert.equal((await r.json()).updated_at, 5);
  r = await W.fetch(R("/userstore/chart-watchlist", { headers: { Authorization: "Bearer tok_a_00000000000000000" } }), e, ctx);
  const got = await r.json();
  assert.deepEqual(got.doc, { lists: { x: 1 } });
  assert.equal(r.headers.get("Cache-Control"), "private, no-store");
  r = await W.fetch(R("/userstore/chart-watchlist", { headers: { Authorization: "Bearer tok_b_00000000000000000" } }), e, ctx);
  assert.equal((await r.json()).doc, null, "another account never sees A's lists");
  assert.ok([...store._m.keys()].every(k => k.startsWith("userstore:" + A + ":")));
});

test("userstore validates kinds, ids, size and supports file delete", async () => {
  const store = kv(), e = env(store), W = await w(); install([]);
  const H = { Authorization: "Bearer tok_a_00000000000000000" };
  assert.equal((await W.fetch(R("/userstore/brain", { headers: H }), e, ctx)).status, 404);
  assert.equal((await W.fetch(R("/userstore/chart-file?id=../x", { headers: H }), e, ctx)).status, 400);
  let r = await W.fetch(R("/userstore/chart-file?id=FILE:GOLD", { method: "PUT", headers: H, body: JSON.stringify({ doc: { rows: [[1, 2]] } }) }), e, ctx);
  assert.equal(r.status, 200);
  assert.ok(store._m.has("userstore:" + A + ":chart-file:FILE:GOLD"));
  r = await W.fetch(R("/userstore/chart-file?id=FILE:GOLD", { method: "PUT", headers: H, body: JSON.stringify({ delete: true }) }), e, ctx);
  assert.equal(r.status, 200);
  assert.ok(!store._m.has("userstore:" + A + ":chart-file:FILE:GOLD"));
  r = await W.fetch(R("/userstore/chart-files", { method: "PUT", headers: H, body: JSON.stringify({ doc: "x".repeat(400000) }) }), e, ctx);
  assert.equal(r.status, 413);
  r = await W.fetch(R("/userstore/chart-files", { method: "PUT", headers: H, body: "[1,2]" }), e, ctx);
  assert.equal(r.status, 400);
});

test("fmp read-through only serves allowlisted endpoints and never echoes the key", async () => {
  const calls = [], e = env(kv()), W = await w(); install(calls);
  assert.equal((await W.fetch(R("/fmp?ep=stock-list&symbol=AAPL"), e, ctx)).status, 400);
  assert.equal((await W.fetch(R("/fmp?ep=profile&symbol=AAPL%26apikey%3Dx"), e, ctx)).status, 400);
  let r = await W.fetch(R("/fmp?ep=income-statement&symbol=aapl&period=quarter&limit=500"), e, ctx);
  assert.equal(r.status, 200);
  const d = await r.json();
  assert.equal(d.symbol, "AAPL");
  assert.equal(d.data[0].revenue, 416161000000);
  assert.match(calls[0], /period=quarter/); assert.match(calls[0], /limit=40/);
  r = await W.fetch(R("/fmp?ep=etf/holdings&symbol=SPY"), e, ctx);
  assert.equal(r.status, 424);
  const t = await r.text();
  assert.ok(!t.includes("fmpkey"), "provider key is redacted");
});
