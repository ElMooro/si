// Full route regression tests use real producer field names, mocked S3 and no network.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const fs = require("node:fs");
const vm = require("node:vm");
const { pathToFileURL } = require("node:url");
const MOD = path.join(__dirname, "..", "cloudflare/workers/justhodl-data-proxy/src/fusion_api.js");
const NOW = new Date().toISOString();
const old = hours => new Date(Date.now() - hours * 3600000).toISOString();
function signal(id, engine, family = "RISK") {
  return { signal_id: id, engine_id: engine, signal_type: "fixture", family, horizon: "SWING", direction: "bullish",
    score: 0.62, confidence: 0.7, freshness: "FRESH", data_asof: NOW, freshness_ttl_seconds: 86400, evidence: [], cluster: engine, effective: 0.4 };
}
function horizon(cap, signals) {
  return { horizon: "SWING", fusion_score: 0.62, conviction: 62, direction: "bullish", confidence: 0.7, fusion_coverage: 0.8,
    independent_evidence_count: signals.length, raw_signal_count: signals.length, contradiction_score: 10, contradiction_class: "LOW", regime_fit: 0.6,
    capital_decision: cap, size_modifier: cap === "BLOCKED" ? 0 : 0.9, signals,
    hard_vetoes: cap === "BLOCKED" ? [{ type: "HARD", reason: "risk-gate SEVERE" }] : [], soft_vetoes: [], missing_families: [], family_scores: {},
    top_supporting_evidence: signals.slice(0, 2), top_opposing_evidence: [] };
}
function docs(shadow = false) {
  const market = [signal("risk", "risk_gate"), signal("crisis", "crisis_composite")];
  const nvda = [signal("nvda", "flow", "FLOW")], meta = [signal("meta", "flow", "FLOW")];
  return { "data/jh-fusion.json": { run_id: "fr_1", generated_at: NOW, snapshot_run_id: "st_1", snapshot_generated_at: NOW, shadow_mode: shadow,
    regime: { label: "MILDLY_SUPPORTIVE", score: 0.27, certainty: 0.6, n_legs: 2, legs: [] },
    critical_dependencies: { failures: [], capital_blocked: false }, stats: { n_entities: 2 },
    entities: { "equity:NVDA": { entity_type: "equity", ticker: "NVDA", best_horizon: "SWING", horizons: { SWING: horizon("OPEN", [...nvda, ...market]) }, updated_at: NOW },
      "equity:META": { entity_type: "equity", ticker: "META", best_horizon: "SWING", horizons: { SWING: horizon("BLOCKED", [...meta, ...market]) }, updated_at: NOW } } },
    "data/jhsignal/state/latest.json": { run_id: "st_1", generated_at: NOW, n_signals: 4, n_entities: 3, freshness_counts: { FRESH: 4, STALE: 0, EXPIRED: 0 },
      entities: { "market:US_EQUITY": market, "equity:NVDA": nvda, "equity:META": meta } } };
}
const F = d => d["data/jh-fusion.json"], S = d => d["data/jhsignal/state/latest.json"], H = d => F(d).entities["equity:NVDA"].horizons.SWING;
function installDocs(d) {
  globalThis.caches = { default: { async match() {}, async put() {} } };
  globalThis.fetch = async url => {
    const key = Object.keys(d).find(key => String(url).endsWith(key));
    if (!key || d[key] === null) return new Response("missing", { status: 404 });
    return new Response(typeof d[key] === "string" ? d[key] : JSON.stringify(d[key]));
  };
}
async function call(pathq) {
  const { handleFusionApi } = await import(pathToFileURL(MOD).href);
  const url = new URL("https://justhodl.ai" + pathq);
  const r = await handleFusionApi(new Request(url), {}, {}, url);
  return { status: r.status, headers: r.headers, body: await r.json() };
}
async function assertClosed() {
  const health = await call("/api/v1/health");
  assert.equal(health.body.ready, false);
  assert.equal(health.body.readiness.execution_eligible, false);
  for (const route of ["/api/v1/fusion", "/api/v1/fusion?full=1", "/api/v1/fusion/NVDA", "/api/v1/fusion/NVDA/changes", "/api/v1/regime/current", "/api/v1/opportunities", "/api/v1/opportunities?mode=research"]) {
    const r = await call(route);
    assert.ok([200, 503].includes(r.status), route);
    assert.equal(r.headers.get("Cache-Control"), "no-store");
    if (r.status === 200) {
      assert.equal(r.body.labels_valid, false, route);
      for (const h of Object.values(r.body.entity?.horizons || {})) assert.equal(h.eligible, false);
      if (route.startsWith("/api/v1/opportunities")) assert.equal(r.body.n, 0, route);
    }
  }
}

test("actual OPEN producer contract: fresh coherent pair permits only eligible execution rows", async () => {
  installDocs(docs());
  const h = await call("/api/v1/health");
  assert.equal(h.body.ready, true); assert.equal(h.body.readiness.execution_eligible, true);
  const o = await call("/api/v1/opportunities");
  assert.deepEqual(o.body.rows.map(r => r.ticker), ["NVDA"]); assert.equal(o.body.excluded_not_actionable, 1);
  assert.equal(o.body.rows[0].capital_decision, "OPEN"); assert.ok(o.body.rows[0].opportunity_rank_score > 0);
  const e = await call("/api/v1/fusion/NVDA");
  assert.equal(e.body.entity.horizons.SWING.eligible, true); assert.equal(e.body.entity.horizons.SWING.signals, undefined);
  const full = await call("/api/v1/fusion?full=1");
  assert.equal(full.body.entities["equity:NVDA"].horizons.SWING.signals.length, 3);
  assert.ok(full.body.readiness.labels_expire_at);
});

test("shadow research is explicit and distinct from execution on every projection", async () => {
  installDocs(docs(true));
  const health = await call("/api/v1/health");
  assert.equal(health.body.ready, true); assert.equal(health.body.readiness.execution_eligible, false);
  assert.equal((await call("/api/v1/opportunities")).body.n, 0);
  const o = await call("/api/v1/opportunities?mode=research");
  assert.equal(o.body.n, 1); assert.equal(o.body.rows[0].research_eligible, true); assert.equal(o.body.rows[0].eligible, false);
  assert.ok(o.body.rows[0].research_rank_score > 0); assert.equal(o.body.rows[0].opportunity_rank_score, 0);
  const full = await call("/api/v1/fusion?full=1");
  const h = full.body.entities["equity:NVDA"].horizons.SWING;
  assert.equal(h.capital_decision, "OPEN"); assert.equal(h.execution_eligible, false); assert.equal(h.research_eligible, true);
  assert.deepEqual(h, (await call("/api/v1/fusion/NVDA?full=1")).body.entity.horizons.SWING);
});

test("blocked rows require research inspection opt-in, retain reasons, and have zero rank", async () => {
  installDocs(docs(true));
  const all = await call("/api/v1/opportunities?mode=research&actionable_only=0");
  const r = all.body.rows.find(r => r.ticker === "META");
  assert.equal(r.research_eligible, false); assert.equal(r.eligible, false); assert.equal(r.rank_score, 0);
  assert.deepEqual(r.veto_reasons, ["risk-gate SEVERE"]);
});

for (const [label, mutate] of [
  ["empty documents", d => { d["data/jh-fusion.json"] = {}; d["data/jhsignal/state/latest.json"] = {}; }],
  ["null entity", d => { F(d).entities["equity:NVDA"] = null; }],
  ["null horizon", d => { F(d).entities["equity:NVDA"].horizons.SWING = null; }],
  ["missing best horizon", d => { F(d).entities["equity:NVDA"].best_horizon = "TACTICAL"; }],
  ["missing decision", d => { delete H(d).capital_decision; }],
  ["unrecognized decision", d => { H(d).capital_decision = "ANYTHING"; }],
  ["missing shadow flag", d => { delete F(d).shadow_mode; }],
  ["null evidence", d => { H(d).signals[0] = null; }],
  ["null supporting evidence", d => { H(d).top_supporting_evidence = [null]; }],
  ["invalid nested family scores", d => { H(d).family_scores = { FLOW: null }; }],
  ["invalid critical dependencies", d => { F(d).critical_dependencies = { failures: {}, capital_blocked: false }; }],
  ["invalid state row", d => { S(d).entities["equity:NVDA"] = {}; }],
  ["count mismatch", d => { S(d).n_signals = 300; }],
  ["future timestamps", d => { F(d).generated_at = new Date(Date.now() + 5 * 3600000).toISOString(); }],
  ["empty signal state", d => { S(d).entities = {}; S(d).n_entities = 0; S(d).n_signals = 0; S(d).freshness_counts = {}; }],
]) test(`malformed data fails closed without route exceptions: ${label}`, async () => { const d = docs(); mutate(d); installDocs(d); await assertClosed(); });

for (const [field, value] of [["fusion_score", 2], ["fusion_score", "0.5"], ["confidence", -1], ["confidence", null], ["fusion_coverage", 2],
  ["contradiction_score", 101], ["regime_fit", 2], ["size_modifier", 2], ["raw_signal_count", 1.5], ["independent_evidence_count", 100], ["conviction", Infinity]]) {
  test(`invalid metric ${field}=${value} is rejected`, async () => { const d = docs(); H(d)[field] = value; installDocs(d); assert.equal((await call("/api/v1/fusion")).status, 503); });
}

for (const [label, mutate] of [
  ["ancient fusion", d => { F(d).generated_at = "2000-01-01T00:00:00Z"; }],
  ["fresh fusion with stale state", d => { S(d).generated_at = old(4); F(d).snapshot_generated_at = S(d).generated_at; }],
  ["snapshot run mismatch", d => { S(d).run_id = "st_2"; }],
  ["same run with different state timestamp", d => { S(d).generated_at = old(1); }],
  ["critical failure but OPEN horizon", d => { F(d).critical_dependencies.failures = [{ engine_id: "risk_gate" }]; }],
  ["critical blocked flag but OPEN horizon", d => { F(d).critical_dependencies.capital_blocked = true; }],
  ["critical signal missing despite claimed ready", d => { S(d).entities["market:US_EQUITY"][0].engine_id = "noncritical"; }],
  ["all source observations stale under fresh wrapper", d => { for (const r of Object.values(S(d).entities).flat()) { r.data_asof = old(30); r.freshness = "STALE"; } S(d).freshness_counts = { FRESH: 0, STALE: 4 }; }],
  ["critical source crosses TTL without snapshot refresh", d => { S(d).entities["market:US_EQUITY"][0].data_asof = old(30); }],
  ["source expires under a fresh wrapper", d => { S(d).entities["equity:NVDA"][0].data_asof = old(72); }],
]) test(`all read views expire capital consistently: ${label}`, async () => { const d = docs(); mutate(d); installDocs(d); await assertClosed(); });

test("source reads report current TTL status without renewing snapshot labels", async () => {
  const d = docs(); S(d).entities["equity:NVDA"][0].data_asof = old(72); installDocs(d);
  const s = await call("/api/v1/signals/NVDA");
  assert.equal(s.body.signals[0].freshness, "EXPIRED"); assert.equal(s.body.signals[0].freshness_at_snapshot, "FRESH");
  assert.equal(s.body.labels_valid, false);
});

test("horizon evidence must belong to exact snapshot, not just reuse run id", async () => {
  const d = JSON.parse(JSON.stringify(docs())); H(d).signals[0].signal_id = "unknown"; installDocs(d);
  const e = await call("/api/v1/fusion/NVDA");
  assert.equal(e.body.entity.horizons.SWING.eligible, false); assert.equal(e.body.entity.horizons.SWING.capital_decision, "EXPIRED");
  assert.equal((await call("/api/v1/opportunities?mode=research")).body.n, 0);
});

test("hard veto and zero size cannot be overridden by an OPEN producer label", async () => {
  for (const modify of [h => { h.hard_vetoes = [{ reason: "must close" }]; }, h => { h.size_modifier = 0; }]) {
    const d = docs(); modify(H(d)); installDocs(d);
    const e = await call("/api/v1/fusion/NVDA");
    assert.equal(e.body.entity.horizons.SWING.capital_decision, "BLOCKED"); assert.equal(e.body.entity.horizons.SWING.research_eligible, false);
  }
});

test("legitimate pilot entity with no horizon evidence stays visible and ineligible", async () => {
  const d = docs(); F(d).entities["equity:EMPTY"] = { entity_type: "equity", best_horizon: null, horizons: {} }; installDocs(d);
  const f = await call("/api/v1/fusion");
  assert.equal(f.status, 200); assert.equal(f.body.rows.find(r => r.entity_id === "equity:EMPTY").capital_decision, "NO_EVIDENCE");
});

test("run pinning rejects snapshot advancement and unknown routes cannot silently resolve", async () => {
  installDocs(docs());
  assert.equal((await call("/api/v1/fusion/NVDA?run_id=fr_0")).status, 409);
  assert.equal((await call("/api/v1/fusion/NVDA?run_id=fr_1")).status, 200);
  assert.equal((await call("/api/v1/regime/fake")).status, 404);
  assert.equal((await call("/api/v1/fusion/%ZZ")).status, 404);
  assert.equal((await call("/api/v1/opportunities?mode=anything")).status, 400);
});

test("network failures produce unavailable responses rather than exceptions", async () => {
  installDocs({}); globalThis.fetch = async () => { throw new Error("offline"); };
  assert.equal((await call("/api/v1/health")).status, 503);
  assert.equal((await call("/api/v1/fusion")).status, 503);
});

async function renderDesk(d) {
  installDocs(d);
  const apiResult = await call("/api/v1/fusion?full=1");
  const nodes = new Map(), calls = [], timers = [];
  function node(key) { if (!nodes.has(key)) nodes.set(key, { textContent: "", innerHTML: "", value: "", className: "", addEventListener() {}, setAttribute() {},
    querySelector(sel) { return node(key + sel); }, querySelectorAll() { return []; } }); return nodes.get(key); }
  const html = fs.readFileSync(path.join(__dirname, "..", "fusion.html"), "utf8");
  const script = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m => m[1]).find(s => s.includes("var FEEDS"));
  const context = { document: { getElementById: node, querySelectorAll() { return []; }, addEventListener() {} },
    fetch: async url => { calls.push(url); return { ok: apiResult.status === 200, status: apiResult.status, async json() { return structuredClone(apiResult.body); } }; },
    Date, console: { warn() {} }, setTimeout(fn) { timers.push(fn); return 1; }, clearTimeout() {}, setInterval() {} };
  vm.runInNewContext(script, context);
  for (let i = 0; i < 10; i++) await new Promise(resolve => setImmediate(resolve));
  return { nodes, calls, timers };
}

test("desk uses one validated full snapshot, excludes blocked rows, and expires on validity deadline", async () => {
  const { nodes, calls, timers } = await renderDesk(docs(true));
  assert.equal(calls.length, 1); assert.match(calls[0], /^\/api\/v1\/fusion\?full=1&cb=/);
  assert.match(nodes.get("readiness").textContent, /RESEARCH READY/);
  assert.match(nodes.get("opps").innerHTML, /NVDA/); assert.doesNotMatch(nodes.get("opps").innerHTML, /META/);
  assert.match(nodes.get("snapshot-data").textContent, /"capital_decision_at_run": "OPEN"/);
  assert.match(nodes.get("opp-cnt").textContent, /execution disabled/);
  assert.equal(timers.length, 1); timers[0]();
  assert.match(nodes.get("readiness").textContent, /DATA HOLD/); assert.doesNotMatch(nodes.get("opps").innerHTML, /NVDA/);
});

test("desk stale state displays DATA HOLD and no actionable opportunities", async () => {
  const d = docs(true); S(d).generated_at = old(4); F(d).snapshot_generated_at = S(d).generated_at;
  const { nodes, calls } = await renderDesk(d);
  assert.equal(calls.length, 1); assert.match(nodes.get("readiness").textContent, /DATA HOLD/);
  assert.doesNotMatch(nodes.get("opps").innerHTML, /NVDA|META/);
});

test("contract accepts output of the real Python state and Fusion producers", async () => {
  const { execFileSync } = require("node:child_process");
  const program = String.raw`
import json, sys
from datetime import datetime, timezone
from pathlib import Path
root = Path.cwd()
sys.path.insert(0, str(root / "aws/shared"))
import jhsignal as J
from jh_state_store import build_snapshot
from jh_fusion_core import run_fusion, shadow_comparison
reg = json.loads((root / "aws/lambdas/justhodl-jh-fusion/source/engine-registry.v1.json").read_text())
uni = json.loads((root / "config/jh-fusion-universe.json").read_text())
now = datetime.now(timezone.utc)
signals = []
for engine, kind, entity, sym in [("risk_gate", "risk_posture", "market", "US_EQUITY"), ("crisis_composite", "systemic_stress", "market", "US_EQUITY"), ("momentum_leaders", "momentum", "equity", "NVDA")]:
    signals.append(J.make_signal(engine_id=engine, engine_version="1", entity_type=entity, symbol=sym, category="macro", signal_type=kind, score=0.6, confidence=0.8, data_asof=J.iso(now), horizon="SWING", now=now))
flags = {"FUSION_SHADOW_MODE": True}
state = build_snapshot(signals, registry_doc=reg, run_id="actual_state", now=now, adapter_reports=[], flags=flags)
fusion = run_fusion(state, registry_doc=reg, universe_doc=uni, flags=flags, now=now, run_id="actual_fusion")
shadow = shadow_comparison(fusion, state, now=now)
print(json.dumps({"data/jhsignal/state/latest.json":state, "data/jh-fusion.json":fusion, "data/jh-fusion/shadow.json":shadow}))
`;
  const d = JSON.parse(execFileSync("python3", ["-c", program], { cwd: path.join(__dirname, ".."), encoding: "utf8" }));
  installDocs(d);
  const full = await call("/api/v1/fusion?full=1");
  assert.equal(full.status, 200, JSON.stringify(full.body.readiness));
  assert.equal(full.body.readiness.schema_valid, true); assert.equal(full.body.readiness.research_ready, true);
  assert.equal(full.body.entities["equity:NVDA"].horizons.SWING.capital_decision, "OPEN");
  assert.ok(full.body.shadow_comparison);
  const { calls, nodes } = await renderDesk(d);
  assert.equal(calls.length, 1); assert.match(nodes.get("readiness").textContent, /RESEARCH READY/);
});
