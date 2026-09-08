// audit 2026-09-08 section D -- fusion read API readiness/validation tests.
// Runs handleFusionApi in Node with a stubbed caches.default and a scripted S3 fetch. No network.
const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

const MOD = path.join(__dirname, "..", "cloudflare", "workers", "justhodl-data-proxy", "src", "fusion_api.js");

function installDocs(docs) {
  globalThis.caches = { default: { async match() { return undefined; }, async put() {} } };
  globalThis.fetch = async (url) => {
    for (const [key, doc] of Object.entries(docs)) {
      if (String(url).endsWith(key)) {
        if (doc === null) return new Response("nope", { status: 404 });
        return new Response(typeof doc === "string" ? doc : JSON.stringify(doc), { status: 200 });
      }
    }
    return new Response("nope", { status: 404 });
  };
}

async function call(pathq) {
  const { handleFusionApi } = await import(pathToFileURL(MOD).href);
  const url = new URL("https://justhodl.ai" + pathq);
  const r = await handleFusionApi(new Request(url.toString()), {}, {}, url);
  return { status: r.status, headers: r.headers, body: await r.json() };
}

const NOW = new Date().toISOString();
function horizon(cap) {
  return { horizon: "SWING", fusion_score: 0.62, conviction: 62, direction: "bullish", confidence: 0.7, fusion_coverage: 0.8, independent_evidence_count: 3,
           raw_signal_count: 5, contradiction_score: 10, contradiction_class: "LOW", regime_fit: 0.6, capital_decision: cap, size_modifier: cap === "BLOCKED" ? 0 : 0.9,
           hard_vetoes: cap === "BLOCKED" ? [{ type: "HARD", reason: "risk-gate SEVERE" }] : [], soft_vetoes: [], missing_families: [], family_scores: {} };
}
function fusionDoc(over) {
  return Object.assign({ run_id: "fr_1", generated_at: NOW, snapshot_run_id: "st_1", shadow_mode: true,
    regime: { label: "MILDLY_SUPPORTIVE", score: 0.27, certainty: 0.6, n_legs: 5 },
    critical_dependencies: { failures: [], capital_blocked: false }, stats: { n_entities: 2 },
    entities: { "equity:NVDA": { entity_type: "equity", ticker: "NVDA", best_horizon: "SWING", horizons: { SWING: horizon("ALLOWED") }, updated_at: NOW },
                "equity:META": { entity_type: "equity", ticker: "META", best_horizon: "SWING", horizons: { SWING: horizon("BLOCKED") }, updated_at: NOW } } }, over || {});
}
function stateDoc(over) {
  return Object.assign({ run_id: "st_1", generated_at: NOW, n_signals: 120, n_entities: 2, freshness_counts: { FRESH: 120 },
    entities: { "equity:NVDA": [{ engine_id: "x", family: "FLOW", horizon: "SWING" }] } }, over || {});
}

test("D1: empty documents are not healthy -- health is 503 no-store and the list route does not throw", async () => {
  installDocs({ "data/jh-fusion.json": {}, "data/jhsignal/state/latest.json": {} });
  const h = await call("/api/v1/health");
  assert.equal(h.status, 503); assert.equal(h.body.ok, false);
  assert.equal(h.headers.get("Cache-Control"), "no-store");
  assert.ok(h.body.readiness.reasons.length >= 2);
  const l = await call("/api/v1/fusion");
  assert.equal(l.status, 503, "list route must not TypeError on {}");
  assert.equal(l.headers.get("Cache-Control"), "no-store");
});

test("D1/D3: ancient + incoherent snapshots pass liveness but are NOT ready; capital labels expire", async () => {
  installDocs({ "data/jh-fusion.json": fusionDoc({ generated_at: "2000-01-01T00:00:00Z", snapshot_run_id: "st_old" }),
                "data/jhsignal/state/latest.json": stateDoc({ generated_at: "2001-01-01T00:00:00Z", run_id: "st_now" }) });
  const h = await call("/api/v1/health");
  assert.equal(h.status, 200); assert.equal(h.body.ok, true); assert.equal(h.body.ready, false);
  assert.equal(h.body.readiness.fresh, false); assert.equal(h.body.readiness.coherent, false);
  assert.equal(h.body.readiness.execution_eligible, false);
  const l = await call("/api/v1/fusion");
  assert.equal(l.status, 200); assert.equal(l.body.labels_valid, false);
  const nvda = l.body.rows.find(r => r.ticker === "NVDA");
  assert.equal(nvda.capital_decision, "EXPIRED"); assert.equal(nvda.capital_decision_at_run, "ALLOWED"); assert.equal(nvda.eligible, false);
  assert.equal(l.headers.get("Cache-Control"), "no-store", "expired labels are never edge-cached");
  const o = await call("/api/v1/opportunities");
  assert.equal(o.body.n, 0, "nothing actionable when labels are expired");
  assert.equal(o.body.excluded_not_actionable, 2, "the ALLOWED row (label expired) and the BLOCKED row are both excluded");
});

test("D2/D3: a fresh coherent snapshot is ready; ALLOWED rows keep labels and shadow mode blocks execution eligibility", async () => {
  installDocs({ "data/jh-fusion.json": fusionDoc(), "data/jhsignal/state/latest.json": stateDoc() });
  const h = await call("/api/v1/health");
  assert.equal(h.status, 200); assert.equal(h.body.ready, true);
  assert.equal(h.body.readiness.execution_eligible, false, "shadow_mode true => not execution eligible");
  const e = await call("/api/v1/fusion/NVDA");
  assert.equal(e.status, 200); assert.equal(e.body.entity.horizons.SWING.capital_decision, "ALLOWED"); assert.equal(e.body.entity.horizons.SWING.eligible, true);
});

test("D4: BLOCKED rows are excluded from /opportunities by default and returned with eligible:false + veto reasons on request", async () => {
  installDocs({ "data/jh-fusion.json": fusionDoc(), "data/jhsignal/state/latest.json": stateDoc() });
  const o = await call("/api/v1/opportunities");
  assert.deepEqual(o.body.rows.map(r => r.ticker), ["NVDA"]);
  assert.equal(o.body.excluded_not_actionable, 1);
  const all = await call("/api/v1/opportunities?actionable_only=0");
  const meta = all.body.rows.find(r => r.ticker === "META");
  assert.equal(meta.eligible, false); assert.equal(meta.opportunity_rank_score, 0);
  assert.deepEqual(meta.veto_reasons, ["risk-gate SEVERE"]);
});

test("critical dependency failures make the pair not ready", async () => {
  installDocs({ "data/jh-fusion.json": fusionDoc({ critical_dependencies: { failures: [{ engine_id: "risk_gate" }], capital_blocked: true } }), "data/jhsignal/state/latest.json": stateDoc() });
  const h = await call("/api/v1/health");
  assert.equal(h.body.ready, false); assert.equal(h.body.readiness.critical_inputs_ready, false);
});

test("zero signals and future timestamps are invalid, not fresh", async () => {
  const future = new Date(Date.now() + 3600 * 1000 * 5).toISOString();
  installDocs({ "data/jh-fusion.json": fusionDoc({ generated_at: future }), "data/jhsignal/state/latest.json": stateDoc({ n_signals: 0 }) });
  const h = await call("/api/v1/health");
  assert.equal(h.status, 503);
  assert.ok(h.body.readiness.reasons.some(r => /future/.test(r)));
  assert.ok(h.body.readiness.reasons.some(r => /n_signals is zero/.test(r)));
});
