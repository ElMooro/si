/**
 * fusion_api.js — JustHodl Intelligence Network read API v1 (Release 2, ops 5215).
 *
 * Read models (written by justhodl-jh-fusion / justhodl-jhsignal-bridge, S3 under data/):
 *   data/jh-fusion.json               fusion result per pilot entity x horizon (shadow mode)
 *   data/jhsignal/state/latest.json   current-state signal snapshot (JHSIGNAL-1.0 compact rows)
 *
 * Routes (all GET, JSON, CORS *, edge-cached 60s, X-JH-API: v1):
 *   /api/v1/fusion                                   screener rows (best horizon per entity) + regime + stats
 *   /api/v1/fusion/{entity}                          one entity; ?horizon=SWING narrows; ?full=1 keeps signal rows
 *   /api/v1/fusion/{entity}/changes                  what_changed + velocity + history per horizon
 *   /api/v1/signals/{entity}                         live signals for the entity; ?family=FLOW ?horizon=SWING
 *   /api/v1/regime/current                           regime axis + legs + critical dependencies
 *   /api/v1/opportunities                            ranked: ?min_fusion ?min_confidence ?min_independent
 *                                                    ?max_contradiction ?horizon ?direction=bullish|bearish ?limit
 *   /api/v1/health                                   read-model freshness
 * {entity} is a canonical id (equity:NVDA) or a bare ticker (NVDA, resolved against the read model).
 * Payloads are bounded by default: signal rows are dropped unless ?full=1 (phase 45: no unbounded payloads).
 *
 * v1.1 (audit 2026-09-08, section D): every route validates the read-model SCHEMA (503 + no-store on
 * empty/wrong-type documents), reports READINESS separately from liveness (available, schema_valid,
 * fresh, coherent, critical_inputs_ready, execution_eligible, per-document ages), EXPIRES capital labels
 * when the snapshot is stale or incoherent (capital_decision -> "EXPIRED", eligible:false), and
 * /opportunities returns actionable rows only unless ?actionable_only=0 (blocked rows carry eligible:false
 * and their veto reasons). A cache TTL never renews authorization age.
 */
const API_MINOR = "1.1";
// Freshness SLAs derived from the producers' cadence: the bridge is hourly (state), fusion runs on
// every batch event and at least daily (26h allows the daily fallback + one missed hour).
const FUSION_MAX_AGE_S = 26 * 3600;
const STATE_MAX_AGE_S = 3 * 3600;

function ageSeconds(iso) {
  const t = Date.parse(iso || "");
  if (!Number.isFinite(t)) return null;
  const a = (Date.now() - t) / 1000;
  return a < -300 ? null : Math.max(0, a);   // impossible future timestamps are invalid, not fresh
}

function validateFusion(f) {
  const reasons = [];
  if (!f || typeof f !== "object" || Array.isArray(f)) return { valid: false, reasons: ["fusion document missing or not an object"] };
  if (typeof f.run_id !== "string" || !f.run_id) reasons.push("fusion.run_id missing");
  if (ageSeconds(f.generated_at) === null) reasons.push("fusion.generated_at missing/unparseable/future");
  if (!f.regime || typeof f.regime !== "object" || typeof f.regime.label !== "string") reasons.push("fusion.regime.label missing");
  if (!f.entities || typeof f.entities !== "object" || Array.isArray(f.entities)) reasons.push("fusion.entities missing");
  else if (!Object.keys(f.entities).length) reasons.push("fusion.entities empty");
  if (!f.critical_dependencies || typeof f.critical_dependencies !== "object") reasons.push("fusion.critical_dependencies missing");
  return { valid: reasons.length === 0, reasons };
}

function validateState(s) {
  const reasons = [];
  if (!s || typeof s !== "object" || Array.isArray(s)) return { valid: false, reasons: ["state document missing or not an object"] };
  if (typeof s.run_id !== "string" || !s.run_id) reasons.push("state.run_id missing");
  if (ageSeconds(s.generated_at) === null) reasons.push("state.generated_at missing/unparseable/future");
  if (!s.entities || typeof s.entities !== "object" || Array.isArray(s.entities)) reasons.push("state.entities missing");
  if (!(Number(s.n_signals) > 0)) reasons.push("state.n_signals is zero");
  return { valid: reasons.length === 0, reasons };
}

// Readiness of the pair. `f`/`s` may be null. Never throws.
function readiness(f, s) {
  const vf = validateFusion(f), vs = validateState(s);
  const fAge = f ? ageSeconds(f.generated_at) : null, sAge = s ? ageSeconds(s.generated_at) : null;
  const reasons = [].concat(vf.reasons, vs.reasons);
  const fusionFresh = vf.valid && fAge !== null && fAge <= FUSION_MAX_AGE_S;
  const stateFresh = vs.valid && sAge !== null && sAge <= STATE_MAX_AGE_S;
  if (vf.valid && !fusionFresh) reasons.push(`fusion is ${Math.round(fAge / 3600)}h old (max ${FUSION_MAX_AGE_S / 3600}h)`);
  if (vs.valid && !stateFresh) reasons.push(`state is ${Math.round(sAge / 3600)}h old (max ${STATE_MAX_AGE_S / 3600}h)`);
  // coherence: the fusion result must have been computed from the state snapshot the readers now see.
  // Readers advance independently (hourly bridge), so a mismatch is reported and makes the pair not
  // execution-eligible, but the fusion document is still served as research with its own snapshot id.
  const coherent = vf.valid && vs.valid && !!f.snapshot_run_id && f.snapshot_run_id === s.run_id;
  if (vf.valid && vs.valid && !coherent) reasons.push(`fusion built from snapshot ${f.snapshot_run_id || "?"} but state is ${s.run_id}`);
  const deps = (f && f.critical_dependencies) || {};
  const criticalReady = vf.valid && !(deps.capital_blocked === true) && !((deps.failures || []).length);
  if (vf.valid && !criticalReady) reasons.push("critical dependency failures present -- capital decisions blocked");
  const available = !!f && !!s;
  const schemaValid = vf.valid && vs.valid;
  const fresh = fusionFresh && stateFresh;
  return {
    available, schema_valid: schemaValid, fresh, coherent, critical_inputs_ready: criticalReady,
    execution_eligible: schemaValid && fresh && coherent && criticalReady && !(f && f.shadow_mode),
    labels_valid: vf.valid && fusionFresh && coherent,          // capital labels may be shown as current
    ages_s: { fusion: fAge === null ? null : Math.round(fAge), state: sAge === null ? null : Math.round(sAge) },
    max_age_s: { fusion: FUSION_MAX_AGE_S, state: STATE_MAX_AGE_S },
    fusion_valid: vf.valid, state_valid: vs.valid, reasons,
  };
}

function unavailable(msg, rd) {
  return api({ error: msg, readiness: rd || null }, 503, { "Cache-Control": "no-store" });
}

const BUCKET_BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
const FUSION_KEY = "data/jh-fusion.json";
const STATE_KEY = "data/jhsignal/state/latest.json";
const API_VER = "v1";
const DOC_TTL = 120;   // seconds the worker keeps the S3 docs at the edge
const RESP_TTL = 60;   // browser/edge cache on API responses

function api(obj, status, extra) {
  return new Response(JSON.stringify(obj), {
    status: status || 200,
    headers: Object.assign({
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": status && status !== 200 ? "no-store" : `public, max-age=${RESP_TTL}, s-maxage=${RESP_TTL}`,
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, If-None-Match",
      "X-JH-API": API_VER,
      "X-JH-API-Minor": API_MINOR,
    }, extra || {}),
  });
}

async function readDoc(key, ctx) {
  const cache = caches.default;
  const ck = new Request(`https://jh-api.internal/__doc_${API_VER}__/${key}`, { method: "GET" });
  let hit = await cache.match(ck);
  if (hit) {
    try { return await hit.json(); } catch (e) { /* fall through to S3 */ }
  }
  const r = await fetch(`${BUCKET_BASE}/${key}`, { cf: { cacheTtl: DOC_TTL, cacheEverything: true } });
  if (!r.ok) return null;
  const text = await r.text();
  if (ctx && ctx.waitUntil) {
    ctx.waitUntil(cache.put(ck, new Response(text, { headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${DOC_TTL}` } })));
  }
  try { return JSON.parse(text); } catch (e) { return null; }
}

function resolveEntity(doc, raw) {
  const q = decodeURIComponent(raw || "").trim();
  if (!q) return null;
  const ents = doc.entities || {};
  if (ents[q]) return q;
  const up = q.toUpperCase().replace(/\./g, "-");
  if (ents[up]) return up;
  const bare = up.includes(":") ? up.split(":")[1] : up;
  for (const eid of Object.keys(ents)) {
    if (eid.split(":")[1] === bare) return eid;
  }
  return null;
}

function stripHorizon(h, full) {
  if (full) return h;
  const out = Object.assign({}, h);
  delete out.signals;
  out.top_supporting_evidence = (h.top_supporting_evidence || []).slice(0, 5).map(lite);
  out.top_opposing_evidence = (h.top_opposing_evidence || []).slice(0, 5).map(lite);
  return out;
}

function lite(r) {
  return {
    engine_id: r.engine_id, signal_type: r.signal_type, family: r.family, cluster: r.cluster, direction: r.direction,
    score: r.score, confidence: r.confidence, freshness: r.freshness, freshness_weight: r.freshness_weight,
    reliability_weight: r.reliability_weight, regime_fit: r.regime_fit, independence_weight: r.independence_weight,
    effective: r.effective, inherited: r.inherited, data_asof: r.data_asof, evidence: (r.evidence || []).slice(0, 3),
  };
}

function capitalLabel(h, labelsValid) {
  if (labelsValid) return h.capital_decision;
  return h.capital_decision ? "EXPIRED" : h.capital_decision;
}

function screenerRow(eid, e, labelsValid) {
  const bh = e.best_horizon;
  const h = (e.horizons || {})[bh] || {};
  return {
    entity_id: eid, entity_type: e.entity_type, ticker: e.ticker, best_horizon: bh,
    fusion_score: h.fusion_score, conviction: h.conviction, direction: h.direction, confidence: h.confidence,
    fusion_coverage: h.fusion_coverage, independent_evidence_count: h.independent_evidence_count,
    raw_signal_count: h.raw_signal_count, contradiction_score: h.contradiction_score, contradiction_class: h.contradiction_class,
    regime_fit: h.regime_fit, capital_decision: capitalLabel(h, labelsValid), capital_decision_at_run: h.capital_decision,
    eligible: labelsValid && h.capital_decision !== "BLOCKED", size_modifier: h.size_modifier,
    hard_vetoes: (h.hard_vetoes || []).length, soft_vetoes: (h.soft_vetoes || []).length,
    missing_families: h.missing_families || [], family_scores: h.family_scores || {},
    velocity: e.velocity ? { classification: e.velocity.classification, delta_1d: e.velocity.delta_1d, delta_5d: e.velocity.delta_5d } : null,
    horizons_available: Object.keys(e.horizons || {}), updated_at: e.updated_at,
  };
}

function num(v, d) {
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : d;
}

export async function handleFusionApi(request, env, ctx, url) {
  if (request.method === "OPTIONS") return api({ ok: true });
  if (request.method !== "GET" && request.method !== "HEAD") return api({ error: "method not allowed" }, 405);
  const parts = url.pathname.replace(/^\/api\/v1\/?/, "").split("/").filter(Boolean);
  const head = parts[0] || "";
  const p = url.searchParams;

  if (head === "health") {
    const [f, s] = await Promise.all([readDoc(FUSION_KEY, ctx), readDoc(STATE_KEY, ctx)]);
    const rd = readiness(f, s);
    // liveness (ok) = both documents exist and validate; readiness (ready) additionally needs fresh + coherent + critical inputs.
    const ok = rd.available && rd.schema_valid;
    return api({
      ok, ready: ok && rd.fresh && rd.coherent && rd.critical_inputs_ready, api: API_VER, api_minor: API_MINOR, readiness: rd,
      fusion: f && rd.fusion_valid ? { run_id: f.run_id, generated_at: f.generated_at, snapshot_run_id: f.snapshot_run_id, n_entities: (f.stats || {}).n_entities, shadow_mode: f.shadow_mode } : null,
      state: s && rd.state_valid ? { run_id: s.run_id, generated_at: s.generated_at, n_signals: s.n_signals, n_entities: s.n_entities, freshness_counts: s.freshness_counts } : null,
    }, ok ? 200 : 503, { "Cache-Control": "no-store" });
  }

  if (head === "regime") {
    const [f, s] = await Promise.all([readDoc(FUSION_KEY, ctx), readDoc(STATE_KEY, ctx)]);
    const rd = readiness(f, s);
    if (!rd.fusion_valid) return unavailable("fusion read model unavailable or invalid", rd);
    return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, regime: f.regime,
                 critical_dependencies: f.critical_dependencies, stats: f.stats, readiness: rd });
  }

  if (head === "fusion") {
    const [f, s] = await Promise.all([readDoc(FUSION_KEY, ctx), readDoc(STATE_KEY, ctx)]);
    const rd = readiness(f, s);
    if (!rd.fusion_valid) return unavailable("fusion read model unavailable or invalid", rd);
    const labelsValid = rd.labels_valid;
    if (parts.length === 1) {
      const rows = Object.entries(f.entities || {}).map(([eid, e]) => screenerRow(eid, e, labelsValid));
      return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, regime: { label: f.regime.label, score: f.regime.score, certainty: f.regime.certainty, n_legs: f.regime.n_legs },
                   critical_dependencies: f.critical_dependencies, stats: f.stats, readiness: rd, labels_valid: labelsValid, n: rows.length, rows, methodology: f.methodology },
                 200, labelsValid ? {} : { "Cache-Control": "no-store" });
    }
    const eid = resolveEntity(f, parts[1]);
    if (!eid) return api({ error: "unknown entity", query: parts[1], known: Object.keys(f.entities || {}) }, 404);
    const e = f.entities[eid];
    if (parts[2] === "changes") {
      const out = {};
      for (const [h, r] of Object.entries(e.horizons || {})) out[h] = { fusion_score: r.fusion_score, conviction: r.conviction, what_changed: r.what_changed || null };
      return api({ api: API_VER, entity_id: eid, generated_at: f.generated_at, run_id: f.run_id, best_horizon: e.best_horizon, velocity: e.velocity, history: e.history, horizons: out });
    }
    if (parts.length > 2) return api({ error: "unknown route" }, 404);
    const full = p.get("full") === "1";
    const hz = (p.get("horizon") || "").toUpperCase();
    const out = Object.assign({}, e, { horizons: {} });
    delete out.history;
    for (const [h, r] of Object.entries(e.horizons || {})) {
      if (hz && h !== hz) continue;
      const sh = stripHorizon(r, full);
      sh.capital_decision_at_run = r.capital_decision;
      sh.capital_decision = capitalLabel(r, labelsValid);
      sh.eligible = labelsValid && r.capital_decision !== "BLOCKED";
      out.horizons[h] = sh;
    }
    if (hz && !out.horizons[hz]) return api({ error: "no evidence for horizon", entity_id: eid, horizon: hz, available: Object.keys(e.horizons || {}) }, 404);
    return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, regime: { label: f.regime.label, score: f.regime.score }, readiness: rd, labels_valid: labelsValid, entity: out },
               200, labelsValid ? {} : { "Cache-Control": "no-store" });
  }

  if (head === "signals") {
    const s = await readDoc(STATE_KEY, ctx);
    const vs = validateState(s);
    if (!vs.valid) return api({ error: "signal state unavailable or invalid", reasons: vs.reasons }, 503, { "Cache-Control": "no-store" });
    const eid = resolveEntity(s, parts[1]);
    if (!eid) return api({ error: "unknown entity", query: parts[1] }, 404);
    const fam = (p.get("family") || "").toUpperCase();
    const hz = (p.get("horizon") || "").toUpperCase();
    let rows = s.entities[eid] || [];
    if (fam) rows = rows.filter((r) => r.family === fam);
    if (hz) rows = rows.filter((r) => r.horizon === hz);
    return api({ api: API_VER, entity_id: eid, generated_at: s.generated_at, run_id: s.run_id, n: rows.length, signals: rows });
  }

  if (head === "opportunities") {
    const [f, s] = await Promise.all([readDoc(FUSION_KEY, ctx), readDoc(STATE_KEY, ctx)]);
    const rd = readiness(f, s);
    if (!rd.fusion_valid) return unavailable("fusion read model unavailable or invalid", rd);
    const labelsValid = rd.labels_valid;
    const minF = num(p.get("min_fusion"), 0), minC = num(p.get("min_confidence"), 0), minI = num(p.get("min_independent"), 0);
    const maxX = num(p.get("max_contradiction"), 100), hz = (p.get("horizon") || "").toUpperCase(), dir = (p.get("direction") || "bullish").toLowerCase();
    const limit = Math.max(1, Math.min(200, num(p.get("limit"), 50) | 0));
    const actionableOnly = p.get("actionable_only") !== "0";
    const rows = []; let excluded = 0;
    for (const [eid, e] of Object.entries(f.entities || {})) {
      for (const [h, r] of Object.entries(e.horizons || {})) {
        if (hz && h !== hz) continue;
        const signed = dir === "bearish" ? -r.fusion_score : r.fusion_score;
        if (signed < Math.max(minF, 0.0001)) continue;
        if (r.confidence < minC || r.independent_evidence_count < minI || r.contradiction_score > maxX) continue;
        const blocked = r.capital_decision === "BLOCKED";
        const eligible = labelsValid && !blocked;
        if (actionableOnly && !eligible) { excluded++; continue; }
        const opportunity = eligible ? Math.abs(r.fusion_score) * r.confidence * (0.5 + 0.5 * r.fusion_coverage) * (1 - r.contradiction_score / 200) * r.size_modifier : 0;
        rows.push({ entity_id: eid, ticker: e.ticker, entity_type: e.entity_type, horizon: h, fusion_score: r.fusion_score, conviction: r.conviction, direction: r.direction,
                    confidence: r.confidence, fusion_coverage: r.fusion_coverage, independent_evidence_count: r.independent_evidence_count, contradiction_score: r.contradiction_score,
                    regime_fit: r.regime_fit, capital_decision: capitalLabel(r, labelsValid), capital_decision_at_run: r.capital_decision, eligible,
                    veto_reasons: (r.hard_vetoes || []).map(v => v && v.reason).filter(Boolean).slice(0, 5),
                    size_modifier: r.size_modifier, opportunity_rank_score: Math.round(opportunity * 10000) / 10000,
                    best_horizon: e.best_horizon === h, velocity: e.velocity ? e.velocity.classification : null });
      }
    }
    rows.sort((a, b) => b.opportunity_rank_score - a.opportunity_rank_score);
    return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, readiness: rd, labels_valid: labelsValid,
                 filters: { min_fusion: minF, min_confidence: minC, min_independent: minI, max_contradiction: maxX, horizon: hz || null, direction: dir, actionable_only: actionableOnly },
                 note: "opportunity_rank_score = |fusion| x confidence x (0.5+0.5 coverage) x (1-contradiction/200) x size_modifier; 0 and eligible:false when capital is BLOCKED or the snapshot's labels have EXPIRED. actionable_only=0 returns those rows for research. Release-6 adds historical edge and asymmetry; neither is fabricated here.",
                 n: rows.length, excluded_not_actionable: excluded, rows: rows.slice(0, limit) },
               200, labelsValid ? {} : { "Cache-Control": "no-store" });
  }

  return api({ error: "unknown route", routes: ["/api/v1/fusion", "/api/v1/fusion/{entity}", "/api/v1/fusion/{entity}/changes", "/api/v1/signals/{entity}", "/api/v1/regime/current", "/api/v1/opportunities", "/api/v1/health"] }, 404);
}
