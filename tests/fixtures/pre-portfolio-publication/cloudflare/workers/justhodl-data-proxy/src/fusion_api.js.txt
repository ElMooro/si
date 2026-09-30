/**
 * Intelligence Network read API. All capital labels are derived from one validated
 * fusion/state pair. `eligible` always means execution eligibility; shadow research
 * has separate research_eligible/research_rank_score fields and requires mode=research
 * on /opportunities. Raw producer decisions remain capital_decision_at_run.
 * The current producer only publishes state/latest.json: a run mismatch fails closed.
 * ?run_id=<fusion run> pins multi-request readers and returns 409 if it advances.
 * /fusion?full=1 returns the full annotated snapshot in one response for the desk.
 */
const API_MINOR = "1.2";
const API_VER = "v1";
const FUSION_MAX_AGE_S = 26 * 3600;
const STATE_MAX_AGE_S = 3 * 3600;
const DOC_TTL = 120;
const BUCKET_BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";
const FUSION_KEY = "data/jh-fusion.json";
const STATE_KEY = "data/jhsignal/state/latest.json";
const HORIZONS = new Set(["TACTICAL", "SWING", "INTERMEDIATE", "STRUCTURAL"]);
// OPEN is the v1 producer contract. ALLOWED is retained for old read models only.
const CAPITAL = new Set(["OPEN", "ALLOWED", "REDUCED", "BLOCKED"]);
const CRITICAL_ENGINES = ["risk_gate", "crisis_composite"];
const object = v => v !== null && typeof v === "object" && !Array.isArray(v);
const string = v => typeof v === "string" && v.length > 0;
const finite = (v, lo, hi) => typeof v === "number" && Number.isFinite(v) && v >= lo && v <= hi;
const count = v => Number.isSafeInteger(v) && v >= 0;
const arrayOf = (v, check) => Array.isArray(v) && v.every(check);

function ageSeconds(iso, now = Date.now()) {
  const t = typeof iso === "string" ? Date.parse(iso) : NaN;
  if (!Number.isFinite(t) || t > now + 300000) return null;
  return Math.max(0, (now - t) / 1000);
}
function sourceFreshness(r, now) {
  const age = ageSeconds(r.data_asof, now);
  if (age === null || !finite(r.freshness_ttl_seconds, 60, Number.MAX_SAFE_INTEGER)) return "INVALID";
  return age <= r.freshness_ttl_seconds ? "FRESH" : age <= 2 * r.freshness_ttl_seconds ? "STALE" : "EXPIRED";
}
function evidenceRow(r) {
  return object(r) && string(r.signal_id) && string(r.engine_id) && string(r.signal_type) && string(r.family)
    && string(r.direction) && finite(r.score, -1, 1) && finite(r.confidence, 0, 1)
    && ageSeconds(r.data_asof) !== null && ["FRESH", "STALE", "EXPIRED"].includes(r.freshness)
    && (r.evidence === undefined || arrayOf(r.evidence, object));
}
function validateHorizon(h, name) {
  if (!object(h) || h.horizon !== name || !HORIZONS.has(name) || !CAPITAL.has(h.capital_decision)) return false;
  const ranges = { fusion_score: [-1, 1], conviction: [0, 100], confidence: [0, 1], fusion_coverage: [0, 1],
    contradiction_score: [0, 100], regime_fit: [0, 1], size_modifier: [0, 1] };
  if (!Object.entries(ranges).every(([k, bounds]) => finite(h[k], ...bounds))) return false;
  if (!string(h.direction) || !count(h.raw_signal_count) || !count(h.independent_evidence_count)
      || h.independent_evidence_count > h.raw_signal_count) return false;
  if (!arrayOf(h.signals, evidenceRow) || h.signals.length !== h.raw_signal_count || new Set(h.signals.map(r => r.signal_id)).size !== h.signals.length) return false;
  if (!arrayOf(h.hard_vetoes, object) || !arrayOf(h.soft_vetoes, object) || !arrayOf(h.missing_families, string)) return false;
  for (const key of ["top_supporting_evidence", "top_opposing_evidence"]) {
    if (h[key] !== undefined && !arrayOf(h[key], evidenceRow)) return false;
  }
  for (const key of ["family_scores", "cluster_scores", "confidence_components", "contradiction_components", "attribution", "what_changed"]) {
    if (h[key] !== undefined && !object(h[key])) return false;
  }
  if (h.family_scores && !Object.values(h.family_scores).every(v => object(v) && finite(v.score, -1, 1) && count(v.n))) return false;
  if (h.what_changed) {
    for (const key of ["new_engines", "gone_engines"]) if (h.what_changed[key] !== undefined && !arrayOf(h.what_changed[key], string)) return false;
    if (h.what_changed.contributions !== undefined && !arrayOf(h.what_changed.contributions, v => object(v) && string(v.contributor) && finite(v.delta, -Infinity, Infinity))) return false;
  }
  return true;
}
function validateFusion(f, now) {
  const reasons = [];
  if (!object(f)) return { valid: false, reasons: ["fusion document missing or not an object"] };
  if (!string(f.run_id)) reasons.push("fusion.run_id missing");
  if (!string(f.snapshot_run_id)) reasons.push("fusion.snapshot_run_id missing");
  if (typeof f.shadow_mode !== "boolean") reasons.push("fusion.shadow_mode must be explicit");
  if (ageSeconds(f.generated_at, now) === null) reasons.push("fusion.generated_at missing/unparseable/future");
  if (ageSeconds(f.snapshot_generated_at, now) === null) reasons.push("fusion.snapshot_generated_at missing/unparseable/future");
  if (!object(f.regime) || !string(f.regime.label) || !finite(f.regime.score, -1, 1) || !finite(f.regime.certainty, 0, 1)) reasons.push("fusion.regime invalid");
  if (f.regime && f.regime.legs !== undefined && !arrayOf(f.regime.legs, r => object(r) && string(r.engine_id) && finite(r.score, -1, 1) && finite(r.confidence, 0, 1))) reasons.push("fusion.regime.legs invalid");
  if (!object(f.critical_dependencies) || typeof f.critical_dependencies.capital_blocked !== "boolean" || !arrayOf(f.critical_dependencies.failures, object)) reasons.push("fusion.critical_dependencies invalid");
  if (!object(f.entities) || !Object.keys(f.entities).length) reasons.push("fusion.entities missing or empty");
  else for (const [eid, e] of Object.entries(f.entities)) {
    if (!object(e) || !string(e.entity_type) || !object(e.horizons)) { reasons.push(`fusion entity ${eid} invalid`); continue; }
    const keys = Object.keys(e.horizons);
    // A pilot asset can legitimately have no evidence; it must not obtain a capital label.
    if (keys.length ? !keys.includes(e.best_horizon) : e.best_horizon !== null) reasons.push(`fusion entity ${eid} best_horizon invalid`);
    for (const [name, h] of Object.entries(e.horizons)) if (!validateHorizon(h, name)) reasons.push(`fusion entity ${eid} horizon ${name} invalid`);
    if (e.velocity !== undefined && (!object(e.velocity) || !string(e.velocity.classification))) reasons.push(`fusion entity ${eid} velocity invalid`);
  }
  return { valid: !reasons.length, reasons };
}
function validateState(s, now) {
  const reasons = [];
  if (!object(s)) return { valid: false, reasons: ["state document missing or not an object"] };
  if (!string(s.run_id)) reasons.push("state.run_id missing");
  if (ageSeconds(s.generated_at, now) === null) reasons.push("state.generated_at missing/unparseable/future");
  if (!count(s.n_signals) || s.n_signals === 0) reasons.push("state.n_signals is zero or invalid");
  if (!count(s.n_entities)) reasons.push("state.n_entities invalid");
  let n = 0;
  const ids = new Set(), counts = { FRESH: 0, STALE: 0, EXPIRED: 0 };
  if (!object(s.entities)) reasons.push("state.entities missing");
  else {
    if (Object.keys(s.entities).length !== s.n_entities) reasons.push("state entity count mismatch");
    for (const [eid, rows] of Object.entries(s.entities)) {
      if (!Array.isArray(rows)) { reasons.push(`state entity ${eid} is not a signal array`); continue; }
      n += rows.length;
      for (const r of rows) {
        if (!evidenceRow(r) || !HORIZONS.has(r.horizon) || !Number.isSafeInteger(r.freshness_ttl_seconds) || r.freshness_ttl_seconds < 60) {
          reasons.push(`state entity ${eid} contains an invalid signal`); continue;
        }
        if (ids.has(r.signal_id)) reasons.push("state contains duplicate signal ids");
        ids.add(r.signal_id); counts[r.freshness]++;
      }
    }
    if (n !== s.n_signals) reasons.push("state signal count mismatch");
  }
  // EXPIRED includes discarded rows in the producer; only active counts reconcile to entities.
  if (!object(s.freshness_counts) || !["FRESH", "STALE"].every(k => count(s.freshness_counts[k] ?? 0) && (s.freshness_counts[k] ?? 0) === counts[k])) reasons.push("state freshness counts invalid or inconsistent");
  return { valid: !reasons.length, reasons };
}
function readiness(f, s, now) {
  const vf = validateFusion(f, now), vs = validateState(s, now);
  const fAge = f ? ageSeconds(f.generated_at, now) : null, sAge = s ? ageSeconds(s.generated_at, now) : null;
  const reasons = [...vf.reasons, ...vs.reasons];
  const fusionFresh = vf.valid && fAge <= FUSION_MAX_AGE_S, stateFresh = vs.valid && sAge <= STATE_MAX_AGE_S;
  if (vf.valid && !fusionFresh) reasons.push("fusion snapshot expired");
  if (vs.valid && !stateFresh) reasons.push("signal state snapshot expired");
  const sourceIndex = new Map(vs.valid ? Object.values(s.entities).flat().map(r => [r.signal_id, r]) : []);
  const evidenceBound = vf.valid && vs.valid && Object.values(f.entities).every(e => Object.values(e.horizons).every(h =>
    h.signals.every(r => sameEvidence(r, sourceIndex.get(r.signal_id), h.horizon))));
  const coherent = vf.valid && vs.valid && evidenceBound && f.snapshot_run_id === s.run_id
    && Date.parse(f.snapshot_generated_at) === Date.parse(s.generated_at) && Date.parse(f.generated_at) >= Date.parse(s.generated_at);
  if (vf.valid && vs.valid && !coherent) reasons.push("fusion/state run, timestamp or evidence mismatch; await a coherent publication");
  const signals = vs.valid ? Object.values(s.entities).flat() : [];
  const sourceCounts = { FRESH: 0, STALE: 0, EXPIRED: 0, INVALID: 0 };
  for (const r of signals) sourceCounts[sourceFreshness(r, now)]++;
  const sourceReady = vs.valid && sourceCounts.FRESH > 0 && signals.every(r => sourceFreshness(r, now) === r.freshness && r.freshness !== "EXPIRED");
  if (vs.valid && !sourceReady) reasons.push("source signal TTLs expired or changed since the snapshot, or no fresh signals remain");
  const market = vs.valid ? (s.entities["market:US_EQUITY"] || []) : [];
  const criticalReady = vf.valid && vs.valid && f.critical_dependencies.capital_blocked === false && f.critical_dependencies.failures.length === 0
    && CRITICAL_ENGINES.every(id => market.some(r => r.engine_id === id && sourceFreshness(r, now) === "FRESH"));
  if (!criticalReady) reasons.push("critical dependency failures or missing/stale critical source signals");
  const schemaValid = vf.valid && vs.valid, fresh = fusionFresh && stateFresh;
  const labelsValid = schemaValid && fresh && coherent && criticalReady && sourceReady;
  const deadlines = [Date.parse(f?.generated_at) + FUSION_MAX_AGE_S * 1000, Date.parse(s?.generated_at) + STATE_MAX_AGE_S * 1000];
  for (const r of signals) deadlines.push(Date.parse(r.data_asof) + r.freshness_ttl_seconds * (r.freshness === "FRESH" ? 1 : 2) * 1000);
  return { available: !!f && !!s, schema_valid: schemaValid, fresh, coherent, critical_inputs_ready: criticalReady,
    sources_ready: sourceReady, source_freshness_counts: sourceCounts, execution_eligible: labelsValid && f.shadow_mode === false,
    research_ready: labelsValid, labels_valid: labelsValid, fusion_valid: vf.valid, state_valid: vs.valid,
    evaluated_at: new Date(now).toISOString(), labels_expire_at: labelsValid ? new Date(deadlines.reduce((min, t) => Math.min(min, t), Infinity)).toISOString() : null,
    ages_s: { fusion: fAge === null ? null : Math.round(fAge), state: sAge === null ? null : Math.round(sAge) },
    max_age_s: { fusion: FUSION_MAX_AGE_S, state: STATE_MAX_AGE_S }, reasons };
}
function api(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: {
    "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store",
    "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, If-None-Match", "X-JH-API": API_VER, "X-JH-API-Minor": API_MINOR,
  } });
}
async function readDoc(key, ctx) {
  try {
    const cache = caches.default;
    const ck = new Request(`https://jh-api.internal/__doc_${API_VER}__/${key}`);
    const hit = await cache.match(ck);
    if (hit) { try { return await hit.json(); } catch (_) { /* reload malformed cache */ } }
    const r = await fetch(`${BUCKET_BASE}/${key}`, { cf: { cacheTtl: DOC_TTL, cacheEverything: true } });
    if (!r.ok) return null;
    const body = await r.text(), doc = JSON.parse(body);
    if (ctx?.waitUntil) ctx.waitUntil(cache.put(ck, new Response(body, { headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${DOC_TTL}` } })).catch(() => {}));
    return doc;
  } catch (_) { return null; }
}
function resolveEntity(doc, raw) {
  let q;
  try { q = decodeURIComponent(raw || "").trim(); } catch (_) { return null; }
  if (Object.hasOwn(doc.entities, q)) return q;
  const up = q.toUpperCase().replace(/\./g, "-");
  if (Object.hasOwn(doc.entities, up)) return up;
  const bare = up.includes(":") ? up.split(":")[1] : up;
  const matches = Object.keys(doc.entities).filter(eid => eid.split(":")[1] === bare);
  return matches.length === 1 ? matches[0] : null;
}
function sameEvidence(r, source, horizon) {
  return !!source && ["engine_id", "signal_type", "data_asof", "score", "confidence", "freshness"].every(k => source[k] === r[k]) && source.horizon === horizon;
}
function decision(h, rd, sourceIndex) {
  const reasons = [...rd.reasons];
  const sources = h.signals.map(r => sourceIndex.get(r.signal_id));
  const bound = h.signals.every((r, i) => sameEvidence(r, sources[i], h.horizon));
  const hasFresh = sources.some(r => r && sourceFreshness(r, Date.parse(rd.evaluated_at)) === "FRESH");
  if (!bound) reasons.push("horizon evidence is not bound to the selected state snapshot");
  if (!h.raw_signal_count || !hasFresh) reasons.push("horizon has no fresh source evidence");
  const labelsValid = rd.labels_valid && bound && h.raw_signal_count > 0 && hasFresh;
  const blocked = h.capital_decision === "BLOCKED" || h.hard_vetoes.length > 0 || h.size_modifier <= 0;
  if (blocked) reasons.push("capital blocked by producer decision, hard veto or zero size");
  const hasWeight = h.confidence > 0 && h.fusion_coverage > 0 && h.independent_evidence_count > 0;
  if (!hasWeight) reasons.push("insufficient weighted evidence");
  const research = labelsValid && !blocked && hasWeight;
  const execution = research && rd.execution_eligible;
  if (research && !execution) reasons.push("shadow research only; execution disabled");
  const rank = research ? Math.round(Math.abs(h.fusion_score) * h.confidence * (0.5 + 0.5 * h.fusion_coverage) * (1 - h.contradiction_score / 200) * h.size_modifier * 10000) / 10000 : 0;
  return { capital_decision: labelsValid ? (blocked ? "BLOCKED" : h.capital_decision) : "EXPIRED", capital_decision_at_run: h.capital_decision,
    labels_valid: labelsValid, eligible: execution, execution_eligible: execution, research_eligible: research,
    opportunity_rank_score: execution ? rank : 0, research_rank_score: rank, eligibility_reasons: reasons };
}
function annotatedEntity(e, rd, sourceIndex, full) {
  const out = { ...e, horizons: {} };
  if (!full) delete out.history;
  for (const [hz, h] of Object.entries(e.horizons)) {
    const row = { ...h, ...decision(h, rd, sourceIndex) };
    if (!full) delete row.signals;
    out.horizons[hz] = row;
  }
  return out;
}
function screenerRow(eid, e) {
  const h = e.horizons[e.best_horizon];
  const common = { entity_id: eid, entity_type: e.entity_type, ticker: e.ticker, best_horizon: e.best_horizon,
    velocity: e.velocity || null, horizons_available: Object.keys(e.horizons), updated_at: e.updated_at };
  if (!h) return { ...common, capital_decision: "NO_EVIDENCE", eligible: false, execution_eligible: false,
    research_eligible: false, labels_valid: false, eligibility_reasons: ["no horizon evidence"] };
  const keys = ["fusion_score", "conviction", "direction", "confidence", "fusion_coverage", "independent_evidence_count",
    "raw_signal_count", "contradiction_score", "contradiction_class", "regime_fit", "capital_decision", "capital_decision_at_run",
    "eligible", "execution_eligible", "research_eligible", "labels_valid", "eligibility_reasons", "opportunity_rank_score", "research_rank_score",
    "size_modifier", "missing_families", "family_scores"];
  return { ...common, ...Object.fromEntries(keys.map(k => [k, h[k]])), hard_vetoes: h.hard_vetoes.length, soft_vetoes: h.soft_vetoes.length };
}
function num(v, d) { const n = v === null || v === "" ? NaN : Number(v); return Number.isFinite(n) ? n : d; }

export async function handleFusionApi(request, env, ctx, url) {
  if (request.method === "OPTIONS") return api({ ok: true });
  if (!["GET", "HEAD"].includes(request.method)) return api({ error: "method not allowed" }, 405);
  const parts = url.pathname.replace(/^\/api\/v1\/?/, "").split("/").filter(Boolean), head = parts[0], p = url.searchParams;
  const validRoute = (head === "health" && parts.length === 1) || (head === "opportunities" && parts.length === 1)
    || (head === "regime" && parts.length === 2 && parts[1] === "current") || (head === "signals" && parts.length === 2)
    || (head === "fusion" && (parts.length <= 2 || (parts.length === 3 && parts[2] === "changes")));
  if (!validRoute) return api({ error: "unknown API route" }, 404);
  const [f, s] = await Promise.all([readDoc(FUSION_KEY, ctx), readDoc(STATE_KEY, ctx)]);
  const rd = readiness(f, s, Date.now());
  const meta = { api: API_VER, api_minor: API_MINOR, generated_at: f?.generated_at, run_id: f?.run_id,
    snapshot_run_id: f?.snapshot_run_id, snapshot_generated_at: f?.snapshot_generated_at, shadow_mode: f?.shadow_mode,
    readiness: rd, labels_valid: rd.labels_valid };
  if (head === "health") return api({ ok: rd.schema_valid, ready: rd.research_ready, ...meta,
    fusion: rd.fusion_valid ? { run_id: f.run_id, generated_at: f.generated_at, shadow_mode: f.shadow_mode } : null,
    state: rd.state_valid ? { run_id: s.run_id, generated_at: s.generated_at, n_signals: s.n_signals, n_entities: s.n_entities, freshness_counts: s.freshness_counts } : null }, rd.schema_valid ? 200 : 503);
  if (p.has("run_id") && p.get("run_id") !== f?.run_id) return api({ error: "fusion snapshot advanced; reload the complete snapshot", ...meta }, 409);
  if (head === "signals") {
    if (!rd.state_valid) return api({ error: "signal state unavailable or invalid", ...meta }, 503);
    const eid = resolveEntity(s, parts[1]);
    if (!eid) return api({ error: "unknown or ambiguous entity" }, 404);
    const fam = (p.get("family") || "").toUpperCase(), hz = (p.get("horizon") || "").toUpperCase();
    const rows = s.entities[eid].filter(r => (!fam || r.family === fam) && (!hz || r.horizon === hz))
      .map(r => ({ ...r, freshness_at_snapshot: r.freshness, freshness: sourceFreshness(r, Date.parse(rd.evaluated_at)) }));
    return api({ ...meta, entity_id: eid, generated_at: s.generated_at, run_id: s.run_id, n: rows.length, signals: rows });
  }
  if (!rd.fusion_valid) return api({ error: "fusion read model unavailable or invalid", ...meta }, 503);
  if (head === "regime") return api({ ...meta, regime: f.regime, critical_dependencies: f.critical_dependencies, stats: f.stats });
  const sourceIndex = new Map(rd.state_valid ? Object.values(s.entities).flat().map(r => [r.signal_id, r]) : []);
  const entities = Object.fromEntries(Object.entries(f.entities).map(([eid, e]) => [eid, annotatedEntity(e, rd, sourceIndex, p.get("full") === "1")]));
  if (head === "fusion") {
    if (parts.length === 1) {
      const rows = Object.entries(entities).map(([eid, e]) => { const row = screenerRow(eid, e); delete row.signals; return row; });
      let comparison = null;
      if (p.get("full") === "1") {
        const sh = await readDoc("data/jh-fusion/shadow.json", ctx);
        if (object(sh) && sh.fusion_run_id === f.run_id && sh.snapshot_run_id === f.snapshot_run_id && object(sh.fleet_context)
          && arrayOf(sh.rows, r => object(r) && object(r.fusion) && finite(r.fusion.score, -1, 1) && string(r.fusion.direction) && object(r.existing) && object(r.agreements)
            && Object.values(r.existing).every(object))) {
          comparison = { ...sh, rows: sh.rows.map(r => ({ ...r, capital_decision_at_comparison: r.fusion.capital_decision,
            fusion: { ...r.fusion, capital_decision: entities[r.entity_id]?.horizons[r.best_horizon]?.capital_decision || "EXPIRED" } })) };
        }
      }
      return api({ ...(p.get("full") === "1" ? { ...f, entities, shadow_comparison: comparison } : {}), ...meta, regime: f.regime,
        critical_dependencies: f.critical_dependencies, stats: f.stats, n: rows.length, rows, methodology: f.methodology });
    }
    const eid = resolveEntity(f, parts[1]);
    if (!eid) return api({ error: "unknown or ambiguous entity", query: parts[1] }, 404);
    const entity = entities[eid];
    if (parts[2] === "changes") return api({ ...meta, entity_id: eid, best_horizon: entity.best_horizon, velocity: entity.velocity,
      history: f.entities[eid].history, horizons: Object.fromEntries(Object.entries(entity.horizons).map(([hz, h]) => [hz,
        { fusion_score: h.fusion_score, conviction: h.conviction, what_changed: h.what_changed || null, capital_decision: h.capital_decision,
          eligible: h.eligible, research_eligible: h.research_eligible, eligibility_reasons: h.eligibility_reasons }])) });
    const hz = (p.get("horizon") || "").toUpperCase();
    if (hz && !entity.horizons[hz]) return api({ error: "no evidence for horizon", entity_id: eid, horizon: hz, available: Object.keys(entity.horizons) }, 404);
    if (hz) entity.horizons = { [hz]: entity.horizons[hz] };
    return api({ ...meta, regime: f.regime, entity });
  }
  const minF = num(p.get("min_fusion"), 0), minC = num(p.get("min_confidence"), 0), minI = num(p.get("min_independent"), 0);
  const maxX = num(p.get("max_contradiction"), 100), hz = (p.get("horizon") || "").toUpperCase(), dir = (p.get("direction") || "bullish").toLowerCase();
  const mode = p.get("mode") || "execution", actionableOnly = p.get("actionable_only") !== "0";
  if (!["execution", "research"].includes(mode) || !["bullish", "bearish"].includes(dir) || (hz && !HORIZONS.has(hz))) return api({ error: "invalid mode, direction or horizon" }, 400);
  const limit = Math.max(1, Math.min(200, Math.floor(num(p.get("limit"), 50))));
  const rows = []; let excluded = 0;
  for (const [eid, e] of Object.entries(entities)) for (const [h, r] of Object.entries(e.horizons)) {
    if (hz && h !== hz) continue;
    if ((dir === "bearish" ? -r.fusion_score : r.fusion_score) < Math.max(minF, 0.0001)
      || r.confidence < minC || r.independent_evidence_count < minI || r.contradiction_score > maxX) continue;
    const modeEligible = mode === "research" ? r.research_eligible : r.execution_eligible;
    if (actionableOnly && !modeEligible) { excluded++; continue; }
    const row = { ...r, entity_id: eid, ticker: e.ticker, entity_type: e.entity_type, horizon: h,
      veto_reasons: r.hard_vetoes.map(v => v.reason).filter(Boolean), best_horizon: e.best_horizon === h,
      rank_score: mode === "research" ? r.research_rank_score : r.opportunity_rank_score };
    delete row.signals; rows.push(row);
  }
  rows.sort((a, b) => b.rank_score - a.rank_score);
  return api({ ...meta, filters: { min_fusion: minF, min_confidence: minC, min_independent: minI, max_contradiction: maxX,
    horizon: hz || null, direction: dir, mode, actionable_only: actionableOnly, limit }, n: rows.length,
    excluded_not_actionable: excluded, rows: rows.slice(0, limit) });
}
