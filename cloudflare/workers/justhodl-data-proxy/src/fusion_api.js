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
 */

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

function screenerRow(eid, e) {
  const bh = e.best_horizon;
  const h = (e.horizons || {})[bh] || {};
  return {
    entity_id: eid, entity_type: e.entity_type, ticker: e.ticker, best_horizon: bh,
    fusion_score: h.fusion_score, conviction: h.conviction, direction: h.direction, confidence: h.confidence,
    fusion_coverage: h.fusion_coverage, independent_evidence_count: h.independent_evidence_count,
    raw_signal_count: h.raw_signal_count, contradiction_score: h.contradiction_score, contradiction_class: h.contradiction_class,
    regime_fit: h.regime_fit, capital_decision: h.capital_decision, size_modifier: h.size_modifier,
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
    return api({
      ok: !!(f && s), api: API_VER,
      fusion: f ? { run_id: f.run_id, generated_at: f.generated_at, snapshot_run_id: f.snapshot_run_id, n_entities: (f.stats || {}).n_entities, shadow_mode: f.shadow_mode } : null,
      state: s ? { run_id: s.run_id, generated_at: s.generated_at, n_signals: s.n_signals, n_entities: s.n_entities, freshness_counts: s.freshness_counts } : null,
    }, f && s ? 200 : 503);
  }

  if (head === "regime") {
    const f = await readDoc(FUSION_KEY, ctx);
    if (!f) return api({ error: "fusion read model unavailable" }, 503);
    return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, regime: f.regime,
                 critical_dependencies: f.critical_dependencies, stats: f.stats });
  }

  if (head === "fusion") {
    const f = await readDoc(FUSION_KEY, ctx);
    if (!f) return api({ error: "fusion read model unavailable" }, 503);
    if (parts.length === 1) {
      const rows = Object.entries(f.entities || {}).map(([eid, e]) => screenerRow(eid, e));
      return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, regime: { label: f.regime.label, score: f.regime.score, certainty: f.regime.certainty, n_legs: f.regime.n_legs },
                   critical_dependencies: f.critical_dependencies, stats: f.stats, n: rows.length, rows, methodology: f.methodology });
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
      out.horizons[h] = stripHorizon(r, full);
    }
    if (hz && !out.horizons[hz]) return api({ error: "no evidence for horizon", entity_id: eid, horizon: hz, available: Object.keys(e.horizons || {}) }, 404);
    return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, regime: { label: f.regime.label, score: f.regime.score }, entity: out });
  }

  if (head === "signals") {
    const s = await readDoc(STATE_KEY, ctx);
    if (!s) return api({ error: "signal state unavailable" }, 503);
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
    const f = await readDoc(FUSION_KEY, ctx);
    if (!f) return api({ error: "fusion read model unavailable" }, 503);
    const minF = num(p.get("min_fusion"), 0), minC = num(p.get("min_confidence"), 0), minI = num(p.get("min_independent"), 0);
    const maxX = num(p.get("max_contradiction"), 100), hz = (p.get("horizon") || "").toUpperCase(), dir = (p.get("direction") || "bullish").toLowerCase();
    const limit = Math.max(1, Math.min(200, num(p.get("limit"), 50) | 0));
    const rows = [];
    for (const [eid, e] of Object.entries(f.entities || {})) {
      for (const [h, r] of Object.entries(e.horizons || {})) {
        if (hz && h !== hz) continue;
        const signed = dir === "bearish" ? -r.fusion_score : r.fusion_score;
        if (signed < Math.max(minF, 0.0001)) continue;
        if (r.confidence < minC || r.independent_evidence_count < minI || r.contradiction_score > maxX) continue;
        const opportunity = Math.abs(r.fusion_score) * r.confidence * (0.5 + 0.5 * r.fusion_coverage) * (1 - r.contradiction_score / 200) * (r.capital_decision === "BLOCKED" ? 0 : r.size_modifier);
        rows.push({ entity_id: eid, ticker: e.ticker, entity_type: e.entity_type, horizon: h, fusion_score: r.fusion_score, conviction: r.conviction, direction: r.direction,
                    confidence: r.confidence, fusion_coverage: r.fusion_coverage, independent_evidence_count: r.independent_evidence_count, contradiction_score: r.contradiction_score,
                    regime_fit: r.regime_fit, capital_decision: r.capital_decision, size_modifier: r.size_modifier, opportunity_rank_score: Math.round(opportunity * 10000) / 10000,
                    best_horizon: e.best_horizon === h, velocity: e.velocity ? e.velocity.classification : null });
      }
    }
    rows.sort((a, b) => b.opportunity_rank_score - a.opportunity_rank_score);
    return api({ api: API_VER, generated_at: f.generated_at, run_id: f.run_id, shadow_mode: f.shadow_mode, filters: { min_fusion: minF, min_confidence: minC, min_independent: minI, max_contradiction: maxX, horizon: hz || null, direction: dir },
                 note: "opportunity_rank_score = |fusion| x confidence x (0.5+0.5 coverage) x (1-contradiction/200) x size_modifier (0 when capital BLOCKED). Release-6 adds historical edge and asymmetry; neither is fabricated here.",
                 n: rows.length, rows: rows.slice(0, limit) });
  }

  return api({ error: "unknown route", routes: ["/api/v1/fusion", "/api/v1/fusion/{entity}", "/api/v1/fusion/{entity}/changes", "/api/v1/signals/{entity}", "/api/v1/regime/current", "/api/v1/opportunities", "/api/v1/health"] }, 404);
}
