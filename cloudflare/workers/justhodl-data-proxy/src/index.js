/**
 * justhodl-data-proxy v2.0.0
 *
 * Edge-cached read-through proxy for all justhodl public S3 feeds.
 * Cloudflare's edge automatically applies Brotli/gzip compression for
 * text/JSON responses, so a 49 KB plain JSON typically reaches the
 * browser as ~10 KB. Invisible to the page (browser auto-decodes).
 *
 * Usage from pages — path passes through VERBATIM to the S3 bucket:
 *   GET https://justhodl-data-proxy.raafouis.workers.dev/crypto-intel.json
 *     -> s3://justhodl-dashboard-live/crypto-intel.json
 *
 *   GET https://justhodl-data-proxy.raafouis.workers.dev/data/pump-radar-summary.json
 *     -> s3://justhodl-dashboard-live/data/pump-radar-summary.json
 *
 * Backward-compat: legacy callers using paths without a slash that worked
 * pre-v2 (mapped to /data/<file>) still work — on 404, we retry under /data/.
 *
 * Browser sees Content-Encoding: gzip|br via Cloudflare edge. Worker
 * returns plain bytes; edge compresses transparently. Wire size
 * reduction: typically 60-80% on text/JSON.
 */

const BUCKET_BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";

const CACHE_RULES = [
  { pattern: /(live|intraday|5min|streaming|tick|realtime)/i, ttl: 30 },
  { pattern: /(hourly|morning|sentiment|news|crypto-intel|options-flow|pump-positioning|pump-radar|catalyst|velocity|momentum)/i, ttl: 300 },
  { pattern: /(episode-reference|historical|crisis-knowledge|forward-returns|catalog|reference|themes)/i, ttl: 21600 },
  { pattern: /(daily|signal-board|portfolio|risk-recommendation)/i, ttl: 3600 },
];
const DEFAULT_TTL = 300;

function ttlFor(path) {
  for (const { pattern, ttl } of CACHE_RULES) {
    if (pattern.test(path)) return ttl;
  }
  return DEFAULT_TTL;
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, If-None-Match, If-Modified-Since",
    "Access-Control-Max-Age":       "86400",
    "Vary":                         "Accept-Encoding",
  };
}

async function fetchUpstream(upstreamUrl, ttl) {
  return fetch(upstreamUrl, {
    cf: { cacheTtl: ttl, cacheEverything: true },
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response(
        JSON.stringify({
          name:           "justhodl-data-proxy",
          version:        "2.0.0",
          status:         "ok",
          upstream:       BUCKET_BASE,
          cache_rules:    CACHE_RULES.length,
          default_ttl_s:  DEFAULT_TTL,
          gzip:           "automatic via Cloudflare edge",
          fallback:       "tries /data/<file> if /<file> 404s and path has no slash",
        }, null, 2),
        { headers: { "Content-Type": "application/json", ...corsHeaders() } }
      );
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
    }

    const safePath = url.pathname.replace(/^\/+/, "");
    if (!/^[a-zA-Z0-9_\-./]+$/.test(safePath) || safePath.includes("..")) {
      return new Response("invalid path", { status: 400, headers: corsHeaders() });
    }

    const ttl = ttlFor(safePath);
    const cacheKey = new Request(url.toString().split("?")[0], { method: "GET" });
    const cache = caches.default;
    let response = await cache.match(cacheKey);
    let cacheStatus = "HIT";

    if (!response) {
      cacheStatus = "MISS";
      let upstreamUrl = `${BUCKET_BASE}/${safePath}`;
      let upstream;
      try {
        upstream = await fetchUpstream(upstreamUrl, ttl);
      } catch (e) {
        return new Response(
          JSON.stringify({ error: "upstream fetch failed", detail: String(e), path: safePath }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } }
        );
      }

      // Backward-compat fallback for callers using legacy paths
      if (!upstream.ok && upstream.status === 404 && !safePath.includes("/")) {
        const fallbackUrl = `${BUCKET_BASE}/data/${safePath}`;
        try {
          const fallback = await fetchUpstream(fallbackUrl, ttl);
          if (fallback.ok) { upstream = fallback; upstreamUrl = fallbackUrl; }
        } catch (_) { /* keep original 404 */ }
      }

      if (!upstream.ok) {
        return new Response(
          JSON.stringify({ error: "upstream not ok", status: upstream.status, path: safePath }),
          { status: upstream.status, headers: { "Content-Type": "application/json", ...corsHeaders() } }
        );
      }

      const upstreamCT = upstream.headers.get("Content-Type") || "application/json";
      const lastMod    = upstream.headers.get("Last-Modified");
      const etag       = upstream.headers.get("ETag");
      const body       = await upstream.arrayBuffer();

      const respHeaders = {
        "Content-Type":   upstreamCT,
        "Cache-Control":  `public, max-age=${Math.min(ttl, 60)}, s-maxage=${ttl}`,
        "X-Edge-TTL":     String(ttl),
        "X-Upstream":     upstreamUrl,
        ...corsHeaders(),
      };
      if (lastMod) respHeaders["Last-Modified"] = lastMod;
      if (etag)    respHeaders["ETag"]          = etag;

      response = new Response(body, { status: 200, headers: respHeaders });
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
    }

    const headers = new Headers(response.headers);
    headers.set("X-Edge-Cache", cacheStatus);
    return new Response(response.body, { status: response.status, headers });
  },
};
