/**
 * justhodl-data-proxy v1.0.0
 *
 * Edge-cached read-through proxy for the JustHodl data feeds. Replaces
 * direct S3 origin fetches from pages with Cloudflare-cached responses,
 * dramatically cutting latency for users far from us-east-1 (e.g. Morocco
 * pages were ~200ms × 12 files; with edge cache that drops to ~20ms per file).
 *
 * Usage from pages:
 *   GET https://justhodl-data-proxy.raafouis.workers.dev/yield-curve.json
 *   GET https://justhodl-data-proxy.raafouis.workers.dev/episode-reference.json
 *   (path under the worker maps to `data/<path>` in the S3 bucket)
 *
 * Cache TTLs by filename pattern:
 *   - intraday/live/5min files   → 30s edge cache
 *   - hourly/morning feeds       → 5min edge cache
 *   - daily/signal-board         → 1h edge cache
 *   - reference/historical/static → 6h edge cache
 *   - default                    → 5min edge cache
 *
 * CORS: open (data is public). Designed to be called from any justhodl.ai page
 * but works for any origin since the upstream S3 bucket is already public.
 */

const S3_BASE = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data";

const CACHE_RULES = [
  // Highly dynamic (every few minutes)
  { pattern: /(live|intraday|5min|streaming|tick|realtime)/i, ttl: 30 },
  // Hourly engines
  { pattern: /(hourly|morning|sentiment|news|crypto-intel|options-flow)/i, ttl: 300 },
  // Reference / slow-moving / curated
  { pattern: /(episode-reference|historical|crisis-knowledge|forward-returns|catalog|reference)/i, ttl: 21600 },
  // Daily defaults
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
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    // Health
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response(
        JSON.stringify({
          name: "justhodl-data-proxy",
          version: "1.0.0",
          status: "ok",
          upstream: S3_BASE,
          cache_rules: CACHE_RULES.length,
          default_ttl_s: DEFAULT_TTL,
        }, null, 2),
        { headers: { "Content-Type": "application/json", ...corsHeaders() } }
      );
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
    }

    // Safety: strip path traversal, only allow alphanumeric / hyphen / underscore / dot / slash
    const safePath = url.pathname.replace(/^\/+/, "");
    if (!/^[a-zA-Z0-9_\-./]+$/.test(safePath) || safePath.includes("..")) {
      return new Response("invalid path", { status: 400, headers: corsHeaders() });
    }

    const upstreamUrl = `${S3_BASE}/${safePath}`;
    const ttl = ttlFor(safePath);

    // Try edge cache first (keyed by canonical URL)
    const cacheKey = new Request(upstreamUrl, { method: "GET" });
    const cache = caches.default;
    let response = await cache.match(cacheKey);
    let cacheStatus = "HIT";

    if (!response) {
      cacheStatus = "MISS";
      let upstream;
      try {
        upstream = await fetch(upstreamUrl, { cf: { cacheTtl: ttl, cacheEverything: true } });
      } catch (e) {
        return new Response(
          JSON.stringify({ error: "upstream fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } }
        );
      }
      if (!upstream.ok) {
        return new Response(
          JSON.stringify({ error: "upstream not ok", status: upstream.status, path: safePath }),
          { status: upstream.status, headers: { "Content-Type": "application/json", ...corsHeaders() } }
        );
      }
      // Re-wrap with our cache+cors headers
      const body = await upstream.arrayBuffer();
      response = new Response(body, {
        status: 200,
        headers: {
          "Content-Type": upstream.headers.get("Content-Type") || "application/json",
          "Cache-Control": `public, max-age=${Math.min(ttl, 60)}, s-maxage=${ttl}`,
          "X-Edge-TTL": String(ttl),
          ...corsHeaders(),
        },
      });
      // Put a clone in the cache
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
    }

    // Add cache-status header to response (clone so original is untouched)
    const headers = new Headers(response.headers);
    headers.set("X-Edge-Cache", cacheStatus);
    return new Response(response.body, { status: response.status, headers });
  },
};
