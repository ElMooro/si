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
    "Access-Control-Allow-Methods": "GET, HEAD, PUT, POST, OPTIONS",
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

    // ─── PER-USER DATA SYNC (watchlists, flags, settings) ───
    // GET  /userdata/:uid           → returns stored JSON blob for user
    // PUT  /userdata/:uid {json}    → stores JSON blob for user
    // Keyed by anonymous device UID generated client-side. Backed by KV.
    if (url.pathname.startsWith("/userdata/")) {
      const uid = url.pathname.slice("/userdata/".length).replace(/[^a-zA-Z0-9_\-]/g, "");
      if (!uid || uid.length < 8 || uid.length > 64) {
        return new Response(JSON.stringify({ error: "invalid uid" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      if (!env.USER_DATA) {
        return new Response(JSON.stringify({ error: "user store unavailable" }),
          { status: 503, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const kvKey = `u:${uid}`;
      if (request.method === "GET") {
        const stored = await env.USER_DATA.get(kvKey);
        return new Response(stored || JSON.stringify({ empty: true }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "no-store", ...corsHeaders() } });
      }
      if (request.method === "PUT" || request.method === "POST") {
        try {
          const bodyText = await request.text();
          if (bodyText.length > 500000) {  // 500KB cap per user
            return new Response(JSON.stringify({ error: "payload too large" }),
              { status: 413, headers: { "Content-Type": "application/json", ...corsHeaders() } });
          }
          // Validate JSON
          JSON.parse(bodyText);
          await env.USER_DATA.put(kvKey, bodyText);
          return new Response(JSON.stringify({ ok: true, saved_at: Date.now() }),
            { headers: { "Content-Type": "application/json", ...corsHeaders() } });
        } catch (e) {
          return new Response(JSON.stringify({ error: "invalid json", detail: String(e) }),
            { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
        }
      }
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
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

    // ─── BATCH QUOTES ROUTE ───
    // GET /quotes?tickers=AAPL,MSFT,NVDA → live snapshot via Polygon
    // Returns {tickers: {AAPL: {price, change, changePct, volume, ...}}}
    // Cached 30s at edge. Polygon key from worker env (server-side, secure).
    if (url.pathname === "/quotes") {
      const tickersParam = (url.searchParams.get("tickers") || "").trim();
      if (!tickersParam) {
        return new Response(JSON.stringify({ error: "missing tickers param" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const tickers = tickersParam.toUpperCase().split(",")
        .map(t => t.trim()).filter(Boolean).slice(0, 60);  // cap 60
      const polygonKey = env.POLYGON_KEY || "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d";

      // Edge cache key per ticker set
      const qCacheKey = new Request(`https://quotes.cache/${tickers.sort().join(",")}`, { method: "GET" });
      const qCache = caches.default;
      let qResp = await qCache.match(qCacheKey);
      if (qResp) {
        const cached = await qResp.json();
        return new Response(JSON.stringify(cached),
          { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } });
      }

      try {
        const snapUrl = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers=${tickers.join(",")}&apiKey=${polygonKey}`;
        const snapResp = await fetch(snapUrl, { cf: { cacheTtl: 30, cacheEverything: true } });
        const snapData = await snapResp.json();
        const out = { tickers: {}, ts: Date.now() };
        for (const t of (snapData.tickers || [])) {
          const day = t.day || {};
          const prevDay = t.prevDay || {};
          const lastTrade = t.lastTrade || {};
          const min = t.min || {};
          const price = lastTrade.p || min.c || day.c || prevDay.c || 0;
          const prevClose = prevDay.c || 0;
          const change = prevClose ? (price - prevClose) : (t.todaysChange || 0);
          const changePct = prevClose ? (change / prevClose * 100) : (t.todaysChangePerc || 0);
          out.tickers[t.ticker] = {
            price: price,
            change: change,
            changePct: changePct,
            volume: day.v || min.v || 0,
            high: day.h || 0,
            low: day.l || 0,
            open: day.o || 0,
            prevClose: prevClose,
          };
        }
        const respBody = JSON.stringify(out);
        const finalResp = new Response(respBody, {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "public, max-age=30, s-maxage=30",
            "X-Cache": "MISS",
            ...corsHeaders(),
          },
        });
        ctx.waitUntil(qCache.put(qCacheKey, new Response(respBody, {
          headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=30" },
        })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "quotes fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/yf-ohlc") {
      // GET /yf-ohlc?symbol=BTC-USD&range=1y → Yahoo Finance chart (crypto/forex/etc)
      const symbol = (url.searchParams.get("symbol") || "").trim();
      const range = (url.searchParams.get("range") || "1y").trim();
      if (!symbol || !/^[A-Za-z0-9.=^\-]{1,20}$/.test(symbol)) {
        return new Response(JSON.stringify({ error: "invalid symbol" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const yCacheKey = new Request(`https://yf.cache/${symbol}/${range}`, { method: "GET" });
      const yc = caches.default;
      const yhit = await yc.match(yCacheKey);
      if (yhit) { const b = await yhit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const yUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=${encodeURIComponent(range)}`;
        const resp = await fetch(yUrl, {
          headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36", "Accept": "application/json" },
          cf: { cacheTtl: 300, cacheEverything: true },
        });
        const data = await resp.json();
        const res = data.chart && data.chart.result && data.chart.result[0];
        const ts = (res && res.timestamp) || [];
        const q = (res && res.indicators && res.indicators.quote && res.indicators.quote[0]) || {};
        const bars = [];
        for (let i = 0; i < ts.length; i++) {
          if (q.close && q.close[i] != null && q.open[i] != null) {
            bars.push({ time: ts[i], open: q.open[i], high: q.high[i], low: q.low[i], close: q.close[i], value: (q.volume && q.volume[i]) || 0 });
          }
        }
        const out = JSON.stringify({ symbol, bars, count: bars.length });
        const finalResp = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=300", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(yc.put(yCacheKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=300" } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "yf fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/dbnomics-search") {
      // GET /dbnomics-search?q=inflation → search 90+ macro providers
      const qq = (url.searchParams.get("q") || "").trim();
      if (!qq) return new Response(JSON.stringify({ series: [] }), { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      try {
        const dUrl = `https://api.db.nomics.world/v22/search?q=${encodeURIComponent(qq)}&limit=30`;
        const resp = await fetch(dUrl, { cf: { cacheTtl: 1800, cacheEverything: true } });
        const data = await resp.json();
        const docs = (data.results && data.results.docs) || [];
        const series = docs.map(d => ({
          id: `${d.provider_code}/${d.dataset_code}/${d.series_code}`,
          name: d.series_name || d.name,
          provider: d.provider_code, dataset: d.dataset_name || d.dataset_code,
        }));
        return new Response(JSON.stringify({ series }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=1800", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ series: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/dbnomics") {
      // GET /dbnomics?series=PROVIDER/DATASET/CODE → observations
      const sid = (url.searchParams.get("series") || "").trim();
      if (!sid || sid.split("/").length !== 3) {
        return new Response(JSON.stringify({ error: "invalid series id (need PROVIDER/DATASET/CODE)" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const dCacheKey = new Request(`https://dbn.cache/${sid}`, { method: "GET" });
      const dc = caches.default;
      const dhit = await dc.match(dCacheKey);
      if (dhit) { const b = await dhit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const [prov, ds, code] = sid.split("/");
        const dUrl = `https://api.db.nomics.world/v22/series/${prov}/${ds}/${encodeURIComponent(code)}?observations=1`;
        const resp = await fetch(dUrl, { cf: { cacheTtl: 3600, cacheEverything: true } });
        const data = await resp.json();
        const doc = (data.series && data.series.docs && data.series.docs[0]) || {};
        const periods = doc.period || [];
        const values = doc.value || [];
        const bars = [];
        for (let i = 0; i < periods.length; i++) {
          const v = values[i];
          if (v == null || v === "NA") continue;
          const t = Math.floor(new Date(periods[i].length === 4 ? periods[i] + "-12-31" : periods[i].length === 7 ? periods[i] + "-01" : periods[i]).getTime() / 1000);
          if (isFinite(t) && isFinite(v)) bars.push({ time: t, value: +v, date: periods[i] });
        }
        bars.sort((a, b) => a.time - b.time);
        const out = JSON.stringify({ series: sid, name: doc.series_name || "", bars, count: bars.length });
        const finalResp = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(dc.put(dCacheKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600" } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "dbnomics fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/fred") {
      // GET /fred?series=DGS10&obs=600 → FRED observations as {bars:[{time,value}]}
      const series = (url.searchParams.get("series") || "").trim().toUpperCase();
      const obs = Math.min(parseInt(url.searchParams.get("obs") || "600", 10) || 600, 5000);
      if (!series || !/^[A-Z0-9._\-]{1,40}$/.test(series)) {
        return new Response(JSON.stringify({ error: "invalid series" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fredKey = env.FRED_KEY || "2f057499936072679d8843d7fce99989";
      const fCacheKey = new Request(`https://fred.cache/${series}/${obs}`, { method: "GET" });
      const fc = caches.default;
      const hit = await fc.match(fCacheKey);
      if (hit) {
        const body = await hit.text();
        return new Response(body, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } });
      }
      try {
        const fredUrl = `https://api.stlouisfed.org/fred/series/observations?series_id=${series}&api_key=${fredKey}&file_type=json&sort_order=desc&limit=${obs}`;
        const resp = await fetch(fredUrl, { cf: { cacheTtl: 3600, cacheEverything: true } });
        const data = await resp.json();
        const obsArr = (data.observations || [])
          .filter(o => o.value !== "." && o.value != null)
          .map(o => ({ time: Math.floor(new Date(o.date).getTime() / 1000), value: parseFloat(o.value), date: o.date }))
          .filter(o => isFinite(o.value))
          .sort((a, b) => a.time - b.time);
        const out = JSON.stringify({ series, bars: obsArr, count: obsArr.length });
        const finalResp = new Response(out, {
          headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600", "X-Cache": "MISS", ...corsHeaders() },
        });
        ctx.waitUntil(fc.put(fCacheKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=3600" } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "fred fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/fred-search") {
      // GET /fred-search?text=inflation → FRED full-text series search (800k+ series)
      const text = (url.searchParams.get("text") || "").trim();
      if (!text) {
        return new Response(JSON.stringify({ series: [] }),
          { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fredKey = env.FRED_KEY || "2f057499936072679d8843d7fce99989";
      try {
        const sUrl = `https://api.stlouisfed.org/fred/series/search?search_text=${encodeURIComponent(text)}&api_key=${fredKey}&file_type=json&limit=30&order_by=popularity&sort_order=desc`;
        const resp = await fetch(sUrl, { cf: { cacheTtl: 1800, cacheEverything: true } });
        const data = await resp.json();
        const series = (data.seriess || []).map(s => ({
          id: s.id, title: s.title, units: s.units_short || s.units,
          frequency: s.frequency_short || s.frequency, popularity: s.popularity,
          obs_start: s.observation_start, obs_end: s.observation_end,
        }));
        return new Response(JSON.stringify({ series }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=1800", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ series: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/ohlc") {
      // GET /ohlc?ticker=AAPL&mult=1&span=day&days=180 → Polygon aggregates (any interval)
      const ticker = (url.searchParams.get("ticker") || "").trim().toUpperCase();
      const days = Math.min(parseInt(url.searchParams.get("days") || "180", 10) || 180, 3650);
      const mult = Math.min(parseInt(url.searchParams.get("mult") || "1", 10) || 1, 60);
      const span = (url.searchParams.get("span") || "day").trim();
      const validSpans = ["minute", "hour", "day", "week", "month"];
      if (!ticker || !/^[A-Z0-9.\-]{1,12}$/.test(ticker) || !validSpans.includes(span)) {
        return new Response(JSON.stringify({ error: "invalid params" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const polygonKey = env.POLYGON_KEY || "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d";
      const to = new Date();
      const from = new Date(to.getTime() - days * 86400000);
      const fmt = (d) => d.toISOString().slice(0, 10);
      const ttl = span === "minute" ? 60 : span === "hour" ? 120 : 300;
      const ohlcKey = new Request(`https://ohlc.cache/${ticker}/${mult}${span}/${days}`, { method: "GET" });
      const oc = caches.default;
      let cached = await oc.match(ohlcKey);
      if (cached) {
        const body = await cached.text();
        return new Response(body, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } });
      }
      try {
        const aggUrl = `https://api.polygon.io/v2/aggs/ticker/${ticker}/range/${mult}/${span}/${fmt(from)}/${fmt(to)}?adjusted=true&sort=asc&limit=50000&apiKey=${polygonKey}`;
        const resp = await fetch(aggUrl, { cf: { cacheTtl: ttl, cacheEverything: true } });
        const data = await resp.json();
        const bars = (data.results || []).map(b => ({
          time: Math.floor(b.t / 1000), open: b.o, high: b.h, low: b.l, close: b.c, value: b.v,
        }));
        const out = JSON.stringify({ ticker, span, mult, bars, count: bars.length });
        const finalResp = new Response(out, {
          headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${ttl}`, "X-Cache": "MISS", ...corsHeaders() },
        });
        ctx.waitUntil(oc.put(ohlcKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${ttl}` } })));
        return finalResp;
      } catch (e) {
        return new Response(JSON.stringify({ error: "ohlc fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/fundamentals") {
      // GET /fundamentals?ticker=AAPL → key quote + profile + ratios snapshot (FMP)
      const ticker = (url.searchParams.get("ticker") || "").trim().toUpperCase();
      if (!ticker || !/^[A-Z0-9.\-]{1,12}$/.test(ticker)) {
        return new Response(JSON.stringify({ error: "invalid ticker" }),
          { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fmpKey = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb";
      const fKey = new Request(`https://fund.cache/${ticker}`, { method: "GET" });
      const fc = caches.default;
      const hit = await fc.match(fKey);
      if (hit) { const b = await hit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const [q, p, rm] = await Promise.all([
          fetch(`https://financialmodelingprep.com/stable/quote?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 600 } }).then(r => r.ok ? r.json() : []),
          fetch(`https://financialmodelingprep.com/stable/profile?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 3600 } }).then(r => r.ok ? r.json() : []),
          fetch(`https://financialmodelingprep.com/stable/ratios-ttm?symbol=${ticker}&apikey=${fmpKey}`, { cf: { cacheTtl: 3600 } }).then(r => r.ok ? r.json() : []),
        ]);
        const quote = Array.isArray(q) ? (q[0] || {}) : (q || {});
        const prof = Array.isArray(p) ? (p[0] || {}) : (p || {});
        const ratios = Array.isArray(rm) ? (rm[0] || {}) : (rm || {});
        const pick = (...keys) => { for (const k of keys) { if (ratios[k] != null) return ratios[k]; } return null; };
        const out = JSON.stringify({
          ticker,
          name: prof.companyName, sector: prof.sector, industry: prof.industry,
          exchange: prof.exchange || quote.exchange, ceo: prof.ceo, country: prof.country,
          description: (prof.description || "").slice(0, 400),
          price: quote.price, changesPct: quote.changePercentage || quote.changesPercentage,
          marketCap: quote.marketCap || prof.marketCap, volume: quote.volume, avgVolume: quote.avgVolume,
          pe: quote.pe || pick("priceToEarningsRatioTTM", "peRatioTTM"), eps: quote.eps,
          yearHigh: quote.yearHigh, yearLow: quote.yearLow,
          beta: prof.beta, dividendYield: pick("dividendYieldTTM", "dividendYielPercentageTTM", "dividendYieldPercentageTTM"),
          pb: pick("priceToBookRatioTTM", "pbRatioTTM"), ps: pick("priceToSalesRatioTTM", "priceSalesRatioTTM"),
          roe: pick("returnOnEquityTTM", "roeTTM"), netMargin: pick("netProfitMarginTTM", "netIncomePerShareTTM") && pick("netProfitMarginTTM"),
          debtToEquity: pick("debtToEquityRatioTTM", "debtEquityRatioTTM"),
          currentRatio: pick("currentRatioTTM"), grossMargin: pick("grossProfitMarginTTM"),
          opMargin: pick("operatingProfitMarginTTM"), fcfYield: pick("freeCashFlowYieldTTM"),
        });
        const fr = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(fc.put(fKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600" } })));
        return fr;
      } catch (e) {
        return new Response(JSON.stringify({ error: "fundamentals fetch failed", detail: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/news") {
      // GET /news?ticker=AAPL → recent headlines via FMP (commercial-licensed)
      const ticker = (url.searchParams.get("ticker") || "").trim().toUpperCase();
      if (!ticker || !/^[A-Z0-9.\-]{1,12}$/.test(ticker)) {
        return new Response(JSON.stringify({ news: [] }),
          { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      const fmpKey = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb";
      const nKey = new Request(`https://news.cache/${ticker}`, { method: "GET" });
      const nc = caches.default;
      const hit = await nc.match(nKey);
      if (hit) { const b = await hit.text(); return new Response(b, { headers: { "Content-Type": "application/json", "X-Cache": "HIT", ...corsHeaders() } }); }
      try {
        const u = `https://financialmodelingprep.com/stable/news/stock?symbols=${ticker}&limit=20&apikey=${fmpKey}`;
        const resp = await fetch(u, { cf: { cacheTtl: 600, cacheEverything: true } });
        let rows = await resp.json();
        if (!Array.isArray(rows)) rows = [];
        const news = rows.slice(0, 20).map(n => ({
          title: n.title, site: n.publisher || n.site, date: n.publishedDate || n.date,
          url: n.url, text: (n.text || "").slice(0, 200), image: n.image || "",
        }));
        const out = JSON.stringify({ ticker, news });
        const fr = new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600", "X-Cache": "MISS", ...corsHeaders() } });
        ctx.waitUntil(nc.put(nKey, new Response(out, { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=600" } })));
        return fr;
      } catch (e) {
        return new Response(JSON.stringify({ news: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
    }

    if (url.pathname === "/tv-search") {
      // GET /tv-search?text=AAPL → proxy TradingView symbol search (full universe)
      const text = (url.searchParams.get("text") || "").trim();
      const type = (url.searchParams.get("type") || "").trim();
      const exchange = (url.searchParams.get("exchange") || "").trim();
      if (!text) {
        return new Response(JSON.stringify({ symbols: [] }),
          { headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
      try {
        const tvUrl = `https://symbol-search.tradingview.com/symbol_search/?text=${encodeURIComponent(text)}&hl=1&lang=en&type=${encodeURIComponent(type)}&exchange=${encodeURIComponent(exchange)}&domain=production`;
        const tvResp = await fetch(tvUrl, {
          headers: {
            "Origin": "https://www.tradingview.com",
            "Referer": "https://www.tradingview.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          },
          cf: { cacheTtl: 300, cacheEverything: true },
        });
        const raw = await tvResp.json();
        const clean = (s) => String(s || "").replace(/<\/?[^>]+>/g, "");
        const arr = Array.isArray(raw) ? raw : (raw.symbols || []);
        const symbols = arr.slice(0, 30).map(s => ({
          symbol: clean(s.symbol),
          description: clean(s.description),
          type: s.type || "",
          exchange: s.exchange || s.exchange_name || "",
          currency: s.currency_code || "",
          full: (s.prefix || s.exchange) ? `${(s.prefix || s.exchange)}:${clean(s.symbol)}` : clean(s.symbol),
        }));
        return new Response(JSON.stringify({ symbols }),
          { headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=300", ...corsHeaders() } });
      } catch (e) {
        return new Response(JSON.stringify({ symbols: [], error: String(e) }),
          { status: 502, headers: { "Content-Type": "application/json", ...corsHeaders() } });
      }
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
