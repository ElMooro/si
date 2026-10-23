// ops 6628 (Khalid, 2026-10-23): "block crawlers from my data at Cloudflare, as long as it doesn't affect my system
// operations and functionality."
//
// Why: search and AI crawlers render the site's pages and every render downloads the data files behind them. Applebot
// alone rendered why.html ~6,000 times a day and pulled 10-30 GB/day of data files out of S3 ($0.9-2.7/day of egress,
// docs/audit/2026-10-23/aws-bill-oct22.md). Crawlers still get every HTML page (GitHub Pages); they no longer get data.
//
// What is refused: GET/HEAD from a SELF-DECLARED crawler (user agent contains one of CRAWLER_TOKENS, case-insensitive)
// on any path except the three below. Never a generic word such as "bot", "crawler" or "spider": the system's own
// clients use those words (JustHodlBot/1.0, JustHodl-Bot/1.0, justhodl-tv-crawler/2.0). Browsers, HeadlessChrome
// (the runner's Playwright probes and the section-registry build), scripts (python, boto3, curl, node), the engines,
// webhooks (Stripe), monitors, link previews and user-initiated AI fetches (ChatGPT-User, Claude-User,
// Perplexity-User) are untouched. POST/PUT/OPTIONS are never gated.
//
// The token list is a copy of config/crawler-user-agents.json `worker_tokens`; tests/worker-crawler-gate.test.js
// fails the deploy if the two ever differ.
export const CRAWLER_TOKENS = [
  "Googlebot", "Google-InspectionTool", "GoogleOther", "AdsBot-Google", "Mediapartners-Google", "Storebot-Google",
  "bingbot", "BingPreview", "msnbot", "adidxbot",
  "Applebot", "DuckDuckBot", "DuckAssistBot", "YandexBot", "yandex.com/bots", "Baiduspider", "Sogou web spider",
  "Exabot", "SeznamBot", "PetalBot", "naver.me/spd", "coccocbot", "MojeekBot", "Qwantbot", "YisouSpider", "360Spider",
  "AhrefsBot", "AhrefsSiteAudit", "SemrushBot", "SiteAuditBot", "MJ12bot", "DotBot", "BLEXBot", "DataForSeoBot",
  "serpstatbot", "rogerbot", "Screaming Frog", "Barkrowler", "MegaIndex", "SeekportBot", "ZoominfoBot", "LinkdexBot",
  "GPTBot", "OAI-SearchBot", "ClaudeBot", "Claude-SearchBot", "anthropic-ai", "CCBot", "Bytespider", "Amazonbot",
  "PerplexityBot", "meta-externalagent", "FacebookBot", "Diffbot", "cohere-ai", "cohere-training-data-crawler",
  "YouBot", "Timpibot", "ImagesiftBot", "omgilibot", "AI2Bot", "PanguBot", "img2dataset", "VelenPublicWebCrawler",
  "ICC-Crawler", "Lightpanda", "Scrapy", "ia_archiver", "archive.org_bot",
];

const LOWER = CRAWLER_TOKENS.map((t) => t.toLowerCase());

// Paths a crawler may still read: the health answer and the robots file.
export const CRAWLER_OPEN_PATHS = new Set(["/", "/health", "/robots.txt"]);

// The Worker's own hostname is an API host: nothing on it is meant for an index.
export const WORKER_ROBOTS = "User-agent: *\nDisallow: /\n";

export function isCrawler(userAgent) {
  if (!userAgent) return false;
  const ua = String(userAgent).toLowerCase();
  for (const token of LOWER) {
    if (ua.includes(token)) return true;
  }
  return false;
}

// Returns a Response when the request must be answered here (refused crawler, or the workers.dev robots file),
// null when the request continues to the normal routes unchanged.
export function crawlerGate(request, url, cors) {
  const method = request.method;
  if (method !== "GET" && method !== "HEAD") return null;
  if (url.pathname === "/robots.txt" && url.hostname.endsWith(".workers.dev")) {
    return new Response(method === "HEAD" ? null : WORKER_ROBOTS, {
      status: 200,
      headers: { ...cors, "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "public, max-age=86400" },
    });
  }
  if (CRAWLER_OPEN_PATHS.has(url.pathname)) return null;
  if (!isCrawler(request.headers.get("User-Agent"))) return null;
  const body = JSON.stringify({ error: "crawler access to JustHodl data is disabled", robots: "https://justhodl.ai/robots.txt" });
  return new Response(method === "HEAD" ? null : body, {
    status: 403,
    headers: {
      ...cors,
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      "X-Robots-Tag": "noindex, nofollow",
      "X-JH-Crawler-Gate": "refused",
    },
  });
}
