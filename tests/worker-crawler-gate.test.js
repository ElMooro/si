// ops 6628 (Khalid, 2026-10-23): "block crawlers from my data at Cloudflare, as long as it doesn't affect my system
// operations and functionality." Runs the REAL worker module in Node with a scripted S3 and a Map-backed Cache API.
// Proves both halves: every self-declared crawler is refused BEFORE any S3 read, and every client the system itself
// uses (browsers, HeadlessChrome probes, python/boto3/curl scripts, the engines' own user agents, Stripe, monitors,
// link previews, user-initiated AI fetches, an empty user agent) reaches the data exactly as before.
const test = require('node:test'), assert = require('node:assert/strict'), path = require('node:path'), fs = require('node:fs');
const { pathToFileURL } = require('node:url');
const source = pathToFileURL(path.join(__dirname, '../cloudflare/workers/justhodl-data-proxy/src/index.js')).href;
const gateSource = pathToFileURL(path.join(__dirname, '../cloudflare/workers/justhodl-data-proxy/src/crawler-gate.js')).href;
const CONFIG = JSON.parse(fs.readFileSync(path.join(__dirname, '../config/crawler-user-agents.json'), 'utf8'));

const CRAWLER_UAS = [
  'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
  'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
  'Mozilla/5.0 (compatible; Google-InspectionTool/1.0;)',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15 (Applebot/0.1; +http://www.apple.com/go/applebot)',
  'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) Chrome/116.0.1938.76 Safari/537.36',
  'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)',
  'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)',
  'CCBot/2.0 (https://commoncrawl.org/faq/)',
  'Mozilla/5.0 (Linux; Android 5.0) AppleWebKit/537.36 (KHTML, like Gecko) Mobile Safari/537.36 (compatible; Bytespider; spider-feedback@bytedance.com)',
  'Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)',
  'Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)',
  'Mozilla/5.0 (Linux; Android 7.0;) AppleWebKit/537.36 (KHTML, like Gecko) Mobile Safari/537.36 (compatible; PetalBot;+https://webmaster.petalsearch.com/site/petalbot)',
  'Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)',
  'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 (Amazonbot/0.1; +https://developer.amazon.com/support/amazonbot)',
  'meta-externalagent/1.1 (+https://developers.facebook.com/docs/sharing/webmasters/crawler)',
  'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; PerplexityBot/1.0; +https://perplexity.ai/perplexitybot)',
  'DuckDuckBot/1.1; (+http://duckduckgo.com/duckduckbot.html)',
  'Lightpanda/1.0',
];

const ENV = {};
function edge(url, init) {
  const request = new Request(url, init || {});
  Object.defineProperty(request, 'cf', { value: { colo: 'TEST', country: 'US' } });
  return request;
}
function setup() {
  const state = { calls: [], stored: new Map(), waits: [] };
  globalThis.caches = { default: {
    async match(req) { return state.stored.get(req.url)?.clone(); },
    async put(req, response) { state.stored.set(req.url, response.clone()); },
    async delete() { return false; },
  } };
  globalThis.fetch = async (input, options) => {
    state.calls.push(String(input));
    return new Response(JSON.stringify({ rows: [1, 2, 3] }), { status: 200, headers: { 'Content-Type': 'application/json', ETag: '"e1"' } });
  };
  state.context = { waitUntil(p) { state.waits.push(p); } };
  return state;
}

test('the Worker token list is exactly config/crawler-user-agents.json worker_tokens, and s3_tokens is a subset', async () => {
  const gate = await import(gateSource);
  assert.deepEqual(gate.CRAWLER_TOKENS, CONFIG.worker_tokens);
  for (const t of CONFIG.s3_tokens) assert.ok(CONFIG.worker_tokens.includes(t), `${t} is in worker_tokens`);
  for (const t of CONFIG.worker_tokens) {
    assert.ok(!/^(bot|crawler|spider|robot)$/i.test(t), `${t} is not a generic word`);
    assert.ok(t.length >= 5, `${t} is specific enough`);
  }
});

test('every crawler user agent is refused on a data file, before any S3 read', async () => {
  const worker = (await import(source)).default;
  for (const ua of CRAWLER_UAS) {
    const state = setup();
    const r = await worker.fetch(edge('https://justhodl.ai/data/best-setups.json', { headers: { 'User-Agent': ua } }), ENV, state.context);
    assert.equal(r.status, 403, ua);
    assert.equal(r.headers.get('X-JH-Crawler-Gate'), 'refused');
    assert.equal(r.headers.get('X-Robots-Tag'), 'noindex, nofollow');
    assert.equal(r.headers.get('Cache-Control'), 'no-store');
    assert.equal(r.headers.get('Access-Control-Allow-Origin'), '*');
    assert.match(JSON.parse(await r.text()).error, /crawler/);
    assert.equal(state.calls.length, 0, 'no S3 read, no egress: ' + ua);
  }
});

test('crawlers are refused on the other data routes too (workers.dev host, API, symbol directory, gov proxy)', async () => {
  const worker = (await import(source)).default;
  const ua = CRAWLER_UAS[0];
  for (const u of ['https://justhodl-data-proxy.raafouis.workers.dev/data/share-flows.json',
                   'https://justhodl-data-proxy.raafouis.workers.dev/equity-research/NVDA.json',
                   'https://justhodl.ai/api/v1/regime', 'https://justhodl-data-proxy.raafouis.workers.dev/symsearch?q=AAPL',
                   'https://justhodl-data-proxy.raafouis.workers.dev/gov?u=https%3A%2F%2Fapi.imf.org%2Fx']) {
    const state = setup();
    const r = await worker.fetch(edge(u, { headers: { 'User-Agent': ua } }), ENV, state.context);
    assert.equal(r.status, 403, u);
    assert.equal(state.calls.length, 0, u);
  }
});

test('every client the system uses reaches the data exactly as before', async () => {
  const worker = (await import(source)).default;
  for (const ua of CONFIG.must_pass) {
    const state = setup();
    const headers = ua ? { 'User-Agent': ua } : {};
    const r = await worker.fetch(edge('https://justhodl.ai/data/best-setups.json', { headers }), ENV, state.context);
    assert.equal(r.status, 200, `must pass: "${ua}"`);
    assert.equal(r.headers.get('X-JH-Crawler-Gate'), null);
    assert.deepEqual(JSON.parse(await r.text()).rows, [1, 2, 3]);
    assert.equal(state.calls.length, 1, `one S3 read for "${ua}"`);
  }
});

test('health and the workers.dev robots file stay open to crawlers; justhodl.ai robots is not served here', async () => {
  const worker = (await import(source)).default;
  const ua = CRAWLER_UAS[1];
  let state = setup();
  let r = await worker.fetch(edge('https://justhodl-data-proxy.raafouis.workers.dev/robots.txt', { headers: { 'User-Agent': ua } }), ENV, state.context);
  assert.equal(r.status, 200);
  assert.equal(await r.text(), 'User-agent: *\nDisallow: /\n');
  assert.equal(state.calls.length, 0);
  state = setup();
  r = await worker.fetch(edge('https://justhodl-data-proxy.raafouis.workers.dev/health', { headers: { 'User-Agent': ua } }), ENV, state.context);
  assert.notEqual(r.status, 403, 'health answers a crawler');
  assert.equal(r.headers.get('X-JH-Crawler-Gate'), null);
  const gate = await import(gateSource);
  const site = new Request('https://justhodl.ai/robots.txt', { headers: { 'User-Agent': ua } });
  assert.equal(gate.crawlerGate(site, new URL(site.url), {}), null, 'justhodl.ai/robots.txt is GitHub Pages, never answered by the gate');
});

test('only GET and HEAD are gated: POST, PUT and OPTIONS from a crawler user agent pass through unchanged', async () => {
  const gate = await import(gateSource);
  const ua = CRAWLER_UAS[0];
  for (const method of ['POST', 'PUT', 'OPTIONS', 'DELETE']) {
    const req = new Request('https://justhodl.ai/data/best-setups.json', { method, headers: { 'User-Agent': ua } });
    assert.equal(gate.crawlerGate(req, new URL(req.url), {}), null, method);
  }
  const head = new Request('https://justhodl.ai/data/best-setups.json', { method: 'HEAD', headers: { 'User-Agent': ua } });
  const r = gate.crawlerGate(head, new URL(head.url), {});
  assert.equal(r.status, 403);
  assert.equal(r.body, null, 'a HEAD answer carries no body');
});

test('matching is case-insensitive and needs the whole token', async () => {
  const gate = await import(gateSource);
  assert.equal(gate.isCrawler('mozilla/5.0 (compatible; GOOGLEBOT/2.1)'), true);
  assert.equal(gate.isCrawler('applebot'), true);
  assert.equal(gate.isCrawler('Mozilla/5.0 (compatible; JustHodlBot/1.0)'), false);
  assert.equal(gate.isCrawler('justhodl-tv-crawler/2.0'), false);
  assert.equal(gate.isCrawler(''), false);
  assert.equal(gate.isCrawler(null), false);
  assert.equal(gate.isCrawler(undefined), false);
});
