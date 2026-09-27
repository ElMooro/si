const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const src = fs.readFileSync(path.join(root, "jh-orderbook.js"), "utf8");
const html = fs.readFileSync(path.join(root, "orderbook.html"), "utf8");

function load() {
  const ctx = { window: {}, module: { exports: {} }, console };
  ctx.globalThis = ctx.window;
  vm.createContext(ctx);
  vm.runInContext(src, ctx);
  return ctx.window.jhOrderbook || ctx.module.exports;
}

const LMT_HIST = [
  { value: 230400000000, end: "2026-06-28", form: "10-Q", fp: "Q2" },
  { value: 186400000000, end: "2026-03-29", form: "10-Q", fp: "Q1" },
  { value: 193600000000, end: "2025-12-31", form: "10-K", fp: "FY" },
  { value: 179100000000, end: "2025-09-28", form: "10-Q", fp: "Q3" },
  { value: 166500000000, end: "2025-06-29", form: "10-Q", fp: "Q2" }
];

test("LMT QoQ and YoY match adjacent SEC prints; MoM is null on a 91-day gap", () => {
  const api = load();
  const ch = api.computeChanges(LMT_HIST, 230400000000, "2026-06-28", "Q2");
  assert.equal(ch.qoq, 23.6);
  assert.equal(ch.yoy, 38.4);
  assert.equal(ch.mom, null);
});

test("MoM prints only for a 20-40 day pair", () => {
  const api = load();
  const hist = [
    { value: 110, end: "2026-02-20" },
    { value: 100, end: "2026-01-21" }
  ];
  const ch = api.computeChanges(hist, 110, "2026-02-20");
  assert.equal(ch.mom, 10);
  assert.equal(ch.qoq, null);
  assert.equal(ch.yoy, null);
});

test("stale 2018 RPO loses to fresh deferred or mined backlog", () => {
  const api = load();
  const now = Date.parse("2026-09-27T00:00:00Z");
  const book = api.chooseBook(
    { rpo: 5.5e9, rpo_asof: "2018-07-31", deferred_rev: 4.459e9, deferred_asof: "2026-07-31", deferred_qoq: 1.5, deferred_yoy: 13.8 },
    { status: "MINED", backlog_usd: 2.74e10, asof: "2026-08-27", backlog_qoq_pct: 0.4, backlog_yoy_pct: 7.9 },
    now
  );
  assert.equal(book.kind, "backlog");
  assert.equal(book.usd, 2.74e10);
});

test("mined LMT unit error is rejected when XBRL RPO is current", () => {
  const api = load();
  const now = Date.parse("2026-09-27T00:00:00Z");
  const book = api.chooseBook(
    { rpo: 230400000000, rpo_asof: "2026-06-28", rpo_qoq: 23.6, rpo_yoy: 38.4 },
    { status: "MINED", backlog_usd: 186400000, asof: "2026-04-23", backlog_qoq_pct: 4.1 },
    now
  );
  assert.equal(book.kind, "rpo");
  assert.equal(book.usd, 230400000000);
  assert.equal(api.plausibleVsXbrl(186400000, 230400000000), false);
});

test("sort keeps null MoM last in both directions", () => {
  const api = load();
  const rows = [
    { ticker: "AAA", mom: null, book_usd: 1 },
    { ticker: "BBB", mom: 5, book_usd: 2 },
    { ticker: "CCC", mom: -2, book_usd: 3 }
  ];
  const desc = api.sortRows(rows, "mom", "desc");
  assert.deepEqual(desc.map((r) => r.ticker), ["BBB", "CCC", "AAA"]);
  const asc = api.sortRows(rows, "mom", "asc");
  assert.deepEqual(asc.map((r) => r.ticker), ["CCC", "BBB", "AAA"]);
});

test("forward-orders-only name becomes a row and mined unit bug loses to FO RPO", () => {
  const api = load();
  const now = Date.parse("2026-09-27T00:00:00Z");
  const fo = {
    ticker: "ORCL",
    name: "Oracle Corporation",
    sector: "Technology",
    data: {
      rpo_latest_usd: 190000000000,
      rpo_as_of: "2026-08-31",
      rpo_tag: "RevenueRemainingPerformanceObligation",
      rpo_growth_yoy_pct: 12.5,
      rpo_history: [
        { value: 190000000000, end: "2026-08-31", fp: "Q1" },
        { value: 170000000000, end: "2025-08-31", fp: "Q1" }
      ]
    }
  };
  const rows = api.buildRows({}, {}, { all_results: [fo] }, now);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].ticker, "ORCL");
  assert.equal(rows[0].book_src, "forward");
  assert.equal(rows[0].book_usd, 190000000000);
  assert.equal(rows[0].yoy, 11.8);
  const book = api.chooseBook(
    {},
    { status: "MINED", backlog_usd: 186400000, asof: "2026-04-23" },
    now,
    { data: { rpo_latest_usd: 230400000000, rpo_as_of: "2026-06-28" } }
  );
  assert.equal(book.src, "forward");
  assert.equal(book.usd, 230400000000);
});

test("price join maps momentum-scanner slices onto rows", () => {
  const api = load();
  const rows = [{ ticker: "NVDA", book_usd: 1 }];
  api.attachPrices(rows, { rankings: { composite_top_50: [{ ticker: "NVDA", ret_1m: 8.12, ret_3m: 21.4, ret_12m: 99.9, last_close: 210.5 }] } });
  assert.equal(rows[0].price_mom, 8.1);
  assert.equal(rows[0].price_qoq, 21.4);
  assert.equal(rows[0].price_yoy, 99.9);
});

test("price book rows come from momentum rankings with MoM/QoQ/YoY", () => {
  const api = load();
  const rows = api.priceBookRows({ rankings: { composite_top_50: [{ ticker: "MRNA", name: "Moderna", sector: "Healthcare", ret_1m: 32.89, ret_3m: 195.64, ret_12m: 668.47, last_close: 198.88 }] } });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].ticker, "MRNA");
  assert.equal(rows[0].price_mom, 32.9);
  assert.equal(rows[0].price_qoq, 195.6);
  assert.equal(rows[0].price_yoy, 668.5);
});

test("page loads the engine and the public feeds; esc and placeholders are safe", () => {
  assert.match(html, /jh-orderbook\.js/);
  assert.match(html, /\/data\/backlog\.json/);
  assert.match(html, /\/data\/backlog-mined\.json/);
  assert.match(html, /\/data\/forward-orders\.json/);
  assert.match(html, /\/data\/momentum-scanner\.json/);
  assert.match(html, /id="q"/);
  assert.match(html, /data-k/);
  assert.match(html, /data-view="price"/);
  assert.match(html, /function esc/);
  assert.match(html, /\\u0026amp;/);
  assert.match(html, /justhodl-dashboard-live\.s3\.us-east-1\.amazonaws\.com/);
  assert.doesNotMatch(html, /\[object Object\]/);
  assert.doesNotMatch(html, /undefined%/);
  assert.doesNotMatch(html, />—</);
  const start = html.indexOf("function esc");
  const snippet = html.slice(start, html.indexOf("function pct"));
  assert.match(snippet, /\\u0026amp;/);
});
