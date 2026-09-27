const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
function loadAll() {
  const ctx = { window: {}, module: { exports: {} }, console, Date };
  ctx.globalThis = ctx.window;
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(root, "jh-orderbook.js"), "utf8"), ctx);
  vm.runInContext(fs.readFileSync(path.join(root, "jh-book-fuse.js"), "utf8"), ctx);
  return ctx.window;
}

const LMT_HIST = [
  { value: 230400000000, end: "2026-06-28", form: "10-Q", fp: "Q2" },
  { value: 186400000000, end: "2026-03-29", form: "10-Q", fp: "Q1" },
  { value: 193600000000, end: "2025-12-31", form: "10-K", fp: "FY" },
  { value: 179100000000, end: "2025-09-28", form: "10-Q", fp: "Q3" },
  { value: 166500000000, end: "2025-06-29", form: "10-Q", fp: "Q2" }
];

test("fuse lookup LMT uses mechanical book QoQ/YoY and radar EV/RPO", () => {
  const w = loadAll();
  const fo = {
    all_results: [{
      ticker: "LMT",
      name: "Lockheed Martin",
      sector: "Industrials",
      score: 81,
      data: {
        rpo_latest_usd: 230400000000,
        rpo_as_of: "2026-06-28",
        rpo_history: LMT_HIST
      }
    }]
  };
  const backlog = {
    by_ticker: {
      LMT: {
        ticker: "LMT",
        rpo: 230400000000,
        rpo_asof: "2026-06-28",
        rpo_yoy: 38.4,
        rpo_qoq: 23.6,
        rev_yoy: 5.1,
        rpo_minus_rev_growth: 33.3,
        ev_to_rpo: 0.52,
        demand_accelerating: true
      }
    }
  };
  w.jhBookFuse.loadFromDocs(backlog, {}, fo, {}, Date.parse("2026-09-27T00:00:00Z"));
  const hit = w.jhBookFuse.lookup("lmt");
  assert.equal(hit.book_usd, 230400000000);
  assert.equal(hit.qoq, 23.6);
  assert.equal(hit.yoy, 38.4);
  assert.equal(hit.mom, null);
  assert.equal(hit.accelerating, true);
  assert.equal(hit.ev_to_rpo, 0.52);
  assert.equal(hit.divergence, 33.3);
  assert.equal(hit.fo_score, 81);
  assert.equal(hit.has_book, true);
  assert.equal(hit.has_radar, true);
  assert.equal(hit.has_fo, true);
});

test("fuse lookup missing ticker is null", () => {
  const w = loadAll();
  w.jhBookFuse.loadFromDocs({ by_ticker: {} }, {}, { all_results: [] }, {});
  assert.equal(w.jhBookFuse.lookup("ZZZZ"), null);
});

test("fuse strip is safe and links the three desks", () => {
  const w = loadAll();
  const fo = { all_results: [{ ticker: "ORCL", data: { rpo_latest_usd: 190000000000, rpo_as_of: "2026-08-31", rpo_history: [{ value: 190000000000, end: "2026-08-31" }, { value: 170000000000, end: "2025-08-31" }] } }] };
  w.jhBookFuse.loadFromDocs({}, {}, fo, {}, Date.parse("2026-09-27T00:00:00Z"));
  const html = w.jhBookFuse.stripHtml(w.jhBookFuse.lookup("ORCL"));
  assert.match(html, /\$190\.0B/);
  assert.match(html, /\/orderbook\.html/);
  assert.match(html, /\/backlog\.html/);
  assert.match(html, /\/forward-orders\.html/);
  assert.doesNotMatch(html, /\[object Object\]/);
  assert.doesNotMatch(html, /undefined%/);
  assert.doesNotMatch(html, />\u2014</);
});
