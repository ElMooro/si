const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function load() {
  const ctx = { window: {}, module: { exports: {} }, console };
  ctx.globalThis = ctx.window;
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname, "..", "jh-growth-stack.js"), "utf8"), ctx);
  return ctx.window.jhGrowthStack;
}

const backlog = {
  by_ticker: {
    LMT: {
      ticker: "LMT", sector: "Industrials",
      rpo: 230400000000, rpo_yoy: 38.4, rpo_qoq: 23.6,
      deferred_rev: 12151000000, deferred_yoy: 23.2,
      rev_yoy: 10.5, demand_accelerating: true
    },
    ORCL: {
      ticker: "ORCL", sector: "Technology",
      rpo: 99000000000, rpo_yoy: 41.0
    }
  }
};
const fo = {
  all_results: [{
    ticker: "LMT", sector: "Industrials",
    data: {
      rpo_latest_usd: 230400000000,
      book_to_bill_spread_pct: 27.9,
      revenue_history: [
        { value: 18000000000, end: "2025-09-28" },
        { value: 18600000000, end: "2025-12-31" },
        { value: 17900000000, end: "2026-03-29" },
        { value: 20500000000, end: "2026-06-28" }
      ]
    }
  }, {
    ticker: "ORCL",
    data: {
      rpo_latest_usd: 99000000000,
      revenue_history: [
        { value: 13000000000, end: "2025-08-31" },
        { value: 14000000000, end: "2025-11-30" },
        { value: 14100000000, end: "2026-02-28" },
        { value: 15900000000, end: "2026-05-31" }
      ]
    }
  }]
};

test("LMT billings uses TTM + deferred change and coverage = RPO/TTM", () => {
  const api = load();
  api.loadFromDocs(backlog, fo, {}, {}, {});
  const b = api.lookup("lmt").billings;
  assert.equal(b.ttm_rev, 75000000000);
  assert.ok(Math.abs(b.coverage_yrs - 230.4/75) < 1e-9);
  const priorDef = 12151000000 / (1 + 23.2/100);
  assert.ok(Math.abs(b.billings - (75000000000 + (12151000000 - priorDef))) < 1);
});

test("billings stays null when deferred change is missing", () => {
  const api = load();
  api.loadFromDocs(backlog, fo, {}, {}, {});
  const b = api.lookup("ORCL").billings;
  assert.equal(b.ttm_rev, 57000000000);
  assert.equal(b.billings, null);
  assert.ok(Math.abs(b.coverage_yrs - 99/57) < 1e-9);
});

test("billings-crpo page has purpose footer and live feeds", () => {
  const html = fs.readFileSync(path.join(__dirname, "..", "billings-crpo.html"), "utf8");
  assert.match(html, /Built for:/);
  assert.match(html, /Data:/);
  assert.match(html, /backlog\.json/);
  assert.match(html, /forward-orders\.json/);
  assert.match(html, /jh-growth-stack\.js/);
  assert.doesNotMatch(html, /PLACEHOLDER/);
  assert.doesNotMatch(html, /\[object Object\]/);
});
