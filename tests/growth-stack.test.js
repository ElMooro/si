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
        { value: 18000000000, start: "2025-06-30", end: "2025-09-28" },
        { value: 18600000000, start: "2025-09-29", end: "2025-12-31" },
        { value: 17900000000, start: "2026-01-01", end: "2026-03-29" },
        { value: 20500000000, start: "2026-03-30", end: "2026-06-28" }
      ]
    }
  }, {
    ticker: "ORCL",
    data: {
      rpo_latest_usd: 99000000000,
      revenue_history: [
        { value: 13000000000, start: "2025-06-01", end: "2025-08-31" },
        { value: 14000000000, start: "2025-09-01", end: "2025-11-30" },
        { value: 14100000000, start: "2025-12-01", end: "2026-02-28" },
        { value: 15900000000, start: "2026-03-01", end: "2026-05-31" }
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

test("absent, blank, boolean and nonfinite inputs cannot manufacture deferred balances", () => {
  for(const bad of [null,undefined,""," ",false,true,[],{},Infinity,"NaN","0x10"]){
    for(const field of ['deferred_rev','deferred_yoy']){
      const b=structuredClone(backlog);b.by_ticker.LMT[field]=bad;
      const api=load();api.loadFromDocs(b,fo,{}, {},{});assert.equal(api.lookup('LMT').billings.billings,null);
    }
  }
  const b=structuredClone(backlog);b.by_ticker.LMT.deferred_rev=0;b.by_ticker.LMT.deferred_yoy=0;b.by_ticker.LMT.rpo=0;
  const f=structuredClone(fo);f.all_results[0].data.rpo_latest_usd=0;
  const api=load();api.loadFromDocs(b,f,{}, {},{});const row=api.lookup('LMT').billings;
  assert.equal(row.billings,75000000000);assert.equal(row.coverage_yrs,0);assert.equal(row.deferred,0);
  for(const flag of ['calls_eligible','forecast_qualified','sizing_eligible','execution_eligible'])assert.equal(row[flag],false);
});

test("revenue requires explicit complete durations, not a last annual or quarterly value", () => {
  const api=load(),quarters=fo.all_results[0].data.revenue_history;
  assert.equal(api.ttmFromHistory(quarters),75000000000);
  assert.equal(api.ttmFromHistory([{value:123,start:'2025-01-01',end:'2025-12-31'}]),123);
  assert.equal(api.ttmFromHistory([{value:123,end:'2025-12-31',fy:2025}]),null);
  assert.equal(api.ttmFromHistory([{value:123,start:'2999-01-01',end:'2999-12-31'}]),null);
  assert.equal(api.ttmFromHistory(quarters.map(({start,...r})=>r)),null);
  assert.equal(api.ttmFromHistory(quarters.slice(1)),null);
  for(const alter of [a=>a[1].start='2025-09-30',a=>a[1].start='2025-09-28',a=>a[1].start='2025-06-30',a=>a[2].end='2026-02-30',a=>a[2].value=null,a=>a[2].value=false,a=>a[2].value=-1,a=>a.push({...a.at(-1)}),a=>a.forEach(r=>r.value=1e308)]){
    const rows=structuredClone(quarters);alter(rows);assert.equal(api.ttmFromHistory(rows),null);
  }
});

test("growth gaps are not book-to-bill and whole predecessor files remain preserved", () => {
  const crypto=require('node:crypto');
  for(const [name,hash] of [['jh-growth-stack.js','3b164cf329593212a8e41f67d7cf013e40737d11f202284019ed1298162972b4'],['billings-crpo.html','6e80cea745ac4e841f5535327f4342a6f3b9dae086b93d71974b82c227561e45']]){
    const raw=fs.readFileSync(path.join(__dirname,'fixtures/pre-billings-integrity-'+name+'.txt'));assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),hash);
  }
  const html=fs.readFileSync(path.join(__dirname,'../billings-crpo.html'),'utf8');assert.match(html,/RPO–revenue growth gap/);assert.match(html,/history omits start dates/);assert.match(html,/unqualified estimate/);assert.doesNotMatch(html,/next-year sales path/);
});
