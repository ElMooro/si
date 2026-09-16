const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const goSrc = fs.readFileSync(path.join(root, "jh-chart-bbgo.js"), "utf8");
const desk = fs.readFileSync(path.join(root, "bb-go.html"), "utf8");
const rail = fs.readFileSync(path.join(root, "jh-chart-tvrail.js"), "utf8");

function loadGo() {
  const ctx = {
    window: {},
    document: {
      readyState: "complete",
      addEventListener() {},
      getElementById() { return null; },
    },
    location: { search: "" },
    URLSearchParams,
  };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(goSrc, ctx);
  return ctx.window.jhBbGo;
}

test("chart header still has ISIN/CUSIP search; GO lives on cmdk and yellow bar", () => {
  assert.match(html, /Symbol, ISIN, or CUSIP/);
  assert.match(html, /id="cmdin"/);
  assert.match(html, /AAPL DES <GO>/);
  assert.match(html, /jh-chart-bbgo\.js/);
  assert.match(html, /id="btn-dtype"/);
  assert.doesNotMatch(html, /\[object Object\]/);
  assert.doesNotMatch(html, /undefined%/);
});

test("yellow-key parser strips US EQUITY <GO> and does not steal a lone ticker", () => {
  const go = loadGo();
  const a = go.parse("AAPL US EQUITY DES <GO>");
  assert.equal(a.fn, "DES");
  assert.equal(a.sym, "AAPL");
  const b = go.parse("des aapl");
  assert.equal(b.fn, "DES");
  assert.equal(b.sym, "AAPL");
  const c = go.parse("NVDA");
  assert.equal(c.fn, null);
  assert.equal(c.sym, "NVDA");
  const d = go.parse("MOST");
  assert.equal(d.fn, "MOST");
  const e = go.parse("AAPL MAGS");
  assert.equal(e.fn, "MAGS");
  assert.equal(e.sym, "AAPL");
  const run = go.tryRun("aapl", { yellow: true });
  assert.equal(run.ok, true);
  assert.equal(run.fn, "GP");
  assert.equal(run.sym, "AAPL");
  const search = go.parse("AAPL");
  assert.equal(search.fn, null, "symbol search must still receive a lone ticker");
});

test("popular Terminal functions are on the keyboard and mapped to harvests", () => {
  const go = loadGo();
  const ids = go.catalog().map((f) => f.id);
  ["DES", "FA", "GP", "GIP", "HP", "EE", "CN", "DVD", "HDS", "OMON", "SPLC", "CF", "MA", "CACS", "ERN", "PEAD", "OWN", "13F", "13D", "MAGS", "MOST", "WEI", "ECO", "YCRV", "WIRP", "FOMC", "COT", "VIX", "DARK", "PORT", "MEMB", "FL", "ETF", "TRA", "GSEAS", "ICHI", "GPEX", "SESS", "LOG", "PCT", "IDX", "HIVOL", "FVG", "OR", "ADR", "GPDESK", "VSSPX", "RSPX", "REL", "RV"].forEach((id) => {
    assert.ok(ids.indexOf(id) >= 0, "missing " + id);
  });
  const splc = go.catalog().filter((f) => f.id === "SPLC")[0];
  assert.equal(splc.ws, "splc");
  const gp = go.catalog().filter((f) => f.id === "GP")[0];
  assert.ok(gp);
});

test("engine ships Ichimoku displacement, cloud, RTH session, splits, and yellow GO bar", () => {
  assert.match(engine, /spanA\.push\(\{time:d\[i\]\.time\+26\*step/);
  assert.match(engine, /chikou\.push/);
  assert.match(engine, /function paintIchCloud/);
  assert.match(engine, /function paintSessionShade/);
  assert.match(engine, /function splitMarks/);
  assert.match(engine, /id=goyell/);
  assert.match(engine, /class=gokey id=btn-go/);
  assert.match(engine, /tryRun\(raw, \{ yellow: true \}/);
  assert.match(engine, /gyKeep/);
  assert.match(engine, /goSymbol\(v\.toUpperCase\(\), "chart"\)/);
  assert.match(engine, /id:"sess"/);
  assert.match(engine, /id:"split"/);
  assert.match(engine, /jhSetKind/);
  assert.match(engine, /jhSetScale/);
  assert.match(engine, /Bloomberg <GO>/);
  assert.doesNotMatch(engine, /\[object Object\]/);
});

test("workspace overlay maps SPLC CF MA PORT MEMB and GO", () => {
  assert.match(rail, /splc: \["Supply chain"/);
  assert.match(rail, /cf: \["SEC filings"/);
  assert.match(rail, /go: \["Bloomberg <GO>/);
  assert.match(desk, /JUSTHODL <GO>/);
  assert.match(desk, /jh-chart-bbgo\.js/);
  assert.doesNotMatch(desk, /\[object Object\]/);
  assert.doesNotMatch(desk, /undefined%/);
});
