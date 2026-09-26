const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const instSrc = fs.readFileSync(path.join(root, "jh-chart-inst.js"), "utf8");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");
const goSrc = fs.readFileSync(path.join(root, "jh-chart-bbgo.js"), "utf8");
const indux = fs.readFileSync(path.join(root, "jh-chart-indux.js"), "utf8");

function loadInst() {
  const ctx = { window: {}, console, Date, Math };
  ctx.window = ctx;
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(instSrc, ctx);
  return ctx.window.jhInst;
}
function bar(t, o, h, l, c, v) {
  return { time: t, open: o, high: h, low: l, close: c, volume: v || 1e6 };
}

test("chart.html loads inst studies and a visible go-to-date", () => {
  assert.match(html, /jh-chart-inst\.js\?v=20260916aa-fix/);
  assert.match(html, /jh-chart-engine\.js\?v=20260926-frame-identity/);
  assert.match(html, /jh-chart-bbgo\.js\?v=20260915af-spx/);
  assert.match(html, /#tfbar #goto/);
  assert.doesNotMatch(html, /\[object Object\]/);
  assert.doesNotMatch(html, /undefined%/);
});

test("engine wires FVG OR ADR AVWAP HUD and weekly/monthly open", () => {
  assert.match(engine, /id:"fvg"/);
  assert.match(engine, /id:"or15"/);
  assert.match(engine, /id:"adr"/);
  assert.match(engine, /id:"eavwap"/);
  assert.match(engine, /id:"lrch"/);
  assert.match(engine, /id:"eqh"/);
  assert.match(engine, /id:"gsess"/);
  assert.match(engine, /id:"ins"/);
  assert.match(engine, /id:"buyb"/);
  assert.match(engine, /id:"ratio"/);
  assert.match(engine, /id:"adrpct"/);
  assert.match(engine, /function paintGlobalSess/);
  assert.match(engine, /wOpen:wOpen, mOpen:mOpen/);
  assert.match(engine, /id=goto type=date/);
  assert.doesNotMatch(engine, /id=goto type=date title='Go to date' style=display:none/);
  assert.match(engine, /ATR /);
  assert.match(engine, /POC /);
  assert.match(engine, /await klines\(active, "5m"/);
  assert.doesNotMatch(engine, /\[object Object\]/);
});

test("FVG detects a 3-candle bullish imbalance and marks it unfilled", () => {
  const inst = loadInst();
  const d = [
    bar(1, 10, 10.2, 9.8, 10),
    bar(2, 10, 10.1, 9.9, 10),
    bar(3, 12, 12.4, 12.0, 12.2),
    bar(4, 12.2, 12.5, 12.1, 12.3)
  ];
  const gaps = inst.fvgGaps(d);
  assert.ok(gaps.length >= 1);
  const g = gaps[0];
  assert.equal(g.dir, 1);
  assert.equal(g.lo, 10.2);
  assert.equal(g.hi, 12.0);
  assert.equal(g.filled, false);
  const pack = inst.fvgPack(d);
  assert.ok(pack.zones.length >= 1);
  assert.match(pack.zones[0].lab, /FVG/);
});

test("FVG marks filled once price trades back through the gap", () => {
  const inst = loadInst();
  const d = [
    bar(1, 10, 10.2, 9.8, 10),
    bar(2, 10, 10.1, 9.9, 10),
    bar(3, 12, 12.4, 12.0, 12.2),
    bar(4, 11, 11.2, 10.0, 10.1)
  ];
  const gaps = inst.fvgGaps(d);
  assert.ok(gaps.some(function (g) { return g.dir === 1 && g.filled; }));
});

test("ADR 20 is the mean of completed daily ranges, not today's", () => {
  const inst = loadInst();
  const d = [];
  for (let i = 0; i < 25; i++) {
    d.push(bar(1_700_000_000 + i * 86400, 100, 102, 100, 101));
  }
  d.push(bar(1_700_000_000 + 25 * 86400, 101, 103, 101, 102));
  const a = inst.adr20(d, 20);
  assert.ok(a);
  assert.equal(a.adr, 2);
  assert.equal(a.hi, 101 + 2);
  assert.equal(a.lo, 101 - 2);
  assert.ok(a.used > 0);
});

test("LinReg channel returns mid ±2σ and extends past the last bar", () => {
  const inst = loadInst();
  const d = [];
  for (let i = 0; i < 40; i++) {
    const c = 10 + i + (i % 2 ? 1.4 : -1.1);
    d.push(bar(1000 + i * 86400, c, c + 0.4, c - 0.4, c));
  }
  const ch = inst.linregChannel(d, 30, 2);
  assert.ok(ch.m.length > 30);
  assert.ok(ch.up.length === ch.m.length);
  assert.ok(ch.sigma > 0);
  assert.ok(ch.dn[10].value < ch.m[10].value);
  assert.ok(ch.up[10].value > ch.m[10].value);
  assert.ok(ch.m[ch.m.length - 1].time > d[d.length - 1].time);
});

test("Opening range uses 5m NY RTH and refuses daily bars", () => {
  const inst = loadInst();
  const daily = [bar(1_700_000_000, 10, 11, 9, 10), bar(1_700_086_400, 10, 12, 8, 11)];
  assert.equal(inst.sessionLevels(daily), null);
  const t0 = Date.parse("2026-09-15T13:30:00Z") / 1000;
  const intra = [];
  for (let i = 0; i < 18; i++) {
    const px = 100 + (i < 3 ? i : 2);
    intra.push(bar(t0 + i * 300, px, px + 1, px - 0.5, px + 0.2));
  }
  const lv = inst.sessionLevels(intra);
  assert.ok(lv && lv.ready);
  assert.ok(lv.or15);
  assert.ok(lv.or15.hi >= lv.or15.lo);
  assert.ok(lv.ib);
});

test("equal highs cluster two swing highs within ATR", () => {
  const inst = loadInst();
  const d = [];
  for (let i = 0; i < 42; i++) {
    let px;
    if (i <= 10) px = 10 + i;
    else if (i <= 20) px = 20 - (i - 10);
    else if (i <= 30) px = 10 + (i - 20);
    else px = 20.04 - (i - 30);
    d.push(bar(1000 + i * 86400, px, px + 0.08, px - 0.08, px));
  }
  const pack = inst.equalHL(d);
  assert.ok(pack.lines.some(function (l) { return /EQH/.test(l.lab); }), JSON.stringify(pack.lines));
});

test("AVWAP from a timestamp starts at that bar, not the series open", () => {
  const inst = loadInst();
  const d = [
    bar(10, 10, 10, 10, 10, 100),
    bar(20, 20, 20, 20, 20, 100),
    bar(30, 30, 30, 30, 30, 100)
  ];
  const a = inst.avwapFrom(d, 20);
  assert.equal(a.length, 2);
  assert.equal(a[0].value, 20);
});

test("GO catalog maps FVG OR ADR EAVWAP GPDESK without stealing INS page", () => {
  const ctx = {
    window: {},
    document: { readyState: "complete", addEventListener() {}, getElementById() { return null; } },
    location: { search: "" },
    URLSearchParams
  };
  ctx.window = ctx;
  vm.createContext(ctx);
  vm.runInContext(goSrc, ctx);
  const ids = ctx.window.jhBbGo.catalog().map((f) => f.id);
  ["FVG", "EQH", "OR", "IB", "ONH", "ADR", "LRCH", "EAVWAP", "GSESS", "SEP", "BUYB", "INSC", "RATIO", "VSSPX", "RSPX", "REL", "GPDESK"].forEach((id) => {
    assert.ok(ids.indexOf(id) >= 0, "missing " + id);
  });
  const ins = ctx.window.jhBbGo.catalog().filter((f) => f.id === "INS")[0];
  assert.equal(ins.href, "/insider-clusters.html");
  const r = ctx.window.jhBbGo.parse("AAPL FVG");
  assert.equal(r.fn, "FVG");
  assert.equal(r.sym, "AAPL");
  const v = ctx.window.jhBbGo.parse("AAPL VSSPX");
  assert.equal(v.fn, "VSSPX");
  assert.equal(v.sym, "AAPL");
  assert.equal(ctx.window.jhBbGo.catalog().filter((f) => f.id === "VSSPX")[0].osc, "vsspx");
});

test("indux documents the new studies honestly", () => {
  assert.match(instSrc, /n - \(opt\.lookback \|\| 280\)/);
  assert.match(instSrc, /d\.length > 260\) d = d\.slice\(-260\)/);
  assert.match(indux, /Fair Value Gaps/);
  assert.match(indux, /Needs 5m bars/);
  assert.match(indux, /No tick, no DOM/);
  assert.match(indux, /AVWAP from earnings/);
  assert.match(indux, /cash index 1927/);
  assert.match(indux, /not SPY's 1993/);
  assert.match(indux, /vsspx: \["vs S&P 500"/);
  assert.match(indux, /Not total-return\. Not SPY/);
});

test("engine wires vs S&P 500 cash, loadBench, and prefers yf GSPC", () => {
  assert.match(engine, /id:"vsspx"/);
  assert.match(engine, /n:"vs S&P 500"/);
  assert.match(engine, /function loadBench/);
  assert.match(engine, /function loadSpxDaily/);
  assert.match(engine, /klines\("\^GSPC"/);
  assert.match(engine, /lastBenchName="SPY"/);
  assert.match(engine, /vs SPY \(intraday\)/);
  assert.match(engine, /window\.lastVsSpx/);
  assert.match(engine, /function fmtXs/);
  assert.match(engine, /vs SPX 1d /);
  assert.doesNotMatch(engine, /7\s*\*\s*86400/);
  assert.doesNotMatch(instSrc, /7\s*\*\s*86400/);
  assert.match(instSrc, /function alignExact/);
  assert.match(instSrc, /function vsSpxPack/);
});

function dayTs(ymd) {
  return Date.parse(ymd + "T18:00:00.000Z") / 1000;
}

test("identical path vs SPX stays RS 100 and zero excess", () => {
  const inst = loadInst();
  const d = [], b = [];
  const px = [100, 102, 105, 110, 108];
  const days = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"];
  for (let i = 0; i < days.length; i++) {
    d.push(bar(dayTs(days[i]), px[i], px[i], px[i], px[i]));
    b.push(bar(dayTs(days[i]), px[i] * 10, px[i] * 10, px[i] * 10, px[i] * 10));
  }
  const pack = inst.vsSpxPack(d, b);
  assert.equal(pack.n, 5);
  pack.rs.forEach((p) => assert.ok(Math.abs(p.value - 100) < 1e-9, String(p.value)));
  assert.ok(Math.abs(pack.last.all) < 1e-12);
  assert.ok(Math.abs(pack.last.d1) < 1e-12);
  assert.equal(pack.last.rs, 100);
});

test("name +20% vs SPX +10% is RS 109.09 and all +9.09%", () => {
  const inst = loadInst();
  const d = [
    bar(dayTs("2026-01-05"), 100, 100, 100, 100),
    bar(dayTs("2026-01-06"), 110, 110, 110, 110),
    bar(dayTs("2026-01-07"), 120, 120, 120, 120)
  ];
  const b = [
    bar(dayTs("2026-01-05"), 1000, 1000, 1000, 1000),
    bar(dayTs("2026-01-06"), 1050, 1050, 1050, 1050),
    bar(dayTs("2026-01-07"), 1100, 1100, 1100, 1100)
  ];
  const pack = inst.vsSpxPack(d, b);
  assert.equal(pack.n, 3);
  assert.ok(Math.abs(pack.last.rs - 100 * 1.2 / 1.1) < 1e-9);
  assert.ok(Math.abs(pack.last.all - (1.2 / 1.1 - 1)) < 1e-9);
  const d1 = (120 / 110) / (1100 / 1050) - 1;
  assert.ok(Math.abs(pack.last.d1 - d1) < 1e-9);
});

test("exact NY-day join skips a missing bench session and never 7-day nearest", () => {
  const inst = loadInst();
  const d = [
    bar(dayTs("2026-01-05"), 100, 100, 100, 100),
    bar(dayTs("2026-01-06"), 110, 110, 110, 110),
    bar(dayTs("2026-01-07"), 120, 120, 120, 120)
  ];
  const b = [
    bar(dayTs("2026-01-05"), 1000, 1000, 1000, 1000),
    bar(dayTs("2026-01-07"), 1100, 1100, 1100, 1100)
  ];
  const j = inst.alignExact(d, b);
  assert.equal(j.length, 2);
  assert.equal(inst.nyClock(j[0].time).key, inst.nyClock(dayTs("2026-01-05")).key);
  assert.equal(inst.nyClock(j[1].time).key, inst.nyClock(dayTs("2026-01-07")).key);
  const fiveOff = inst.alignExact(
    [bar(dayTs("2026-01-12"), 100, 100, 100, 100)],
    [bar(dayTs("2026-01-07"), 1000, 1000, 1000, 1000)]
  );
  assert.equal(fiveOff.length, 0, "5-day gap must not join (old 7-day window would)");
  const twentyOff = inst.alignExact(
    [bar(dayTs("2026-01-05"), 100, 100, 100, 100), bar(dayTs("2026-01-25"), 120, 120, 120, 120)],
    [bar(dayTs("2026-01-05"), 1000, 1000, 1000, 1000)]
  );
  assert.equal(twentyOff.length, 1);
  const sameDayDiffUnix = inst.alignExact(
    [bar(Date.parse("2026-01-05T14:30:00.000Z") / 1000, 100, 100, 100, 100)],
    [bar(Date.parse("2026-01-05T05:00:00.000Z") / 1000, 1000, 1000, 1000, 1000)]
  );
  assert.equal(sameDayDiffUnix.length, 1, "same NY day, different unix, still joins");
});

test("YTD excess is vs prior-year last close, not first print of the year", () => {
  const inst = loadInst();
  const d = [
    bar(dayTs("2025-12-31"), 100, 100, 100, 100),
    bar(dayTs("2026-01-02"), 110, 110, 110, 110),
    bar(dayTs("2026-01-05"), 120, 120, 120, 120)
  ];
  const b = [
    bar(dayTs("2025-12-31"), 1000, 1000, 1000, 1000),
    bar(dayTs("2026-01-02"), 1050, 1050, 1050, 1050),
    bar(dayTs("2026-01-05"), 1100, 1100, 1100, 1100)
  ];
  const pack = inst.vsSpxPack(d, b);
  const ytd = (120 / 100) / (1100 / 1000) - 1;
  assert.ok(Math.abs(pack.last.ytd - ytd) < 1e-9, String(pack.last.ytd));
  const fromFirst = (120 / 110) / (1100 / 1050) - 1;
  assert.ok(Math.abs(pack.last.ytd - fromFirst) > 1e-6);
});

test("full-history join keeps pre-1993 overlap (cash SPX, not SPY)", () => {
  const inst = loadInst();
  const t1928 = dayTs("1928-01-03");
  const t1980 = dayTs("1980-12-12");
  const t1993 = dayTs("1993-01-29");
  const bench = [], name = [];
  for (let i = 0; i < 24000; i++) {
    const t = t1928 + i * 86400;
    bench.push(bar(t, 20, 20, 20, 20 + i * 0.01));
  }
  for (let i = 0; i < 5000; i++) {
    const t = t1980 + i * 86400;
    name.push(bar(t, 1, 1, 1, 1 + i * 0.02));
  }
  const pack = inst.vsSpxPack(name, bench);
  assert.ok(pack.n >= 4000, "n=" + pack.n);
  assert.ok(pack.from < t1993, "from=" + pack.from);
  assert.equal(inst.nyClock(pack.from).y, 1980);
  assert.ok(pack.n > 2000, "must keep the 1980–1993 overlap, not SPY-era only");
  assert.ok(pack.last.rs > 0);
});

test("2h bars count as intra and a 1h offset still joins on the same NY day", () => {
  const inst = loadInst();
  const t0 = Date.parse("2026-01-05T14:30:00.000Z") / 1000;
  const d = [
    bar(t0, 100, 100, 100, 100),
    bar(t0 + 7200, 101, 101, 101, 101),
    bar(t0 + 14400, 102, 102, 102, 102)
  ];
  const b = [
    bar(t0 + 3600, 1000, 1000, 1000, 1000),
    bar(t0 + 10800, 1010, 1010, 1010, 1010),
    bar(t0 + 18000, 1020, 1020, 1020, 1020)
  ];
  assert.equal(inst.isIntra(d), true);
  assert.equal(inst.isIntra([bar(t0, 1, 1, 1, 1), bar(t0 + 86400, 1, 1, 1, 1)]), false);
  const j = inst.alignExact(d, b);
  assert.ok(j.length >= 2, "n=" + j.length);
  j.forEach((row) => {
    assert.equal(inst.nyClock(row.time).key, inst.nyClock(t0).key);
  });
});
