const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.join(__dirname, "..");
const engine = fs.readFileSync(path.join(root, "jh-chart-engine.js"), "utf8");
const html = fs.readFileSync(path.join(root, "chart.html"), "utf8");

function loadQx() {
  const start = engine.indexOf("function tickSize(");
  const end = engine.indexOf("function fmtVol(");
  assert.ok(start > 0 && end > start, "tick helpers present");
  const slice = engine.slice(start, end);
  const ctx = { Math, Number, isFinite, Infinity, console };
  vm.createContext(ctx);
  vm.runInContext(
    'var UP="#089981", DN="#f23645";\n' + slice +
    "; this.jhQx={tickSize:tickSize,roundTick:roundTick,roundBar:roundBar,roundBars:roundBars,pxFormat:pxFormat,hollowPaint:hollowPaint,volCandlePaint:volCandlePaint,rvolAt:rvolAt,tickPrec:tickPrec,tickFromBars:tickFromBars,fmt:fmt,crossedAlert:crossedAlert,retCal:retCal,escHtml:escHtml,safeHref:safeHref};",
    ctx
  );
  return ctx.jhQx;
}

test("chart.html cache-busts the quality engine", () => {
  assert.match(html, /jh-chart-engine\.js\?v=20260926-frame-identity/);
  assert.match(html, /jh-chart-quality\.js\?v=20260915ae-qx/);
  assert.match(html, /font-variant-numeric:tabular-nums/);
  assert.doesNotMatch(html, /\[object Object\]/);
});

test("engine ships Bloomberg-grade candle, scale, and magnet defaults", () => {
  assert.match(engine, /__jhChartEngineV1239/);
  assert.match(engine, /v12\.34/);
  assert.match(engine, /crossMode=1/);
  assert.match(engine, /thinBars:true/);
  assert.match(engine, /ticksVisible:true/);
  assert.match(engine, /IBM Plex Mono/);
  assert.match(engine, /function volCandlePaint/);
  assert.match(engine, /function hollowPaint/);
  assert.match(engine, /wickColor/);
  assert.match(engine, /borderColor/);
  assert.match(engine, /priceFormat:pxF/);
  assert.match(engine, /window\.jhQx=/);
  assert.match(engine, /rightOffset:12/);
  assert.doesNotMatch(engine, /\[object Object\]/);
});

test("tick-round kills LW float junk at equity cents", () => {
  const qx = loadQx();
  assert.equal(qx.tickSize(333.0799865722656), 0.01);
  assert.equal(qx.roundTick(333.0799865722656), 333.08);
  assert.equal(qx.fmt(333.0799865722656), "333.08");
  assert.equal(qx.roundTick(0.452345), 0.4523);
  const pepe = qx.roundBar({ time: 1, open: 0.00000342, high: 0.00000351, low: 0.00000330, close: 0.00000348, volume: 1 });
  assert.ok(pepe.open !== pepe.high && pepe.open !== pepe.close && pepe.low !== pepe.close);
  assert.ok(pepe.high > pepe.close && pepe.low < pepe.open);
  assert.ok(qx.roundTick(5e-10) > 0);
  assert.ok(qx.roundTick(0.00000342) > 0.000003);
  const pf = qx.pxFormat([{ close: 210.96 }]);
  assert.equal(pf.type, "price");
  assert.equal(pf.precision, 2);
  assert.equal(pf.minMove, 0.01);
});

test("roundBar sanitizes wick vs body and snaps to tick", () => {
  const qx = loadQx();
  const b = qx.roundBar({ time: 1, open: 10.001, high: 9.5, low: 11, close: 10.009, volume: 1e6 });
  assert.equal(b.open, 10.00);
  assert.equal(b.close, 10.01);
  assert.ok(b.high >= Math.max(b.open, b.close));
  assert.ok(b.low <= Math.min(b.open, b.close));
});

test("hollow up is empty fill with colored wick; down is filled", () => {
  const qx = loadQx();
  const up = qx.hollowPaint({ time: 1, open: 10, high: 11, low: 9, close: 10.5 }, "#131722");
  assert.equal(up.color, "#131722");
  assert.equal(up.borderColor, "#089981");
  assert.equal(up.wickColor, "#089981");
  const dn = qx.hollowPaint({ time: 2, open: 10.5, high: 11, low: 9, close: 10 }, "#131722");
  assert.equal(dn.color, "#f23645");
  assert.equal(dn.borderColor, "#f23645");
  assert.equal(dn.wickColor, "#f23645");
});

test("volume candles color by RVOL buckets, never invent width", () => {
  const qx = loadQx();
  const hot = qx.volCandlePaint({ time: 1, open: 10, high: 11, low: 9, close: 10.4, volume: 9e6 }, 3.1);
  assert.equal(hot.color, "#00c896");
  assert.equal(hot.wickColor, "#14e0aa");
  const thin = qx.volCandlePaint({ time: 2, open: 10.4, high: 11, low: 9, close: 10, volume: 1e5 }, 0.4);
  assert.match(thin.color, /rgba\(242,54,69/);
  assert.equal(hot.width, undefined);
});
