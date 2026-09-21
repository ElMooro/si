const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function load(name) {
  const ctx = { window: {}, console };
  ctx.window = ctx;
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '..', name), 'utf8'), ctx);
  return ctx;
}

test('chart toolbar places ETF / Strong / Bonds / 13F between correlation and alert', () => {
  const src = fs.readFileSync(path.join(__dirname, '../jh-chart-engine.js'), 'utf8');
  const iMacro = src.indexOf('id=btn-macro');
  const iHeat = src.indexOf('id=btn-heat');
  const iCorr = src.indexOf('id=btn-corm');
  const iEtf = src.indexOf('id=btn-etf');
  const iStr = src.indexOf('id=btn-str');
  const iBnd = src.indexOf('id=btn-bnd');
  const i13f = src.indexOf('id=btn-13f');
  const iAlrt = src.indexOf('id=btn-alrt');
  assert.ok(iMacro > 0 && iHeat > iMacro && iCorr > iHeat);
  assert.ok(iEtf > iCorr && iStr > iEtf && iBnd > iStr && i13f > iBnd && iAlrt > i13f);
  const rail = fs.readFileSync(path.join(__dirname, '../jh-chart-tvrail.js'), 'utf8');
  assert.match(rail, /etf: \["ETF Desk"/);
  assert.match(rail, /strong: \["Strength vs S&P 500"/);
  assert.match(rail, /bonds: \["Bonds & Yields"/);
  assert.match(rail, /"13f": \["13F/);
});

test('etf.html and strong.html are real desks, not stubs', () => {
  const etf = fs.readFileSync(path.join(__dirname, '../etf.html'), 'utf8');
  const strong = fs.readFileSync(path.join(__dirname, '../strong.html'), 'utf8');
  assert.ok(etf.length > 2500);
  assert.ok(strong.length > 2500);
  assert.match(etf, /ETF Research Desk/);
  assert.match(etf, /jh-etf-desk-page\.js/);
  assert.match(strong, /STRENGTH vs S/);
  assert.doesNotMatch(etf, /\[object Object\]|undefined%/);
  assert.doesNotMatch(strong, /\[object Object\]|undefined%/);
  assert.doesNotMatch(etf, /jh-wire\.js/);
  assert.doesNotMatch(strong, /jh-wire\.js/);
});

test('desk core formats and relative-strength posture', () => {
  const core = load('jh-desk-core.js');
  const D = core.JHDesk;
  assert.equal(D.fmtPct(1.234), '+1.23%');
  assert.equal(D.fmtPct(-0.5), '-0.50%');
  assert.equal(D.cls(1), 'up');
  assert.equal(D.cls(-1), 'dn');
  assert.equal(D.horizons([100, 101, 102, 103, 104, 110]).d, 5.77);
  const strong = load('jh-strong-engine.js');
  const p = strong.JHStrong.posture({ d: 0.2 }, { d: -1.5 });
  assert.equal(p.code, 'BID');
  const h = strong.JHStrong.posture({ d: -0.4 }, { d: -1.5 });
  assert.equal(h.code, 'HOLD');
});
