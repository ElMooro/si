'use strict';
// Twelve desk buttons on chart.html (Khalid 2026-11-26, chosen from the scan of 2026-11-26):
// Research, Insiders, Options, Value, Corp bonds, Plumbing, Liquidity, Canaries, Breadth, Reversal, Calendar, Crypto.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const R = path.join(__dirname, '..');
const ledger = require('./helpers/chart-macro-line-preservation.cjs');
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const ENGINE = fs.readFileSync(path.join(R, 'jh-chart-engine.js'), 'utf8');
const RAIL = fs.readFileSync(path.join(R, 'jh-chart-tvrail.js'), 'utf8');
const HTML = fs.readFileSync(path.join(R, 'chart.html'), 'utf8');
const TABS = [
  ['btn-rsch', 'brsch', 'research', 'Rs', 'Research', '/why.html'],
  ['btn-ins', 'bins', 'insiders', 'In', 'Insiders', '/insiders.html'],
  ['btn-opta', 'bopta', 'optx', 'Op', 'Options', '/options-analytics.html'],
  ['btn-val', 'bval', 'value', 'Va', 'Value', '/valuations.html'],
  ['btn-cbond', 'bcbond', 'corpbonds', 'CB', 'Corp bonds', '/corpbonds.html'],
  ['btn-plumb', 'bplumb', 'plumbing', 'Pl', 'Plumbing', '/plumbing.html'],
  ['btn-liq', 'bliq', 'liquidity', 'Lq', 'Liquidity', '/liquidity.html'],
  ['btn-can', 'bcan', 'canaries', 'Cn', 'Canaries', '/canaries.html'],
  ['btn-brd', 'bbrd', 'breadth', 'Br', 'Breadth', '/breadth-thrust.html'],
  ['btn-rev', 'brev', 'reversal', 'Rv', 'Reversal', '/trend-reversal.html'],
  ['btn-cal', 'bcal', 'calendar', 'Ca', 'Calendar', '/catalyst-calendar.html'],
  ['btn-cryp', 'bcryp', 'crypto', '₿', 'Crypto', '/btc-cycle.html'],
];

test('each of the twelve buttons is on the bar once, under the name Khalid gave it, between Khalid and Alert', () => {
  const ids = [...ENGINE.matchAll(/<button class='wsico wsdesk' id=(btn-[a-z0-9]+) /g)].map((m) => m[1]);
  assert.equal(new Set(ids).size, ids.length);
  const at = ids.indexOf('btn-rsch');
  assert.equal(ids[at - 1], 'btn-snip');
  assert.deepEqual(ids.slice(at, at + 12), TABS.map((t) => t[0]));
  assert.equal(ids[at + 12], 'btn-alrt');
  assert.equal(ids.length, 44);          // 43 with the twelve; the Held by funds button of 2026-11-27 makes 44 (beside Credit, far from these)
  for (const [id, , , glyph, label] of TABS) {
    const m = new RegExp("<button class='wsico wsdesk' id=" + id + " title='([^']+)'><span class=g>([^<]+)</span><span class=l>([^<]+)</span></button>").exec(ENGINE);
    assert.ok(m, id);
    assert.equal(m[2], glyph, id);
    assert.equal(m[3], label, id);
    assert.ok(m[1].startsWith(label + ' · '), id + ': his name first, the description beside it');
  }
});

test('each button opens its own workspace entry, and every earlier button and entry is as it was', () => {
  for (const [id, v, key, , label, page] of TABS) {
    const click = key === 'research'
      ? `var ${v}=document.getElementById("${id}"); if(${v}) ${v}.onclick=function(){ window.jhDeskSymbol=active; if(window.jhOpenWorkspace) window.jhOpenWorkspace("${key}"); };`
      : `var ${v}=document.getElementById("${id}"); if(${v}) ${v}.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("${key}"); };`;
    assert.equal(ENGINE.split(click).length, 2, id);
    const entry = new RegExp('\\n      ' + key + ': \\["' + label + ' \\u00b7 [^"]+", "' + page.replace(/[.]/g, '\\.') + '"\\],\\n');
    assert.match(RAIL, entry, key);
    assert.equal(RAIL.split('\n      ' + key + ': [').length, 2, key + ' listed once');
    assert.ok(fs.existsSync(path.join(R, page.slice(1))), page);
  }
  // nothing that was there was renamed, moved or removed
  for (const kept of ['      opt: ["Options", "/options.html"],', '      eco: ["Economic calendar", "/econ-calendar.html"],', '      mflow: ["Money flow · where money is flowing in and out", "/global-flow-desk.html"],', '      alert: ["Alert Center", "/alerts.html"],']) assert.ok(RAIL.includes(kept), kept);
  const keys = [...RAIL.matchAll(/\n      ("?[a-z0-9]+"?): \["/g)].map((m) => m[1]);
  assert.equal(new Set(keys).size, keys.length, 'no workspace key is used twice');
});

test('Research opens the company report on the symbol the chart is showing', () => {
  assert.ok(RAIL.includes('if (kind === "research" && /^[A-Z][A-Z.\\-]{0,6}$/.test(String(window.jhDeskSymbol || ""))) { href += "?ticker=" + encodeURIComponent(window.jhDeskSymbol); title += " · " + window.jhDeskSymbol; }'));
  const ok = /^[A-Z][A-Z.\-]{0,6}$/;
  for (const s of ['NVDA', 'BRK.B', 'F']) assert.ok(ok.test(s), s);
  for (const s of ['fred:DGS10', 'X:BTCUSD', 'CALC:DXY', '', 'nvda']) assert.ok(!ok.test(s), s);
});

test('both chart scripts carry the new mark, so a browser never pairs new buttons with an old side list', () => {
  const tags = HTML.match(/<script src="\/jh-chart-(?:engine|tvrail)\.js\?[^"]+"><\/script>/g);
  assert.equal(tags.length, 2);
  for (const t of tags) assert.ok(t.includes('&mflow=20261126&tabs=20261126&'), t.slice(0, 60));
  assert.equal(HTML.split('&tabs=20261126').length, 3);
});

test('each page behind a button carries the Engine data section', () => {
  for (const [, , , , , page] of TABS) assert.match(fs.readFileSync(path.join(R, page.slice(1)), 'utf8'), /<div data-jed="[a-z]+"><\/div>\n<script src="\/jh-engine-data\.js\?v=\d{8}[a-z]" defer><\/script>/, page);
});

test('the byte ledger takes the twelve buttons back out first, to the files as they were after the Flow button', () => {
  const e0 = ledger.peelChartTabsEngine(ledger.peelHeldByFundsEngine(ENGINE, 'jh-chart-engine.js'), 'jh-chart-engine.js'), r0 = ledger.peelChartTabsRail(ledger.peelHeldByFundsRail(RAIL, 'jh-chart-tvrail.js'), 'jh-chart-tvrail.js'), h0 = ledger.peelChartTabsHtml(ledger.peelHeldByFundsHtml(HTML, 'chart.html'), 'chart.html');
  assert.equal(sha(e0), '58158760d2bb516d844c1d7a319189679f26e74c83791a24c7aae2923815fd08');
  assert.equal(sha(h0), '6fff5f592f849ef38d8392b4ad6bb7eeb72cba205fb1ffee4946b7fbb8a2dc7f');
  assert.equal(sha(ledger.peelMoneyFlowRail(r0, 'jh-chart-tvrail.js')), '05f865f3572218e7cd8ff2c35641296e3406807b3815cbc9a900b99840987f10');
  for (const [id] of TABS) assert.ok(!e0.includes(id + ' '), id);
  assert.ok(!r0.includes('jhDeskSymbol') && !e0.includes('jhDeskSymbol') && !h0.includes('&tabs='));
  assert.equal(ledger.peelChartTabsEngine(ledger.peelHeldByFundsEngine(e0, 'jh-chart-engine.js'), 'jh-chart-engine.js'), e0);
  assert.equal(ledger.peelChartTabsRail(ledger.peelHeldByFundsRail(RAIL, 'chart.html'), 'chart.html'), RAIL);
  assert.match(fs.readFileSync(path.join(R, 'tests/helpers/chart-macro-line-preservation.cjs'), 'utf8'), /function normalize\(raw,file\)\{raw=peelHeldByFundsEngine\(raw,file\);raw=peelHeldByFundsRail\(raw,file\);raw=peelHeldByFundsHtml\(raw,file\);raw=peelChartTabsEngine\(raw,file\);raw=peelChartTabsRail\(raw,file\);raw=peelChartTabsHtml\(raw,file\);raw=peelMoneyFlowEngine\(raw,file\);/);
  for (const [f, raw] of [['jh-chart-engine.js', ENGINE], ['jh-chart-tvrail.js', RAIL], ['chart.html', HTML]]) assert.notEqual(sha(ledger.normalize(raw, f)), sha(raw), f);
});

test('the Demand and Supply icons are gone from the desk buttons; the text buttons and the rail pair still switch the zones', () => {
  // Khalid 2026-11-26: "remove the demand, supply icons from there bcs they arent serving no purpose they dont belong there"
  const sd = fs.readFileSync(path.join(R, 'jh-chart-sd-ui.js'), 'utf8'), boot = fs.readFileSync(path.join(R, 'jh-chart-ai-desk.js'), 'utf8');
  assert.ok(!/mkBtn\("btn-(demand|supply)"/.test(sd), 'the icon pair is no longer made');
  assert.ok(!sd.includes('wsico wsdesk'), 'nothing from this script joins the desk buttons');
  assert.ok(sd.includes('if (old && old.parentNode) old.parentNode.removeChild(old);'), 'an icon left by an older copy is taken off');
  for (const kept of ['mkBtn("btn-demand2", "Demand zones", "Demand")', 'mkBtn("btn-supply2", "Supply zones", "Supply")', 'mkBtn("btn-demand-rail", "Demand zones", "D")', 'mkBtn("btn-supply-rail", "Supply zones", "S")',
    'bind(document.getElementById("btn-demand2"), "demand");', 'bind(document.getElementById("btn-supply2"), "supply");']) assert.ok(sd.includes(kept), kept);
  assert.ok(boot.includes('s.src = "/jh-chart-sd-ui.js?v=5";'), 'a browser holding the old script asks for the new one');
  const ids = [...ENGINE.matchAll(/<button class='wsico wsdesk' id=(btn-[a-z0-9]+) /g)].map((m) => m[1]);
  assert.equal(ids[ids.indexOf('btn-cryp') + 1], 'btn-alrt', 'Alert follows Crypto');
});
