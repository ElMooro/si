'use strict';
// canaries.html, "What the record says" (jh-canary-record.js) on the real documents of 2026-11-27.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const ROOT = path.join(__dirname, '..');
const R = require(path.join(ROOT, 'jh-canary-record.js'));
const FX = JSON.parse(zlib.gunzipSync(fs.readFileSync(path.join(__dirname, 'fixtures', 'canary-record-page.json.gz'))).toString('utf8'));
const DOC = FX.document, REC = FX.record, CAL = DOC.calibrated;
const ST = { verdict: '', mech: '', q: '', key: '', dir: 'desc' };
const SRC = fs.readFileSync(path.join(ROOT, 'jh-canary-record.js'), 'utf8');
const PAGE = fs.readFileSync(path.join(ROOT, 'canaries.html'), 'utf8');
const text = (html) => html.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/\s+/g, ' ');
const rowsOf = (html) => (html.split('<tbody>')[1] || '').split('</tbody>')[0].split('<tr').length - 1;

test('the section is five blocks, each opening with its own heading, and prints no broken figure', () => {
  const html = R.sectionHtml(DOC, REC, ST);
  const ids = [...html.matchAll(/<div class="cr-block" id="([^"]+)"><h3>([^<]+)<\/h3>/g)].map((m) => [m[1], m[2]]);
  assert.deepEqual(ids, [['cr-now', 'Today on the record’s scale'], ['cr-follow', 'What followed each reading'], ['cr-replay', 'The barometer through history'],
    ['cr-canaries', 'Every canary’s record'], ['cr-method', 'How it was counted, and what it cannot say']]);
  assert.equal((html.match(/class="cr-block"/g) || []).length, 5);
  assert.ok(html.startsWith('<h2 class="cr-h2">What the record says'));
  assert.doesNotMatch(text(html), /undefined|NaN|\[object|Infinity(?! [A-Z])|null/);
});

test('today is placed on the record by the document’s own figures', () => {
  const t = text(R.gauges(CAL, REC));
  const L = CAL.level, T = CAL.turn;
  assert.ok(t.includes(R.ord(L.percentile) + ' percentile') && t.includes(L.band) && t.includes('score ' + L.score.toFixed(1)));
  assert.ok(t.includes(`in ${L.after.fell} of ${L.after.months} months (${R.share(L.after.fall_share)})`));
  assert.ok(t.includes(R.signed(T.change) + ' points') && t.includes(T.band) && t.includes(`${T.days_back} days back`));
  assert.ok(t.includes(`in ${T.after.fell} of ${T.after.months} months (${R.share(T.after.fall_share)})`) && t.includes(`in ${T.after.early_fell} of ${T.after.early_months} (${R.share(T.after.early_fall_share)})`));
  assert.ok(t.includes('The level confirms; it has not warned.') && t.includes('Early score ' + CAL.record.level_score_early.toFixed(2)));
  assert.ok(t.includes('The turn has tilted the odds ahead of falls.') && t.includes('Early score ' + CAL.record.turn_score_early.toFixed(2) + ' ± ' + CAL.record.turn_score_early_error.toFixed(2)));
  assert.ok(t.includes('CISS (ECB) ' + R.signed(T.by_mechanism.ciss) + ' · no weight') && t.includes('Macro grid ' + R.signed(T.by_mechanism.macro_grid)));
  assert.ok(text(R.gauges(null, REC)).includes('The record has not been published yet'));
});

test('what followed each reading is the record’s table, with today’s band and cell marked', () => {
  const t3 = REC.barometer.turns['3'], lvl = REC.barometer.views.per_mechanism;
  const turn = R.followTable('The turn, by band', t3.reliability, {}, CAL.turn.band, 'x');
  assert.equal(rowsOf(turn), 4);
  for (const r of t3.reliability) assert.ok(text(turn).includes(`${r.from_pct}–${r.to_pct} ${r.months} ${r.fell} ${R.share(r.fall_share)}`), r.band);
  assert.equal((turn.match(/class="cr-on"/g) || []).length, 1);
  assert.ok(turn.split('class="cr-on"')[1].split('</tr>')[0].includes(CAL.turn.band));
  assert.equal(rowsOf(R.followTable('The level, by band', lvl.reliability, {}, CAL.level.band, 'y')), 5);
  const cross = R.crossTable(REC.barometer.level_against_turn, CAL.cell);
  assert.equal((cross.match(/outline:2px solid/g) || []).length, 1);
  for (const c of REC.barometer.level_against_turn) assert.ok(text(cross).includes(`${R.share(c.fall_share)} ${c.fell} of ${c.months}`), c.level + c.turn);
  assert.ok(cross.split('outline:2px solid')[1].split('</td>')[0].includes(`${CAL.cell.fell} of ${CAL.cell.months} · today`));
});

test('the chart draws the level, the turn and every fall of the index inside its span', () => {
  const svg = R.chart(REC);
  const months = REC.barometer.months.filter((m) => m.per_mechanism != null);
  const falls = REC.market.falls.filter((f) => f.low >= months[0].day);
  assert.equal((svg.match(/<rect /g) || []).length, falls.length);
  assert.equal((svg.match(/<path /g) || []).length, 2);
  assert.ok(svg.includes('S&amp;P 500 -33.9% from its high of 2020-02-19'));
  const level = svg.match(/<path d="([^"]+)" fill="none" stroke="#e8c26a"/)[1];
  assert.equal(level.trim().split(' ').length / 2, months.length);
  const last = months[months.length - 1];
  assert.equal(R.nearest(months, 1).day, last.day);
  assert.equal(R.nearest(months, 0).day, months[0].day);
  assert.ok(R.readout(last).includes('level ' + last.per_mechanism.toFixed(1)) && R.readout(last).includes('turn ' + R.signed(last.turn)) && R.readout(last).includes(last.n + ' canaries'));
  assert.equal(R.chart({ barometer: { months: [] } }), '');
});

test('every canary is listed whole, with its verdict and why it has no record', () => {
  const html = R.tableHtml(REC, ST);
  assert.equal(rowsOf(html.split('cr-all')[1]), REC.canaries.length);
  assert.equal(REC.canaries.length, 192);
  for (const v of Object.keys(REC.verdicts)) assert.ok(html.includes(`data-cr-verdict="${v}"`) && text(html).includes(`${R.VERDICT[v][0]} ${REC.verdicts[v]}`), v);
  const first = R.defaultOrder(REC.canaries)[0];
  assert.equal(first.verdict, 'leads');
  assert.ok(html.split('<tbody>')[1].split('</tr>')[0].includes(first.name.replace(/&/g, '&amp;')));
  assert.ok(html.includes('not a warehouse series (NYFED_RATES)') && html.includes('tested, but it does not vote on the board'));
  assert.ok(html.includes('newest 18 months are left out'));
  assert.ok(text(html).includes('192 of 192 canaries'));
  const heads = [...html.matchAll(/data-cr-sort="([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(heads, R.COLS.map((c) => c[0]));
  const tested = REC.canaries.find((c) => c.months);
  for (const c of R.COLS) assert.ok(c[0] in tested, c[0]);                      // every column is a field the record publishes
});

test('every column sorts both ways, with blanks last either way', () => {
  for (const c of R.COLS) {
    for (const dir of ['asc', 'desc']) {
      const out = R.sorted(REC.canaries, c[0], dir);
      assert.equal(out.length, REC.canaries.length);
      const keys = out.map((r) => (c[2] === 'verdict' ? R.VERDICT_ORDER.indexOf(r.verdict) : c[2] === 'warned' ? (r.falls_covered ? r.warned_before_high / r.falls_covered : null)
        : (c[2] === 'text' || c[2] === 'mech') ? String(r[c[0]] == null ? '' : r[c[0]]).toLowerCase() : r[c[0]]));
      const blank = (v) => v == null || v === '' || v === -1;
      const firstBlank = keys.findIndex(blank);
      const body = firstBlank < 0 ? keys : keys.slice(0, firstBlank);
      assert.ok(firstBlank < 0 || keys.slice(firstBlank).every(blank), `${c[0]} ${dir}: a blank is not last`);
      for (let i = 1; i < body.length; i++) assert.ok(dir === 'asc' ? body[i - 1] <= body[i] : body[i - 1] >= body[i], `${c[0]} ${dir} at ${i}`);
    }
  }
  const html = R.tableHtml(REC, Object.assign({}, ST, { key: 'auc_early', dir: 'asc' }));
  assert.ok(html.includes('aria-sort="ascending"') && html.includes('Early score ▲') && html.includes('data-cr-reset'));
});

test('filters, search and the saved file keep to what is shown', () => {
  assert.equal(R.filtered(REC.canaries, Object.assign({}, ST, { verdict: 'leads' })).length, REC.verdicts.leads);
  assert.equal(R.filtered(REC.canaries, Object.assign({}, ST, { mech: 'plumbing' })).length, 40);
  const credit = R.filtered(REC.canaries, Object.assign({}, ST, { q: 'OAS' }));
  assert.ok(credit.length >= 4 && credit.every((r) => /oas/i.test(r.name + r.rule + r.id)));
  assert.ok(text(R.tableHtml(REC, Object.assign({}, ST, { verdict: 'stuck' }))).includes(`${REC.verdicts.stuck} of 192 canaries`));
  const lines = R.csv(R.filtered(REC.canaries, Object.assign({}, ST, { verdict: 'wrong way' }))).split('\r\n');
  assert.equal(lines.length, REC.verdicts['wrong way'] + 2);
  assert.ok(lines[0].startsWith('id,Canary,Mechanism,Verdict,Months,Since,Fires') && lines[1].split(',')[0].includes(':'));
  assert.equal(R.csv(REC.canaries).split('\r\n').length, 194);
});

test('mechanisms, spans, periods and the method are printed from the record', () => {
  const m = text(R.mechTable(REC));
  for (const k of REC.barometer.mechanisms) assert.ok(m.includes(`${k.canaries} ${k.level.auc_early.toFixed(3)} ± ${k.level.auc_early_se.toFixed(3)} ${k.turn.auc_early.toFixed(3)}`), k.mechanism);
  for (const k of ['1', '3', '6', '12']) assert.ok(m.includes(REC.barometer.turns[k].earned.auc_early.toFixed(3) + ' ± ' + REC.barometer.turns[k].earned.auc_early_se.toFixed(3)), k);
  assert.equal(REC.barometer.turns['3'].by_period.length, 3);
  const meth = text(R.method(REC));
  assert.ok(meth.includes('about one in twenty would look like a leader by luck') && meth.includes('weak before 1998') && meth.includes('not a forecast'));
  assert.ok(meth.includes(`${REC.tested} of ${REC.defined} canaries`) && meth.includes('stray quarter rows'));
  const not = text(R.notRebuilt(DOC));
  assert.ok(not.includes('Funding & Internals') && not.includes('Market Leadership') && not.includes('Dollar observations'));
});

test('the page mounts the section beside what was there, and the colours live in the script', () => {
  assert.ok(PAGE.includes('<section id="s-record"></section>') && PAGE.indexOf('id="s-record"') < PAGE.indexOf('<div id="baro"></div>'));
  assert.ok(PAGE.indexOf('/jh-canary-record.js?v=') < PAGE.indexOf('const COL={CALM'));
  assert.ok(PAGE.includes('JHCanaryRecord.mount(window,d)'));
  for (const id of ['hero', 'baro', 'divWrap', 'mechs', 'filters', 'tbl', 'playbook']) assert.ok(PAGE.includes(`id="${id}"`), id);   // the previous view, untouched
  assert.ok(PAGE.includes("fetch('data/warroom-replay.json") && PAGE.includes('Earned weights (') && PAGE.includes('HIGHEST-CONVICTION LEADS'));
  assert.ok(!PAGE.includes('Unified across 6 mechanisms'));
  assert.ok(SRC.includes('jh-reskin-skip'));
  const css = PAGE.split('What the record says (jh-canary-record.js)')[1].split('</style>')[0];
  assert.doesNotMatch(css.replace(/#16202e|#16324e|#2f81f7|#cfe3ff|#cdd7e2/g, ''), /#[0-9a-f]{6}\b/i);                         // only neutral surfaces in the page's own CSS
  const kit = fs.readFileSync(path.join(ROOT, 'jh-engine-data.js'), 'utf8');
  assert.ok(kit.includes("key: 'data/canary-record.json', engine: 'justhodl-warroom-weights'"));
});
