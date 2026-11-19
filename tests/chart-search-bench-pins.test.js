// Symbol Search: a benchmark word leads with its index at once, before the directory has answered (2026-11-19).
//
// The directory pins "sp500" to I:SPX, SPY and FRED's copy, in that order. Its own answer took 22-26 s on three of six
// words typed on the live page (ops 6849) - whenever the request landed on a directory process that had not loaded the
// index - and until it arrived the dialog led with FRED's copy and then an ETF. jh-uni-search.js therefore knows the
// first pin of each index word and shows it first until the directory has spoken. These tests hold that table to the
// directory's own SEARCH_POLICY, and pin down when the row shows and when it steps aside.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const R = path.join(__dirname, '..'), read = p => fs.readFileSync(path.join(R, p), 'utf8');

function dialog() {
  const c = { console, setTimeout, clearTimeout, setInterval: () => 0, fetch: () => Promise.reject(new Error('offline')), Promise, JSON, Math, Object, Array, String, Number, RegExp, Date, Map, Set };
  c.window = c;
  c.document = { readyState: 'complete', addEventListener() {}, getElementById() { return null; }, createElement() { return { style: {}, setAttribute() {}, addEventListener() {}, querySelector() { return null; } }; }, head: { appendChild() {} }, body: { appendChild() {} } };
  c.localStorage = { getItem() { return null; }, setItem() {} }; c.addEventListener = () => {}; c.MutationObserver = function () { this.observe = () => {}; };
  vm.createContext(c); vm.runInContext(read('jh-uni-search.js'), c);
  return c.JHUniSearch;
}
function policy() {
  const src = read('aws/lambdas/justhodl-symdir/source/lambda_function.py'), m = /\nSEARCH_POLICY = (\{[\s\S]*?\n\})\n/.exec(src);
  assert.ok(m, 'the directory still declares SEARCH_POLICY as one literal');
  return JSON.parse(m[1].replace(/,\s*\}/g, '}'));
}
const server = (id, name, kind, pinned) => ({ id, sym: id, name, kind, type: kind === 'instrument' ? 'index' : '', provider: kind === 'series' ? 'fred' : 'instrument', src: 'x', pinned: !!pinned, chartable: true, raw: { id } });
const LOCAL = [{ id: 'FRED:SP500', sym: 'FRED:SP500', name: 'S&P 500', kind: 'instrument', type: '', src: 'Watchlist', provider: 'watchlist' }];
const TV = [{ id: 'AMEX:SPY', sym: 'SPY', name: 'SPDR S&P 500 ETF TRUST', kind: 'instrument', type: 'fund', src: 'AMEX', provider: 'tv', tv: true }];
const ANSWER = [server('I:SPX', "Standard & Poor's 500", 'instrument', true), server('SPY', 'SPDR S&P 500 ETF Trust', 'instrument', true), server('fred:SP500', 'S&P 500', 'series', true), server('SPCFD:SP500LVOL', 'SP500LVOL (SPCFD)', 'instrument', false)];
// rows come out of the dialog's own realm: copy the ids into this one before comparing
const ids = rows => Array.from(rows, r => r.id);
const group = (q, extra) => Object.assign({ q, local: [], server: [], alt: [], tv: [], catalog: [], wl: [], done: false }, extra || {});

test('the dialog knows the first pin of every index word the directory pins, and of no other word', () => {
  const U = dialog(), P = policy();
  const indexWords = Object.keys(P).filter(w => /^I:/.test(P[w][0]));
  assert.deepEqual(indexWords.sort(), ['dow', 'dow jones', 'nasdaq', 'ndx', 'russell', 's&p', 's&p 500', 'sp500', 'spx'], 'the index words of the directory policy');
  for (const w of Object.keys(P)) {
    const rows = U.benchRows(w);
    if (indexWords.includes(w)) { assert.equal(rows.length, 1, w); assert.equal(rows[0].id, P[w][0], w + ' leads with the directory\'s first pin'); }
    else assert.equal(rows.length, 0, w + ' is not an index word: nothing is known here');
  }
  for (const w of ['aapl', 'sp', 'sp5000', 'spy', 'vix', 'gold', '', 'nasdaq 100', 'dow inc']) assert.equal(U.benchRows(w).length, 0, JSON.stringify(w));
});

test('the row is the same instrument row the directory sends: the Polygon index id, an index, pinned', () => {
  const U = dialog();
  for (const [w, id, name] of [['sp500', 'I:SPX', "Standard & Poor's 500"], ['SPX', 'I:SPX', "Standard & Poor's 500"], [' S&P 500 ', 'I:SPX', "Standard & Poor's 500"], ['ndx', 'I:NDX', 'NASDAQ-100'], ['Dow Jones', 'I:DJI', 'Dow Jones Industrial Average'], ['russell', 'I:RUT', 'Russell 2000 Index']]) {
    const r = U.benchRows(w)[0];
    assert.equal(r.id, id); assert.equal(r.sym, id); assert.equal(r.name, name); assert.equal(r.kind, 'instrument'); assert.equal(r.pinned, true); assert.equal(r.chartable, true);
    assert.equal(U.classify(r), 'indices');
    // jh-uni-search.js pick(): an I: id is a directory id and goes to jhGoSymbol, not to the watchlist's TradingView route
    assert.ok(!(/^[A-Z][A-Z0-9_]*:/.test(r.id) && !/^(X|C|I|CQ|CISS|DESK|DATA):/.test(r.id)), id + ' opens through jhGoSymbol');
  }
  // the directory matches the word exactly (trimmed, lower-cased); so does the dialog
  assert.equal(U.benchRows('s&p  500').length, 0);
});

test('before the directory answers, the index leads - ahead of the local FRED copy and the ETF', () => {
  const U = dialog();
  U._setG(group('sp500', { local: LOCAL.slice(), tv: TV.slice() }));
  const rows = U.merged();
  assert.equal(rows[0].id, 'I:SPX'); assert.equal(rows[0].grp, 'Best match'); assert.equal(rows[0].bench, true);
  assert.deepEqual(ids(rows), ['I:SPX', 'FRED:SP500', 'AMEX:SPY']);
});

test('alternates that land first do not push the index down, and it is listed once', () => {
  const U = dialog();
  const viaSpy = Object.assign(server('SPY', 'SPDR S&P 500 ETF Trust', 'instrument', true), { via: 'SPY', viaTicker: false });
  const viaName = Object.assign(server('I:SPX', "Standard & Poor's 500", 'instrument', true), { via: 'S&P 500', viaTicker: false });
  U._setG(group('sp500', { local: LOCAL.slice(), tv: TV.slice(), alt: [viaSpy, viaName] }));
  const rows = U.merged();
  assert.equal(rows[0].id, 'I:SPX'); assert.equal(rows[0].via, undefined, 'the leading row is not labelled as matched through another word');
  assert.equal(rows.filter(r => r.id === 'I:SPX').length, 1); assert.equal(rows[1].id, 'SPY');
});

test('once the directory has answered, its own rows and order stand and the local pin is gone', () => {
  const U = dialog();
  U._setG(group('sp500', { local: LOCAL.slice(), tv: TV.slice(), server: ANSWER.slice(), answered: true }));
  const rows = U.merged();
  assert.deepEqual(ids(rows.slice(0, 3)), ['I:SPX', 'SPY', 'fred:SP500']);
  assert.ok(rows[0].raw, 'the first row is the directory\'s own'); assert.equal(rows[0].bench, undefined);
  assert.equal(rows.filter(r => r.id === 'I:SPX').length, 1);
});

test('a directory answer that does not list the index is not overruled', () => {
  const U = dialog();
  U._setG(group('sp500', { server: ANSWER.slice(1), answered: true }));
  assert.ok(!U.merged().some(r => r.id === 'I:SPX'));
});

test('a directory that failed to answer leaves the index leading', () => {
  const U = dialog();
  U._setG(group('dow', { local: [{ id: 'DOW', sym: 'DOW', name: 'Dow Inc', kind: 'instrument', type: 'stock', src: 'NYSE', provider: 'instrument', exact: true }], failed: true, done: true }));
  const rows = U.merged();
  assert.deepEqual(ids(rows.slice(0, 2)), ['I:DJI', 'DOW']);
});

test('one provider\'s scope and the non-index tabs do not show it; the Indices tab does', () => {
  const U = dialog(), S = U._state;
  U._setG(group('sp500', { local: LOCAL.slice() }));
  S.prov = 'fred'; assert.ok(!U.merged().some(r => r.bench)); S.prov = '';
  S.tab = 'economy'; assert.ok(!U.merged().some(r => r.bench));
  S.tab = 'indices'; assert.deepEqual(ids(U.merged()), ['I:SPX']);
  S.tab = 'all';
});

test('a word that is not a benchmark is untouched', () => {
  const U = dialog();
  U._setG(group('aapl', { local: [{ id: 'AAPL', sym: 'AAPL', name: 'Apple Inc', kind: 'instrument', type: 'stock', src: 'NASDAQ', provider: 'instrument', exact: true }] }));
  assert.deepEqual(ids(U.merged()), ['AAPL']);
});

test('the search pipeline marks a word answered only when the directory really answered', () => {
  const s = read('jh-uni-search.js');
  assert.match(s, /G\.failed = !!d\.failed; G\.ambiguous = !!d\.ambiguous;\s*\n\s*if \(!d\.failed\) G\.answered = true;/);
  assert.match(s, /var bench = S\.prov \|\| G\.answered \? \[\] : benchRows\(G\.q\);\s*\n\s*bench\.concat\(server, G\.local \|\| \[\], G\.wl \|\| \[\], inline \? \[\] : tickAlt\)/);
});
