const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

// Synthetic public-schema fixtures; execute the actual page's load/render path.
const page = fs.readFileSync('bonds.html', 'utf8');
const source = [...page.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)]
  .find(match => match[1].includes('function render(D)') && match[1].includes('const FEED ='))[1];
function packet() {
  return {
    heartbeat: {score: 0, regime: 'CALM'}, panels: {},
    auction: {prediction: [{name: 'Fixture asset', call: null, d1: {median: 0}}], composite_now: {composite: null}},
    fleet: {usd_funding_stress_z: {sum_z: 4.58, mean_z: 0.92}},
  };
}
async function render(data, response = 'ok') {
  const elements = new Map(), requests = [], listeners = {}, intervals = [];
  function element() {
    const children = new Map();
    return {textContent: '', innerHTML: '', className: '', setAttribute() {},
      querySelector(selector) {if (!children.has(selector)) children.set(selector, element()); return children.get(selector);}};
  }
  const context = {window: {}, document: {
    getElementById(id) {if (!elements.has(id)) elements.set(id, element()); return elements.get(id);},
    addEventListener(event, callback) {listeners[event] = callback;},
  }, setInterval(callback, ms) {intervals.push({callback, ms});}, async fetch(url) {
    requests.push(url);
    if (response === 'network') throw new Error('Fixture network failure');
    return {ok: response !== 'http', async json() {if (response === 'json') throw new Error('Fixture malformed JSON'); return data;}};
  }};
  vm.runInNewContext(source, context);
  await listeners.DOMContentLoaded();
  return {elements, requests, intervals, context, html: id => elements.get(id)?.innerHTML || ''};
}
test('null calls and composite are unavailable while a measured zero median survives', async () => {
  const result = await render(packet());
  assert.match(result.html('wr-auction'), /unavailable 0\.00%/);
  assert.match(result.html('wr-auction'), /auction-stress composite unavailable/);
  assert.doesNotMatch(result.html('wr-auction'), /\bnull\b|undefined/);
  assert.match(result.html('wr-auction'), /href="\/auctions\.html"/);
});
test('finite composite and median values preserve zero, signs, and call direction', async () => {
  for (const value of [0, -0.25, 2.5]) {
    const data = packet(); data.auction.composite_now.composite = value;
    data.auction.prediction[0] = {name: 'Fixture', call: '↑', d1: {median: value}};
    const result = await render(data);
    assert.match(result.html('wr-auction'), new RegExp('auction-stress composite ' + String(value).replace('.', '\\.')));
    assert.ok(result.html('wr-auction').includes('class="up">↑ ' + (value > 0 ? '+' : '') + value.toFixed(2) + '%'));
  }
});
test('missing or invalid medians cannot crash the rest of the war room or become zero', async () => {
  for (const value of [undefined, null, '', '0', 'bad', false, true, {}, [], NaN, Infinity, -Infinity]) {
    const data = packet(); data.auction.prediction[0].d1.median = value;
    const result = await render(data);
    assert.doesNotMatch(result.elements.get('wr-headline').textContent, /Render error/);
    assert.match(result.html('wr-auction'), /median unavailable/);
    assert.doesNotMatch(result.html('wr-auction'), /0\.00%|NaN|Infinity/);
    assert.match(result.html('wr-grid'), /US Treasuries/);
    assert.match(result.html('wr-foot'), /USD-funding stress z 4\.58/);
  }
});
test('missing and malformed composite values are unavailable, including a missing object', async () => {
  for (const value of [undefined, null, '', '0', false, {}, NaN, Infinity]) {
    const data = packet(); data.auction.composite_now.composite = value;
    assert.match((await render(data)).html('wr-auction'), /auction-stress composite unavailable/);
  }
  const data = packet(); delete data.auction.composite_now;
  assert.match((await render(data)).html('wr-auction'), /auction-stress composite unavailable/);
});
test('malformed median parent values cannot fabricate a measured zero', async () => {
  for (const parent of [undefined, null, 0, 1, false, true, '', '0', [], [0],
    Object.assign([], {median: 0}), Object.create({median: 0})]) {
    const data = packet(); data.auction.prediction[0].d1 = parent;
    const result = await render(data);
    assert.match(result.html('wr-auction'), /median unavailable/);
    assert.doesNotMatch(result.html('wr-auction'), /0\.00%/);
    assert.doesNotMatch(result.elements.get('wr-headline').textContent, /Render error/);
  }
});
test('malformed composite parent values cannot fabricate a measured zero', async () => {
  for (const parent of [undefined, null, 0, 1, false, true, '', '0', [], [0],
    Object.assign([], {composite: 0}), Object.create({composite: 0})]) {
    const data = packet(); data.auction.composite_now = parent;
    const html = (await render(data)).html('wr-auction');
    assert.match(html, /auction-stress composite unavailable/);
    assert.doesNotMatch(html, /auction-stress composite 0/);
  }
});
test('funding stress uses sum_z, preserves finite legacy scalars and rejects unmeasured values', async () => {
  for (const [value, expected] of [[{sum_z: 4.58, mean_z: 0.92}, '4.58'], [{sum_z: 0}, '0'], [0, '0'], [-1.5, '-1.5'],
    [null, 'unavailable'], [undefined, 'unavailable'], [{mean_z: 1}, 'unavailable'], [{sum_z: '4.58'}, 'unavailable'],
    [{sum_z: false}, 'unavailable'], [{sum_z: Infinity}, 'unavailable'], [[], 'unavailable'], ['', 'unavailable'], [false, 'unavailable'], [NaN, 'unavailable']]) {
    const data = packet(); data.fleet.usd_funding_stress_z = value;
    const html = (await render(data)).html('wr-foot');
    assert.ok(html.includes('USD-funding stress z ' + expected + ','));
    assert.doesNotMatch(html, /\[object Object\]|NaN|Infinity/);
  }
});
test('auction call markup is escaped as text, with missing calls explicitly unavailable', async () => {
  const data = packet(); data.auction.prediction[0].call = '<em title="fixture">& marker</em>';
  const html = (await render(data)).html('wr-auction');
  assert.ok(html.includes('&lt;em title=&quot;fixture&quot;&gt;&amp; marker&lt;/em&gt;'));
  assert.doesNotMatch(html, /<em/);
  for (const value of [undefined, null, '', '   ', false, {}, 0]) {
    data.auction.prediction[0].call = value;
    assert.match((await render(data)).html('wr-auction'), /unavailable 0\.00%/);
  }
});
test('load and refresh use only the canonical same-origin public feed', async () => {
  const data = packet(), before = JSON.stringify(data), result = await render(data);
  assert.equal(result.requests.length, 1);
  assert.match(result.requests[0], /^\/data\/bond-warroom\.json\?t=\d+$/);
  assert.equal(result.intervals[0].ms, 5 * 60 * 1000);
  await result.intervals[0].callback();
  assert.equal(result.requests.length, 2);
  assert.match(result.requests[1], /^\/data\/bond-warroom\.json\?t=\d+$/);
  assert.equal(JSON.stringify(data), before);
});
test('HTTP, JSON and network failures stay unavailable without alternate-host fallback', async () => {
  for (const response of ['http', 'json', 'network']) {
    const result = await render(packet(), response);
    assert.equal(result.requests.length, 1);
    assert.match(result.requests[0], /^\/data\/bond-warroom\.json\?t=\d+$/);
    assert.match(result.elements.get('wr-headline').textContent, /War-room feed unavailable/);
  }
});
