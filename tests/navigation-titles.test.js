const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');

function harness(file, favorites = [], texts = []) {
  const ids = new Map();
  for (const id of ['jhnav-groups', 'jhnav-count', 'jhnav-search']) {
    const listeners = new Map();
    ids.set(id, {innerHTML: '', textContent: '', value: '', listeners,
      addEventListener(name, fn) { if (!listeners.has(name)) listeners.set(name, []); listeners.get(name).push(fn); },
      querySelectorAll() { return []; }});
  }
  // Minimal DOM adapter: replacing groups resets row visibility; input persists.
  const container = ids.get('jhnav-groups');
  let html = '', rows = [], group;
  Object.defineProperty(container, 'innerHTML', {get: () => html, set(value) {
    html = value; rows = texts.map(textContent => ({textContent, style: {}}));
    const classes = new Set();
    group = {style: {}, classList: {toggle(c, on) { if (on === undefined) on = !classes.has(c); on ? classes.add(c) : classes.delete(c); }, contains: c => classes.has(c)},
      querySelector: () => ({addEventListener() {}}), querySelectorAll: () => rows, getAttribute: () => '0'};
  }});
  container.querySelectorAll = () => texts.length ? [group] : [];
  const local = new Map([['jh_sw_gen', '3372'], ['jh_favs', JSON.stringify(favorites)]]);
  const session = new Map([['jh_diag_3276', '1']]);
  const storage = map => ({getItem: k => map.get(k) || null, setItem: (k, v) => map.set(k, v), length: 0});
  let networkCalls = 0;
  const context = {window: {}, document: {readyState: 'loading', addEventListener() {}, getElementById: id => ids.get(id) || null},
    localStorage: storage(local), sessionStorage: storage(session), navigator: {}, location: {pathname: '/screener-fixture'},
    fetch() { networkCalls++; throw Error('Network prohibited'); }};
  const source = fs.readFileSync(file, 'utf8');
  const marker = '  if (document.readyState === "loading") {';
  assert.equal(source.split(marker).length, 2);
  vm.runInNewContext(source.replace(marker, '  window.testRender = render;\n' + marker), context);
  return {render(m) { context.window.testRender(m); assert.equal(networkCalls, 0); return ids.get('jhnav-groups').innerHTML; }, ids,
    get rows() {return rows;}, get group() {return group;},
    input(value) {const el = ids.get('jhnav-search'); el.value = value; for (const fn of el.listeners.get('input') || []) fn.call(el);}};
}

const candidate = path.join(__dirname, '..', 'jh-nav-drawer.js');
const manifest = (title, encoding) => ({n_pages: 1, ...(encoding ? {title_encoding: encoding} : {}),
  categories: [{name: 'Research & Tools', count: 1, pages: [{href: '/invented.html', title}]}]});

test('legacy entity-encoded manifests still render readable titles', () => {
  const h = harness(candidate);
  assert.match(h.render(manifest('Rates &amp; FX')), /jhnav-t">Rates &amp; FX<\/span>/);
});
test('plain Unicode manifest titles preserve literal entity-looking text', () => {
  const h = harness(candidate);
  assert.match(h.render(manifest('Café Ω & FX &lt;literal&gt;', 'unicode_text')),
    /jhnav-t">Café Ω &amp; FX &amp;lt;literal&amp;gt;<\/span>/);
});
test('plain title markup is text rather than an element', () => {
  const h = harness(candidate);
  const html = h.render(manifest('<img src=x onerror="throw 1"> \'title\'', 'unicode_text'));
  assert.ok(!html.includes('<img'));
  assert.match(html, /&lt;img src=x onerror=&quot;throw 1&quot;&gt; &#39;title&#39;/);
});
test('favorite and category retain the same full title and destination', () => {
  const h = harness(candidate, ['/invented.html']);
  const html = h.render(manifest('Ω &lt;literal&gt;', 'unicode_text'));
  assert.equal(html.split('jhnav-t">Ω &amp;lt;literal&amp;gt;</span>').length - 1, 2);
  assert.equal(html.split('href="/invented.html"').length - 1, 2);
  assert.equal(h.ids.get('jhnav-count').textContent, '1 pages');
});

test('render refresh preserves search filtering and does not multiply listeners', () => {
  const h = harness(candidate, [], ['Rates & FX', 'Café Research']);
  const m = manifest('Rates & FX', 'unicode_text');
  h.render(m); h.input('rates');
  for (let n = 0; n < 25; n++) h.render(m);
  assert.deepEqual(h.rows.map(row => row.style.display), ['', 'none']);
  assert.equal(h.group.classList.contains('jhnav-gopen'), true);
  assert.equal(h.ids.get('jhnav-search').listeners.get('input').length, 2); // search + tag intersection
  h.input('café'); assert.deepEqual(h.rows.map(row => row.style.display), ['none', '']);
  h.input('no matches'); assert.equal(h.group.style.display, 'none');
  h.input(''); assert.deepEqual(h.rows.map(row => row.style.display), ['', '']);
});

test('whole predecessor reproduces lost refresh filtering and duplicate listeners', () => {
  const h = harness(path.join(__dirname, 'fixtures/navigation/pre-unicode-drawer.js.txt'), [], ['Rates & FX', 'Café Research']);
  const m = manifest('Rates &amp; FX');
  h.render(m); h.input('rates'); h.render(m);
  assert.equal(h.ids.get('jhnav-search').value, 'rates');
  assert.deepEqual(h.rows.map(row => row.style.display), [undefined, undefined]);
  assert.equal(h.group.classList.contains('jhnav-gopen'), false);
  assert.equal(h.ids.get('jhnav-search').listeners.get('input').length, 3);
});
