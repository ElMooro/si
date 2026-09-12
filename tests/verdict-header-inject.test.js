const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const helper = fs.readFileSync(path.join(__dirname, '../private-artifacts.js'), 'utf8');
const header = fs.readFileSync(path.join(__dirname, '../jh-verdict-header.js'), 'utf8');
const HEADER_SRC = '/jh-verdict-header.js?v=20260912';

// Minimal browser: records every <script src> appended by the helper's page binder.
function loadsFor(pathname) {
  const loads = [];
  const element = () => ({ style: {}, children: [], setAttribute() {}, appendChild(c) { this.children.push(c); } });
  const document = {
    baseURI: 'https://justhodl.ai' + (pathname == null ? '/portfolio/index.html' : pathname),
    documentElement: element(), createElement: element,
    getElementById() { return null; },
    body: { prepend() {} },
    addEventListener() {},
    head: { appendChild(script) { loads.push(script.src); } },
  };
  const location = { hostname: 'justhodl.ai', href: document.baseURI, reload() {} };
  if (pathname != null) location.pathname = pathname;
  const window = { location, document, addEventListener() {}, fetch: async () => new Response('{}'), JustHodlAuth: { ready: async () => ({ uid: null, token: null }), onChange() {} } };
  window.window = window;
  vm.runInContext(helper, vm.createContext({ window, document, location, URL, Request, Response, console }));
  return loads;
}

test('verdict header binds to the exact home paths only', () => {
  assert.ok(loadsFor('/').includes(HEADER_SRC));
  assert.ok(loadsFor('/index.html').includes(HEADER_SRC));
});

test('verdict header never binds on an absent pathname or on other pages', () => {
  // The auth-asset ordering test runs the helper with no pathname at all; a binder that
  // fires there miscounts that gate. A browser never yields '' either.
  assert.ok(!loadsFor(null).includes(HEADER_SRC));
  for (const p of ['/portfolio/index.html', '/command-center.html', '/workspace.html', '/fusion.html', '/data.html', '/ticker.html']) {
    assert.ok(!loadsFor(p).includes(HEADER_SRC), p);
  }
});

test('page binder still adds nothing to the auth-asset load order on account pages', () => {
  assert.deepEqual(loadsFor(null).filter((s) => s.startsWith('/jh-')), []);
});

test('header script reads verdict.json like every page reads data/ and never fabricates a call', () => {
  assert.ok(!header.includes('amazonaws.com'), 'bucket URL bypasses the edge and CSP');
  assert.ok(header.includes('"/data/verdict.json"'), 'same-origin zone route first');
  assert.ok(header.includes('PROXY + "/data/verdict.json"'), 'data-proxy fallback second');
  assert.ok(header.includes('v.writer !== "jh-fusion-projection"'), 'only the fusion projection may title the call');
  assert.ok(header.includes('no fabricated call'), 'missing projection renders as unavailable');
  assert.ok(header.includes('document.body.prepend(bar)') && !header.includes('innerHTML = ""') && !header.includes('body.innerHTML'), 'additive banner, never replaces the page');
  assert.ok(header.includes('data-marker", "JH_VERDICT_HEADER_V1"'), 'edge marker present for live verification');
});
