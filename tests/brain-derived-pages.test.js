const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.join(__dirname, '..');
function pageFunction(file, changedDuringFetch = false, authenticated = true, allowed = true) {
  const html = fs.readFileSync(path.join(root, file), 'utf8');
  const calls = [];
  let user = authenticated ? { id: 'owner-fixture' } : null;
  const context = { window: { JustHodlAuth: { getUser: () => user, getAccessToken: async () => 'fixture-token' } },
    PROXY: 'https://proxy.test', Date, console, fetch: async (url, opts = {}) => {
      calls.push({ url, opts });
      const privateRoute = url.includes('/private-artifact?');
      if (changedDuringFetch && privateRoute) user = null;
      return { ok: !privateRoute || allowed, async json() { return privateRoute ? { brief: 'PRIVATE_SYNTHETIC' } : { private_text: true }; } };
    } };
  if (file === 'cockpit.html') {
    const start = html.indexOf('async function gj('), end = html.indexOf('function rcol', start);
    vm.runInNewContext(html.slice(start, end) + '\nglobalThis.run=()=>gj("data/my-brief.json");', context);
  } else {
    const start = html.indexOf('class DataService {'), end = html.indexOf('\n}\n', start) + 3;
    vm.runInNewContext(html.slice(start, end) + '\nglobalThis.run=()=>DataService.privateReview();', context);
  }
  return { run: context.run, calls };
}
for (const file of ['cockpit.html', 'chart-pro.html']) {
  test(`${file}: private review uses Bearer identity and no-store`, async () => {
    const { run, calls } = pageFunction(file);
    assert.equal((await run()).brief, 'PRIVATE_SYNTHETIC'); assert.equal(calls.length, 1);
    assert.equal(calls[0].opts.headers.Authorization, 'Bearer fixture-token'); assert.equal(calls[0].opts.cache, 'no-store');
    assert.match(calls[0].url, /\/private-artifact\?kind=(my-brief|devils-advocate)/);
  });
  test(`${file}: signed-out and unauthorized readers only receive safe public summaries`, async () => {
    for (const auth of [false, true]) {
      const { run, calls } = pageFunction(file, false, auth, false);
      assert.equal((await run()).private_text, true);
      assert.match(calls.at(-1).url, /data\/(my-brief|devils-advocate)-public\.json/);
      assert.ok(!calls.some(c => /data\/(my-brief|devils-advocate)\.json/.test(c.url)));
      if (!auth) assert.equal(calls.length, 1);
    }
  });
  test(`${file}: in-flight owner response is discarded after sign-out`, async () => {
    const { run } = pageFunction(file, true);
    const out = await run(); assert.equal(out.private_text, true); assert.equal(out.brief, undefined);
  });
}
