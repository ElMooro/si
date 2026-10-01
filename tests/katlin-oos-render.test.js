const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.join(__dirname, '..');

test('actual Katlin renderer withholds legacy OOS and retains the prior/cohort display', () => {
  const evidence = JSON.parse(fs.readFileSync(path.join(root, 'docs/audit/katlin-oos-paired-evidence.json'), 'utf8'));
  const html = fs.readFileSync(path.join(root, 'katlin.html'), 'utf8');
  const start = html.indexOf('  function renderValidation()');
  assert.ok(start >= 0);
  const source = html.slice(start, html.indexOf('\n  function ', start + 1));
  const elements = {};
  const context = { D: {validation: evidence.legacy_validation_projection},
    $: id => elements[id] || (elements[id] = {}), esc: v => String(v ?? ''),
    pct: v => v == null ? '—' : String(v), cls: () => '' };
  vm.runInNewContext(source + '\nrenderValidation();', context);
  assert.match(elements['val-note'].textContent, /OOS validation unavailable/);
  assert.doesNotMatch(elements['val-extra'].innerHTML, /out-of-sample|test obs/);
  assert.match(elements['val-extra'].innerHTML, /per-feature base rates/);
  assert.match(elements.val.innerHTML, /below200/);
});
