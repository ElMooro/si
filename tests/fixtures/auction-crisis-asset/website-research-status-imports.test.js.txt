const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const {execFileSync} = require('node:child_process');
const root = path.join(__dirname, '..');
const manifest = require('./fixtures/website-research-status/imports-preservation.json');
const hash = value => crypto.createHash('sha256').update(value).digest('hex');

test('all eight complete pages change only the widget cache parameter', () => {
  assert.equal(manifest.pages.length, 8);
  for (const row of manifest.pages) {
    const prior = fs.readFileSync(path.join(root, row.predecessor));
    const current = fs.readFileSync(path.join(root, row.page));
    assert.equal(hash(prior), row.previous_sha256, row.page);
    assert.equal(prior.toString('utf8').split(manifest.old).length, 2, row.page);
    assert.equal(current.toString('utf8'), prior.toString('utf8').replace(manifest.old, manifest.new), row.page);
    assert.equal(hash(current), row.sha256, row.page);
  }
});

test('the actual build stamper invalidates every widget importer when its bytes change', () => {
  const output = execFileSync('python3', ['-B', 'tests/test_website_status_asset_versions.py'], {
    cwd: root, encoding: 'utf8'
  });
  assert.match(output, /8 legacy references reproduced; 8 current references follow both asset revisions; idempotent/);
});

test('the correlation preservation gate retains every previous assertion', () => {
  const row=manifest.test_adaptation, prior=fs.readFileSync(path.join(root,row.predecessor),'utf8');
  let current=fs.readFileSync(path.join(root,row.path),'utf8');
  assert.equal(hash(prior),row.previous_sha256);assert.equal(hash(current),row.sha256);
  for(const [old,fresh] of row.replacements.slice().reverse()) {
    assert.equal(current.split(fresh).length,2);current=current.replace(fresh,old);
  }
  assert.equal(current,prior);
});
