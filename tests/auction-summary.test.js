const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const page = fs.readFileSync(path.join(__dirname, '../bonds.html'), 'utf8');
const source = page.slice(page.indexOf('function renderAuctionSummary(){'), page.indexOf('/* ─────────────── MAIN', page.indexOf('function renderAuctionSummary(){')));

function render(data) {
  const el = {innerHTML:'', textContent:''};
  const scope = {AUCTION_DATA:data, document:{getElementById:()=>el}};
  vm.runInNewContext(source + '\nrenderAuctionSummary();', scope);
  return el;
}

test('auction summary retains genuine zero and never invents calm for missing data', () => {
  assert.match(render({composite_score:0,regime:'CALM'}).innerHTML, />0<\/div>/);
  for (const d of [{}, {regime:'CALM'}, {composite_score:80}, {composite_score:NaN,regime:'CALM'}]) {
    const el = render(d);
    assert.equal(el.innerHTML, '');
    assert.match(el.textContent, /incomplete/);
  }
});

test('auction provider text and keys cannot become active HTML', () => {
  const attack = '<img src=x onerror=alert(1)>';
  const el = render({composite_score:60,regime:'ELEVATED',interpretation:attack,n_recent_auctions_14d:attack,indicator_aggregate_14d:{[attack]:{}}});
  assert.ok(!el.innerHTML.includes(attack));
  assert.match(el.innerHTML, /&lt;img/);
  const missing = render({status:'no_data',message:attack});
  assert.equal(missing.innerHTML, '');
  assert.ok(missing.textContent.includes(attack));
});
