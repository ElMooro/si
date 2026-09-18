const {test} = require('node:test');
const assert = require('node:assert/strict');
const ui = require('../jh-treasury-fiscal.js');
const now = Date.parse('2026-09-18T20:00:00Z');
const row = {dimension_contract: 'treasury-fiscal-warehouse.v2', freshness: {status:'fresh', max_age_days:430},
  measurements:[{field:'debt_outstanding_amt', as_of:'2025-09-30', value_decimal:'37637553494935.61', unit:'USD', dimensions:{}, row_index:0,
    series_id:'TREASURY:debt', source_key:'data/raw/v2/source.bin.gz'}], replay:{manifest_key:'data/replay/run.json'}};
test('exact decimal, units, period and original row are inspectable', () => {
  const html = ui.render({debt_outstanding:row}, now);
  assert.match(html,/37637553494935\.61/); assert.match(html,/Original response row 0/);
  assert.match(html,/observed 2025-09-30/); assert.match(html,/Replay inputs/);
});
test('fresh processing cannot conceal stale observation and future dates unavailable', () => {
  assert.match(ui.freshness(row, Date.parse('2028-01-01')), /stale/);
  assert.equal(ui.freshness(row, Date.parse('2020-01-01')), 'unavailable');
});
test('provider content is escaped and unsafe evidence paths cannot become links', () => {
  const bad = {...row, usage:'<script>bad</script>', measurements: [{...row.measurements[0], dimensions:{name:'<img onerror=bad>'}}]};
  const html = ui.render({debt_outstanding:bad}, now);
  assert.doesNotMatch(html,/<script>|<img/); assert.match(html,/&lt;script&gt;/);
  for (const key of ['javascript:alert(1)','data/../private','https://evil.test/']) assert.equal(ui.link(key,'x'),'Evidence unavailable');
});
test('missing value is not zero and failed refresh removes previously rendered data', async () => {
  const html = ui.render({debt_outstanding:{...row,measurements:[{...row.measurements[0],value_decimal:null}]}},now);
  assert.match(html,/—/);
  const host = {innerHTML:'old verified value'};
  await ui.refresh(host, async () => ({ok:false}));
  assert.doesNotMatch(host.innerHTML,/old verified value/); assert.match(host.innerHTML,/unavailable/);
});
