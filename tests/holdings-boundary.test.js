const test = require('node:test');
const assert = require('node:assert/strict');
const api = require('../jh-holdings-boundary.js');
test('old, missing and forged eligibility packets cannot claim exclusion', () => {
  for (const packet of [null, {}, {calls_eligible:false}, {holdings_exclusions:{basis:'wrong'}}]) {
    assert.equal(api.describe(packet).revised,false);
    assert.match(api.describe(packet).text,/may still include/);
  }
});
test('revised packet discloses limited scope and its own time', () => {
  const state=api.describe({generated_at:'2026-09-20T01:00:00Z',holdings_exclusions:{basis:'holdings-direct-and-cluster-excluded.v1'}});
  assert.equal(state.asof,'2026-09-20T01:00:00Z'); assert.equal(state.revised,true);
  assert.match(state.text,/Other indirect paths/); assert.match(state.text,/remain unverified/);
});

test('ranker as_of clock is retained',()=>{assert.equal(api.describe({as_of:'2026-09-20T01:00:00Z'}).asof,'2026-09-20T01:00:00Z');});
