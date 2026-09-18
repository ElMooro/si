const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const html = fs.readFileSync(path.join(__dirname, '../auctions.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

test('Treasury banner shows measurement eligibility and deterministic note', () => {
  const elements = new Map();
  const $ = id => { if (!elements.has(id)) elements.set(id, {}); return elements.get(id); };
  const start = html.indexOf('  function renderBanner(D) {');
  const end = html.indexOf('  function takeBar(a) {', start);
  assert.ok(start > 0 && end > start);
  const scope = {$, esc, fmtDate: x=>x, ago: ()=> 'now', toneOf: ()=>'', tagClass: ()=>''};
  vm.runInNewContext(html.slice(start, end), scope);
  scope.renderBanner({generated_at:'2026-09-18T12:00:00Z', freshness:{newest_auction:'2026-09-17'},
    today:{verdict:{date:'2026-09-17', headline:'Observed operations', risk_assets:'neutral',
      liquidity:'cash_management_context', rates:'stronger participation', tags:[], bullets:[]},
      ai_note:{generation_method:'deterministic_treasury_v1', what_happened:'<unsafe>', what_it_means:'Context', watch_next:'Settlement'}}});
  assert.match($('desk-implications').innerHTML, /Measurement only/);
  assert.match($('desk-implications').innerHTML, /financing offsets/);
  assert.doesNotMatch($('desk-implications').innerHTML, /BULLISH|EASY/);
  assert.match($('desk-ai').innerHTML, /no paid AI/);
  assert.doesNotMatch($('desk-ai').innerHTML, /<unsafe>/);
});

test('Missing bidder participation remains unavailable rather than a measured zero', () => {
  const start = html.indexOf('  function takeBar(a) {');
  const end = html.indexOf('  function auctionCard(a, imp) {', start);
  const scope = {pct:x=>x == null ? 'unavailable' : x+'%', bn:String};
  vm.runInNewContext(html.slice(start,end),scope);
  const result = scope.takeBar({indirect_pct:null,direct_pct:0,pd_pct:20});
  assert.match(result,/indirect <b>unavailable/);
  assert.match(result,/direct <b>0%/);
});

test('Tape distinguishes TIPS and FRN and gives the prior-close gap no directional color', () => {
  const elements = new Map();
  const $ = id => { if (!elements.has(id)) elements.set(id, {querySelectorAll:()=>[]}); return elements.get(id); };
  const scope = {$, esc, bn:String, num:String, pct:String, zs:String};
  const label = html.slice(html.indexOf('  function securityLabel(a) {'), html.indexOf('  function renderCalendar(D) {'));
  const tape = html.slice(html.indexOf('  let tapeFilter = "All";'), html.indexOf('  function renderTenors(D) {'));
  vm.runInNewContext(label+tape,scope);
  scope.renderTape({freshness:{bank_records:2},auctions:[
    {type:'Note',instrument_kind:'TIPS',term:'10-Year',cusip:'tips',tail_bp:null,z:{}},
    {type:'Note',instrument_kind:'FRN',term:'2-Year',cusip:'frn',tail_bp:null,z:{}}
  ]});
  assert.match($('desk-tape').innerHTML,/10-Year TIPS/);
  assert.match($('desk-tape').innerHTML,/2-Year FRN/);
  assert.match($('tape-filters').innerHTML,/data-t="TIPS"/);
  assert.doesNotMatch($('desk-tape').innerHTML,/class="(?:pos|neg)"/);
});
