'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '..', 'engines.html'), 'utf8');
const script = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]).find(s => s.includes('function badge'));
function render() {
  const nodes = Object.fromEntries(['stamp','kpis','q','count','rows'].map(id => [id, {innerHTML:'',textContent:'',value:'', handlers:{},addEventListener(name,fn){this.handlers[name]=fn;}}]));
  const row = {name:'invented-engine',outs:['data/invented.json'],pages:['invented.html'],status:'wired-unverified-feed',n_outputs:1,n_present:null,n_referenced:1,age_h:null,outputs:[{key:'data/invented.json',state:'not_checked_during_build',pages:['invented.html'],ref_kind:['exact_path'],age_h:null}]};
  vm.runInNewContext(script, {window:{__jhEngineData:{build_mode:'offline',generated_at:'2026-01-01T00:00:00Z',rows:[row],counts:{'wired-unverified-feed':1},total:1}},document:{getElementById:id=>nodes[id],querySelectorAll:()=>[]}});
  return nodes;
}
test('source directory keeps unknown availability distinct from measured zero', () => {
  const nodes=render();
  assert.match(nodes.stamp.textContent,/LIVE AVAILABILITY NOT CHECKED/);
  assert.match(nodes.rows.innerHTML,/availability unchecked/);
  assert.doesNotMatch(nodes.rows.innerHTML,/0 present/);
  assert.match(nodes.rows.innerHTML,/not_checked_during_build/);
  assert.match(nodes.rows.innerHTML,/REFERENCED · UNVERIFIED/);
  assert.match(nodes.rows.innerHTML,/invented\.html/);
});
test('search and unknown-status filter retain source rows', () => {
  const nodes=render();nodes.q.value='absent';nodes.q.handlers.input();assert.equal(nodes.count.textContent,'0 of 1');
  nodes.q.value='invented';nodes.q.handlers.input();assert.equal(nodes.count.textContent,'1 of 1');
  nodes.kpis.handlers.click({target:{closest:()=>({getAttribute:()=> 'wired-unverified-feed'})}});
  assert.equal(nodes.count.textContent,'1 of 1');
});
