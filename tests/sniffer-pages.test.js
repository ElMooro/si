const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function kit(file, payload) {
  const calls = [], el = {innerHTML:'', textContent:'', classList:{add(){}}, querySelectorAll:()=>[], querySelector:()=>null};
  const window = {}, document = {getElementById:id=>id==='panel'?el:null, createElement:()=>({}), head:{appendChild(){}}};
  const context = vm.createContext({window,document,Date,Promise,console,fetch:async(url,opts)=>{
    calls.push({url,opts}); return {ok:true,json:async()=>payload};
  }});
  vm.runInContext(fs.readFileSync(path.join(__dirname,'..',file),'utf8'),context);
  return {calls,el,window};
}

test('Dedicated sniffer renderers fetch only fixed canonical public URLs and reject arbitrary contexts',async()=>{
  for(const [file,name,slug,type] of [['ai-frontrun-kit.js','JHAIFront','frontrun-sniffer','frontrun'],['ai-macro-frontrun-kit.js','JHAIMacroFront','macro-frontrun-sniffer','macro_frontrun']]) {
    const view=kit(file,{brief_type:type,context:slug,headline:'Fixture',anomaly_regime:'NORMAL',macro_regime:'NORMAL'});
    await view.window[name].mount('panel',slug);
    assert.equal(new URL(view.calls[0].url).origin,'https://api.justhodl.ai');
    assert.equal(new URL(view.calls[0].url).pathname,'/data/'+slug+'.json');
    assert.equal(view.calls[0].opts.cache,'no-store');
    await view.window[name].mount('panel','brain');
    assert.equal(view.calls.length,1);assert.match(view.el.textContent,/Unsupported/);
  }
});

test('Generic fallback schemas produce explicit unavailable state instead of normal research readings',async()=>{
  for(const [file,name,slug] of [['ai-frontrun-kit.js','JHAIFront','frontrun-sniffer'],['ai-macro-frontrun-kit.js','JHAIMacroFront','macro-frontrun-sniffer']]) {
    const view=kit(file,{mode:'deterministic',regime:'NORMAL',one_liner:'Generic regime'});
    await assert.rejects(view.window[name].mount('panel',slug),/incompatible schema/);
    assert.match(view.el.innerHTML,/unavailable/);assert.doesNotMatch(view.el.innerHTML,/regime-pill/);
  }
});

test('Model-supplied regime strings cannot escape HTML class attributes',async()=>{
  const bad='X" onmouseover="alert(1)';
  for(const [file,name,slug,type] of [['ai-frontrun-kit.js','JHAIFront','frontrun-sniffer','frontrun'],['ai-macro-frontrun-kit.js','JHAIMacroFront','macro-frontrun-sniffer','macro_frontrun']]) {
    const view=kit(file,{brief_type:type,context:slug,anomaly_regime:bad,macro_regime:bad});
    await view.window[name].mount('panel',slug);
    assert.match(view.el.innerHTML,/&quot;/);assert.doesNotMatch(view.el.innerHTML,/"\s+ONMOUSEOVER="/);
  }
});

test('Macro history displays dedicated instrument fields and rejects private aliases without fetching',async()=>{
  const payload={snapshots:[{ts:'2026-09-09T00:00:00Z',score:80,regime:'EXTREME',top_setup_instr:'ZN'}],
    stats_7d:{most_targeted_instruments:[{instrument:'ZN',n_times:3}]},
    events:[{ts:'2026-09-09T00:00:00Z',score:80,regime:'EXTREME',top_setup_instr:'ZN',top_setup_dir:'LONG'}]};
  const view=kit('ai-frontrun-history-kit.js',payload);
  await view.window.JHAIFrontHist.mount('panel','macro-frontrun-sniffer-history');
  assert.equal(new URL(view.calls[0].url).pathname,'/data/macro-frontrun-sniffer-history.json');
  assert.match(view.el.innerHTML,/ZN<span class="n">×3/);assert.match(view.el.innerHTML,/target: <b>ZN<\/b> LONG/);
  await view.window.JHAIFrontHist.mount('panel','brain-history');
  assert.equal(view.calls.length,1);assert.match(view.el.textContent,/Unsupported/);
});
