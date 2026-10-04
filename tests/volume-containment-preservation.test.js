// These gates retain whole predecessors and reject any unreviewed source delta.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const ROOT=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/volume-containment'),P=require('./helpers/volume-containment-preservation.cjs');
const read=p=>fs.readFileSync(path.join(ROOT,p),'utf8');
for(const [name,t]of Object.entries(P.manifest.sources))test('complete reviewed '+name+' source reverses exactly; defective baseline cannot pass new acceptance',()=>{
 const actual=read(t.path),old=read(t.before_path),restore=name==='worker'?P.restoreWorker:P.restoreEngine;
 assert.equal(P.hash(actual),t.after_sha256);assert.equal(P.hash(old),t.before_sha256);assert.equal(restore(actual),old);assert.throws(()=>restore(old),/exact reviewed containment candidate/);
});
test('complete baseline Worker inventory and every unchanged dependency survive',()=>{
 const inventory=JSON.parse(fs.readFileSync(path.join(D,'worker-module-inventory.json'),'utf8'));assert.equal(Object.keys(inventory).length,11);
 for(const [name,row]of Object.entries(inventory)){const raw=fs.readFileSync(path.join(D,'worker-before',name));assert.equal(raw.length,row.bytes);assert.equal(P.hash(raw),row.sha256);if(name!=='index.js')assert.deepEqual(fs.readFileSync(path.join(ROOT,row.source_path)),raw);}
});
for(const [name,h]of Object.entries(P.manifest.guard_hooks))test('reviewed '+name+' hook preserves every original assertion and complete prior helper/reader',()=>{
 const old=read(h.before_path);assert.equal(P.hash(old),h.before_sha256);let expected=old;for(const e of h.edits){assert.equal(expected.split(e.before).length,2);expected=expected.replace(e.before,e.after);}const actual=h.path==='tests/helpers/watchlist-source-preservation.cjs'?require('./helpers/chart-cache-transition-preservation.cjs').restoreHook(read(h.path),'watchlist'):read(h.path);assert.equal(actual,expected);assert.equal(P.hash(actual),h.after_sha256);
});
for(const [name,restore,marker,replacement]of[
 ['Worker unrelated handler',P.restoreWorker,'function corsHeaders','function changedCorsHeaders'],
 ['Worker policy',P.restoreWorker,'No volume unit is inferred or converted.','Quantity may be guessed.'],
 ['Worker quantity rule',P.restoreWorker,'const merged = mergeBarsPrefer(hist, warm.bars);','const merged = mergeBarsPrefer(hist, warm.bars); merged[0].value=0;'],
 ['Worker coverage',P.restoreWorker,'bars: merged,','bars: merged.slice(1),'],
 ['Worker OHLC',P.restoreWorker,'bars: merged,','bars: merged.map(b=>({...b,close:0})),'],
 ['engine paint computation',P.restoreEngine,'async function paint(d){','async function paint(d){ d[0].close=0;'],
 ['engine private timeframe',P.restoreEngine,'interval===tf&&generation===loadGen','interval===window.tf&&generation===loadGen'],
 ['engine reentrant sequence',P.restoreEngine,'var seq=paintSeq+1,pending=paint(d);','var pending=paint(d),seq=paintSeq;'],
 ['engine lost replay identity',P.restoreEngine,'replay.full=sliceIdentifiedBars(lastBars);','replay.full=lastBars.slice();'],
])test('exact preservation rejects '+name+' mutation',()=>{const t=P.manifest.sources[name.startsWith('Worker')?'worker':'engine'],raw=read(t.path);assert.equal(raw.split(marker).length,2,name);assert.throws(()=>restore(raw.replace(marker,replacement)),/exact reviewed containment candidate/);});
