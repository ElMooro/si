const test=require('node:test'),fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const R=path.join(__dirname,'..'),{manifest,normalize}=require('./helpers/observation-diagnostics-preservation.cjs');
test('diagnostic repair retains every unrelated function and complete predecessor',()=>{for(const file of Object.keys(manifest.entries))normalize(fs.readFileSync(path.join(R,file),'utf8'),file);});
test('predecessor grouping and category failure remain fully reproducible',()=>{
 const group=require('./fixtures/observation-diagnostics/projection-reproduction.json'),category=require('./fixtures/observation-diagnostics/category-reproduction.json');
 assert.equal(new Date(group.result[0].time*1000).toISOString().slice(0,10),'1970-01-01');assert.ok(category.whole_html.includes('[object Object]'));
 for(const file of ['tests/chart-observation-browser-qa.cjs','tests/chart-observation-cache-browser-qa.cjs']){
  const old=fs.readFileSync(path.join(__dirname,'fixtures/observation-diagnostics/pre501',file+'.txt'),'utf8'),current=fs.readFileSync(path.join(R,file),'utf8');
  assert.equal(current,old.replaceAll("assert.equal(await page.evaluate(()=>JHStockDeskController.getModel()),null);","assert.equal(await page.evaluate(()=>JHStockDeskController.getModel()?.valid),false);assert.equal(await page.evaluate(()=>JHStockDeskController.getModel()?.observations.reason),'unknown_series_identity');"));
 }
});
