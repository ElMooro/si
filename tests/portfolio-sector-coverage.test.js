const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),zlib=require('node:zlib'),vm=require('node:vm');
const model=require('../jh-portfolio-risk-contract.js');
const fixture=JSON.parse(zlib.gunzipSync(fs.readFileSync('tests/fixtures/portfolio-sector-coverage-synthetic.json.gz')));
const copy=name=>structuredClone(fixture.cases[name].output);

test('all complete current Python sector books validate across runtimes',()=>{
 for(const [name,case_] of Object.entries(fixture.cases))assert.equal(model.sectors(case_.output).available,true,name);
});
test('missing and sentinel classifications are unknown exposure with no fabricated HHI',()=>{
 for(const name of ['missing','all_unknown','whitespace','sentinel']){const v=model.sectors(copy(name));assert.equal(v.unknownPct,100);assert.equal(v.hhi,null);assert.equal(v.alert,false);assert.deepEqual(v.rows,[]);}
});
test('partial known weights use the whole gross denominator and distinguish a proven threshold breach',()=>{
 let v=model.sectors(copy('partial'));assert.equal(v.rows[0].weight_pct,35);assert.equal(v.unknownPct,65);assert.equal(v.hhi,null);assert.equal(v.alert,false);
 v=model.sectors(copy('known_partial'));assert.equal(v.rows[0].weight_pct,65);assert.equal(v.alert,true);assert.equal(v.hhi,null);
});
test('unrounded threshold, signed offsets, unknown zero and empty books retain their meanings',()=>{
 assert.equal(model.sectors(copy('boundary_exact')).alert,false);assert.equal(model.sectors(copy('boundary_over')).alert,true);
 assert.equal(copy('boundary_over').sector_exposure.records[0].reported_market_value,40);
 assert.equal(model.sectors(copy('boundary_over'),fixture.cases.boundary_over.bundle.inputs.snapshot).alert,true);
 assert.equal(model.sectors(copy('offset')).hhi,5000);assert.equal(model.sectors(copy('unknown_zero')).hhi,10000);
 for(const name of ['all_zero','empty','unpriced']){const v=model.sectors(copy(name));assert.equal(v.hhi,null);assert.equal(v.unknownPct,null);assert.equal(v.alert,false);}
 assert.equal(model.sectors(copy('conflict')).unknownPct,100);
});
test('legacy and self-qualified evidence cannot grant HHI or alerts',()=>{
 for(const d of [null,{}, {...copy('complete'),sector_exposure:null}])assert.equal(model.sectors(d).available,false);
 for(const key of ['classification_verified','sizing_eligible']){const d=copy('complete');d.sector_exposure[key]=true;assert.equal(model.sectors(d).available,false);}
});
test('every sector row must correspond to its complete loaded holding when provided',()=>{
 const frame=fixture.cases.complete,s=structuredClone(frame.bundle.inputs.snapshot);
 assert.equal(model.sectors(frame.output,s).available,true);
 for(const field of ['sector','symbol','market_value']){const changed=structuredClone(s);changed.positions[0][field]=field==='market_value'?1:'OTHER';assert.equal(model.sectors(frame.output,changed).available,false);}
 assert.equal(model.sectors(frame.output,{positions:[]}).available,false);
 const reversed=structuredClone(s);reversed.positions.reverse();assert.equal(model.sectors(frame.output,reversed).available,false);
});
test('all populations, identities, original labels and group membership reconcile',()=>{
 const changes=[e=>e.records.pop(),e=>e.position_count++,e=>e.priced_position_count--,e=>e.classified_position_count--,e=>e.unclassified_position_count++,e=>e.records[0].input_index=1,e=>e.records[0].reported_sector=null,e=>e.records[0].sector='Unknown',e=>e.known_sectors[0].input_indices=[],e=>e.known_sectors.push(e.known_sectors[0])];
 for(const change of changes){const d=copy('complete');change(d.sector_exposure);assert.equal(model.sectors(d).available,false);}
});
test('invalid amounts, weights, denominators, HHI and false threshold metadata are refused',()=>{
 const changes=[e=>e.records[0].signed_marked_value=true,e=>e.records[0].gross_marked_value=-1,e=>e.gross_marked_value=1,e=>e.unclassified_weight_pct=1,e=>e.classification_coverage_pct=99,e=>e.known_sectors[0].weight_pct=100,e=>e.known_sectors[0].signed_marked_value=0,e=>e.concentration_hhi=0,e=>e.maximum_sector_weight_pct=100,e=>e.known_sector_above_40pct=false,e=>e.status='PARTIAL_CLASSIFICATION'];
 for(const change of changes){const d=copy('complete');change(d.sector_exposure);assert.equal(model.sectors(d).available,false);}
 const d=copy('partial');d.sector_exposure.concentration_hhi=5450;assert.equal(model.sectors(d).available,false);
});
function page(){
 const elements=new Map(),document={hidden:false,addEventListener(){},querySelector(){return {style:{}};},getElementById(id){if(!elements.has(id))elements.set(id,{textContent:'',innerHTML:'',style:{}});return elements.get(id);}};
 const scope=vm.createContext({document,window:{addEventListener(){}},JHPortfolioRisk:model,Date,console,AbortController,setInterval(){},clearInterval(){},fetch(){return new Promise(()=>{});}});
 const html=fs.readFileSync('portfolio/index.html','utf8');vm.runInContext([...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n'),scope);
 return {elements,show(name){scope.frame=copy(name);scope.holdings=structuredClone(fixture.cases[name].bundle.inputs.snapshot);vm.runInContext('risk=frame;snapshot=holdings;renderTopMetrics();renderAlerts();renderSectors();',scope);},run:code=>vm.runInContext(code,scope)};
}
test('actual page shows unknown coverage without an Unknown concentration alert',()=>{
 const p=page();p.show('all_unknown');assert.equal(p.elements.get('mv-hhi').textContent,'—');assert.equal(p.elements.get('alert-zone').innerHTML,'');
 assert.match(p.elements.get('sector-summary').textContent,/100.00%.*unclassified/);assert.match(p.elements.get('sector-body').innerHTML,/Complete sector coverage records/);
 p.show('complete');assert.equal(p.elements.get('mv-hhi').textContent,'5000');assert.match(p.elements.get('alert-zone').innerHTML,/REPORTED SECTOR EXPOSURE/);
});
test('actual page preserves partial known exposure and escapes every complete metadata field',()=>{
 const p=page();p.show('partial');assert.match(p.elements.get('sector-body').innerHTML,/35.00%/);assert.equal(p.elements.get('alert-zone').innerHTML,'');
 p.show('literal');const h=p.elements.get('sector-body').innerHTML;assert.match(h,/&lt;img/);assert.match(h,/日本/);assert.doesNotMatch(h,/<img/);
 p.run('risk=null;renderSectors();renderAlerts();');assert.equal(p.elements.get('sector-body').innerHTML,'');assert.equal(p.elements.get('alert-zone').innerHTML,'');
});
