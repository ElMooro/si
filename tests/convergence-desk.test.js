const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),{execFileSync}=require('node:child_process');
const M=require('../jh-convergence-desk.js'),CONTRACT='compound-overlays.v1';
function row(symbol,score){return {symbol,desk_score:score,compound_score:score,lifecycle_decay:1,n_systems:2,desk_score_calculation:{contract:CONTRACT,status:'usable',score}};}
const packet=rows=>({overlay_contract:CONTRACT,compound:rows});
test('whole Compound handler reaches actual desk with zeros, negatives, nulls and prior-day cohorts',()=>{
 const p=JSON.parse(execFileSync(process.env.PYTHON||'python3',['-X','utf8','tests/compound_overlay_public_fixture.py'],{cwd:path.join(__dirname,'..'),encoding:'utf8'}));
 const m=M.model(p),r=name=>m.rows.find(r=>r.name===name);
 assert.equal(m.rows.length,30);assert.equal(r('QA2').metrics.desk_score,0);assert.equal(r('QA2').metrics.compound_score,0);assert.equal(r('QA29').metrics.desk_score,null);assert.ok(r('QA0').metrics.desk_score<0);
 assert.deepEqual(M.cohorts(p).map(r=>r.max),[-10,0,15]);assert.equal(M.sorted(m.rows).at(-1).name,'QA29');
});
test('legacy desk score has no fallback to base score and original remains inspectable',()=>{const r={symbol:'Q',desk_score:999,compound_score:20};const m=M.model({compound:[r]});assert.equal(m.rows[0].metrics.desk_score,null);assert.equal(m.rows[0].metrics.compound_score,20);assert.equal(m.rows[0].value,r);});
test('present malformed, null or empty canonical collection never revives ranked alias',()=>{for(const value of [null,[],{},false])assert.equal(M.model({compound:value,ranked:[row('OLD',999)]}).rows.length,0);assert.equal(M.model({ranked:[row('OLD',0)]}).rows[0].pointer,'/ranked/0');});
test('numeric missingness is not zero, and null sorts after valid zero and negatives',()=>{
 const rows=M.model(packet([row('NULL',null),row('NEG',-4),row('ZERO',0),row('BOOL',false),row('STRING','0')])).rows;
 assert.deepEqual(M.sorted(rows).map(r=>r.name),['ZERO','NEG','NULL','BOOL','STRING']);for(const name of ['NULL','BOOL','STRING'])assert.equal(rows.find(r=>r.name===name).metrics.desk_score,null);
});
test('duplicate symbols preserve every occurrence but cannot get an adjusted score',()=>{const source=[row('Q',0),row(' q ',10)];const m=M.model(packet(source));assert.deepEqual(m.rows.map(r=>r.value),source);assert.ok(m.rows.every(r=>r.duplicate&&r.metrics.desk_score===null));});
test('sort and filters do not mutate received populations or truncate records',()=>{const p=packet(Array.from({length:650},(_,i)=>({...row('Q'+i,i),systems:['invented']}))),before=JSON.stringify(p);assert.equal(M.sorted(M.model(p).rows,'compound_score','invented').length,650);assert.equal(JSON.stringify(p),before);});
test('contradictory calculation score does not validate desk number',()=>{const r=row('Q',20);r.desk_score_calculation.score=30;assert.equal(M.model(packet([r])).rows[0].metrics.desk_score,null);});
test('history omits malformed, empty, today, future, duplicate and nonfinite cohorts',()=>{
 const dates=['2026-10-01','2026-10-01','2026-10-02','2026-10-03','2026-07-03','2026-07-04','invalid'];
 const p={overlay_contract:CONTRACT,overlay_evidence:{history:{window_start:'2026-07-04',window_end_exclusive:'2026-10-02',cohorts:dates.map(d=>({status:'included',record:{d},scores:{Q:0}}))}}};
 assert.deepEqual(M.cohorts(p).map(x=>x.date),['2026-07-04']);for(const scores of [{}, {Q:null},{Q:NaN},{Q:true}]){p.overlay_evidence.history.cohorts.at(-2).scores=scores;assert.deepEqual(M.cohorts(p),[]);}
});
test('CSV neutralizes spreadsheet formulas, quotes text, keeps negative numbers and missingness distinct',()=>{const result=M.csv(M.model(packet([row('=HYPERLINK("bad")',-4),row('MISSING',null)])).rows);assert.match(result,/'=HYPERLINK\(""BAD""\)/);assert.match(result,/"-4","-4"/);assert.match(result,/"MISSING","",""/);assert.match(result,/"investment_use"/);});

test('desk source contract names its real primary producer and preserves companion access',()=>{
 const fs=require('node:fs'),html=fs.readFileSync(path.join(__dirname,'../convergence-desk.html'),'utf8');
 assert.match(html,/<meta name="jh-primary-engine" content="justhodl-compound-aggregator">/);
 const p=JSON.parse(fs.readFileSync(path.join(__dirname,'../config/page-data-contracts.json'),'utf8')).pages['convergence-desk.html'];
 assert.ok(p.primary_producers.includes('justhodl-compound-aggregator'));
 for(const key of ['data/fed-pivot-factor-trades.json','data/macro-confluence.json'])assert.ok(html.includes(key));
});
