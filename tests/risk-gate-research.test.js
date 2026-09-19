const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto'),zlib=require('node:zlib');
const api=require('../jh-risk-gate-research.js');
const now=Date.parse('2026-09-18T21:00:00Z');
function packet(){return {contract:'risk-gate-research.v1',generated_at:'2026-09-18T20:00:00Z',source_generated_at:'2026-09-18T20:00:00Z',replay:{manifest_key:'data/risk-gate-research/runs/aaa.json'},series:{RRPONTSYD:{series_id:'RRPONTSYD',_label:'Reverse repo',_units:'Billions of US Dollars',_category:'balance_sheet',quality:{status:'fresh'},available:true,frequency:'D',latest_date:'2026-09-18',acquired_at:'2026-09-18T20:00:00Z',latest_value_decimal:'0.576',last_observed_value:'0.576',source_row:0,calendar_comparisons:{month:{change_decimal:'0.076',change_unit:'Billions of US Dollars',baseline_date:'2026-08-18',target_date:'2026-08-18',baseline_decimal:'0.5',pct_change:15.2}}}}};}
test('Risk Gate rejects legacy allocation packets and page independently declares abstention',()=>{
 assert.throws(()=>api.render({regime:'NORMAL',interpretation:{target_allocation:[{ticker:'SPY',weight_pct:100}]}}));
 const html=fs.readFileSync('risk-gate.html','utf8');
 assert.match(html,/WAIT · abstain/);assert.match(html,/No trade, hedge or target allocation is authorized/);
 assert.ok(!html.includes('src="/jh-page-ai.js"'));assert.ok(!html.includes('data-bars="interpretation.target_allocation'));
});
test('native units, zeroes and exact calendar baseline render without an invented allocation',()=>{
 const p=packet(),html=api.render(p,{},now);
 assert.match(html,/0.576/);assert.match(html,/Billions of US Dollars/);assert.match(html,/baseline 2026-08-18/);
 p.series.RRPONTSYD.latest_value_decimal='0';assert.equal(api.status(p.series.RRPONTSYD,p,now),'fresh');
 assert.match(api.render(p,{},now),/<strong>0<\/strong>/);
});
test('page clears current values and changes when clocks expire or move into the future',()=>{
 const p=packet(),row=p.series.RRPONTSYD;
 assert.equal(api.status(row,p,now+2*86400000),'stale_source');
 const html=api.render(p,{},now+2*86400000);
 assert.match(html,/<strong>Unavailable<\/strong>/);assert.ok(!html.includes('<strong>0.076'));
 assert.match(html,/Last observed 0.576/);
 p.generated_at='2099-01-01T00:00:00Z';assert.equal(api.status(row,p,now),'invalid_clock');
});
test('source text is escaped, unsafe links rejected, missing history breaks chart line',()=>{
 const p=packet();p.series.RRPONTSYD._label='<img src=x onerror=bad>';
 assert.ok(!api.render(p,{},now).includes('<img'));assert.match(api.render(p,{},now),/&lt;img/);
 assert.equal(api.path('data/evidence/../../private'),null);assert.equal(api.path('javascript:bad'),null);
 const chart=api.chart([{date:'2026-09-18',value:2},{date:'2026-09-17',value:null},{date:'2026-09-16',value:0}]);
 assert.equal((chart.match(/M\d/g)||[]).length,2);assert.ok(!chart.includes('NaN'));
});
test('original browser verification checks gzip hash, exact row identity and decimal value',async()=>{
 const row=packet().series.RRPONTSYD,raw=Buffer.from(JSON.stringify({observations:[{date:'2026-09-18',value:'.576'}]}));
 row.evidence={observations:{key:'data/evidence/fred/test.bin.gz',sha256:crypto.createHash('sha256').update(raw).digest('hex'),bytes:raw.length}};
 const zipped=zlib.gzipSync(raw),fetcher=async()=>({ok:true,arrayBuffer:async()=>zipped.buffer.slice(zipped.byteOffset,zipped.byteOffset+zipped.byteLength)});
 assert.match(await api.verify(row,fetcher),/Verified original response SHA-256 and row 0/);
 row.last_observed_value='0.0576';await assert.rejects(()=>api.verify(row,fetcher),/value differs/);
 row.last_observed_value='.576';row.source_row=1;await assert.rejects(()=>api.verify(row,fetcher),/identity differs/);
 row.source_row=0;row.evidence.observations.sha256='0'.repeat(64);await assert.rejects(()=>api.verify(row,fetcher),/hash or byte count differs/);
});

test('matched differences expire with the source rows and preserve zero and negative values',()=>{
 const p=packet();p.derived={example:{label:'Matched CP rates',status:'descriptive',series_ids:['RRPONTSYD'],value_decimal:'0',unit:'basis_points',formula:'(A - B) * 100',inputs:[],observation_date:'2026-09-18',limitation:'No executable quote'}};
 assert.match(api.derivedHTML(p,now),/0 basis_points/);
 p.derived.example.value_decimal='-23';assert.match(api.derivedHTML(p,now),/-23 basis_points/);
 const expired=api.derivedHTML(p,now+2*86400000);assert.ok(!expired.includes('-23 basis_points'));assert.match(expired,/Unavailable/);
 p.derived.example.label='<script>bad</script>';assert.ok(!api.derivedHTML(p,now).includes('<script>'));
});

test('Quantum sizing history distinguishes unavailable, genuine zero and old positive values',()=>{
 const html=fs.readFileSync('quantum-desk.html','utf8'),fn=html.slice(html.indexOf('function sizingHistory('),html.indexOf('Promise.all([fetch(FEED,'));
 const render=new Function('esc',fn+'; return sizingHistory;')(v=>String(v).replaceAll('<','&lt;'));
 assert.match(render([{sizing_x:null}]),/sizing unavailable/);assert.ok(!render([{sizing_x:null}]).includes('×0'));
 assert.match(render([{sizing_x:0}]),/historical sizing ×0/);assert.match(render([{sizing_x:.75}]),/not a current recommendation/);
 assert.match(render([{sizing_x:'1'}]),/sizing unavailable/);assert.match(render([{sizing_x:NaN}]),/sizing unavailable/);
});
