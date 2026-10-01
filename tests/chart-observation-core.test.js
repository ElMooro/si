const test=require('node:test'),assert=require('node:assert/strict');
const core=require('../jh-observation-series.js');
const days=Array.from({length:14},(_,i)=>'2026-09-'+String(i+1).padStart(2,'0'));
const values=[1,2,3,4,5,6,7,8,null,false,'',[],0,'0'];
const packet=()=>({generated_at:'2026-10-01T00:00:00Z',extra:{retained:'whole'},series:{invented:{d:days.slice(),v:structuredClone(values)}},twins:{invented:{d:['2010-01-01'],v:[999]}}});
test('missing/coercible values cannot become zero and whole packet remains unchanged',()=>{
 const p=packet(),raw=JSON.stringify(p),r=core.cq(p,'CQ:invented');assert.equal(r.d.length,10);assert.equal(r.evidence.rejected_records,4);
 assert.deepEqual(r.d.slice(-2).map(x=>x.close),[0,0]);assert.ok(r.d.every(x=>x.volume===null));
 assert.deepEqual(r.evidence.records.slice(8,12).map(x=>x.reason),Array(4).fill('missing_or_invalid_scalar'));
 assert.deepEqual(r.evidence.whole_packet,p);assert.equal(JSON.stringify(p),raw);assert.equal(r.evidence.proxy_histories[0].joined,false);
 assert.deepEqual(r.evidence.proxy_histories[0].whole_series,p.twins.invented);assert.equal(r.d[0].close,1);
});
test('strict finite decimal scalars preserve signs and reject non-numbers',()=>{
 for(const x of [null,undefined,false,true,[],[1],{},'', ' ', '\t', '0x10','1,000','NaN','Infinity','1e-999','-1e-999',Infinity,NaN])assert.equal(core.numeric(x),null,String(x));
 for(const [x,y] of [[0,0],[-5,-5],['0',0],[' -1.25 ',-1.25],['2e-3',.002],['.5',.5],['+0',0],['0e-999',0]])assert.equal(core.numeric(x),y);
});
test('dates are strict calendars; monthly coordinates are month-end, not availability',()=>{
 for(const x of ['2026-02-29','2026-04-31','2026-13','2026-00','0000-01','2026-2-1','2026-01-01T00:00:00Z',0,null,[],false])assert.equal(core.period(x),null,String(x));
 assert.equal(core.period('2024-02','M').period_end,'2024-02-29');assert.equal(core.period('2026-02','M').period_end,'2026-02-28');
 assert.equal(core.period('2026-12','M').period_end,'2026-12-31');assert.equal(core.period('0001-01-01','D').period_end,'0001-01-01');
 assert.equal(core.period('2026-09','D'),null);assert.equal(core.period('2026-09-01','M'),null);
 assert.equal(core.period('2026-09','M').source_availability_at,null);
});
test('unknown and ambiguous IDs cannot select a nearby series or a proxy',()=>{
 const p=packet();assert.equal(core.cq(p,'CQ:invent').evidence.reason,'unknown_series_identity');
 p.series.INVENTED=p.series.invented;assert.equal(core.cq(p,'CQ:invented').evidence.reason,'ambiguous_series_identity');
 delete p.series.invented;delete p.series.INVENTED;assert.equal(core.cq(p,'CQ:invented').d.length,0);
});
test('unpaired arrays, invalid periods and conflicting duplicates retain each original',()=>{
 const p={series:{x:{d:['2026-09-03','2026-09-01','2026-09-02','2026-09-02','2026-09-01','2026-02-30','2026-09-04'],v:[3,0,2,99,'0',6]}}};
 const r=core.cq(p,'CQ:x');assert.deepEqual(r.d.map(x=>x.close),[0,3]);assert.equal(r.evidence.records.length,7);
 assert.deepEqual(r.d[0].observation_ordinals,[1,4]);assert.equal(r.evidence.records[6].reason,'unpaired_or_malformed_row');
 assert.equal(r.evidence.records[2].reason,'conflicting_duplicate_period');assert.equal(r.evidence.records[3].reason,'conflicting_duplicate_period');
});
test('CISS EA alias is one exact canonical ECB key and never the first headline label',()=>{
 const p={series:[{id:'other',label:'EA headline composite',category:'ea_headline',points:days.map((d,i)=>[d,i])},
 {id:'official',key:'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX',freq:'D',unit:'dimensionless_index',points:days.map((d,i)=>[d,values[i]])}]};
 const r=core.ciss(p,'CISS:ea');assert.equal(r.evidence.selected_id,p.series[1].key);assert.equal(r.d.length,10);assert.equal(r.evidence.selected_path,'series[1]');
 assert.equal(core.ciss(p,'CISS:unknown').d.length,0);assert.equal(core.ciss(p,'CISS:EA headline composite').d.length,0);
 p.series.push(structuredClone(p.series[1]));assert.equal(core.ciss(p,'CISS:ea').evidence.reason,'ambiguous_series_identity');
});
test('monthly observations retain period labels, exact metadata and unqualified clocks',()=>{
 const row={id:'monthly',key:'INVENTED.MONTHLY',freq:'M',unit:'dimensionless_index',points:[['2024-01',-1],['2024-02',0],['2024-03',1]],
 source_published_at:null,acquired_at:'2026-09-30T01:00:00Z',points_scope:'entire invented source',quality:{status:'fresh',note:'reported, not verified'}};
 const p={series:[row],generated_at:'2026-10-01T00:00:00Z'},r=core.ciss(p,'CISS:monthly');
 assert.deepEqual(r.d.map(x=>new Date(x.time*1000).toISOString().slice(0,10)),['2024-01-31','2024-02-29','2024-03-31']);
 assert.deepEqual(r.evidence.records.map(x=>x.raw_period),['2024-01','2024-02','2024-03']);assert.equal(r.evidence.unit,row.unit);
 assert.equal(r.evidence.source_freshness_verified,false);assert.equal(r.evidence.source_published_at,null);assert.equal(r.evidence.calls_eligible,false);assert.equal(r.evidence.sizing_eligible,false);
});
test('malformed source and no numeric observations fail explicitly without synthesizing a point',()=>{
 for(const row of [{}, {d:true,v:[]},{d:[],v:{}},{d:days,v:Array(14).fill(null)}]){const p={series:{x:row}},r=core.cq(p,'CQ:x');assert.equal(r.d.length,0);assert.equal(r.evidence.status,'unavailable');assert.deepEqual(r.evidence.whole_packet,p);}
 assert.equal(core.ciss({series:{}},'CISS:ea').evidence.reason,'malformed_series_list');
});
