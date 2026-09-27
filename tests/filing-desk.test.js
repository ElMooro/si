const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const api=require('../jh-filing-desk.js'),values=require('../jh-table-values.js'),root=path.join(__dirname,'..');
const fixture=x=>({packet:x,raw:JSON.stringify(x)});
async function page(file,received){
 const nodes=new Map();let document;const calls=[];
 const get=id=>{
  if(!nodes.has(id)){
   const n={value:'',headers:[],contains:()=>true};let html='',text='';
   Object.defineProperty(n,'innerHTML',{get:()=>html,set:v=>{html=v;text='';n.headers=[...v.matchAll(/<th data-k="([^"]+)"/g)].map(m=>({dataset:{k:m[1]},ownerDocument:document,setAttribute(k,v){this[k]=v;},focus(){document.activeElement=this;}}));}});
   Object.defineProperty(n,'textContent',{get:()=>text,set:v=>{text=String(v);html='';}});nodes.set(id,n);
  }return nodes.get(id);
 };
 document={getElementById:get,activeElement:null,querySelectorAll:s=>s.includes('th[data-k]')?get('board').headers:[]};
 const ctx=vm.createContext({document,URL,console,JHTableValues:{...values,load:async p=>{calls.push(p);if(received instanceof Error)throw received;return received;}}});
 vm.runInContext(fs.readFileSync(path.join(root,'jh-filing-desk.js'),'utf8'),ctx);
 const source=fs.readFileSync(path.join(root,file),'utf8');
 for(const m of source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))vm.runInContext(m[1],ctx);
 for(let i=0;i<3;i++)await new Promise(r=>setImmediate(r));
 return{get,document,calls,source};
}
test('document URLs only permit reviewed SEC Archives HTTPS documents',()=>{
 const good='https://www.sec.gov/Archives/edgar/data/123/0001234567/test-index.htm';assert.equal(api.secURL(good),good);
 for(const bad of ['javascript:void(0)','data:text/html,x','http://www.sec.gov/Archives/edgar/data/123/test.htm','https://www.sec.gov.evil.test/Archives/edgar/data/123/a.htm','https://www.sec.gov@evil.test/Archives/edgar/data/123/a.htm','https://user@www.sec.gov/Archives/edgar/data/123/a.htm','https://www.sec.gov:4433/Archives/edgar/data/123/a.htm','//www.sec.gov/Archives/edgar/data/123/a.htm',good+'?next=javascript:1',good+'#x',good+'\n',good.replace('/test','/../test'),good.replace('/test','/%2e%2e/test'),good.replace('/test','/a\\test'),{},null])assert.equal(api.secURL(bad),null,String(bad));
 assert.ok(!api.documentLink('javascript:1','<img src=x>').includes('<img'));assert.ok(!api.documentLink('javascript:1','title').includes('href='));
});
test('dates validate calendar, timezone and missingness before sorting in either direction',()=>{
 for(const bad of [null,false,0,{},'2026-02-30','2025-02-29','2026-13-01','2026-09-27T12:00:00','2026-09-27T24:00:00Z','2026-09-27T00:00:00+25:00','09/27/2026'])assert.equal(api.date(bad),null,String(bad));
 assert.notEqual(api.date('2024-02-29'),null);assert.notEqual(api.date('2026-09-27T12:00:00.123456+00:00'),null);
 const rows=api.model({filings:[{filed_at:'bad',accession:'a'},{filed_at:'2026-09-20',accession:'b'},{filed_at:'2026-09-21',accession:'c'},{accession:'d'}]},'8k').rows;
 assert.deepEqual(rows.slice().sort((a,b)=>api.compare(a,b,'filed_time',1)).map(x=>x.view.accession),['b','c','a','d']);
 assert.deepEqual(rows.slice().sort((a,b)=>api.compare(a,b,'filed_time',-1)).map(x=>x.view.accession),['c','b','a','d']);
 assert.equal(api.compare(rows[0],rows[3],'filed_time',-1),0);
});
test('all modern and legacy signal occurrences survive absent identifiers and overlapping populations',()=>{
 const event={signal_id:'going_concern',filed_at:'2026-09-27',unknown:{retain:true}},other={...event,filed_at:'2026-09-26'};
 const p={all_tickers:[{ticker:'PARENT',events:[event,other,null,{signal_id:'__proto__'}]}],highlights:{critical:[{events:[event]}],risks:[{events:[other]}],opportunities:[{events:[event]}]},critical:[event],opportunities:[other],risks:[event]};
 const before=JSON.stringify(p),m=api.model(p,'flags');assert.equal(m.rows.length,8);assert.equal(m.malformed,1);assert.equal(m.rows[0].view.ticker,'PARENT');
 assert.equal(m.rows[0].source,event);assert.equal(m.rows[1].source,other);assert.equal(new Set(m.rows.map(x=>x.sourcePath)).size,8);assert.equal(JSON.stringify(p),before);
 const many=api.model({all_tickers:[{events:Array.from({length:501},()=>event)}]},'flags');assert.equal(many.rows.length,501);
});
test('missing, malformed, zero-row and partial populations are distinguishable',()=>{
 assert.equal(api.model({filings:[]},'8k').primaryAvailable,true);assert.equal(api.model({},'8k').primaryAvailable,false);
 const m=api.model({filings:false,amended:[null,{form:'10-K/A'}]},'10kq');assert.equal(m.rows.length,1);assert.equal(m.malformed,1);assert.match(m.warnings.join(' '),/filings is missing/);
 const red=api.model({all_tickers:[],highlights:{critical:'wrong'}},'flags');assert.match(red.warnings.join(' '),/highlights.critical/);
});
test('8-K fixture renders inert text and full labels, strict counts and every original field',async()=>{
 const p={generated_at:'bad',window_days:false,stats:{total_filings:'<img src=x>',red_flag_filings:0},item_labels:{'4.02':'Full non-reliance label longer than 28 characters <img src=x>'},by_item_counts:{'4.02':'<img src=x>'},filings:[{company:'A < B',filed_at:'2026-02-30',items:['4.02'],filing_url:'javascript:void(0)',accession:0},null,{items:false}],unknown:'preserve'};
 const received=fixture(p),s=await page('8k-items.html',received),h=s.get('board').innerHTML;
 assert.equal(s.calls[0],'/data/8k-filings.json');assert.equal(s.calls.length,1);assert.equal(s.get('original').textContent,received.raw);assert.ok(!h.includes('<img'));assert.ok(!h.includes('javascript:'));assert.match(h,/Full non-reliance label longer than 28 characters/);assert.match(h,/Invalid date/);assert.match(s.get('kpis').textContent,/Source red flags: 0/);assert.match(s.get('kpis').textContent,/Source filings: Unavailable/);assert.match(s.get('status').textContent,/1 malformed/);assert.ok(!s.get('items').innerHTML.includes('<img'));assert.match(s.get('items').innerHTML,/<button/);
});
test('10-K/Q counts cannot concatenate and keyboard sort retains focus and both missing rows',async()=>{
 const p={stats:{total:0,total_10k_amended:'2',total_10q_amended:'10'},filings:[{company:'X',filed_at:'2026-09-27'},{company:'Y',filed_at:'invalid'},{company:'Z'}],amended:[{company:'X',form:'10-K/A'}]};
 const s=await page('10kq-filings.html',fixture(p));assert.match(s.get('kpis').textContent,/Source total: 0/);assert.match(s.get('kpis').textContent,/Source 10-K\/A: 2 · Source 10-Q\/A: 10/);assert.match(s.get('status').textContent,/4 of 4/);
 const header=s.get('board').headers[0];header.focus();header.onkeydown({key:'Enter',preventDefault(){}});assert.equal(s.document.activeElement.dataset.k,'filed_time');assert.equal(s.document.activeElement['aria-sort'],'ascending');s.document.activeElement.onkeydown({key:' ',preventDefault(){}});assert.equal(s.document.activeElement['aria-sort'],'descending');
 s.get('q').value='Z';s.get('q').oninput();assert.match(s.get('status').textContent,/1 of 4/);assert.ok(!s.source.includes('An amendment is a restated'));
});
test('red flags show modern highlights, weight missingness and lineage without invented bullish labels',async()=>{
 const p={n_tickers_with_signals:'<img src=x>',all_tickers:[],highlights:{critical:[{events:[{signal_id:'bankruptcy',weight:'<img src=x>',ticker:'X',accession:'<img src=x>',filed_at:'2026-09-26',filing_url:'javascript:1',polarity:'unknown'},{signal_id:'bankruptcy',weight:0,ticker:'X',filed_at:'2026-09-27'}]}]}};
 const s=await page('filing-redflags.html',fixture(p)),h=s.get('board').innerHTML;assert.match(s.get('status').textContent,/2 of 2/);assert.match(h,/highlights.critical\[0\].events\[1\]/);assert.match(h,/<td>0.00<\/td>/);assert.ok(!h.includes('<img'));assert.ok(!h.includes('class="bull"'));assert.equal(s.get('original').textContent,JSON.stringify(p));
});
test('HTTP and rejected JSON failures are explicit and original rejected text stays inert',async()=>{
 for(const name of ['8k-items.html','10kq-filings.html','filing-redflags.html']){
  const error=Error('Duplicate JSON key');error.original_text='{"x":"<img>","x":2}';const s=await page(name,error);assert.match(s.get('board').textContent,/Publication unavailable/);assert.equal(s.get('original').textContent,error.original_text);assert.equal(s.calls.length,1);
  const fail=await page(name,Error('HTTP 403'));assert.match(fail.get('board').textContent,/403/);assert.equal(fail.get('original').textContent,'Unavailable');
 }
});
test('three exact stored routes use a single non-generating read; unknown paths never request',async()=>{
 for(const key of ['8k-filings','10kq-filings','sec-filings-intel']){
  let calls=0;const p=await values.load('/data/'+key+'.json',{fetcher:async(url,options)=>{calls++;assert.equal(url,'/data/'+key+'.json?exact=1&nogen=1');assert.equal(options.redirect,'error');return new Response('{"unknown":9007199254740993}');}});assert.equal(calls,1);assert.equal(p.raw,'{"unknown":9007199254740993}');
 }
 await assert.rejects(values.load('/data/account-filings.json',{fetcher:()=>assert.fail('No private source reads')}),/Unreviewed/);
});
test('complete predecessors remain byte-identical and pages expose labelled scrollable evidence',()=>{
 const hashes={'8k-items.html':'b226dc0dfb1a38602d23b6329777aca8f8150f47bb0557e3ca19d9ab1e7baa4b','10kq-filings.html':'a41f4568de568bcde957d1e55ff1da0ad0f3688b3d0151e0377f97ab94c83d2b','filing-redflags.html':'fab848119542179c5ba91dd27d7a665dbcf74c114c36372dcb26f958b5e2ae39'};
 for(const[name,hash]of Object.entries(hashes)){assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(__dirname,'fixtures','pre-filing-desk-'+name+'.txt'))).digest('hex'),hash);const html=fs.readFileSync(path.join(root,name),'utf8');assert.match(html,/<label for="q">/);assert.match(html,/role="region"[^>]*tabindex="0"/);assert.match(html,/id="original"/);assert.match(html,/source:"\/data\//);}
});
