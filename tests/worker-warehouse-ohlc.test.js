const test = require('node:test');
const assert = require('node:assert/strict');
const {pathToFileURL} = require('node:url');
const path = require('node:path');
const {gzipSync} = require('node:zlib');
const crypto = require('node:crypto');
const workerURL = pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/index.js'));
const helperURL = pathToFileURL(path.join(__dirname,'../cloudflare/workers/justhodl-data-proxy/src/warehouse-ohlc.js'));
const bank = {bars:[[946684800,1,2,.5,1.5,100],[946771200,1.5,3,1,2.5,200]],last_date:'2000-01-02'};

async function request(route, entries, vendor) {
  const calls=[];
  const original = global.fetch;
  global.caches = {default:{match:async()=>null,put:async()=>{}}};
  global.fetch = async input => {
    const u = new URL(typeof input === 'string' ? input : input.url);
    calls.push(u.href);
    if (u.hostname.endsWith('.amazonaws.com')) {
      assert.equal(u.pathname,'/data/symdir/endpoint.json');
      return Response.json({url:'https://bank.example'});
    }
    if (u.hostname === 'bank.example') {
      assert.equal(u.pathname,'/warehouse-ohlc');
      const hit = Object.entries(entries)[0];
      if (hit && hit[1] === 403) return Response.json({error:'AccessDenied',warehouse_empty:false},{status:502});
      if (!hit || ['minute','hour'].includes(u.searchParams.get('span'))) return Response.json({warehouse_empty:true,bars:[]});
      return Response.json({warehouse_empty:false,warehouse_key:hit[0],last_modified:'2000-01-01T00:00:00Z',bars:hit[1].bars||hit[1].ohlc,source_span:'day',source_mult:1});
    }
    if (vendor) return vendor(u);
    throw new Error('Vendor forbidden: '+u.hostname);
  };
  try {
    const worker = (await import(workerURL)).default;
    const response = await worker.fetch(new Request('https://worker.example'+route),{POLYGON_KEY:'fixture'}, {waitUntil:()=>{}});
    return {status:response.status,doc:await response.json(),calls};
  } finally {global.fetch=original;}
}

test('stale gzip bank serves daily Polygon route without vendor HTTP',async()=>{
  const r=await request('/ohlc?ticker=AAPL&span=day&days=12000',{'data/warm/tv-bars/universe/US__AAPL.json.gz':bank});
  assert.equal(r.status,200); assert.equal(r.doc.source,'warehouse'); assert.equal(r.doc.count,2);
  assert.equal(r.doc.bars[1].close,2.5); assert(r.calls.every(u=>u.includes('.amazonaws.com/') || u.startsWith('https://bank.example/')));
});
test('Yahoo route uses banked daily bars to form monthly candles',async()=>{
  const r=await request('/yf-ohlc?symbol=^VIX&range=max&interval=1mo',{'data/warm/tv-bars/universe/TVC__VIX.json.gz':bank});
  assert.equal(r.status,200); assert.equal(r.doc.count,1); assert.equal(r.doc.bars[0].value,300);
  assert(r.calls.every(u=>u.includes('.amazonaws.com/') || u.startsWith('https://bank.example/')));
});
test('expired SymDir materialization is also a bank',async()=>{
  const h=crypto.createHash('sha1').update('AAPL').digest('hex');
  const r=await request('/ohlc?ticker=AAPL&span=day',{['data/series-cache/'+h.slice(0,2)+'/'+h+'.json']:{ohlc:[['2000-01-01',1,2,.5,1.5,100]],freq:'D'}});
  assert.equal(r.doc.source,'warehouse'); assert.equal(r.doc.count,1);
});
test('daily bank cannot fabricate intraday; empty span permits vendor',async()=>{
  let vendorCalls=0;
  const r=await request('/ohlc?ticker=AAPL&span=minute',{'data/warm/tv-bars/universe/US__AAPL.json.gz':bank},u=>{
    assert.equal(u.hostname,'api.polygon.io'); vendorCalls++;
    return Response.json({results:[{t:1700000000000,o:1,h:2,l:1,c:2,v:5}]});
  });
  assert.equal(vendorCalls,1); assert.equal(r.doc.count,1);
});
test('unreadable warehouse is not an empty bank',async()=>{
  let vendorCalls=0;
  const r=await request('/ohlc?ticker=AAPL&span=day',{'data/warm/us-equities-daily/AAPL.json.gz':403},()=>{vendorCalls++;return Response.json({});});
  assert.equal(r.status,502); assert.equal(vendorCalls,0);
});
test('forming tail returns at most two daily bars and a one-day request window',async()=>{
  const RealDate = global.Date;
  global.Date = class extends RealDate {constructor(...args){super(...(args.length?args:['2026-09-11T18:00:00Z']));} static now(){return RealDate.parse('2026-09-11T18:00:00Z');}};
  try {
    const r=await request('/ohlc?ticker=AAPL&span=day&days=12000&tail=1',{},u=>{
      assert(u.pathname.endsWith('/1/day/2026-09-10/2026-09-11'));
      return Response.json({results:[1,2,3].map(i=>({t:1700000000000+i*86400000,o:1,h:2,l:1,c:2,v:5}))});
    });
    assert.equal(r.doc.count,2); assert.equal(r.doc.source,'forming-session tail');
  } finally {global.Date=RealDate;}
});
test('weekend tail is empty and never contacts vendor',async()=>{
  const {formingSession,aggregateBars}=await import(helperURL);
  assert.equal(formingSession(new Date('2026-09-13T18:00:00Z')),false);
  assert.equal(formingSession(new Date('2026-09-11T18:00:00Z')),true);
  assert.deepEqual(aggregateBars([{time:1}], 'minute',1,'day'),[]);
});
