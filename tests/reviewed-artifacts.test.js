const test = require('node:test');
const assert = require('node:assert/strict');
const {pathToFileURL} = require('node:url');
const path = require('node:path');
const modulePath = pathToFileURL(path.join(__dirname, '../cloudflare/workers/justhodl-data-proxy/src/reviewed-artifacts.js')).href;

test('carry research exposes fixed source-status codes without weakening raw-diagnostic blocking or caching', async()=>{
  const {reviewedArtifact,serveReviewedArtifact}=await import(modulePath),original=globalThis.fetch;
  const document={contract:'carry-original-research.v1',errors:2,source_status_codes:{MUNI:'HTTP_400',INR:'original_source_unavailable'},calls_eligible:false};
  try{
    globalThis.fetch=async(url,options)=>{assert.equal(options.cache,'no-store');assert.equal(options.cf,undefined);return Response.json(document);};
    const request=new Request('https://example.test/data/carry-surface.json'),review=reviewedArtifact('data/carry-surface.json');
    const response=await serveReviewedArtifact(request,review,'https://origin.test',{});assert.equal(response.status,200);assert.deepEqual(await response.json(),document);
    document.equities={X:{headers:{token:'SYNTHETIC_SECRET'}}};assert.equal((await serveReviewedArtifact(request,review,'https://origin.test',{})).status,503);
  }finally{globalThis.fetch=original;}
});

test('digest transport replies and unreviewed generations never reach public users', async () => {
  const {reviewedArtifact, serveReviewedArtifact} = await import(modulePath);
  const review = reviewedArtifact('data/_alerts/digest-2026-01-01-close.json');
  assert.equal(review.key, 'data/_alerts/digest-2026-01-01-close.json');
  const original = globalThis.fetch;
  try {
    for (const document of [{telegram_info: 'SYNTHETIC_SECRET'}, {public_digest_schema: 'digest-public.v1', telegram_info: 'SYNTHETIC_SECRET'}]) {
      globalThis.fetch = async () => Response.json(document);
      const r = await serveReviewedArtifact(new Request('https://example.test/data/digest.json'), review, 'https://origin.test', {});
      assert.equal(r.status, 503); assert.ok(!(await r.text()).includes('SYNTHETIC_SECRET'));
    }
    globalThis.fetch = async () => Response.json({public_digest_schema: 'digest-public.v1', equity_score: 0});
    const r = await serveReviewedArtifact(new Request('https://example.test/data/digest.json'), review, 'https://origin.test', {});
    assert.equal(r.status, 200); assert.equal((await r.json()).equity_score, 0);
    assert.equal(r.headers.get('X-JH-Artifact-Key'), review.key); assert.equal(r.headers.get('Cache-Control'), 'no-store');
  } finally { globalThis.fetch = original; }
});

test('range cannot bypass redaction and unknown digest names are not approved', async () => {
  const {reviewedArtifact, serveReviewedArtifact} = await import(modulePath);
  const review = reviewedArtifact('data/_alerts/digest-latest.json');
  const r = await serveReviewedArtifact(new Request('https://example.test/digest.json', {headers: {Range: 'bytes=0-20'}}), review, 'https://origin.test', {});
  assert.equal(r.status, 416);
  assert.equal(reviewedArtifact('data/_alerts/digest-private/x.json'), null);
});

test('every reviewed history family and current head requires the versioned marker', async () => {
  const {reviewedArtifact, serveReviewedArtifact, REVIEWED_HISTORY_PATTERNS, REVIEWED_HISTORY_KEYS} = await import(modulePath);
  const original = globalThis.fetch;
  try {
    for (const key of [...REVIEWED_HISTORY_PATTERNS.map(value => value.replaceAll('*', 'fixture')), ...REVIEWED_HISTORY_KEYS]) {
      const review = reviewedArtifact(key);
      assert.equal(review.marker, 'public_history_review', key);
      globalThis.fetch = async () => Response.json({raw_response: 'SYNTHETIC_SECRET'});
      assert.equal((await serveReviewedArtifact(new Request('https://example.test/' + key), review, 'https://origin.test', {})).status, 503);
      globalThis.fetch = async () => Response.json({public_history_review: '20260910.v1', metric: 0});
      assert.equal((await serveReviewedArtifact(new Request('https://example.test/' + key), review, 'https://origin.test', {})).status, 200);
    }
  } finally { globalThis.fetch = original; }
});

test('valid markers never authorize nested raw diagnostics or alert transport replies', async () => {
  const {reviewedArtifact,serveReviewedArtifact} = await import(modulePath);
  const original = globalThis.fetch;
  try {
    const fixtures = [
      ['data/floor-audit/history/x.json',{public_history_review:'20260910.v1',raw_response:'SYNTHETIC_SECRET'}],
      ['macro/history/x.json',{public_history_review:'20260910.v1',partial:{metric:0,headers:{token:'SYNTHETIC_SECRET'}}}],
      ['stock-analysis/SPY.json',{public_history_review:'20260910.v1',nested:[{error:'SYNTHETIC_SECRET'}]}],
      ['data/carry-surface.json',{public_history_review:'20260910.v1',errors:[{message:'SYNTHETIC_SECRET'}]}],
      ['data/alert-history.json',{public_alert_schema:'alert-history-public.v1',alerts:[{telegram_info:'SYNTHETIC_SECRET'}]}],
      ['data/alert-history.json',{public_alert_schema:'alert-history-public.v1',alerts:[{webhook_results:[{ok:false,type:'generic',url:'SYNTHETIC_SECRET'}]}]}],
    ];
    for (const [key,document] of fixtures) {
      globalThis.fetch = async()=>Response.json(document);
      const response = await serveReviewedArtifact(new Request('https://example.test/'+key),reviewedArtifact(key),'https://origin.test',{});
      assert.equal(response.status,503,key);
      assert.ok(!(await response.text()).includes('SYNTHETIC_SECRET'));
    }
  } finally {globalThis.fetch=original;}
});

test('legitimate redacted history and alerts retain analytics including zero and fixed reason codes', async () => {
  const {reviewedArtifact,serveReviewedArtifact} = await import(modulePath);
  const original = globalThis.fetch;
  try {
    const fixtures = [
      ['data/cascade-validation-log.json',{public_history_review:'20260910.v1',generated_at:'2020-01-01',
        all_results:[{ticker:'SPY',metric:0,error:'no_price_at_pred_date',reason:'No observation at prediction date',raw_response:'DIAGNOSTIC_REDACTED'}],errors:0}],
      ['data/alert-history.json',{public_alert_schema:'alert-history-public.v1',alerts:[{ticker:'SPY',price:0,detail:'Market observation',
        webhook_results:[{ok:false,type:'generic',details:'REDACTED_PRIVATE_DESTINATION'}]}]}],
    ];
    for (const [key,document] of fixtures) {
      globalThis.fetch=async()=>Response.json(document);
      const response=await serveReviewedArtifact(new Request('https://example.test/'+key),reviewedArtifact(key),'https://origin.test',{});
      assert.equal(response.status,200,key);
      assert.deepEqual(await response.json(),document);
    }
  } finally {globalThis.fetch=original;}
});

test('a fresh, clean history head without the marker is served; raw diagnostics still block it', async () => {
  const {reviewedArtifact, serveReviewedArtifact, REVIEWED_HISTORY_KEYS} = await import(modulePath);
  const original = globalThis.fetch;
  try {
    for (const key of REVIEWED_HISTORY_KEYS) {
      const review = reviewedArtifact(key);
      globalThis.fetch = async () => Response.json({generated_at: '2026-09-17T14:49:26Z', ok: true, rows: [{metric: 1}]});
      const r = await serveReviewedArtifact(new Request('https://example.test/' + key), review, 'https://origin.test', {});
      assert.equal(r.status, 200, key); assert.equal(r.headers.get('X-JH-Review'), 'clean-unmarked');
      globalThis.fetch = async () => Response.json({generated_at: '2026-09-17T14:49:26Z', rows: [{headers: {token: 'SYNTHETIC_SECRET'}}]});
      const blocked = await serveReviewedArtifact(new Request('https://example.test/' + key), review, 'https://origin.test', {});
      assert.equal(blocked.status, 503, key); assert.ok(!(await blocked.text()).includes('SYNTHETIC_SECRET'));
    }
    // digests stay projection-only: no marker, no service
    const digest = reviewedArtifact('data/_alerts/digest-2026-01-01-close.json');
    globalThis.fetch = async () => Response.json({equity_score: 0});
    assert.equal((await serveReviewedArtifact(new Request('https://example.test/d.json'), digest, 'https://origin.test', {})).status, 503);
  } finally { globalThis.fetch = original; }
});
