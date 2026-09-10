const test = require('node:test');
const assert = require('node:assert/strict');
const {pathToFileURL} = require('node:url');
const path = require('node:path');
const modulePath = pathToFileURL(path.join(__dirname, '../cloudflare/workers/justhodl-data-proxy/src/reviewed-artifacts.js')).href;

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
