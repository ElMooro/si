export const REVIEWED_HISTORY_PATTERNS = [
  "data/floor-audit/history/*.json",
  "data/cascade-validation-history/*.json",
  "data/nobrainer-thesis/*_*.json",
  "investor-analysis/*.json",
  "stock-analysis/*.json",
  "data/carry-surface/history/*.json",
  "data/usd-funding/history/*.json",
  "etf-flows/history/*.json",
  "macro/history/*.json",
  "data/jh-fusion/ledger/*/*/*/*.json.gz",
  "data/warm/blackswan/cc_*.json"
];
export const REVIEWED_HISTORY_KEYS = [
  "data/floor-audit.json",
  "data/cascade-validation-log.json",
  "data/carry-surface.json",
  "data/usd-funding.json",
  "etf-flows/daily.json",
  "macro/regime.json",
  "data/jh-fusion.json"
];
const historyPatterns = REVIEWED_HISTORY_PATTERNS.map(pattern => new RegExp('^' + pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '[^/]+') + '$'));
// Keep aligned with public_history_review.public_diagnostic_channels. A marker
// never authorizes a raw transport channel retained by an older/partial writer.
const diagnosticFields = new Set(['body','raw_sample','raw_status','request_id','request_url',
  'response','response_body','raw_response','headers','exception','traceback','stack_trace',
  'error_message','telegram_info','wss_broadcast_info']);
const errorFields = new Set(['error','err','report_error','error_code','fetch_err']);
const errorCodes = new Set(['PROVIDER_REQUEST_FAILED','PROVIDER_NO_RESULTS','PROVIDER_HTTP_ERROR',
  'PROVIDER_KEY_UNAVAILABLE','ANALYSIS_UNAVAILABLE','no_price_data','no_price_at_pred_date',
  'no benchmark with >=55 NAV days']);
const emptyDiagnostic = value => value === null || value === '' || value === false || value === 0 ||
  (Array.isArray(value) ? value.length === 0 : value && typeof value === 'object' && Object.keys(value).length === 0);
const safeError = value => emptyDiagnostic(value) || (typeof value === 'string' && errorCodes.has(value));
export function hasRawDiagnostics(value) {
  if (Array.isArray(value)) return value.some(hasRawDiagnostics);
  if (!value || typeof value !== 'object') return false;
  return Object.entries(value).some(([key, child]) => {
    if (diagnosticFields.has(key)) return !emptyDiagnostic(child) && child !== 'DIAGNOSTIC_REDACTED';
    if (errorFields.has(key)) return !safeError(child);
    if (key === 'errors') return Array.isArray(child) ? child.some(row => row && typeof row === 'object' || !safeError(row)) :
      !(safeError(child) || typeof child === 'number' && Number.isFinite(child) && child >= 0);
    if (key === 'webhook_results') return !Array.isArray(child) || child.some(row => !row || typeof row !== 'object' ||
      Object.keys(row).some(field => !['ok','type','details'].includes(field)) || typeof row.ok !== 'boolean' ||
      !['slack','discord','generic','other'].includes(row.type) || row.details !== 'REDACTED_PRIVATE_DESTINATION');
    return hasRawDiagnostics(child);
  });
}
// Public histories containing legacy transport diagnostics are served only
// after source-specific redaction. This guard runs before Range/cache/aliases.
export function reviewedArtifact(path) {
  const canonical = path.replace(/^\/+/, '').replace(/^data\//, '');
  if (/^_alerts\/digest-[^/]+\.json$/.test(canonical)) {
    return {key: 'data/' + canonical, marker: 'public_digest_schema', version: 'digest-public.v1'};
  }
  if (canonical === 'alert-history.json') return {key: 'data/alert-history.json', marker: 'public_alert_schema', version: 'alert-history-public.v1'};
  for (const key of [canonical, 'data/' + canonical]) {
    if (REVIEWED_HISTORY_KEYS.includes(key) || historyPatterns.some(pattern => pattern.test(key))) {
      return {key, marker: 'public_history_review', version: '20260910.v1'};
    }
  }
  return null;
}

async function boundedRead(response, limit = 20000000) {
  if (+response.headers.get('Content-Length') > limit) throw new Error('too large');
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      size += value.length;
      if (size > limit) throw new Error('too large');
      chunks.push(value);
    }
  } catch (error) {
    await reader.cancel();
    throw error;
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
  return bytes;
}

export async function serveReviewedArtifact(request, review, bucket, cors) {
  const headers = {...cors, 'Content-Type': 'application/json', 'Cache-Control': 'no-store',
    'X-JH-Artifact-Key': review.key, 'Access-Control-Expose-Headers': 'X-JH-Artifact-Key'};
  const unavailable = status => new Response(JSON.stringify({error: 'reviewed artifact unavailable'}), {status, headers});
  if (!['GET', 'HEAD'].includes(request.method)) return unavailable(405);
  if (request.headers.has('Range')) return unavailable(416);
  try {
    const source = await fetch(bucket + '/' + review.key + '?public_review=20260910.v1',
      {headers: {'Cache-Control': 'no-cache'}, cf: {cacheEverything: false, cacheTtl: 0}});
    if (!source.ok) return unavailable(source.status === 404 ? 404 : 503);
    let bytes = await boundedRead(source);
    if (bytes[0] === 0x1f && bytes[1] === 0x8b) {
      bytes = await boundedRead(new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'))));
    }
    const document = JSON.parse(new TextDecoder().decode(bytes));
    if (!document || typeof document !== 'object' || Array.isArray(document) || document[review.marker] !== review.version) return unavailable(503);
    if (hasRawDiagnostics(document)) return unavailable(503);
    if (review.marker === 'public_digest_schema' && Object.hasOwn(document, 'telegram_info')) return unavailable(503);
    return new Response(request.method === 'HEAD' ? null : JSON.stringify(document), {status: 200, headers});
  } catch (_) { return unavailable(503); }
}
