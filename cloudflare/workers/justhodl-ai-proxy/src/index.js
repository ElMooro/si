/**
 * justhodl-ai-proxy
 *
 * Cloudflare Worker that proxies browser requests to the justhodl-ai-chat
 * AWS Lambda. The Lambda's auth token lives here as an encrypted Worker
 * secret (AI_CHAT_TOKEN) and is never exposed to the browser.
 *
 * Request flow:
 *   Browser  ──POST──▶  Worker  ──POST + x-justhodl-token──▶  AWS Lambda
 *
 * Security layers:
 *   1. Origin allowlist (only justhodl.ai / www.justhodl.ai)
 *   2. Method allowlist (POST, OPTIONS only)
 *   3. Body size cap (prevents abuse)
 *   4. Upstream auth (token attached by this Worker, not by the client)
 *
 * CORS preflight (OPTIONS) is handled here without touching Lambda.
 * Rate limiting is delegated to Lambda's reserved concurrency (3).
 */

const LAMBDA_URL = 'https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/';

const ALLOWED_ORIGINS = new Set([
  'https://justhodl.ai',
  'https://www.justhodl.ai',
]);

const MAX_BODY_BYTES = 32 * 1024; // 32 KB per request — ample for chat prompts

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.has(origin) ? origin : 'https://justhodl.ai';
  return {
    'Access-Control-Allow-Origin': allowed,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '300',
    'Vary': 'Origin',
  };
}

function json(status, data, origin) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...corsHeaders(origin),
      'Content-Type': 'application/json; charset=utf-8',
    },
  });
}

export default {
  async fetch(request, env, ctx) {
    const origin = request.headers.get('Origin') || '';

    // Preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    // Origin check
    if (!ALLOWED_ORIGINS.has(origin)) {
      return json(403, { error: 'Forbidden' }, origin);
    }

    // Method
    if (request.method !== 'POST') {
      return json(405, { error: 'Method not allowed' }, origin);
    }

    // Secret present?
    if (!env.AI_CHAT_TOKEN) {
      return json(500, { error: 'Worker misconfigured (missing AI_CHAT_TOKEN)' }, origin);
    }

    // Body size guard
    const cl = parseInt(request.headers.get('Content-Length') || '0', 10);
    if (cl > MAX_BODY_BYTES) {
      return json(413, { error: 'Request body too large' }, origin);
    }

    const body = await request.text();
    if (body.length > MAX_BODY_BYTES) {
      return json(413, { error: 'Request body too large' }, origin);
    }

    // Forward to Lambda
    let upstream;
    try {
      upstream = await fetch(LAMBDA_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Origin': 'https://justhodl.ai',
          'x-justhodl-token': env.AI_CHAT_TOKEN,
        },
        body,
      });
    } catch (e) {
      return json(502, { error: 'Upstream unreachable', detail: String(e) }, origin);
    }

    const text = await upstream.text();
    return new Response(text, {
      status: upstream.status,
      headers: {
        ...corsHeaders(origin),
        'Content-Type': upstream.headers.get('Content-Type') || 'application/json',
      },
    });
  },
};
