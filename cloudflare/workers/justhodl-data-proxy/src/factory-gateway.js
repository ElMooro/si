// Only these factory routes can reach the existing authenticated Brain gateway.
export async function factoryGateway(request, env, url, deps) {
  const { resolveIdentity, aiLambdaUrl, corsHeaders, boundedBody, jsonResp } = deps;
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders() });
  const action = url.pathname.slice('/api/v1/factory/'.length);
  if (!['sandbox', 'view', 'predictions', 'traces', 'control', 'invites'].includes(action)) return jsonResp({ error: 'unknown factory action' }, 404);
  if ((['sandbox', 'view'].includes(action) && request.method !== 'GET') || (!['sandbox', 'view'].includes(action) && request.method !== 'POST')) return jsonResp({ error: 'method not allowed' }, 405);
  const identity = await resolveIdentity(request, env);
  if (!['owner', 'user'].includes(identity.role) || !identity.uid) return jsonResp({ error: 'factory sign-in required' }, 401);
  if (['control', 'invites'].includes(action) && identity.role !== 'owner') return jsonResp({ error: 'owner action' }, 403);
  if (!env.ADMIN_TOKEN) return jsonResp({ error: 'factory service unavailable' }, 503);
  const base = String(env.AI_LAMBDA_URL || '') || await aiLambdaUrl();
  if (!/^https:\/\/[a-z0-9]+\.lambda-url\.us-east-1\.on\.aws\/?$/.test(base)) return jsonResp({ error: 'factory gateway unavailable' }, 503);
  const body = request.method === 'POST' ? await boundedBody(request, 16384) : undefined;
  if (body === null) return jsonResp({ error: 'request too large' }, 413);
  try {
    const init = { method: request.method, redirect: 'error', headers: {
      'Content-Type': 'application/json', 'X-JH-Service-Token': env.ADMIN_TOKEN,
      'X-JH-Factory-Uid': identity.uid, 'X-JH-Factory-Role': identity.role
    }};
    if (body !== undefined) init.body = body;
    const upstream = await fetch(base.replace(/\/$/, '') + '/factory/' + action + (action === 'view' ? url.search : ''), init);
    return new Response(await upstream.text(), { status: upstream.status, headers: {
      ...corsHeaders(), 'Content-Type': 'application/json', 'Cache-Control': 'private, no-store', 'Vary': 'Authorization'
    }});
  } catch { return jsonResp({ error: 'factory gateway unavailable' }, 502); }
}
