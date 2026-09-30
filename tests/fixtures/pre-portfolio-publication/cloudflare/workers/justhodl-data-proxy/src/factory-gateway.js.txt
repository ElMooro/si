// Same fetch shape as the proven /ai bridge. cache/redirect options throw on this runtime.
export async function factoryGateway(request, env, url, deps) {
  const { resolveIdentity, aiLambdaUrl, corsHeaders, boundedBody, jsonResp } = deps;
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders() });
  const action = url.pathname.slice('/api/v1/factory/'.length);
  if (!['sandbox', 'view', 'predictions', 'traces', 'control', 'invites', 'chat', 'spawn'].includes(action)) return jsonResp({ error: 'unknown factory action' }, 404);
  const getOk = ['sandbox', 'view', 'chat'].includes(action) && (request.method === 'GET' || request.method === 'HEAD');
  const postOk = ['predictions', 'traces', 'control', 'invites', 'chat', 'spawn'].includes(action) && request.method === 'POST';
  if (!getOk && !postOk) return jsonResp({ error: 'method not allowed' }, 405);
  const identity = await resolveIdentity(request, env);
  if (!['owner', 'user'].includes(identity.role) || !identity.uid) return jsonResp({ error: 'factory sign-in required' }, 401);
  if (['control', 'invites', 'spawn'].includes(action) && identity.role !== 'owner') return jsonResp({ error: 'owner action' }, 403);
  if (!env.ADMIN_TOKEN) return jsonResp({ error: 'factory service unavailable' }, 503);
  const shape = /^https:\/\/[a-z0-9]+\.lambda-url\.us-east-1\.on\.aws\/?$/;
  let base = String(env.AI_LAMBDA_URL || '');
  if (!shape.test(base)) base = await aiLambdaUrl();
  if (!shape.test(base || '')) return jsonResp({ error: 'factory gateway unavailable', detail: 'no lambda url' }, 503);
  const body = request.method === 'POST' ? await boundedBody(request, 16384) : undefined;
  if (body === null) return jsonResp({ error: 'request too large' }, 413);
  try {
    const init = { method: request.method === 'HEAD' ? 'GET' : request.method, headers: {
      'Content-Type': 'application/json', 'X-JH-Service-Token': env.ADMIN_TOKEN,
      'X-JH-Factory-Uid': identity.uid, 'X-JH-Factory-Role': identity.role
    }};
    if (body !== undefined) init.body = body;
    const upstream = await fetch(base.replace(/\/$/, '') + '/factory/' + action + ((action === 'view' || action === 'chat') ? url.search : ''), init);
    return new Response(await upstream.text(), { status: upstream.status, headers: {
      ...corsHeaders(), 'Content-Type': 'application/json', 'Cache-Control': 'private, no-store', 'Vary': 'Authorization'
    }});
  } catch (e) {
    return jsonResp({ error: 'factory gateway unavailable', detail: String(e).slice(0, 140) }, 502);
  }
}
