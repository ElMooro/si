/* Owner-authenticated research. The public pointer contains no credential. */
export async function handleAskDesk(request, env, helpers) {
  const {resolveIdentity, corsHeaders, boundedBody, jsonResp, unauthorized, forbidden} = helpers;
  if (request.method === 'OPTIONS') return new Response('{}', {headers: corsHeaders()});
  const identity = await resolveIdentity(request, env);
  if (identity.role === 'anon') return unauthorized('Sign in to ask the desk');
  if (!['owner', 'service'].includes(identity.role)) return forbidden('Owner research desk');
  if (request.method !== 'POST') return jsonResp({error:'method_not_allowed'}, 405);
  if (!env.ADMIN_TOKEN) return jsonResp({error:'desk_service_unavailable'}, 503);
  const raw = await boundedBody(request, 8192);
  if (raw === null) return jsonResp({error:'request_too_large'}, 413);
  try {
    const question = JSON.parse(raw)?.question;
    if (typeof question !== 'string' || !question.trim() || question.length > 600) return jsonResp({error:'question_required_max_600_characters'}, 400);
    const pointer = await fetch('https://justhodl-dashboard-live.s3.amazonaws.com/data/askdesk-config.json');
    if (!pointer.ok) return jsonResp({error:'desk_endpoint_unavailable'}, 503);
    const config = await pointer.json();
    if (config.schema_version !== 'public-api-pointer.v1' || config.function !== 'justhodl-ask-desk' ||
        !/^https:\/\/[a-z0-9]{20,64}\.lambda-url\.us-east-1\.on\.aws\/$/.test(config.url || '')) return jsonResp({error:'desk_endpoint_unverified'}, 503);
    const upstream = await fetch(config.url, {method:'POST', headers:{'Content-Type':'application/json', 'X-JH-Service-Token':env.ADMIN_TOKEN}, body:JSON.stringify({question:question.trim()})});
    const text = await upstream.text();
    return new Response(text, {status:upstream.status, headers:{...corsHeaders(), 'Content-Type':'application/json', 'Cache-Control':'private, no-store', Vary:'Authorization'}});
  } catch (_) {
    return jsonResp({error:'desk_research_unavailable'}, 503);
  }
}
