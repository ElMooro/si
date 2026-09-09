/* Owner-account feeds use authenticated mirrors. Public model feeds retain
 * their existing fetch path. Install before page scripts start data requests. */
(function () {
  'use strict';
  if (window.JustHodlPrivateArtifacts) return;
  const nativeFetch = window.fetch.bind(window);
  const WORKER = 'https://justhodl-data-proxy.raafouis.workers.dev';
  const paths = {
    'portfolio/snapshot.json': 'portfolio-snapshot', 'portfolio/risk.json': 'portfolio-risk',
    'portfolio/sizing.json': 'portfolio-sizing', 'portfolio/catalysts.json': 'portfolio-catalysts',
    'risk-sizer.json': 'risk-sizer', 'risk/recommendations.json': 'risk-sizer',
    'pm-decision.json': 'pm-decision', 'pm-decision-history.json': 'pm-decision-history',
    'behavior-mirror.json': 'behavior-mirror', 'ai-brief.json': 'ai-brief',
    'brain.json': 'brain', 'brain-history.json': 'brain-history', 'journal-graded.json': 'journal-graded',
    'my-brief.json': 'my-brief', 'devils-advocate.json': 'devils-advocate',
    'notes-index.json': 'notes-index', 'notes-themes.json': 'notes-themes', 'playbook-rules.json': 'playbook-rules',
  };
  const hosts = new Set([
    location.hostname, 'justhodl.ai', 'www.justhodl.ai', 'api.justhodl.ai',
    'justhodl-data-proxy.raafouis.workers.dev', 'justhodl-dashboard-live.s3.amazonaws.com',
    'justhodl-dashboard-live.s3.us-east-1.amazonaws.com',
  ]);
  function kindFor(input) {
    try {
      const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url, document.baseURI || location.href);
      if (!hosts.has(url.hostname) || !['http:', 'https:'].includes(url.protocol)) return null;
      const key = url.pathname.replace(/^\/+/, '').replace(/^data\//, '');
      return Object.hasOwn(paths, key) ? paths[key] : null;
    } catch (_) { return null; }
  }
  function unavailable(message, status) {
    return new Response(JSON.stringify({error: message, private_account: true}), {
      status, headers: {'Content-Type': 'application/json', 'Cache-Control': 'private, no-store'},
    });
  }
  function showAccessStatus(status) {
    const render = () => {
      if (!document.body || document.getElementById('private-account-status')) return;
      const panel = document.createElement('div'); panel.id = 'private-account-status';
      panel.setAttribute('role', 'status');
      panel.style.cssText = 'padding:12px 16px;background:#172033;color:#f3f4f6;border-bottom:1px solid #536078;font:14px system-ui;position:relative;z-index:10000';
      const text = document.createElement('span');
      text.textContent = status === 401 ? 'Sign in to view your private account data. ' : status === 403
        ? 'This account data is available only to its owner.' : 'Private account data is temporarily unavailable.';
      panel.appendChild(text);
      if (status === 401) {
        const button = document.createElement('button'); button.type = 'button'; button.textContent = 'Sign in';
        button.onclick = () => window.JustHodlAuth?.openSignIn(); panel.appendChild(button);
      }
      document.body.prepend(panel);
    };
    if (document.body) render(); else document.addEventListener('DOMContentLoaded', render, {once: true});
  }
  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const script = document.createElement('script'); script.src = src;
      script.onload = resolve; script.onerror = () => reject(new Error('authentication unavailable'));
      document.head.appendChild(script);
    });
  }
  let ready = null, observedUid;
  function reloadPrivateView() {
    // Discard a prior account's already-rendered DOM before navigation starts.
    document.documentElement.style.visibility = 'hidden';
    location.reload();
  }
  async function authReady() {
    if (!ready) ready = (async () => {
      if (!window.JustHodlAuth) {
        if (!window.JUSTHODL_AUTH_CONFIG) await loadScript('/auth-config.js?v=20260909');
        if (!window.supabase) await loadScript('https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2');
        if (!window.JustHodlAuth) await loadScript('/auth.js?v=20260909');
      }
      const auth = window.JustHodlAuth;
      if (!auth) throw new Error('authentication unavailable');
      await auth.init();
      observedUid = auth.getUser()?.id || null;
      auth.onChange(user => {
        const nextUid = user?.id || null;
        if (nextUid !== observedUid) { observedUid = nextUid; reloadPrivateView(); }
      });
      return auth;
    })().catch(error => { ready = null; throw error; });
    return ready;
  }
  async function privateFetch(input, init) {
    const kind = kindFor(input);
    if (!kind) return nativeFetch(input, init);
    const method = String(init?.method || input?.method || 'GET').toUpperCase();
    if (!['GET', 'HEAD'].includes(method)) return unavailable('private artifact is read only', 405);
    try {
      const auth = await authReady(), uid = auth.getUser()?.id;
      if (!uid) { showAccessStatus(401); return unavailable('Sign in to view your private account data.', 401); }
      const token = await auth.getAccessToken();
      if (!token || auth.getUser()?.id !== uid) return unavailable('Account session changed.', 401);
      const response = await nativeFetch(WORKER + '/private-artifact?kind=' + encodeURIComponent(kind), {
        method, headers: {Authorization: 'Bearer ' + token}, cache: 'no-store', signal: init?.signal || input?.signal,
      });
      // Buffer before releasing the response, so a delayed body from a previous
      // account cannot repopulate a page after sign-out or account replacement.
      const body = method === 'HEAD' ? null : await response.arrayBuffer();
      if (auth.getUser()?.id !== uid) return unavailable('Account session changed.', 401);
      if ([401, 403, 503].includes(response.status)) showAccessStatus(response.status);
      return new Response(body, {status: response.status, headers: response.headers});
    } catch (_) { showAccessStatus(503); return unavailable('Private account data is temporarily unavailable.', 503); }
  }
  window.JustHodlPrivateArtifacts = {kindFor, fetch: privateFetch};
  window.fetch = privateFetch;
  window.addEventListener('pageshow', event => { if (event.persisted) reloadPrivateView(); });
})();
