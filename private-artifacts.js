/* Owner-account feeds use authenticated mirrors. Public model feeds retain
 * their existing fetch path. Install before page scripts start data requests. */
(function () {
  'use strict';
  if (window.JustHodlPrivateArtifacts) return;
  const nativeFetch = window.fetch.bind(window);
  const PRIVATE_API = 'https://api.justhodl.ai';
  const paths = {
    'portfolio/snapshot.json': 'portfolio-snapshot', 'portfolio/risk.json': 'portfolio-risk',
    'portfolio/sizing.json': 'portfolio-sizing', 'portfolio/catalysts.json': 'portfolio-catalysts',
    'risk-sizer.json': 'risk-sizer', 'risk/recommendations.json': 'risk-sizer',
    'pm-decision.json': 'pm-decision', 'pm-decision-history.json': 'pm-decision-history',
    'behavior-mirror.json': 'behavior-mirror', 'ai-brief.json': 'ai-brief',
    'user-watchlist.json': 'user-watchlist', 'vol-regime-private.json': 'vol-regime-private',
    'user-trades.json': 'personal-trades', 'user-trades-stats.json': 'personal-trades-stats',
    'portfolio/catalyst-alert-history.json': 'portfolio-catalyst-history',
    'portfolio/risk-alert-history.json': 'portfolio-risk-history',
    'portfolio/sizing-alert-history.json': 'portfolio-sizing-history',
    'history/behavior-mirror-history.json': 'behavior-mirror-history',
    'portfolio-manager-brief.json': 'portfolio-manager-brief',
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
  function ownerApiFor(input) {
    try {
      const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url, document.baseURI || location.href);
      return hosts.has(url.hostname) && ['http:', 'https:'].includes(url.protocol) && /^\/owner-api\/(watchlist|trades)(\/(add|remove|replace|close|update|delete|mtm))?$/.test(url.pathname) ? url.pathname : null;
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
  // Read-only acquisition keeps the complete-body owner check without an unbounded buffer.
  // Public requests and the existing account mutation path below are unchanged.
  const PRIVATE_READ_LIMIT = 32 * 1024 * 1024, PRIVATE_READ_TIMEOUT = 12000;
  let evidenceReady = null;
  const readStates = new Map();
  let readPanel = null, readPanelStatus = null, readRenderPending = false;
  let separateOperationNotice = false;
  const originalAccessStatus = showAccessStatus;
  // The unchanged mutation handler reports through this binding. A read recovery
  // must not erase its error, even if it reused an already visible read notice.
  showAccessStatus = function (status) { separateOperationNotice = true; originalAccessStatus(status); };
  function renderReadNotices() {
    if (separateOperationNotice) return;
    if (!document.body) {
      if (!readRenderPending) {
        readRenderPending = true;
        document.addEventListener('DOMContentLoaded', () => { readRenderPending = false; renderReadNotices(); }, {once: true});
      }
      return;
    }
    const statuses = [...readStates.values()].map(row => row.status);
    const status = [401, 403, 503].find(value => statuses.includes(value)) || null;
    const current = document.getElementById('private-account-status');
    // Never remove a notice installed by the separate, unchanged mutation path.
    if (current && current !== readPanel) return;
    if (current === readPanel && current && status === readPanelStatus) return;
    if (current && current === readPanel) current.remove();
    readPanel = null; readPanelStatus = null;
    if (status) {
      originalAccessStatus(status);
      readPanel = document.getElementById('private-account-status'); readPanelStatus = status;
    }
  }
  function beginReadState(method, key) {
    const id = method + '|' + key, previous = readStates.get(id);
    const row = {generation: (previous?.generation || 0) + 1, status: previous?.status || null};
    readStates.set(id, row);
    return status => {
      if (readStates.get(id) !== row) return;
      row.status = [401, 403, 503].includes(status) ? status : null;
      renderReadNotices();
    };
  }
  function cancelledRead() { const error = new Error('Private read cancelled'); error.name = 'AbortError'; return error; }
  async function readIO(signal, timeoutMs) {
    if (signal?.aborted) throw cancelledRead();
    if (window.JHEvidenceIO?.readComplete) return window.JHEvidenceIO;
    if (!evidenceReady) evidenceReady = loadScript('/jh-evidence-io.js?v=20260930').then(() => {
      if (typeof window.JHEvidenceIO?.readComplete !== 'function') throw new Error('Complete reader unavailable');
      return window.JHEvidenceIO;
    }).catch(error => { evidenceReady = null; throw error; });
    let timer, onAbort;
    const interrupted = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('Private reader loading timed out')), timeoutMs);
      onAbort = () => reject(cancelledRead()); signal?.addEventListener('abort', onAbort, {once: true});
    });
    try { return await Promise.race([evidenceReady, interrupted]); }
    finally { clearTimeout(timer); signal?.removeEventListener('abort', onAbort); }
  }
  async function privateRead(kind, ownerApi, method, signal) {
    const clock = () => window.performance?.now?.() ?? Date.now(), deadline = clock() + PRIVATE_READ_TIMEOUT;
    const remaining = () => { const ms = Math.ceil(deadline - clock()); if (ms < 1) throw new Error('Private read timed out'); return ms; };
    let response, auth, uid, report = () => {};
    try {
      if (signal?.aborted) throw cancelledRead();
      report = beginReadState(method, kind || ownerApi);
      const io = await readIO(signal, remaining());
      const raw = await io.readComplete(async requestSignal => {
        auth = await authReady();
        if (requestSignal.aborted) throw cancelledRead();
        uid = auth.getUser()?.id;
        if (!uid) response = unavailable('Sign in to view your private account data.', 401);
        else {
          const token = await auth.getAccessToken();
          if (requestSignal.aborted) throw cancelledRead();
          if (!token || auth.getUser()?.id !== uid) response = unavailable('Account session changed.', 401);
          else response = await nativeFetch(PRIVATE_API + (ownerApi || '/private-artifact?kind=' + encodeURIComponent(kind)), {
            method, headers: {Authorization: 'Bearer ' + token, ...(ownerApi ? {'Content-Type':'application/json'} : {})}, cache: 'no-store', signal: requestSignal,
          });
        }
        // Preserve complete denial bodies and HTTP status. No response is parsed as JSON here.
        // HEAD and bodyless HTTP statuses still close any unexpected supplied body.
        if (method === 'HEAD' || [204, 205, 304].includes(response.status)) {
          try { Promise.resolve(response.body?.cancel?.()).catch(() => {}); } catch (_) {}
          return {ok: true, body: new Response('').body};
        }
        return {ok: true, body: response.body};
      }, {limit: PRIVATE_READ_LIMIT, timeoutMs: remaining(), signal});
      if (signal?.aborted) throw cancelledRead();
      remaining();
      if (uid && auth.getUser()?.id !== uid) return unavailable('Account session changed.', 401);
      report(response.status);
      return new Response(method === 'HEAD' || [204, 205, 304].includes(response.status) ? null : raw, {status: response.status, headers: response.headers});
    } catch (error) {
      if (signal?.aborted || error?.name === 'AbortError') throw cancelledRead();
      if (uid && auth.getUser()?.id !== uid) return unavailable('Account session changed.', 401);
      report(503); return unavailable('Private account data is temporarily unavailable.', 503);
    }
  }
  async function privateFetch(input, init) {
    const kind = kindFor(input), ownerApi = ownerApiFor(input);
    if (!kind && !ownerApi) return nativeFetch(input, init);
    const method = String(init?.method || input?.method || 'GET').toUpperCase();
    if (!(ownerApi ? ['GET', 'POST'] : ['GET', 'HEAD']).includes(method)) return unavailable('private operation does not support this method', 405);
    if (method === 'GET' || method === 'HEAD') return privateRead(kind, ownerApi, method, init?.signal || input?.signal);
    try {
      const auth = await authReady(), uid = auth.getUser()?.id;
      if (!uid) { showAccessStatus(401); return unavailable('Sign in to view your private account data.', 401); }
      const token = await auth.getAccessToken();
      const requestBody = method === 'POST' ? (init?.body === undefined && input instanceof Request ? await input.clone().arrayBuffer() : init?.body) : undefined;
      if (!token || auth.getUser()?.id !== uid) return unavailable('Account session changed.', 401);
      const response = await nativeFetch(PRIVATE_API + (ownerApi || '/private-artifact?kind=' + encodeURIComponent(kind)), {
        method, body: requestBody, headers: {Authorization: 'Bearer ' + token, ...(ownerApi ? {'Content-Type':'application/json'} : {})}, cache: 'no-store', signal: init?.signal || input?.signal,
      });
      const body = method === 'HEAD' ? null : await response.arrayBuffer();
      if (auth.getUser()?.id !== uid) return unavailable('Account session changed.', 401);
      if ([401, 403, 503].includes(response.status)) showAccessStatus(response.status);
      return new Response(body, {status: response.status, headers: response.headers});
    } catch (_) { showAccessStatus(503); return unavailable('Private account data is temporarily unavailable.', 503); }
  }
  window.JustHodlPrivateArtifacts = {kindFor, fetch: privateFetch, ready: authReady};
  window.fetch = privateFetch;
  window.addEventListener('pageshow', event => { if (event.persisted) reloadPrivateView(); });
  try {
    var p = (location.pathname || '').toLowerCase();
    function inject(src) {
      var s = document.createElement('script');
      s.src = src; s.defer = true;
      (document.head || document.documentElement).appendChild(s);
    }
    if ((/ticker\.html$/).test(p) || p === '/ticker') inject('/jh-ticker-research.js');
    if ((/data\.html$/).test(p) || p === '/data') inject('/jh-data-feeds.js');
    // Home only. Exact paths: a browser never yields '' (only the test fixture does) and a
    // substring match would bind the header to any page whose name contains a word.
    if (p === '/' || p === '/index.html') inject('/jh-verdict-header.js?v=20260912');
  } catch (_) {}
})();
