/**
 * JustHodl TV Notes — Background Service Worker (MV3)
 *
 * Responsibilities:
 *  1. Store the ingest config (URL + token) — sourced from justhodl.ai at install
 *  2. Handle cross-origin POST to the Lambda ingest endpoint
 *  3. Manage badge (note count) and harvest scheduling
 *  4. Auto-trigger harvest when TV tab becomes active
 */

'use strict';

// ── Ingest config (baked in by ops 2963) ─────────────────────────────────────
const INGEST_URL = 'INGEST_URL_PLACEHOLDER';
const INGEST_TOKEN = 'INGEST_TOKEN_PLACEHOLDER';
const CONFIG_URL = 'https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/tv-ingest-config.json';

// ── State ─────────────────────────────────────────────────────────────────────
let harvestResults = { count: 0, tickers: 0, lastRun: null };

// ── Install: fetch live config in case baked values are stale ────────────────
chrome.runtime.onInstalled.addListener(async () => {
  try {
    const r = await fetch(CONFIG_URL + '?t=' + Date.now());
    if (r.ok) {
      const cfg = await r.json();
      if (cfg.ingest_url && cfg.token) {
        await chrome.storage.local.set({ ingest_url: cfg.ingest_url, token: cfg.token });
        console.log('[JH] Config loaded from S3');
      }
    }
  } catch (e) {
    // fallback to baked-in constants
    await chrome.storage.local.set({ ingest_url: INGEST_URL, token: INGEST_TOKEN });
    console.log('[JH] Using baked-in config');
  }
});

// ── Message handler ───────────────────────────────────────────────────────────
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  switch (msg.action) {

    case 'upload':
      getConfig().then(cfg => uploadNotes(msg.notes, cfg, msg.watchlists))
        .then(result => {
          harvestResults.lastRun = new Date().toISOString();
          sendResponse(result);
          // Update badge on the sending tab
          if (sender.tab?.id) {
            chrome.action.setBadgeText({ text: '✓', tabId: sender.tab.id });
            chrome.action.setBadgeBackgroundColor({ color: '#6fce8a', tabId: sender.tab.id });
          }
        })
        .catch(e => sendResponse({ ok: false, error: e.message }));
      return true; // keep channel open for async

    case 'harvest_update':
      harvestResults = { count: msg.count, tickers: msg.tickers, lastRun: null };
      if (sender.tab?.id) {
        const label = msg.count > 999 ? '999+' : String(msg.count);
        chrome.action.setBadgeText({ text: label, tabId: sender.tab.id });
        chrome.action.setBadgeBackgroundColor({ color: '#F0B429', tabId: sender.tab.id });
      }
      break;

    case 'harvest_complete':
      harvestResults = { count: msg.count, tickers: msg.tickers, lastRun: null };
      if (sender.tab?.id) {
        const label = msg.count > 999 ? '999+' : String(msg.count);
        chrome.action.setBadgeText({ text: label, tabId: sender.tab.id });
        chrome.action.setBadgeBackgroundColor({ color: '#F0B429', tabId: sender.tab.id });
      }
      sendResponse({ ok: true });
      break;

    case 'get_status':
      sendResponse({ ...harvestResults, config_ok: INGEST_URL !== 'INGEST_URL_PLACEHOLDER' });
      break;
  }
});

// ── Config helper ─────────────────────────────────────────────────────────────
async function getConfig() {
  const stored = await chrome.storage.local.get(['ingest_url', 'token']);
  return {
    url: stored.ingest_url || INGEST_URL,
    token: stored.token || INGEST_TOKEN,
  };
}

// ── Upload notes to Lambda ────────────────────────────────────────────────────
async function uploadNotes(notes, cfg, watchlists) {
  if (!cfg?.url || cfg.url.includes('PLACEHOLDER')) {
    return { ok: false, error: 'Ingest URL not configured' };
  }
  const post = async (body) => {
    const r = await fetch(cfg.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const txt = await r.text();
    let data = {};
    try { data = JSON.parse(txt); } catch (e) {}
    return { status: r.status, ok: r.ok, data, txt };
  };

  let wlSaved = 0, firstErr = null;

  // ops 3161: watchlists go FIRST, in their own request. Previously they
  // rode chunk 0 — so a note-chunk failure silently killed the watchlists
  // too. They are the smaller, more valuable payload; they land first.
  if (watchlists?.length) {
    try {
      const r = await post({ token: cfg.token, notes: [], watchlists });
      if (r.ok) wlSaved = r.data.watchlists_saved || 0;
      else firstErr = `watchlists HTTP ${r.status}: ${r.txt.slice(0, 120)}`;
    } catch (e) { firstErr = 'watchlists: ' + String(e).slice(0, 100); }
  }

  // notes: small chunks so no single request can blow the budget
  let brainOk = 0, brainErr = 0, mirrorAdded = 0, sent = 0;
  const SIZE = 40;
  const total = notes?.length || 0;
  for (let i = 0; i < total; i += SIZE) {
    const chunk = notes.slice(i, i + SIZE);
    try {
      const r = await post({ token: cfg.token, notes: chunk });
      if (!r.ok) {
        brainErr += chunk.length;
        if (!firstErr) firstErr = `HTTP ${r.status}: ${r.txt.slice(0, 140)}`;
      } else {
        brainOk += r.data.brain_upserted || 0;
        mirrorAdded += r.data.mirror_added || 0;
        if (!firstErr && r.data.brain_error_sample) {
          firstErr = 'brain: ' + String(r.data.brain_error_sample).slice(0, 140);
        }
      }
    } catch (e) {
      brainErr += chunk.length;
      if (!firstErr) firstErr = String(e).slice(0, 140);
    }
    sent += chunk.length;
    try {
      chrome.runtime.sendMessage({ action: 'upload_progress', sent, total, brainOk });
    } catch (e) {}
    await new Promise((r) => setTimeout(r, 80));
  }

  return {
    ok: wlSaved > 0 || brainOk > 0 || mirrorAdded > 0,
    brain_upserted: brainOk,
    brain_errors: brainErr,
    mirror_added: mirrorAdded,
    watchlists_saved: wlSaved,
    total_sent: total,
    error: firstErr,
  };
}

