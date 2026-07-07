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
      getConfig().then(cfg => uploadNotes(msg.notes, cfg))
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
async function uploadNotes(notes, cfg) {
  if (!notes?.length) return { ok: false, error: 'no notes' };
  if (!cfg.url || cfg.url === 'INGEST_URL_PLACEHOLDER') {
    return { ok: false, error: 'Ingest URL not configured' };
  }

  let brainOk = 0, brainErr = 0, mirrorAdded = 0;
  const chunkSize = 200;

  for (let i = 0; i < notes.length; i += chunkSize) {
    const chunk = notes.slice(i, i + chunkSize);
    try {
      const resp = await fetch(cfg.url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'User-Agent': 'JH-TV-Extension/1.0' },
        body: JSON.stringify({ token: cfg.token, notes: chunk }),
      });
      if (!resp.ok) {
        brainErr += chunk.length;
        console.warn('[JH] Upload chunk failed:', resp.status);
        continue;
      }
      const data = await resp.json();
      brainOk    += data.brain_upserted || 0;
      brainErr   += data.brain_errors   || 0;
      mirrorAdded += data.mirror_added  || 0;
    } catch (e) {
      brainErr += chunk.length;
      console.error('[JH] Upload error:', e);
    }
    await new Promise(r => setTimeout(r, 150));
  }

  return {
    ok: brainErr === 0 || brainOk > 0,
    brain_upserted: brainOk,
    brain_errors: brainErr,
    mirror_added: mirrorAdded,
    total_sent: notes.length,
  };
}
