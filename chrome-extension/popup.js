'use strict';

let capturedNotes = [];
let capturedTickers = 0;

async function getActiveTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab;
}

function setMsg(msg, cls) {
  const el = document.getElementById('msg');
  el.textContent = msg;
  el.className = 'msg' + (cls ? ' ' + cls : '');
}

function setStatus(msg, cls) {
  const el = document.getElementById('statusLine');
  el.textContent = msg;
  el.className = 'msg' + (cls ? ' ' + cls : '');
}

async function refreshStatus() {
  const tab = await getActiveTab();
  const isTV = tab?.url?.includes('tradingview.com');

  if (!isTV) {
    setStatus('Open tradingview.com to harvest notes', 'warn');
    document.getElementById('harvestBtn').disabled = true;
    document.getElementById('showBtn').disabled = true;
    return;
  }

  try {
    const res = await chrome.tabs.sendMessage(tab.id, { action: 'get_status' });
    document.getElementById('count').textContent   = res.count   ?? '–';
    document.getElementById('tickers').textContent = res.tickers ?? '–';
    capturedNotes   = []; // will get from upload flow
    capturedTickers = res.tickers ?? 0;

    if (res.harvesting) {
      setStatus('Harvest in progress…', 'warn');
      document.getElementById('harvestBtn').disabled = true;
    } else if (res.count > 0) {
      setStatus(`Ready — ${res.count} notes from ${res.tickers} tickers`, '');
      document.getElementById('uploadBtn').disabled = false;
    } else {
      setStatus('On tradingview.com — ready to harvest', '');
    }
  } catch (e) {
    // Content script not yet injected
    setStatus('Click "Start Harvest" to begin', '');
  }

  // Check config
  const bg = await chrome.runtime.sendMessage({ action: 'get_status' });
  document.getElementById('confOk').textContent = bg?.config_ok ? '✓ config' : '⚠ config';
}

// Harvest
document.getElementById('harvestBtn').addEventListener('click', async () => {
  const tab = await getActiveTab();
  if (!tab?.url?.includes('tradingview.com')) {
    setMsg('Navigate to tradingview.com first', 'err');
    return;
  }
  document.getElementById('harvestBtn').disabled = true;
  setStatus('Harvesting — check the overlay on TV…', 'warn');
  setMsg('');
  try {
    await chrome.tabs.sendMessage(tab.id, { action: 'start_harvest' });
    setTimeout(refreshStatus, 3000);
    setTimeout(refreshStatus, 8000);
    setTimeout(refreshStatus, 20000);
  } catch (e) {
    // Content script may need page injection
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
    setTimeout(() => {
      chrome.tabs.sendMessage(tab.id, { action: 'start_harvest' });
      setTimeout(refreshStatus, 5000);
    }, 1000);
  }
  window.close(); // close popup so user can see TV overlay
});

// Show overlay
document.getElementById('showBtn').addEventListener('click', async () => {
  const tab = await getActiveTab();
  if (!tab?.url?.includes('tradingview.com')) return;
  try {
    await chrome.tabs.sendMessage(tab.id, { action: 'show_overlay' });
  } catch (e) {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
  }
  window.close();
});

// Upload
document.getElementById('uploadBtn').addEventListener('click', async () => {
  const tab = await getActiveTab();
  if (!tab) return;
  setMsg('Triggering upload from TV page…', 'warn');
  // Tell content script to upload
  try {
    const result = await chrome.tabs.sendMessage(tab.id, { action: 'get_status' });
    if (result?.count > 0) {
      // The upload button in the overlay does the actual work
      await chrome.tabs.sendMessage(tab.id, { action: 'show_overlay' });
      setMsg('Overlay shown — press UPLOAD in the amber panel', '');
    } else {
      setMsg('No notes yet — harvest first', 'warn');
    }
  } catch (e) {
    setMsg('Could not reach content script', 'err');
  }
});

// Init
refreshStatus();
