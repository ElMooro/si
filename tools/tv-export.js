/* JustHodl TradingView Notes Extractor v2.0
 * FULLY AUTONOMOUS: enumerates all your watchlists, fetches notes for every
 * ticker via TradingView's internal API, and uploads the complete harvest to
 * your Brain — no manual clicking required.
 * Run in TradingView's DevTools console (F12 → Console) while logged in.
 * Nothing leaves your browser until you press UPLOAD. */
(async function JH_TV_v2() {
  if (window.__JH_TV && window.__JH_TV.v === 2) { window.__JH_TV.show(); return; }
  if (window.__JH_TV) { delete window.__JH_TV; }  // kill v1 if present

  /* ── config ─────────────────────────────────────────────────────── */
  var CFG = null;
  var CFGURLS = [
    "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/tv-ingest-config.json",
    "https://justhodl.ai/data/tv-ingest-config.json"
  ];
  for (var ci = 0; ci < CFGURLS.length && !CFG; ci++) {
    try { var cr = await fetch(CFGURLS[ci] + "?t=" + Date.now());
          if (cr.ok) CFG = await cr.json(); } catch (e) {}
  }
  if (!CFG || !CFG.ingest_url) {
    var u = prompt("CORS blocked config.\nOpen justhodl.ai/tv-notes.html → copy INGEST URL:");
    var t = prompt("Paste INGEST TOKEN:");
    if (!u || !t) { console.warn("JH: cancelled"); return; }
    CFG = { ingest_url: u.trim(), token: t.trim() };
  }

  /* ── store ───────────────────────────────────────────────────────── */
  var STORE = new Map();
  var WATCHLISTS = [];  // ops 3158: membership rides the upload   // dedupe key -> note obj
  var TICKERS = new Set(); // all symbols discovered
  var ERRORS = [];

  function hashId(sym, ts, text) {
    var s = sym + "|" + ts + "|" + String(text).slice(0, 160), h = 0;
    for (var i = 0; i < s.length; i++) h = ((h * 31) + s.charCodeAt(i)) >>> 0;
    return "tv2-" + h.toString(36);
  }

  function keep(sym, text, title, created, updated) {
    text = String(text || "").trim();
    if (text.length < 2) return false;
    sym = String(sym || "UNTAGGED").replace(/[a-z]/g, function(c){return c.toUpperCase();});
    var ts = created || updated || Date.now();
    try { if (typeof ts === "string") ts = Date.parse(ts) || Date.now(); } catch(e) {}
    var id = hashId(sym, ts, text);
    if (!STORE.has(id)) {
      STORE.set(id, { symbol: sym, text: text.slice(0, 100000),
        title: String(title || "").slice(0, 200),
        created: ts, updated: updated || ts });
      TICKERS.add(sym);
      repaint(); return true;
    }
    return false;
  }
