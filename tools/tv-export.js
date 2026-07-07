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
  var STORE = new Map();   // dedupe key -> note obj
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
      STORE.set(id, { symbol: sym, text: text.slice(0, 8000),
        title: String(title || "").slice(0, 200),
        created: ts, updated: updated || ts });
      TICKERS.add(sym);
      repaint(); return true;
    }
    return false;
  }

  /* ── JSON miner ──────────────────────────────────────────────────── */
  function mine(obj, symHint, depth) {
    if (!obj || typeof obj !== "object" || (depth || 0) > 8) return;
    if (Array.isArray(obj)) {
      obj.forEach(function(o) { mine(o, symHint, (depth||0)+1); }); return;
    }
    var sym = obj.symbol || obj.ticker || obj.symbol_full ||
              obj.market_symbol || obj.s || symHint;
    var text = obj.text || obj.note || obj.content || obj.body || obj.description;
    var hasIdentity = obj.id != null || obj.created != null ||
                      obj.created_at != null || obj.updated_at != null;
    if (typeof text === "string" && text.length > 1 && hasIdentity) {
      keep(sym, text, obj.title || obj.name || obj.subject,
           obj.created || obj.created_at, obj.updated || obj.updated_at);
    }
    for (var k in obj) {
      if (obj[k] && typeof obj[k] === "object")
        mine(obj[k], sym || symHint, (depth||0)+1);
    }
  }

  /* ── fetch wrapper ───────────────────────────────────────────────── */
  var _fetch = window.fetch.bind(window);
  var SNIFF = /note|annot/i;
  window.fetch = function(input, init) {
    var url = typeof input === "string" ? input : (input && input.url) || "";
    var p = _fetch(input, init);
    if (SNIFF.test(url) && !/notification|notice/i.test(url)) {
      p.then(function(r) {
        try { r.clone().json().then(function(j) { mine(j, null, 0); }).catch(Object); }
        catch(e) {}
      }).catch(Object);
    }
    return p;
  };
  var _xhrOpen = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function(m, url) {
    if (SNIFF.test(String(url)) && !/notification|notice/i.test(url)) {
      this.addEventListener("load", function() {
        try { mine(JSON.parse(this.responseText), null, 0); } catch(e) {}
      });
    }
    return _xhrOpen.apply(this, arguments);
  };

  /* ── TV internal API helper ──────────────────────────────────────── */
  async function tvFetch(path, params) {
    var url = "https://www.tradingview.com" + path +
              (params ? "?" + new URLSearchParams(params) : "");
    try {
      var r = await _fetch(url, { credentials: "include",
        headers: { "Accept": "application/json, */*",
                   "X-Requested-With": "XMLHttpRequest" }});
      if (r.ok) return await r.json();
    } catch(e) {}
    return null;
  }

  /* ── STEP 1: get watchlists ──────────────────────────────────────── */
  async function getWatchlists() {
    log("Fetching watchlists…");
    var syms = new Set();
    // primary: user lists
    var lists = await tvFetch("/api/v2/lists/") ||
                await tvFetch("/lists/") ||
                await tvFetch("/api/v1/lists/");
    if (lists) {
      mine(lists, null, 0);
      var arr = lists.data || lists.lists || lists.results || (Array.isArray(lists) ? lists : []);
      arr.forEach(function(lst) {
        var items = lst.symbols || lst.items || lst.data || [];
        items.forEach(function(it) {
          var s = typeof it === "string" ? it : (it.symbol || it.s || it.ticker);
          if (s) syms.add(s);
        });
      });
    }
    // fallback: scan for symbols in the watchlist DOM
    document.querySelectorAll("[data-symbol], [class*='symbolName'], [class*='symbol-name']")
      .forEach(function(el) {
        var s = el.getAttribute("data-symbol") || el.textContent.trim();
        if (s && /^[A-Z]{1,6}(:[A-Z]{1,10})?$/.test(s)) syms.add(s);
      });
    log("Found " + syms.size + " tickers across watchlists");
    return Array.from(syms);
  }

  /* ── STEP 2: fetch notes for every ticker ────────────────────────── */
  var NOTE_PATHS = [
    function(s) { return ["/note-manager/api/notes/",    { symbol: s, limit: 500 }]; },
    function(s) { return ["/note-manager/api/notes/",    { symbol_id: s, page_size: 500 }]; },
    function(s) { return ["/api/v1/text_notes/",         { symbol: s, limit: 500 }]; },
    function(s) { return ["/api/v1/text-notes/",         { symbol: s, limit: 500 }]; },
    function(s) { return ["/textnotes/list/",            { symbol: s }]; },
    function(s) { return ["/api/v2/symbols/"+encodeURIComponent(s)+"/notes/", null]; },
    function(s) { return ["/api/v1/symbols/notes/",      { symbol: s }]; },
  ];
  // Also try global note list without symbol filter
  var GLOBAL_PATHS = [
    ["/note-manager/api/notes/",  { limit: 5000 }],
    ["/api/v1/text_notes/",       { limit: 5000 }],
    ["/api/v1/text-notes/",       { limit: 5000 }],
    ["/textnotes/list/",          {}],
    ["/note-manager/api/notes/",  { page_size: 5000 }],
  ];

  async function fetchNotesForSymbol(sym) {
    for (var i = 0; i < NOTE_PATHS.length; i++) {
      var pp = NOTE_PATHS[i](sym);
      var data = await tvFetch(pp[0], pp[1]);
      if (data) { mine(data, sym, 0); break; }
    }
  }

  async function fetchAllNotes(symbols) {
    log("Probing global note endpoints…");
    var hitGlobal = false;
    for (var i = 0; i < GLOBAL_PATHS.length; i++) {
      var data = await tvFetch(GLOBAL_PATHS[i][0], GLOBAL_PATHS[i][1]);
      if (data) {
        mine(data, null, 0);
        hitGlobal = true;
        log("Global endpoint hit — got " + STORE.size + " notes so far");
        break;
      }
    }
    if (symbols.length > 0) {
      log("Fetching notes per-ticker (" + symbols.length + " symbols)…");
      for (var si = 0; si < symbols.length; si++) {
        await fetchNotesForSymbol(symbols[si]);
        if ((si + 1) % 10 === 0)
          log("Per-ticker: " + (si+1) + "/" + symbols.length + " — " + STORE.size + " notes captured");
        await new Promise(function(r) { setTimeout(r, 120); }); // gentle throttle
      }
    }
    return STORE.size;
  }

  /* ── STEP 3: chart layouts (text drawings = notes on charts) ─────── */
  async function fetchChartLayouts() {
    log("Scanning chart layouts for text annotations…");
    var layouts = await tvFetch("/api/v2/chart-layouts/", { sort: "recent", limit: 50 }) ||
                  await tvFetch("/api/v2/chart-layouts/", { pin_enabled: false, limit: 50 });
    if (!layouts) return 0;
    var arr = layouts.data || layouts.layouts || (Array.isArray(layouts) ? layouts : []);
    var found = 0;
    for (var i = 0; i < arr.length; i++) {
      var layout = arr[i];
      if (!layout) continue;
      // fetch the actual content if available
      var id = layout.id || layout.chart_id;
      var sym = layout.symbol || layout.name;
      if (id) {
        var content = await tvFetch("/api/v2/chart-layouts/" + id + "/content/") ||
                      await tvFetch("/api/v2/chart-layouts/" + id + "/");
        if (content) { mine(content, sym, 0); found++; }
      } else {
        mine(layout, sym, 0);
      }
    }
    log("Chart layouts scanned: " + found + " layouts");
    return found;
  }

  /* ── UI ──────────────────────────────────────────────────────────── */
  var panel, logEl, cntEl, upBtn, msgEl;
  function buildUI() {
    panel = document.createElement("div");
    panel.style.cssText = [
      "position:fixed;z-index:2147483647;bottom:16px;right:16px",
      "background:#0C0B09;color:#e8e2d4;border:2px solid #F0B429",
      "border-radius:10px;padding:14px 16px;font:12px/1.55 'IBM Plex Mono',monospace",
      "box-shadow:0 8px 32px rgba(0,0,0,.7);width:320px;max-height:80vh;overflow-y:auto"
    ].join(";");
    panel.innerHTML =
      "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>" +
        "<b style='color:#F0B429;font-size:13px'>JH · TV Notes v2</b>" +
        "<button id='jh_close' style='background:none;border:none;color:#8a836f;cursor:pointer;font-size:16px'>×</button>" +
      "</div>" +
      "<div id='jh_cnt' style='color:#F0B429;font-size:15px;font-weight:bold;margin-bottom:4px'>0 notes · 0 tickers</div>" +
      "<div id='jh_log' style='background:#080705;border:1px solid #2B2820;border-radius:5px;padding:8px;" +
        "max-height:200px;overflow-y:auto;font-size:10px;color:#8a836f;margin-bottom:10px;white-space:pre-wrap'>" +
        "Starting autonomous harvest…\n</div>" +
      "<button id='jh_up' style='background:#F0B429;color:#0C0B09;border:none;border-radius:6px;" +
        "padding:8px 14px;font-weight:bold;cursor:pointer;width:100%;font-size:12px;margin-bottom:6px'>" +
        "UPLOAD 0 NOTES TO BRAIN</button>" +
      "<div id='jh_msg' style='color:#6fce8a;font-size:11px;min-height:16px'></div>";
    document.body.appendChild(panel);
    logEl = document.getElementById("jh_log");
    cntEl = document.getElementById("jh_cnt");
    upBtn = document.getElementById("jh_up");
    msgEl = document.getElementById("jh_msg");
    document.getElementById("jh_close").onclick = function() {
      panel.style.display = "none";
    };
    upBtn.onclick = upload;
  }
  function log(t) {
    if (logEl) { logEl.textContent += t + "\n"; logEl.scrollTop = 9999; }
    console.log("[JH-TV]", t);
  }
  function repaint() {
    if (cntEl) cntEl.textContent = STORE.size + " notes · " + TICKERS.size + " tickers";
    if (upBtn) upBtn.textContent = "UPLOAD " + STORE.size + " NOTES TO BRAIN";
  }
  function setMsg(t, color) {
    if (msgEl) { msgEl.textContent = t; msgEl.style.color = color || "#6fce8a"; }
  }

  /* ── upload ──────────────────────────────────────────────────────── */
  async function upload() {
    var all = Array.from(STORE.values());
    if (!all.length) { setMsg("Nothing captured — harvest may still be running", "#F0B429"); return; }
    upBtn.disabled = true;
    setMsg("Uploading " + all.length + " notes…", "#F0B429");
    var totalBrain = 0, totalSent = 0;
    for (var i = 0; i < all.length; i += 100) {
      try {
        var chunk = all.slice(i, i + 100);
        var r = await _fetch(CFG.ingest_url, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ token: CFG.token, notes: chunk })
        });
        var d = await r.json();
        if (!d.ok && !d.brain_upserted) {
          setMsg("Batch " + i + " failed: " + (d.error || r.status), "#E07A6A");
          upBtn.disabled = false; return;
        }
        totalSent += d.normalized || 0;
        totalBrain += d.brain_upserted || 0;
        setMsg("Uploading… " + Math.min(i + 100, all.length) + "/" + all.length, "#F0B429");
      } catch(e) {
        setMsg("Upload error: " + e, "#E07A6A");
        upBtn.disabled = false; return;
      }
    }
    var syms = new Set(); all.forEach(function(n){syms.add(n.symbol);});
    setMsg("DONE: " + totalBrain + " notes written to Brain (" + syms.size + " tickers). Brain-compiler will route them to your engines on the next run.", "#6fce8a");
    log("Upload complete: brain=" + totalBrain + " sent=" + totalSent);
    upBtn.disabled = false;
    // trigger brain-compiler run via the ingest endpoint's /compile action
    try {
      await _fetch(CFG.ingest_url.replace(/\/$/, "") + "?action=compile", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ token: CFG.token, trigger_compile: true })
      });
    } catch(e) {}  // best-effort
  }

  /* ── main ────────────────────────────────────────────────────────── */
  buildUI();
  log("Config loaded. Starting harvest…");
  try {
    var symbols = await getWatchlists();
    var n = await fetchAllNotes(symbols);
    await fetchChartLayouts();
    log("Harvest complete: " + STORE.size + " total notes across " + TICKERS.size + " tickers.");
    if (STORE.size === 0) {
      log("No notes captured via API. Try scrolling through All Notes panel — any that load will be captured automatically.");
    } else {
      log("Ready to upload. Press UPLOAD TO BRAIN.");
    }
    repaint();
  } catch(e) {
    log("Harvest error: " + e);
    ERRORS.push(String(e));
  }

  window.__JH_TV = {
    v: 2, show: function() { if(panel) panel.style.display = "block"; },
    store: STORE, tickers: TICKERS, cfg: CFG, upload: upload
  };
})();
