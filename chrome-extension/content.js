/* JustHodl TV harvester v1.2 (ops 3160) — content script, ISOLATED world.
 *
 * v1.1 returned "0 notes from 0 tickers" because it GUESSED endpoints
 * (/api/v2/lists/, /api/v1/text_notes/ — both dead). v1.2 does not guess:
 * inject.js taps the page's real fetch/XHR, and we mine whatever
 * TradingView itself returns. We also replay the endpoints seen in the
 * live network trace: symbols_list/custom/ (watchlists) and notes getall/.
 *
 * The panel is self-diagnosing: every captured endpoint is listed with its
 * note/list yield, so a miss is debuggable instead of silent.
 */
(function () {
  if (window.__JH_HARVEST_V12) return;
  window.__JH_HARVEST_V12 = 1;

  var STORE = new Map();   // note id -> note
  var TICKERS = new Set();
  var LISTS = new Map();   // list id -> {id,name,symbols[]}
  var SRCS = new Map();    // symbol -> {source, description} (v1.5.0)
  var SRC_KEYS = ["source", "source_id", "source-logoid", "source_description", "source-description",
                  "provider", "provider_id", "exchange_source"];
  function keepSource(sym, src, desc) {
    sym = String(sym || "").trim();
    src = String(src || "").trim();
    if (!sym || !src || src.length > 120) return;
    if (JUNK_RX.test(src)) return;   // v1.7.5: template internals are not attribution
    var cur = SRCS.get(sym) || {};
    if (SRCS.size && SRCS.size % 40 === 0) saveSrcs();
    SRCS.set(sym, { source: src.slice(0, 120),
                    description: String(desc || cur.description || "").slice(0, 160) });
  }
  var HRX = [/"source"\s*:\s*\{[^{}]{0,200}?"description"\s*:\s*"([^"]{2,120})"/,
             /"source_description"\s*:\s*"([^"]{2,120})"/,
             /"source-description"\s*:\s*"([^"]{2,120})"/,
             /"source"\s*:\s*"([^"]{2,90})"/];
  var JUNK_RX = /^[a-z0-9_]{3,}$/;   // django_model-class template strings
  var DRX = /"description"\s*:\s*"([^"]{2,140})"/;
  function fromHtml(sym, text) {
    for (var i = 0; i < HRX.length; i++) {
      var m = HRX[i].exec(text);
      if (m) {
        var d = DRX.exec(text);
        keepSource(sym, m[1], d ? d[1] : "");
        return;
      }
    }
  }
  function saveSrcs() {
    try {
      var o = {};
      SRCS.forEach(function (v, k) { o[k] = v; });
      chrome.storage.local.set({ jh_srcs: o });
    } catch (e) {}
  }
  try {
    chrome.storage.local.get(["jh_srcs"], function (r) {
      var o = (r && r.jh_srcs) || {};
      var purged = 0;
      Object.keys(o).forEach(function (k) {
        var v = o[k] || {};
        if (JUNK_RX.test(String(v.source || ""))) { purged++; return; }
        if (!SRCS.has(k)) SRCS.set(k, v);
      });
      if (purged) { saveSrcs(); }
    });
  } catch (e) {}
  /* v1.7.8 — PRIORITY WALK.  ops 4071 measured what the raw watchlist
   * order actually cost: of the first 500 symbols walked, 278 were pure
   * trading venues (NASDAQ/AMEX/NYSE — knowing NVDA lists on NASDAQ
   * teaches this system nothing) and only 117 carried agency attribution.
   * The 4,598 agency-bearing rows — ECONOMICS 3317, FRED 765, TVC 353,
   * CBOE 162, the CFTC COT sets — sat at a MEDIAN index of 4,985: half
   * the payoff was past the 5,000th symbol, many hours of walking away,
   * which is why 407 symbols in produced ZERO agency rows and 100%
   * venue junk.  Those rows are the entire point of the arc: they
   * resolve to the BLS, the BEA, the ECB, the BOJ, and become real
   * provenance in gov-sources and the vault.  Walk them FIRST.       */
  var TIER1 = ["ECONOMICS", "FRED", "TVC", "COT", "COT3", "CBOE",
               "QUANDL", "USCF", "USI", "EIA", "BLS", "BEA"];
  var TIER2 = ["GLASSNODE", "INTOTHEBLOCK", "CRYPTOCAP", "FX_IDC",
               "INDEX", "DJ", "SPCFD"];
  function tierOf(s2) {
    var p = s2.indexOf(":") > 0 ? s2.split(":")[0].toUpperCase() : "";
    if (TIER1.indexOf(p) >= 0) return 0;
    if (TIER2.indexOf(p) >= 0) return 1;
    return 2;
  }
  function allWatchlistSymbols() {
    var seen = {}, b = [[], [], []];
    LISTS.forEach(function (l) {
      (l.symbols || []).forEach(function (s2) {
        if (!seen[s2] && !SRCS.has(s2)) {
          seen[s2] = 1;
          b[tierOf(s2)].push(s2);
        }
      });
    });
    return b[0].concat(b[1], b[2]);
  }
  function sniffSources(o, depth) {
    if (!o || depth > 6) return;
    if (Array.isArray(o)) { for (var i = 0; i < o.length && i < 400; i++) sniffSources(o[i], depth + 1); return; }
    if (typeof o !== "object") return;
    if (o.__dom && o.symbol) { keepSource(o.symbol, o.source_text, o.title); return; }
    if (o.__htmlsrc && o.symbol) { fromHtml(o.symbol, o.text || ""); return; }
    if (o.__progress) {
      if (o.finished) { saveSrcs(); msg("Harvest complete — " + SRCS.size +
        " sources. Auto-uploading…", "#4CC38A"); try { upload(); } catch (e) {} }
      else { msg("Harvesting sources… " + o.done + "/" + o.total +
        " (resumable — safe to close)", "#F0B429"); }
      return;
    }
    var sym = "";
    for (var k = 0; k < SYM_KEYS.length; k++) if (typeof o[SYM_KEYS[k]] === "string") { sym = o[SYM_KEYS[k]]; break; }
    if (!sym && typeof o.s === "string") sym = o.s;
    if (sym) {
      for (var j = 0; j < SRC_KEYS.length; j++) {
        var v = o[SRC_KEYS[j]];
        if (typeof v === "string" && v) { keepSource(sym, v, o.description || o.title || o.short_description); break; }
      }
    }
    // scanner shape: {columns:[...], data:[{s, d:[...]}]}
    if (Array.isArray(o.columns) && Array.isArray(o.data)) {
      var ci = o.columns.indexOf("source"), di = o.columns.indexOf("description");
      if (ci >= 0) for (var r = 0; r < o.data.length && r < 500; r++) {
        var row = o.data[r];
        if (row && row.s && Array.isArray(row.d)) keepSource(row.s, row.d[ci], di >= 0 ? row.d[di] : "");
      }
    }
    for (var kk in o) { var vv = o[kk]; if (vv && typeof vv === "object") sniffSources(vv, depth + 1); }
  }
  var SEEN = new Map();    // endpoint -> {notes,lists} | {err}

  function hashId(sym, ts, text) {
    var s = sym + "|" + ts + "|" + String(text).slice(0, 160), h = 0;
    for (var i = 0; i < s.length; i++) h = ((h * 31) + s.charCodeAt(i)) >>> 0;
    return "tv3-" + h.toString(36);
  }

  function keepNote(sym, text, title, ts) {
    text = String(text == null ? "" : text).trim();
    if (text.length < 2 || text.length > 20000) return false;
    sym = String(sym || "UNTAGGED").toUpperCase();
    if (sym.indexOf(":") >= 0) sym = sym.split(":")[1] || sym;  // NASDAQ:AAPL -> AAPL
    var t = ts || Date.now();
    if (typeof t === "string") { var p = Date.parse(t); t = isNaN(p) ? Date.now() : p; }
    if (t < 1e12) t = t * 1000;
    var id = hashId(sym, t, text);
    if (STORE.has(id)) return false;
    STORE.set(id, { symbol: sym, text: text.slice(0, 8000),
                    title: String(title || "").slice(0, 200),
                    created: t, updated: t });
    if (sym !== "UNTAGGED") TICKERS.add(sym);
    return true;
  }

  var SYM_RE = /^[A-Z0-9_]{1,12}:[A-Z0-9._!$-]{1,20}$|^[A-Z]{1,5}$/;
  var TEXT_KEYS = ["text", "content", "note", "body", "description", "comment"];
  var SYM_KEYS = ["symbol", "symbol_name", "short_name", "ticker", "full_name", "name"];

  function symOf(o, hint) {
    for (var i = 0; i < SYM_KEYS.length; i++) {
      var v = o[SYM_KEYS[i]];
      if (typeof v === "string" && SYM_RE.test(v.toUpperCase())) return v.toUpperCase();
    }
    return hint || null;
  }

  function mine(obj, hint, depth) {
    if (!obj || typeof obj !== "object" || (depth || 0) > 9) return 0;
    var n = 0, i;
    if (Array.isArray(obj)) {
      for (i = 0; i < obj.length && i < 5000; i++) n += mine(obj[i], hint, (depth || 0) + 1);
      return n;
    }
    for (i = 0; i < TEXT_KEYS.length; i++) {
      var t = obj[TEXT_KEYS[i]];
      if (typeof t === "string" && t.trim().length >= 2) {
        var ts = obj.created_at || obj.created || obj.modified_at ||
                 obj.updated_at || obj.timestamp || obj.date || null;
        if (keepNote(symOf(obj, hint), t, obj.title || obj.name, ts)) n++;
      }
    }
    for (var key in obj) {
      if (!Object.prototype.hasOwnProperty.call(obj, key)) continue;
      var v = obj[key];
      if (v && typeof v === "object") {
        var h = SYM_RE.test(String(key).toUpperCase()) ? String(key).toUpperCase() : hint;
        n += mine(v, h, (depth || 0) + 1);
      }
    }
    return n;
  }

  function symList(x) {
    var out = [];
    (x || []).forEach(function (s) {
      var v = typeof s === "string" ? s
        : (s && (s.symbol || s.s || s.full_name || s.name)) || "";
      v = String(v).trim().toUpperCase();
      if (v && v.length <= 40) out.push(v);
    });
    return out.slice(0, 500);
  }

  function harvestLists(data) {
    var arr = Array.isArray(data) ? data
      : (data && (data.data || data.results || data.lists || data.watchlists)) || null;
    if (!Array.isArray(arr)) {
      if (data && (data.symbols || data.list_symbols) && (data.name || data.id)) arr = [data];
      else return 0;
    }
    var added = 0;
    arr.forEach(function (l) {
      if (!l || typeof l !== "object") return;
      var syms = symList(l.symbols || l.list_symbols || l.items);
      var name = String(l.name || l.title || l.id || "").trim();
      if (!name || !syms.length) return;
      var id = String(l.id || name);
      LISTS.set(id, { id: id, name: name.slice(0, 120), symbols: syms,
                      color: l.color || null });
      syms.forEach(function (s) {
        var bare = s.indexOf(":") >= 0 ? s.split(":")[1] : s;
        if (bare) TICKERS.add(bare);
      });
      added++;
    });
    return added;
  }

  function short(u) {
    try {
      var x = new URL(u, location.origin);
      return (x.hostname.replace("www.tradingview.com", "tv") + x.pathname).slice(0, 58);
    } catch (e) { return String(u).slice(0, 58); }
  }

  window.addEventListener("message", function (e) {
    try { if (e.data && e.data.__jh && e.data.data) sniffSources(e.data.data, 0); } catch (_) {}
    var d = e && e.data;
    if (!d || !d.__jh) return;
    if (d.__jh === "tap-ready") { paint(); return; }
    if (d.__jh === "tap-err") {
      SEEN.set(short(d.url), { err: (d.data && (d.data.__err || ("HTTP " + d.data.__http))) || "err" });
      paint(); return;
    }
    if (d.__jh !== "tap" || !d.data) return;
    var nL = harvestLists(d.data);
    var nN = mine(d.data, null, 0);
    var k = short(d.url);
    var prev = SEEN.get(k) || { notes: 0, lists: 0 };
    SEEN.set(k, { notes: (prev.notes || 0) + nN, lists: (prev.lists || 0) + nL });
    paint();
  });

  function replay(url) { window.postMessage({ __jh: "replay", url: url }, "*"); }

  var statusEl, listEl, btn;
  function paint() {
    if (!statusEl) return;
    var tagged = 0;
    STORE.forEach(function (n) { if (n.symbol !== "UNTAGGED") tagged++; });
    statusEl.innerHTML = '<b style="color:#F0B429">' + STORE.size + '</b> notes (' + tagged +
      ' tagged) · <b style="color:#F0B429">' + LISTS.size + '</b> watchlists · <b style="color:#7EA6F0">' + SRCS.size + '</b> sources · ' +
      TICKERS.size + ' tickers';
    var rows = [];
    SEEN.forEach(function (v, k) {
      rows.push('<div style="font-size:10px;color:#8a836f;font-family:monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
        + k + ' → ' + (v.err ? '<span style="color:#E07A6A">' + v.err + '</span>'
                             : (v.notes || 0) + 'n/' + (v.lists || 0) + 'l') + '</div>');
    });
    listEl.innerHTML = rows.slice(-10).join("") ||
      '<div style="font-size:10px;color:#8a836f">no TradingView traffic captured yet…</div>';
    if (btn) btn.disabled = (STORE.size + LISTS.size) === 0;
  }

  function msg(t, c) {
    var m = document.getElementById("jh-msg");
    if (m) { m.textContent = t; m.style.color = c || "#8a836f"; }
  }

  /* ── v1.7.0 FULL AUTONOMY — "capture every watchlist, every indicator,
   * every note without me touching anything" (Khalid). Lists arrive via
   * symbols_list/all on plain page load (982 events in his own session,
   * zero clicks), notes via the existing auto-scroll, sources via a
   * self-starting harvest (once per day, resume-aware = free at full
   * coverage), sync at T+75s, at harvest finish, and every 15 min when
   * anything grew. */
  /* v1.7.3: harvest moved INTO the content script. MAIN-world fetches
   * obey the PAGE's CSP — if TradingView's connect-src blocks
   * symbol-search, every request throws into a silent catch and SRCS
   * stays 0 forever (four hours of server-side absence proved it).
   * Content-script fetches run under the EXTENSION's permissions and
   * ignore page CSP. Telemetry ships with every sync even at 0 sources,
   * so the server can see exactly what happened. */
  var DIAG = { started: 0, done: 0, total: 0, sc_ok: 0, sc_err: 0,
               sc2_ok: 0, sc2_err: 0, ss_ok: 0, ss_err: 0,
               matched: 0, first_err: "",
               tier1_done: 0, rate_per_min: 0, elapsed_s: 0 };
  var HRUN = false;
  function contentHarvest(syms) {
    if (HRUN || !syms.length) return;
    HRUN = true;
    DIAG.started = Date.now(); DIAG.total = syms.length; DIAG.done = 0;
    var i = 0;
    function step() {
      if (i >= syms.length) {
        HRUN = false;
        saveSrcs();
        msg("Harvest complete \u2014 " + SRCS.size + " sources (ss " +
            DIAG.ss_ok + "/" + DIAG.ss_err + " \u00b7 html " +
            DIAG.html_ok + "/" + DIAG.html_err + "). Auto-syncing\u2026",
            "#4CC38A");
        try { upload(); } catch (e) {}
        return;
      }
      var sym = syms[i++];
      var bare = String(sym).split(":").pop();
      var before = SRCS.size;
      var scUrl = "https://scanner.tradingview.com/symbol?symbol=" +
        encodeURIComponent(sym) +
        "&fields=source,source_description,source-description," +
        "source-logoid,description,type&no_404=true";
      // r1: scanner via background (extension perms)
      chrome.runtime.sendMessage({ action: "ssfetch", url: scUrl },
        function (resp) {
          if (resp && resp.ok) {
            DIAG.sc_ok++;
            try { resp.j.symbol = sym; } catch (e) {}
            sniffSources(resp.j, 0);
          } else {
            DIAG.sc_err++;
            if (!DIAG.first_err)
              DIAG.first_err = "sc:" + String((resp && resp.e) ||
                                              "no-response").slice(0, 90);
          }
        });
      // r2: scanner direct from content (page-origin CORS may pass)
      try {
        fetch(scUrl)
          .then(function (r) { return r.json(); })
          .then(function (j) {
            DIAG.sc2_ok++;
            try { j.symbol = sym; } catch (e) {}
            sniffSources(j, 0);
          })
          .catch(function (e) {
            DIAG.sc2_err++;
            if (!DIAG.first_err)
              DIAG.first_err = "sc2:" + String(e).slice(0, 90);
          });
      } catch (e) { DIAG.sc2_err++; }
      /* r3: symbol_search — bot-walled.  ops 4069/4070 measured ss 0 ok /
       * 407 err: a 100% failure on every single symbol walked.  v1.7.8
       * demotes it from a per-symbol tax to a 1-in-200 CANARY, so the
       * wall is still watched for the day it lifts while 99.5% of the
       * dead requests disappear — that is what pays for the tighter step
       * delay below WITHOUT raising the real request rate.            */
      if (i % 200 === 0) chrome.runtime.sendMessage(
        { action: "ssfetch",
          url: "https://symbol-search.tradingview.com/symbol_search/v3/?text=" +
               encodeURIComponent(bare) + "&hl=0&lang=en&domain=production" },
        function (resp) {
          if (resp && resp.ok) { DIAG.ss_ok++; sniffSources(resp.j, 0); }
          else { DIAG.ss_err++; }
        });
      setTimeout(function () {
        if (SRCS.size > before) DIAG.matched++;
      }, 900);
      DIAG.done = i;
      if (tierOf(sym) === 0) DIAG.tier1_done++;
      DIAG.elapsed_s = Math.round((Date.now() - DIAG.started) / 1000);
      DIAG.rate_per_min = DIAG.elapsed_s
        ? Math.round(i / (DIAG.elapsed_s / 60) * 10) / 10 : 0;
      if (i % 5 === 0)
        msg("Harvesting\u2026 " + i + "/" + syms.length + " \u00b7 " +
            SRCS.size + " sources \u00b7 ss " + DIAG.ss_ok + "/" +
            DIAG.ss_err + " html " + DIAG.html_ok + "/" + DIAG.html_err,
            "#F0B429");
      setTimeout(step, 240);   // v1.7.8: 2 reqs/sym now, not 3
    }
    step();
  }
  function startHarvest() {
    var syms = allWatchlistSymbols();
    if (!syms.length) {
      msg("All " + SRCS.size + " sources already harvested \u2713", "#4CC38A");
      return 0;
    }
    contentHarvest(syms);
    return syms.length;
  }
  var LAST_SIG = "";
  function autoSync() {
    var g = STORE.size + ":" + LISTS.size + ":" + SRCS.size;
    // v1.7.4: the sig-guard starved DIAG exactly when diagnosis mattered
    // most — zero source-growth meant zero syncs meant the telemetry
    // explaining the zero could never ship. During or after any harvest,
    // sync unconditionally.
    if (g === LAST_SIG && !DIAG.total) return;
    if (!STORE.size && !LISTS.size && !SRCS.size) return;
    LAST_SIG = g;
    try { upload(); } catch (e) {}
  }
  /* v1.7.1: the one-shot 30s auto-start RACED the watchlist load and
   * died silently (Khalid's badge: v1.7.0, 491 lists, no harvest line).
   * Now it retries every 20s until the lists exist, then starts once. */
  var AUTO_TRIES = 0;
  /* v1.7.9 — THE UPDATE-DAY TRAP.  The guard stored only the date, so an
   * extension updated on a day the old build had already walked would see
   * jh_auto_day == today and return immediately: the new build sits idle
   * until tomorrow.  That would have silently swallowed the entire v1.7.8
   * priority walk on the very day it shipped.  Stamping the VERSION into
   * the guard makes any upgrade re-arm the walk at once, while still
   * holding the once-per-day rule within a single version.            */
  function EXTV() {
    try { return chrome.runtime.getManifest().version; } catch (e) { return "0"; }
  }
  function autoKey() {
    return new Date().toISOString().slice(0, 10) + "|" + EXTV();
  }
  function autoStart() {
    AUTO_TRIES++;
    try {
      chrome.storage.local.get(["jh_auto_day"], function (r) {
        var today = autoKey();
        if ((r && r.jh_auto_day) === today) return;
        if (startHarvest() > 0) {
          chrome.storage.local.set({ jh_auto_day: today });
        } else if (AUTO_TRIES < 30) {
          setTimeout(autoStart, 20000);
        }
      });
    } catch (e) {}
  }
  setTimeout(autoStart, 20000);
  setTimeout(autoSync, 75000);
  setInterval(autoSync, 15 * 60 * 1000);

  function upload() {
    var notes = Array.from(STORE.values());
    var lists = Array.from(LISTS.values());
    if (!notes.length && !lists.length) { msg("Nothing captured yet.", "#E07A6A"); return; }
    btn.disabled = true;
    msg("Uploading " + notes.length + " notes · " + lists.length + " watchlists · " + SRCS.size + " sources…", "#F0B429");
    chrome.runtime.onMessage.addListener(function (m, _s, sendResponse) {
    if (m && m.action === "harvest") {
      var syms = allWatchlistSymbols();
      if (!syms.length) { msg("All " + SRCS.size + " sources already harvested \u2713", "#4CC38A"); }
      else {
        msg("Harvesting " + syms.length + " symbols (\u2248" +
            Math.ceil(syms.length / 170) + " min)…", "#F0B429");
        window.postMessage({ __jh_cmd: "harvest", symbols: syms }, "*");
      }
      try { sendResponse({ ok: 1, n: syms.length, have: SRCS.size }); } catch (e) {}
      return;
    }
  
      if (m && m.action === "upload_progress") {
        msg("Uploading " + m.sent + "/" + m.total + " notes \u00b7 " +
            (m.brainOk || 0) + " in Brain\u2026", "#F0B429");
      }
    });
    var sources = [];
    SRCS.forEach(function (v, k) { sources.push({ symbol: k, source: v.source, description: v.description }); });
    chrome.runtime.sendMessage({ action: "upload", notes: notes, watchlists: lists, sources: sources, harvest_diag: DIAG },
      function (res) {
        if (res && (res.ok || res.brain_upserted > 0 || res.watchlists_saved > 0)) {
          msg("\u2705 " + (res.brain_upserted || 0) + " notes \u2192 Brain \u00b7 " +
              (res.watchlists_saved || 0) + " watchlists \u2192 tracker" +
              (res.brain_errors ? " (" + res.brain_errors + " failed)" : ""), "#6fce8a");
          btn.textContent = "SYNC COMPLETE";
        } else {
          msg("\u274c " + ((res && res.error) || "upload failed \u2014 see service worker console"),
              "#E07A6A");
          btn.disabled = false;
        }
      });
  }

  function mount() {
    if (document.getElementById("jh-tv-panel")) return;
    var panel = document.createElement("div");
    panel.id = "jh-tv-panel";
    panel.style.cssText = "position:fixed;right:16px;bottom:16px;z-index:2147483647;width:340px;" +
      "background:#12110C;border:1px solid #2B2820;border-radius:10px;padding:12px;" +
      "font-family:Inter,system-ui,sans-serif;color:#e8e2d4;box-shadow:0 8px 30px rgba(0,0,0,.5)";
    panel.innerHTML =
      '<div style="display:flex;align-items:center;gap:6px;margin-bottom:8px">' +
        '<span style="width:7px;height:7px;background:#F0B429;border-radius:50%"></span>' +
        '<b style="font-size:12px">JustHodl \u00b7 TV Harvest v' + (chrome.runtime.getManifest().version) + '</b>' +
        '<span id="jh-x" style="margin-left:auto;cursor:pointer;color:#8a836f">\u2715</span></div>' +
      '<div id="jh-status" style="font-size:12px;margin-bottom:8px">listening\u2026</div>' +
      '<div id="jh-seen" style="max-height:110px;overflow:auto;margin-bottom:10px;border-top:1px solid #2B2820;padding-top:6px"></div>' +
      '<div style="font-size:11px;color:#8a836f;margin-bottom:8px">Fully automatic \u2014 watchlists, notes and sources capture, harvest and sync on their own. Leave this tab in the foreground while a harvest counts.</div>' +
      '<button id="jh-harvest" style="width:100%;background:#1f6feb;border:0;border-radius:8px;padding:9px;' +
        'font-weight:700;font-size:12px;cursor:pointer;color:#fff;margin-bottom:7px">\u26a1 HARVEST SOURCES (all symbols)</button>' +
      '<button id="jh-up" style="width:100%;background:#F0B429;border:0;border-radius:8px;padding:9px;' +
        'font-weight:700;font-size:12px;cursor:pointer;color:#12110C">SYNC TO JUSTHODL</button>' +
      '<div id="jh-msg" style="font-size:11px;margin-top:7px;color:#8a836f"></div>';
    document.body.appendChild(panel);
    statusEl = document.getElementById("jh-status");
    listEl = document.getElementById("jh-seen");
    btn = document.getElementById("jh-up");
    document.getElementById("jh-x").onclick = function () { panel.remove(); };
    btn.onclick = upload;
    document.getElementById("jh-harvest").onclick = function () {
      // v1.7.2: in-panel, same scope as startHarvest — no popup, no
      // messaging, nothing between the click and the walk.
      var n = startHarvest();
      if (n > 0)
        chrome.storage.local.set({ jh_auto_day: autoKey() });
    };
    paint();
  }

  function boot() {
    mount();
    var O = location.origin;
    [O + "/api/v1/symbols_list/custom/?source=web",
     O + "/api/v1/symbols_list/colored/?source=web",
     O + "/textnotes/getall/",
     O + "/textnotes/getall/?source=web"]
      .forEach(function (u, i) { setTimeout(function () { replay(u); }, 500 + i * 350); });
    setTimeout(function () { replay(O + "/api/v1/symbols_list/custom/?source=web"); }, 4500);
  }

  if (document.body) boot();
  else document.addEventListener("DOMContentLoaded", boot);
})();

  /* v1.7.2: top-level popup listener — the earlier one was injected into a
   * nested handler and depended on upload() having run first. */
  try {
    chrome.runtime.onMessage.addListener(function (m, _s, sendResponse) {
      if (m && m.action === "harvest") {
        var n = startHarvest();
        try { sendResponse({ ok: 1, n: n, have: SRCS.size }); } catch (e) {}
        return true;
      }
    });
  } catch (e) {}
