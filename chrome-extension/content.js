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
      ' tagged) · <b style="color:#F0B429">' + LISTS.size + '</b> watchlists · ' +
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

  function upload() {
    var notes = Array.from(STORE.values());
    var lists = Array.from(LISTS.values());
    if (!notes.length && !lists.length) { msg("Nothing captured yet.", "#E07A6A"); return; }
    btn.disabled = true;
    msg("Uploading " + notes.length + " notes · " + lists.length + " watchlists…", "#F0B429");
    chrome.runtime.onMessage.addListener(function (m) {
      if (m && m.action === "upload_progress") {
        msg("Uploading " + m.sent + "/" + m.total + " notes \u00b7 " +
            (m.brainOk || 0) + " in Brain\u2026", "#F0B429");
      }
    });
    chrome.runtime.sendMessage({ action: "upload", notes: notes, watchlists: lists },
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
        '<b style="font-size:12px">JustHodl \u00b7 TV Harvest v1.2</b>' +
        '<span id="jh-x" style="margin-left:auto;cursor:pointer;color:#8a836f">\u2715</span></div>' +
      '<div id="jh-status" style="font-size:12px;margin-bottom:8px">listening\u2026</div>' +
      '<div id="jh-seen" style="max-height:110px;overflow:auto;margin-bottom:10px;border-top:1px solid #2B2820;padding-top:6px"></div>' +
      '<div style="font-size:11px;color:#8a836f;margin-bottom:8px">Open your <b>Watchlist</b> panel and click through your lists, then open <b>Notes \u2192 All notes</b> and scroll. Everything TradingView loads gets captured live.</div>' +
      '<button id="jh-up" style="width:100%;background:#F0B429;border:0;border-radius:8px;padding:9px;' +
        'font-weight:700;font-size:12px;cursor:pointer;color:#12110C">SYNC TO JUSTHODL</button>' +
      '<div id="jh-msg" style="font-size:11px;margin-top:7px;color:#8a836f"></div>';
    document.body.appendChild(panel);
    statusEl = document.getElementById("jh-status");
    listEl = document.getElementById("jh-seen");
    btn = document.getElementById("jh-up");
    document.getElementById("jh-x").onclick = function () { panel.remove(); };
    btn.onclick = upload;
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
