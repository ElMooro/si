/* JustHodl TradingView Notes Extractor v1.0
 * Runs in YOUR OWN logged-in tradingview.com tab (DevTools console).
 * Endpoint-agnostic: it wraps fetch/XHR, so whichever private API
 * TradingView uses for text notes, the responses are captured as YOU
 * browse the "All notes" widget. Nothing leaves your browser except the
 * harvested notes, POSTed to your own JustHodl ingest endpoint.
 * Nothing is sent until you press UPLOAD. */
(async function () {
  if (window.__JH_TV) { window.__JH_TV.ui(); return; }
  var CFG = null;
  try {
    CFG = await (await fetch(
      "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/tv-ingest-config.json?t=" + Date.now()
    )).json();
  } catch (e) {
    try {
      CFG = await (await fetch("https://justhodl.ai/data/tv-ingest-config.json?t=" + Date.now())).json();
    } catch (e2) {}
  }
  if (!CFG || !CFG.ingest_url) {
    var u = prompt("Config fetch was blocked by CORS.\nOpen justhodl.ai/tv-notes.html, copy the INGEST URL, paste it here:");
    var t = prompt("Now paste the INGEST TOKEN from the same page:");
    if (!u || !t) { alert("JH: cancelled"); return; }
    CFG = { ingest_url: u.trim(), token: t.trim() };
  }

  var STORE = new Map(); // id -> note
  function idOf(sym, created, text) {
    var s = sym + "|" + created + "|" + String(text).slice(0, 160), h = 0;
    for (var i = 0; i < s.length; i++) { h = (h * 31 + s.charCodeAt(i)) >>> 0; }
    return "c" + h.toString(36);
  }
  function pageSymbol() {
    var m = location.pathname.match(/symbols?\/([A-Z0-9:._-]+)/i) ||
            location.search.match(/symbol=([A-Z0-9:._%-]+)/i);
    return m ? decodeURIComponent(m[1]).replace(/-/g, ":").toUpperCase() : null;
  }
  function keep(sym, text, title, created, updated) {
    text = String(text || "").trim();
    if (text.length < 3 || text.length > 40000) return;
    sym = String(sym || pageSymbol() || "UNTAGGED").toUpperCase();
    var n = { symbol: sym, text: text, title: title || "",
              created: created || updated || Date.now(), updated: updated || null };
    STORE.set(idOf(sym, n.created, text), n);
    paint();
  }
  function mine(obj, symHint) {
    // walk any JSON payload looking for note-shaped things
    if (!obj || typeof obj !== "object") return;
    if (Array.isArray(obj)) { obj.forEach(function (o) { mine(o, symHint); }); return; }
    var text = obj.text || obj.note || obj.content || obj.body;
    var looksNote = typeof text === "string" &&
      (obj.id !== undefined || obj.created !== undefined ||
       obj.created_at !== undefined || obj.updated_at !== undefined ||
       obj.symbol !== undefined || obj.ticker !== undefined);
    if (looksNote) {
      keep(obj.symbol || obj.ticker || obj.symbol_full || symHint, text,
           obj.title || obj.name,
           Date.parse(obj.created_at || obj.created) || obj.created,
           Date.parse(obj.updated_at || obj.updated) || obj.updated);
    }
    for (var k in obj) {
      if (obj[k] && typeof obj[k] === "object") {
        mine(obj[k], obj.symbol || obj.ticker || symHint);
      }
    }
  }
  function sniffUrl(u) { return /note/i.test(u) && !/notification|notice/i.test(u); }

  // ---- wrap fetch ----
  var _fetch = window.fetch;
  window.fetch = function (input, init) {
    var url = (typeof input === "string") ? input : (input && input.url) || "";
    var p = _fetch.apply(this, arguments);
    if (sniffUrl(url)) {
      p.then(function (r) {
        try { r.clone().json().then(function (j) { mine(j, null); }).catch(function () {}); }
        catch (e) {}
      }).catch(function () {});
    }
    return p;
  };
  // ---- wrap XHR ----
  var _open = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (m, url) {
    if (sniffUrl(String(url))) {
      this.addEventListener("load", function () {
        try { mine(JSON.parse(this.responseText), null); } catch (e) {}
      });
    }
    return _open.apply(this, arguments);
  };

  // ---- silent probes of plausible private endpoints (best-effort) ----
  ["/api/v1/text_notes/?limit=500", "/api/v1/text-notes/?limit=500",
   "/textnotes/list/", "/api/v1/symbols/notes/"].forEach(function (ep) {
    _fetch(ep, { credentials: "include" }).then(function (r) {
      if (r.ok) { r.json().then(function (j) { mine(j, null); }).catch(function () {}); }
    }).catch(function () {});
  });

  // ---- overlay ----
  var box;
  function ui() {
    if (box) { box.style.display = "block"; return; }
    box = document.createElement("div");
    box.style.cssText = "position:fixed;z-index:2147483647;bottom:18px;right:18px;background:#0C0B09;color:#e8e2d4;border:1px solid #F0B429;border-radius:8px;padding:12px 14px;font:12px/1.5 monospace;box-shadow:0 6px 24px rgba(0,0,0,.5);max-width:300px";
    box.innerHTML = "<b style='color:#F0B429'>JustHodl · TV Notes</b>" +
      "<div id='jh_cnt' style='margin:6px 0'>captured: 0 notes / 0 symbols</div>" +
      "<div style='color:#8a836f;margin-bottom:8px'>Open the Notes widget → <b>All notes</b>, click through each symbol. Everything is captured automatically.</div>" +
      "<button id='jh_up' style='background:#F0B429;color:#0C0B09;border:0;border-radius:5px;padding:6px 12px;font-weight:bold;cursor:pointer'>UPLOAD 0 NOTES</button> " +
      "<button id='jh_x' style='background:none;color:#8a836f;border:1px solid #2B2820;border-radius:5px;padding:6px 8px;cursor:pointer'>hide</button>" +
      "<div id='jh_msg' style='margin-top:6px;color:#8a836f'></div>";
    document.body.appendChild(box);
    document.getElementById("jh_x").onclick = function () { box.style.display = "none"; };
    document.getElementById("jh_up").onclick = upload;
  }
  function paint() {
    if (!box) return;
    var syms = new Set(); STORE.forEach(function (n) { syms.add(n.symbol); });
    document.getElementById("jh_cnt").textContent =
      "captured: " + STORE.size + " notes / " + syms.size + " symbols";
    document.getElementById("jh_up").textContent = "UPLOAD " + STORE.size + " NOTES";
  }
  async function upload() {
    var all = Array.from(STORE.values());
    if (!all.length) { msg("nothing captured yet — open All notes first"); return; }
    msg("uploading…");
    var sent = 0, brain = 0;
    for (var i = 0; i < all.length; i += 100) {
      try {
        var r = await _fetch(CFG.ingest_url, {
          method: "POST", headers: { "content-type": "application/json" },
          body: JSON.stringify({ token: CFG.token, notes: all.slice(i, i + 100) })
        });
        var d = await r.json();
        if (!d.ok && !d.brain_upserted) { msg("batch failed: " + (d.error || r.status)); return; }
        sent += d.normalized || 0; brain += d.brain_upserted || 0;
        msg("uploaded " + Math.min(i + 100, all.length) + "/" + all.length + "…");
      } catch (e) { msg("upload error: " + e); return; }
    }
    msg("DONE — " + brain + " notes written to your Brain (" + sent + " normalized). They are live at justhodl.ai/brain.html");
  }
  function msg(t) { var m = document.getElementById("jh_msg"); if (m) m.textContent = t; }
  window.__JH_TV = { ui: ui, store: STORE };
  ui(); paint();
  console.log("%cJustHodl TV extractor armed — browse All notes, then UPLOAD.", "color:#F0B429");
})();
