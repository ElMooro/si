/* JustHodl chart · full-page workspace with auto-hiding toolbars, readouts, watchlist and side bars (2026-10-05).
 * The chart fills the page. Move the mouse to an edge to reveal what lives there; it stays while the mouse is over it,
 * while one of its menus/inputs is in use, and hides shortly after the mouse leaves.
 *   top edge    → symbol/tabs bar, interval + tool bars, quote readouts
 *   left edge   → drawing tools
 *   right edge  → watchlist + panel rail
 *   bottom edge → bottom panels + status bar
 * Pin (📌 button on the top sheet, or Shift+H) turns auto-hide off and restores the classic layout. Preference is kept
 * in localStorage "jh-autohide" ("1" default on, "0" pinned). Elements are moved, never re-created, so every engine
 * handler and id stays intact; turning auto-hide off puts each element back exactly where it was. */
(function (root) {
  "use strict";
  if (root.JHAutoHide) return;
  var doc = root.document, KEY = "jh-autohide", EDGE = 6, HIDE_MS = 380;
  var TOP_IDS = ["tabbar", "tfbar", "favs", "chgbar", "quote", "etfhud", "volhud"];
  var on = false, built = false, homes = [], sheets = {}, timers = {}, hover = {};
  function pref() { try { return localStorage.getItem(KEY) !== "0"; } catch (e) { return true; } }
  function savePref(v) { try { localStorage.setItem(KEY, v ? "1" : "0"); } catch (e) {} }
  function desktop() { try { return !root.matchMedia("(pointer:coarse)").matches && root.innerWidth >= 760; } catch (e) { return true; } } // touch screens keep the classic layout
  function css() {
    if (doc.getElementById("jh-ah-css")) return;
    var st = doc.createElement("style"); st.id = "jh-ah-css";
    st.textContent =
      "html.jh-ah .jh-ah-sheet{position:fixed;z-index:29;transition:transform .16s ease,opacity .16s ease;will-change:transform;box-shadow:0 8px 28px rgba(0,0,0,.35)}" +
      "html.jh-ah #jh-ah-top{top:0;left:0;right:0;transform:translateY(calc(-100% - 2px));display:flex;flex-direction:column;background:var(--bg,#131722)}" +
      "html.jh-ah #jh-ah-bot{bottom:0;left:0;right:0;transform:translateY(calc(100% + 2px));display:flex;flex-direction:column;background:var(--bg,#131722)}" +
      "html.jh-ah #jh-ah-left{top:0;bottom:0;left:0;transform:translateX(calc(-100% - 2px));display:flex}" +
      "html.jh-ah #jh-ah-right{top:0;bottom:0;right:0;transform:translateX(calc(100% + 2px));display:flex;flex-direction:row}" +
      "html.jh-ah #jh-ah-top.show,html.jh-ah #jh-ah-bot.show,html.jh-ah #jh-ah-left.show,html.jh-ah #jh-ah-right.show{transform:none}" +
      "html.jh-ah #jh-ah-left>.rail{height:100%;display:flex}" +
      "html.jh-ah #jh-ah-right>#watch{height:100%}" +
      "html.jh-ah #jh-ah-right>#watch.is-collapsed{display:none}" +
      "html.jh-ah #jh-ah-right>#rrail{height:100%}" +
      "html.jh-ah #app>.row{flex:1;min-height:0}" +
      ".jh-ah-hint{position:fixed;z-index:28;pointer-events:none;opacity:0;transition:opacity .2s;background:rgba(41,98,255,.55)}" +
      "html.jh-ah .jh-ah-hint{opacity:.0}html.jh-ah.jh-ah-hinting .jh-ah-hint{opacity:1}" +
      "#jh-ah-pin{position:absolute;right:8px;bottom:6px;z-index:6;background:var(--raised,#1e222d);border:1px solid var(--line,#2a2e39);color:var(--fg,#d1d4dc);border-radius:4px;font-size:12px;line-height:18px;padding:1px 6px;cursor:pointer}" +
      "#jh-ah-pin:hover{border-color:#2962ff}" +
      "#jh-ah-unpin{position:fixed;z-index:29;top:4px;right:52px;display:none;background:var(--raised,#1e222d);border:1px solid var(--line,#2a2e39);color:var(--mut,#787b86);border-radius:4px;font-size:11px;padding:1px 6px;cursor:pointer}" +
      "html:not(.jh-ah) #jh-ah-unpin.avail{display:block}";
    doc.head.appendChild(st);
  }
  function el(id) { return doc.getElementById(id); }
  function sheet(id) {
    var s = el(id);
    if (!s) { s = doc.createElement("div"); s.id = id; s.className = "jh-ah-sheet"; doc.body.appendChild(s); }
    return s;
  }
  function park(node, into) {
    if (!node || node.parentNode === into) return;
    var mark = doc.createComment("jh-ah:" + (node.id || node.className));
    node.parentNode.insertBefore(mark, node);
    homes.push({ node: node, mark: mark });
    into.appendChild(node);
  }
  function restore() {
    for (var i = homes.length - 1; i >= 0; i--) {
      var h = homes[i];
      if (h.mark.parentNode) { h.mark.parentNode.insertBefore(h.node, h.mark); h.mark.parentNode.removeChild(h.mark); }
    }
    homes = [];
  }
  function show(name, v) {
    var s = sheets[name]; if (!s) return;
    clearTimeout(timers[name]);
    if (v) { s.classList.add("show"); return; }
    timers[name] = setTimeout(function () {
      if (hover[name] || busy(s)) { show(name, false); return; }
      s.classList.remove("show");
    }, HIDE_MS);
  }
  // a sheet stays while one of its menus, dialogs or inputs is in use
  function busy(s) {
    var a = doc.activeElement;
    if (a && s.contains(a) && /^(INPUT|SELECT|TEXTAREA)$/.test(a.tagName)) return true;
    var m = el("menu"); if (m && /\bon\b/.test(m.className) && getComputedStyle(m).display !== "none") return true;
    var f = el("fly"); if (f && getComputedStyle(f).display !== "none") return true;
    var ld = el("listdrop"); if (ld && getComputedStyle(ld).display !== "none" && s.id === "jh-ah-right") return true;
    var wm = doc.querySelector(".jhwl-menu"); if (wm && s.id === "jh-ah-right") return true;
    return false;
  }
  function build() {
    css();
    var app = el("app"), row = app && app.querySelector(":scope>.row"); if (!app || !row) return false;
    sheets.top = sheet("jh-ah-top"); sheets.bot = sheet("jh-ah-bot"); sheets.left = sheet("jh-ah-left"); sheets.right = sheet("jh-ah-right");
    TOP_IDS.forEach(function (id) { park(el(id), sheets.top); });
    park(el("rail"), sheets.left);
    park(el("watch"), sheets.right); park(el("rrail"), sheets.right);
    park(el("dock"), sheets.bot); park(app.querySelector(":scope>.foot"), sheets.bot);
    var pin = el("jh-ah-pin");
    if (!pin) { pin = doc.createElement("button"); pin.id = "jh-ah-pin"; pin.type = "button"; pin.title = "Pin toolbars and panels (Shift+H)"; pin.textContent = "📌 Pin"; pin.onclick = function () { set(false); }; }
    sheets.top.appendChild(pin);
    Object.keys(sheets).forEach(function (k) {
      var s = sheets[k];
      if (s.__jhAh) return; s.__jhAh = 1;
      s.addEventListener("mouseenter", function () { hover[k] = true; show(k, true); });
      s.addEventListener("mouseleave", function () { hover[k] = false; show(k, false); });
      s.addEventListener("focusin", function () { show(k, true); });
      s.addEventListener("focusout", function () { setTimeout(function () { if (!s.contains(doc.activeElement)) show(k, false); }, 0); });
    });
    built = true; return true;
  }
  function onMove(e) {
    if (!on) return;
    var x = e.clientX, y = e.clientY, W = root.innerWidth, H = root.innerHeight;
    if (y <= EDGE) show("top", true);
    if (x <= EDGE) show("left", true);
    if (x >= W - EDGE) show("right", true);
    if (y >= H - EDGE) show("bot", true);
  }
  function set(v) {
    v = !!v && desktop();
    if (v === on && built === v) return on;
    on = v; savePref(v);
    var html = doc.documentElement;
    if (v) { if (!build()) { on = false; return false; } html.classList.add("jh-ah"); }
    else {
      html.classList.remove("jh-ah");
      Object.keys(sheets).forEach(function (k) { sheets[k].classList.remove("show"); });
      restore(); built = false;
    }
    var un = el("jh-ah-unpin");
    if (!un) { un = doc.createElement("button"); un.id = "jh-ah-unpin"; un.type = "button"; un.title = "Full-page chart: hide toolbars until the mouse reaches an edge (Shift+H)"; un.textContent = "⤢ Full page"; un.onclick = function () { set(true); }; doc.body.appendChild(un); }
    un.classList.add("avail");
    try { root.dispatchEvent(new Event("resize")); } catch (e) {}
    return on;
  }
  // the right sheet opens itself when the watchlist is toggled from the keyboard or the rail
  function watchOpened() {
    if (!on) return;
    var w = el("watch"); if (w && w.classList.contains("is-open")) { show("right", true); show("right", false); }
  }
  function init() {
    if (!el("app") || !el("tfbar")) { setTimeout(init, 200); return; }
    doc.addEventListener("mousemove", onMove, { passive: true });
    doc.addEventListener("keydown", function (e) {
      if (e.shiftKey && !e.ctrlKey && !e.metaKey && !e.altKey && (e.key === "H" || e.key === "h")) {
        var t = e.target; if (t && /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName) || t && t.isContentEditable) return;
        e.preventDefault(); set(!on);
      }
    });
    try { new MutationObserver(watchOpened).observe(el("watch") || doc.body, { attributes: true, attributeFilter: ["class"] }); } catch (e) {}
    set(pref());
  }
  root.JHAutoHide = { set: set, on: function () { return on; }, show: function (n) { show(n, true); show(n, false); } };
  if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", init); else init();
})(window);
