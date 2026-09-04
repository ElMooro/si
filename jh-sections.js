/* jh-sections.js — fleet-wide section numbering + embed mode (ops 5200, 2026-09-04).
 *
 * Every page on justhodl.ai gets its real content blocks numbered §1, §2 …
 * with one level of sub-panels §1.1, §1.2 … so any block anywhere in the
 * system has a stable, human-typeable address:  <page>#<n>  e.g. bonds#1.3
 *
 * Numbers are assigned by KEY (explicit data-jh-key, else a stable id, else a
 * slug of the block's heading). The deploy bakes this page's key→number map
 * into window.JH_SECTION_MAP from config/section-registry.json (append-only),
 * so numbers never shift when a section is added — new keys get the next
 * free number. Pages may pin a number with data-jh-sec="7" (and data-jh-key).
 *
 * Embed mode: <page>.html?embed=1.3 isolates that block (everything else is
 * display:none, including site chrome added later by scripts) and reports its
 * height to the parent frame via postMessage({jh:"embed-height"}). The home
 * page uses this to show any section of any page live, rendered by the page's
 * own JS. Same-origin only (CSP frame-ancestors 'self').
 *
 * Exports window.JH_SECTIONS (array) and fires "jh:sections" on document.
 */
(function () {
  "use strict";
  if (window.__jhSectionsLoaded) return;
  window.__jhSectionsLoaded = true;

  var BIG_H = 96;            // px: a block must be at least this tall
  var BIG_W = 0.40;          // fraction of viewport width
  var SUB_MIN = 2;           // sub-panels only when a section has ≥2 big children
  var MAX_FANOUT = 40;       // a container with more big children than this is a LIST (notes, news) → one section
  var MAX_SUB = 16;          // sub-panels per section
  var CHROME = "nav,header,footer,aside,script,style,template,noscript,dialog,[role=dialog],[role=navigation],[role=banner],[role=contentinfo],[data-jh-chrome],#jh-chrome,.jh-topbar,.jh-drawer,#jh-drawer,.jh-rail,#__jhRail,.jhtag-pop,#jh-page-ai,.jh-page-ai,#jh-ai-insights,.jh-ai-insights,#jh-auth-slot,.skip-link,.sr-only";
  var MAP = window.JH_SECTION_MAP || {};
  var page = pageKey();
  var params = new URLSearchParams(location.search);
  var EMBED = params.get("embed");
  var sections = [];          // [{n, key, title, el, sub:[...]}]
  var assigned = new Map();   // element -> {n,key}
  var usedNumbers = new Set();
  var passTimer = null, observer = null, settled = false, lastSig = "";

  function pageKey() {
    var p = location.pathname.replace(/\/index\.html$/, "/").replace(/\.html?$/, "").replace(/^\/+/, "");
    return p === "" ? "index" : p;
  }
  function slug(s) {
    return String(s || "").toLowerCase().replace(/§\s*[\d.]+/g, " ").replace(/[\d%$€£¥.,+\-:]+(?=\s|$)/g, " ")
      .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 40).replace(/-+$/, "") || "";
  }
  function text(el) { return (el.textContent || "").replace(/\s+/g, " ").trim(); }
  function headText(h) {
    var own = "";
    for (var i = 0; i < h.childNodes.length; i++) { var c = h.childNodes[i]; if (c.nodeType === 3) own += c.textContent; else if (c.nodeType === 1 && /^(B|STRONG|EM|I|SPAN)$/.test(c.tagName) && !c.classList.contains("jh-secbadge") && (c.className || "").indexOf("badge") < 0 && (c.className || "").indexOf("cnt") < 0 && text(c).length <= 60 && !own.trim()) own += c.textContent; }
    own = own.replace(/\s+/g, " ").trim();
    if (own.length >= 3) return own;
    var first = h.firstElementChild; while (first && first.classList.contains("jh-secbadge")) first = first.nextElementSibling;
    return first ? text(first) : text(h);
  }
  function isChrome(el) { return !!(el.closest && el.closest(CHROME)); }
  function visible(el) {
    var cs = getComputedStyle(el);
    if (cs.display === "none" || cs.visibility === "hidden" || cs.position === "fixed") return false;
    return true;
  }
  function rect(el) { return el.getBoundingClientRect(); }
  function bigChildren(el, vw) {
    var out = [];
    for (var i = 0; i < el.children.length; i++) {
      var c = el.children[i];
      if (!(c instanceof HTMLElement) || c.matches(CHROME) || c.dataset.jhBadge != null) continue;
      if (c.hasAttribute("data-jh-sec") || c.hasAttribute("data-jh-key")) { out.push(c); continue; }
      if (!visible(c)) continue;
      var r = rect(c);
      if (r.height >= BIG_H && r.width >= BIG_W * vw) out.push(c);
    }
    return out;
  }
  // The content axis: the element whose children are the page's major blocks.
  // Max fan-out of big children; ties → larger covered height; explicit
  // [data-jh-axis] wins outright.
  function findAxis(root, vw) {
    var pin = root.querySelector("[data-jh-axis]");
    if (pin) return pin;
    var best = null, bestScore = 0, bestCover = 0;
    var walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT, {
      acceptNode: function (n) {
        if (n.matches(CHROME)) return NodeFilter.FILTER_REJECT;
        return n.children.length >= 2 ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_SKIP;
      }
    });
    var n = root;
    while (n) {
      if (n.children.length >= 2) {
        var kids = bigChildren(n, vw);
        if (kids.length >= 2 && kids.length <= MAX_FANOUT) {
          var cover = 0; kids.forEach(function (k) { cover += rect(k).height; });
          if (kids.length > bestScore || (kids.length === bestScore && cover > bestCover)) { best = n; bestScore = kids.length; bestCover = cover; }
        }
      }
      n = walker.nextNode();
    }
    return best;
  }
  var HEAD = "h1,h2,h3,h4,.ph,.card-title,.panel-title,.section-title,.title,[data-jh-title]";
  // the heading that belongs to THIS block (not to a sub-panel inside it)
  function ownHeading(el) {
    var vw = document.documentElement.clientWidth || window.innerWidth || 1200;
    var subs = bigChildren(el, vw);
    var panels = subs.length >= SUB_MIN ? subs : [];
    var list = el.querySelectorAll(HEAD);
    for (var i = 0; i < list.length; i++) {
      var h = list[i];
      if (isChrome(h) || !text(h) || h.classList.contains("jh-secbadge")) continue;
      var inPanel = false;
      for (var j = 0; j < panels.length; j++) { if (panels[j] !== h && panels[j].contains(h)) { inPanel = true; break; } }
      if (inPanel) continue;
      var owner = h.closest("[data-jh-sec],[data-jh-sub]");
      if (owner === el || owner == null || !el.contains(owner)) return h;
    }
    return null;
  }
  // a small heading that sits right before a heading-less block owns it (<h2>Title</h2><table>…)
  function prevHeading(el) {
    var p = el.previousElementSibling, hops = 0;
    while (p && hops < 3) {
      if (p.hasAttribute("data-jh-sec") || p.hasAttribute("data-jh-sub")) return null;
      if (p.matches(HEAD) && text(p)) return p;
      var inner = p.children.length === 1 && p.firstElementChild.matches(HEAD) ? p.firstElementChild : null;
      if (inner && text(inner)) return inner;
      if (rect(p).height >= BIG_H || (text(p) && !p.matches(".kicker,.eyebrow,.label,small"))) return null;
      p = p.previousElementSibling; hops++;
    }
    return null;
  }
  function titleOf(el) {
    if (el.dataset.jhTitle) return el.dataset.jhTitle;
    var h = ownHeading(el) || prevHeading(el);
    var t = h ? headText(h) : "";
    if (!t && el.getAttribute("aria-label")) t = el.getAttribute("aria-label");
    if (!t) { var k = el.querySelector(".kicker,.eyebrow,.label,.wr-kicker,b,strong"); t = k ? text(k.children.length ? k.firstElementChild : k) : ""; }
    if (!t) t = text(el).slice(0, 60);
    return t.replace(/^§\s*[\d.]+\s*/, "").replace(/§\s*[\d.]+\s*/g, "").trim().slice(0, 90);
  }
  function stableId(el) {
    var id = el.id || "";
    if (!id || /^(jh-s|card-|uid-|el-|w-)|[0-9a-f]{8,}|\d{4,}/i.test(id)) return "";
    return id.toLowerCase();
  }
  function keyOf(el, used) {
    var k = el.dataset.jhKey || stableId(el) || slug(titleOf(el)) || el.tagName.toLowerCase();
    var base = k, i = 2;
    while (used.has(k)) k = base + "-" + (i++);
    used.add(k);
    return k;
  }
  var reserved = new Set(Object.keys(MAP).map(function (k) { return String(MAP[k]); }));
  function nextNumber(prefix) {
    var i = 1;
    while (usedNumbers.has(prefix + i) || reserved.has(prefix + i)) i++;
    return prefix + i;
  }
  function numberFor(el, key, prefix) {
    var pinned = el.dataset.jhSec && /^[\d.]+$/.test(el.dataset.jhSec) ? el.dataset.jhSec : null;
    var n = pinned || (MAP[key] != null ? String(MAP[key]) : null);
    if (n != null && usedNumbers.has(n)) n = null;          // taken by another element this pass
    if (n == null) n = nextNumber(prefix);
    usedNumbers.add(n);
    return n;
  }
  // sub-panels: the big children of a section; a heading-less wrapper (a grid) is expanded one level
  function subPanels(el, vw) {
    var kids = bigChildren(el, vw).filter(function (c) { return !c.classList.contains("jh-secbadge"); });
    var out = [];
    kids.forEach(function (k) {
      var inner = bigChildren(k, vw);
      if (inner.length >= SUB_MIN && !ownHeading(k)) inner.forEach(function (c) { out.push(c); }); else out.push(k);
    });
    return out.length >= SUB_MIN && out.length <= MAX_SUB ? out : [];
  }
  function collect(root, vw) {
    var axis = findAxis(root, vw);
    if (!axis) return [];
    // sections = big children of the axis + the axis' big siblings up the tree (a hero above a grid), document order
    var set = new Set();
    bigChildren(axis, vw).forEach(function (c) { set.add(c); });
    var a = axis;
    while (a && a !== root && a.parentElement) {
      var p = a.parentElement;
      bigChildren(p, vw).forEach(function (c) { if (c !== a && !c.contains(axis)) set.add(c); });
      a = p;
    }
    if (root.hasAttribute("data-jh-sec")) set.add(root);
    var all = Array.from(set);
    var els = all.filter(function (e) { return !all.some(function (o) { return o !== e && o.contains(e); }); });
    els.sort(function (x, y) { return (x.compareDocumentPosition(y) & Node.DOCUMENT_POSITION_FOLLOWING) ? -1 : 1; });
    return els;
  }
  function badge(el, n, key, sub) {
    var old = el.querySelector(":scope > .jh-secbadge, :scope > * > .jh-secbadge") || document.querySelector('.jh-secbadge[data-for="' + CSS.escape(el.id || "") + '"]');
    if (old && old.dataset.n === n) return;
    if (old) old.remove();
    var b = document.createElement("a");
    b.className = "jh-secbadge" + (sub ? " sub" : "");
    b.dataset.jhBadge = "1"; b.dataset.n = n; b.dataset.for = el.id || "";
    b.textContent = "§" + n;
    b.href = "#" + (el.id || "");
    b.title = page + "#" + n + " · click to copy this address for the home Add bar";
    b.addEventListener("click", function (ev) {
      ev.preventDefault(); ev.stopPropagation();
      var ref = page + "#" + n;
      try { navigator.clipboard.writeText(ref); } catch (e) { }
      toast("copied  " + ref);
    });
    var h = ownHeading(el) || prevHeading(el);
    if (h) {
      h.insertBefore(b, h.firstChild);
    } else {
      b.classList.add("float");
      el.insertBefore(b, el.firstChild);
    }
  }
  function toast(msg) {
    var t = document.getElementById("jh-sec-toast");
    if (!t) { t = document.createElement("div"); t.id = "jh-sec-toast"; document.body.appendChild(t); }
    t.textContent = msg; t.classList.add("on");
    clearTimeout(t._tm); t._tm = setTimeout(function () { t.classList.remove("on"); }, 1600);
  }
  function css() {
    if (document.getElementById("jh-sections-css")) return;
    var s = document.createElement("style"); s.id = "jh-sections-css";
    s.textContent = [
      ".jh-secbadge{display:inline-block;font:700 10px/1 var(--jh-mono,ui-monospace,Menlo,monospace);letter-spacing:.06em;color:var(--jh-amber,#F0B429);background:rgba(240,180,41,.10);border:1px solid rgba(240,180,41,.35);border-radius:4px;padding:3px 6px;margin:0 8px 0 0;vertical-align:middle;text-decoration:none;cursor:copy;opacity:.85;white-space:nowrap}",
      ".jh-secbadge:hover{opacity:1;background:rgba(240,180,41,.2)}",
      ".jh-secbadge.sub{font-size:9px;padding:2px 5px;opacity:.7}",
      ".jh-secbadge.float{position:relative;z-index:5;margin:6px 0 0 6px}",
      "#jh-sec-toast{position:fixed;left:50%;bottom:28px;transform:translateX(-50%) translateY(20px);background:#12110C;color:#e8e2d4;border:1px solid #2B2820;border-radius:6px;padding:8px 14px;font:600 12px var(--jh-mono,monospace);opacity:0;pointer-events:none;transition:.2s;z-index:99999}",
      "#jh-sec-toast.on{opacity:1;transform:translateX(-50%) translateY(0)}",
      "html.jh-embed,html.jh-embed body{background:transparent!important;margin:0!important;padding:0!important;min-height:0!important;overflow:hidden!important}",
      "html.jh-embed .jh-secbadge,html.jh-embed #jh-sec-toast{display:none!important}",
      "html.jh-embed [data-jh-embed-target]{margin:0!important;box-shadow:none!important}",
    ].join("\n");
    document.head.appendChild(s);
  }
  function pass() {
    if (!document.body) return;
    var vw = document.documentElement.clientWidth || window.innerWidth || 1200;
    var root = document.querySelector("main") || document.body;
    if (root !== document.body && rect(root).height < BIG_H * 2) root = document.body;
    var els = collect(root, vw);
    usedNumbers = new Set();
    assigned.forEach(function (v, k) { if (!document.contains(k)) assigned.delete(k); else usedNumbers.add(v.n); });   // numbered elements keep their numbers; detached ones free them
    var used = new Set(), out = [];
    els.forEach(function (el) {
      var prev = assigned.get(el);
      var kids = subPanels(el, vw);
      el.setAttribute("data-jh-sec", prev ? prev.n : "");
      kids.forEach(function (c) { if (!c.hasAttribute("data-jh-sec")) c.setAttribute("data-jh-sub", "1"); });
      var key = prev ? prev.key : keyOf(el, used);
      if (prev) used.add(key);
      var n = prev ? prev.n : numberFor(el, key, "");
      assigned.set(el, { n: n, key: key });
      el.setAttribute("data-jh-sec", n); el.setAttribute("data-jh-key", key);
      if (!el.id) el.id = "jh-s" + n.replace(/\./g, "-");
      badge(el, n, key, false);
      var sub = [], subUsed = new Set();
      kids.forEach(function (c) {
        var sp = assigned.get(c);
        var sk = sp ? sp.key : key + "/" + keyOf(c, subUsed);
        if (sp) subUsed.add(sp.key.split("/").pop());
        var sn = sp ? sp.n : numberFor(c, sk, n + ".");
        assigned.set(c, { n: sn, key: sk });
        c.removeAttribute("data-jh-sub");
        c.setAttribute("data-jh-sec", sn); c.setAttribute("data-jh-key", sk);
        if (!c.id) c.id = "jh-s" + sn.replace(/\./g, "-");
        badge(c, sn, sk, true);
        sub.push({ n: sn, key: sk, title: titleOf(c), id: c.id });
      });
      out.push({ n: n, key: key, title: titleOf(el), id: el.id, sub: sub });
    });
    sections = out;
    var sig = out.map(function (s) { return s.n + ":" + s.key + "(" + s.sub.length + ")"; }).join("|");
    window.JH_SECTIONS = out.map(function (s) { return { n: s.n, key: s.key, title: s.title, id: s.id, sub: s.sub.slice() }; });
    window.JH_SECTIONS_PAGE = page;
    if (sig !== lastSig) { lastSig = sig; try { document.dispatchEvent(new CustomEvent("jh:sections", { detail: { page: page, sections: window.JH_SECTIONS, settled: settled } })); } catch (e) { } }
    if (window.parent !== window) { try { parent.postMessage({ jh: "sections", page: page, title: document.title, sections: window.JH_SECTIONS, settled: settled }, location.origin); } catch (e) { } }
  }
  function schedule(ms) { clearTimeout(passTimer); passTimer = setTimeout(function () { try { pass(); } catch (e) { console.warn("jh-sections", e); } }, ms || 250); }

  // ---------- embed mode ----------
  function findTarget() {
    if (!EMBED) return null;
    var el = document.querySelector('[data-jh-sec="' + CSS.escape(EMBED) + '"]') || document.querySelector('[data-jh-key="' + CSS.escape(EMBED) + '"]');
    if (!el && /^[\w-]+$/.test(EMBED)) el = document.getElementById(EMBED);
    return el;
  }
  var embedState = { target: null, keep: null, ro: null, lastH: 0, lastSig: "" };
  function isolate(target) {
    var keep = new Set(); var n = target;
    while (n && n !== document.documentElement) { keep.add(n); n = n.parentElement; }
    embedState.target = target; embedState.keep = keep;
    target.setAttribute("data-jh-embed-target", "1");
    n = target.parentElement;
    while (n && n !== document.documentElement) {
      for (var i = 0; i < n.children.length; i++) {
        var c = n.children[i];
        if (!keep.has(c) && !/^(SCRIPT|STYLE|LINK)$/.test(c.tagName)) c.style.setProperty("display", "none", "important");
      }
      n.style.setProperty("padding", "0", "important"); n.style.setProperty("margin", "0", "important");
      n.style.setProperty("min-height", "0", "important"); n.style.setProperty("max-width", "none", "important");
      n.style.setProperty("background", "transparent", "important");
      n = n.parentElement;
    }
    document.documentElement.classList.add("jh-embed");
    postHeight(true);
    if (window.ResizeObserver) { embedState.ro = new ResizeObserver(function () { postHeight(false); }); embedState.ro.observe(target); embedState.ro.observe(document.body); }
    setInterval(function () { postHeight(false); }, 1500);
    try { parent.postMessage({ jh: "embed-ready", page: page, sec: EMBED, title: titleOf(target), href: location.pathname + "#" + target.id }, location.origin); } catch (e) { }
  }
  function postHeight(force) {
    var t = embedState.target; if (!t) return;
    var h = Math.ceil(t.getBoundingClientRect().height + 2);
    var red = t.querySelectorAll(".RED,.red-flag,[data-flag=RED]").length, amber = t.querySelectorAll(".AMBER,[data-flag=AMBER]").length;
    var sig = h + ":" + red + ":" + amber;
    if (force || sig !== embedState.lastSig) {
      embedState.lastH = h; embedState.lastSig = sig;
      try { parent.postMessage({ jh: "embed-height", page: page, sec: EMBED, h: h, red: red, amber: amber, title: titleOf(t) }, location.origin); } catch (e) { }
    }
  }
  function embedTick(deadline) {
    var t = findTarget();
    if (t) { isolate(t); return; }
    if (Date.now() > deadline) { try { parent.postMessage({ jh: "embed-missing", page: page, sec: EMBED, have: (window.JH_SECTIONS || []).map(function (s) { return s.n; }) }, location.origin); } catch (e) { } return; }
    setTimeout(function () { embedTick(deadline); }, 300);
  }

  // ---------- boot ----------
  function boot() {
    css();
    pass();
    observer = new MutationObserver(function (muts) {
      var relevant = false;
      for (var i = 0; i < muts.length; i++) {
        var m = muts[i];
        if (m.target && m.target.closest && m.target.closest(".jh-secbadge,#jh-sec-toast")) continue;
        if (embedState.keep && m.type === "childList" && m.target === document.body) {
          for (var j = 0; j < m.addedNodes.length; j++) { var a = m.addedNodes[j]; if (a instanceof HTMLElement && !embedState.keep.has(a) && !/^(SCRIPT|STYLE|LINK)$/.test(a.tagName)) a.style.setProperty("display", "none", "important"); }
        }
        relevant = true;
      }
      if (relevant) schedule(600);
    });
    observer.observe(document.body, { childList: true, subtree: true, attributes: false });
    window.addEventListener("load", function () { schedule(400); setTimeout(function () { settled = true; schedule(50); }, 4000); });
    if (EMBED) embedTick(Date.now() + 20000);
    window.addEventListener("resize", function () { schedule(300); });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
  function canonical() {
    assigned = new Map(); usedNumbers = new Set(); lastSig = "";
    document.querySelectorAll(".jh-secbadge").forEach(function (b) { b.remove(); });
    document.querySelectorAll("[data-jh-sec],[data-jh-key],[data-jh-sub]").forEach(function (e) { if (!e.closest("#hd-grid")) { e.removeAttribute("data-jh-sec"); e.removeAttribute("data-jh-key"); e.removeAttribute("data-jh-sub"); } });
    pass();
    return window.JH_SECTIONS;
  }
  window.JustHodlSections = { rerun: function () { pass(); return window.JH_SECTIONS; }, canonical: canonical, page: page, ref: function (n) { return page + "#" + n; } };
})();
