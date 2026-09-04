/* home.js — JustHodl.AI command desk v3 (ops 5200, 2026-09-04)
 *
 * The homepage is an ordered list of BLOCKS. Each block is one line of address:
 *   bonds#1.4            a numbered section of a page, rendered live by the page itself (same-origin embed)
 *   bond-warroom/japan   a panel of an engine's output feed, rendered here in the war-room table
 *   auctions             a whole page
 * Addresses come from jh-sections.js (every page numbers its sections §1, §1.1 …) and from the
 * engine registry (851 engines, learned contracts). Layout persists to the signed-in account via the
 * data-proxy /workspace/home store (revisioned) and to this device otherwise.
 */
(function () {
  "use strict";
  var PROXY = (window.JUSTHODL_AUTH_CONFIG && window.JUSTHODL_AUTH_CONFIG.syncBase) || "https://justhodl-data-proxy.raafouis.workers.dev";
  var SCHEMA = 2, LS_KEY = "jh_home_v3", LS_SEC = "jh_home_sections_v1", MAX_BLOCKS = 120;
  var $ = function (id) { return document.getElementById(id); };
  var esc = function (s) { return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); };
  var uid = function () { return "b" + Date.now().toString(36) + Math.random().toString(36).slice(2, 7); };
  var ago = function (iso) { if (!iso) return ""; var t = Date.parse(iso); if (isNaN(t)) return ""; var m = Math.round((Date.now() - t) / 60000); return m < 1 ? "now" : m < 60 ? m + "m ago" : m < 2880 ? Math.round(m / 60) + "h ago" : Math.round(m / 1440) + "d ago"; };
  var isObj = function (v) { return v && typeof v === "object" && !Array.isArray(v); };
  var FLAGS = /^(RED|AMBER|GREEN|OK|WATCH|WARN|STALE|LIVE|BULLISH|BEARISH|NEUTRAL|PUMP|DUMP|PASS|FAIL|ACUTE|ELEVATED|CALM|STRONG|WEAK|EXPANSION|SLOWDOWN|CONTRACTION|RECESSION|RECOVERY)$/i;

  var state = { schema: SCHEMA, blocks: [], revision: 0, updatedAt: null };
  var engines = [], engineByName = new Map(), engineByShort = new Map(), contracts = {}, pages = [], pageByKey = new Map();
  var registry = { pages: {} };       // config/section-registry.json (baked numbers)
  var discovered = {};                // page -> {title, sections:[...], at}  (live discovery through a hidden frame)
  var feedCache = new Map(), embedMeta = new Map(), saveTimer = null, cloudUser = null, cloudRevision = 0, scope = "local";
  var palette = { items: [], sel: -1, q: "" };

  // ------------------------------------------------------------------ helpers
  function short(name) { return String(name || "").replace(/^justhodl-/, ""); }
  function cleanFeed(p) { return String(p || "").replace(/^s3:\/\/[^/]+\//, "").replace(/^https?:\/\/justhodl-dashboard-live[^/]*\//, "").replace(/^\/+/, ""); }
  function isFeed(p) { return typeof p === "string" && /\.json(l)?(\.gz)?(\?|$)/i.test(p) && p.indexOf("data/") === 0 && !/\.gz$/i.test(p); }
  async function getJson(paths, opts) {
    var err;
    for (var i = 0; i < paths.length; i++) {
      try { var r = await fetch(paths[i], Object.assign({ headers: { Accept: "application/json" } }, opts || {})); if (!r.ok) throw new Error("HTTP " + r.status); return await r.json(); }
      catch (e) { err = e; }
    }
    throw err || new Error("unavailable");
  }
  function get(obj, path) {
    if (!path) return obj;
    var cur = obj, parts = String(path).replace(/\[(\d+)\]/g, ".$1").split(".").filter(Boolean);
    for (var i = 0; i < parts.length; i++) { if (cur == null) return undefined; cur = cur[parts[i]]; }
    return cur;
  }
  function toast(msg) { var t = $("hd-toast"); t.textContent = msg; t.classList.add("on"); clearTimeout(t._tm); t._tm = setTimeout(function () { t.classList.remove("on"); }, 1800); }
  function pageKeyOf(href) { return String(href || "").replace(/^\/+/, "").replace(/\/index\.html$/, "").replace(/\.html?$/, "").replace(/\/$/, "") || "index"; }
  function pageHref(key) { return "/" + key + ".html"; }
  function blockRef(b) { return b.type === "engine" ? b.ref + (b.panel ? "/" + b.panel : "") : b.type === "section" ? b.page + "#" + b.sec : b.page; }

  // ------------------------------------------------------------------ catalogs
  async function loadCatalogs() {
    var res = await Promise.allSettled([
      getJson(["/engine-manifest.json", PROXY + "/engine-manifest.json"]),
      getJson(["/data/engine-registry.json", PROXY + "/data/engine-registry.json"]),
      getJson(["/config/engine-contracts.json", PROXY + "/config/engine-contracts.json"]),
      getJson(["/nav-manifest.json"]),
      getJson(["/config/section-registry.json"]),
      getJson(["/config/home-layout.json"]),
    ]);
    var manifest = res[0].status === "fulfilled" ? res[0].value : {}, reg = res[1].status === "fulfilled" ? res[1].value : {};
    contracts = res[2].status === "fulfilled" ? (res[2].value.contracts || {}) : {};
    var nav = res[3].status === "fulfilled" ? res[3].value : { categories: [] };
    registry = res[4].status === "fulfilled" && res[4].value && res[4].value.pages ? res[4].value : { pages: {} };
    var defaults = res[5].status === "fulfilled" ? res[5].value : null;
    var joined = new Map();
    (Array.isArray(manifest.engines) ? manifest.engines : []).forEach(function (e) { if (e && e.engine) joined.set(e.engine, { name: e.engine, description: e.description || "", keys: e.keys || [], outs: [] }); });
    var records = isObj(reg.engines) ? reg.engines : {};
    Object.keys(records).forEach(function (name) {
      var r = records[name] || {}, prior = joined.get(name) || { name: name, description: "", keys: [], outs: [] };
      joined.set(name, Object.assign({}, prior, { name: name, description: r.description || r.doc || prior.description || "", keys: [].concat(prior.keys || [], r.keys || []), outs: r.outs || r.outputs || [] }));
    });
    engines = Array.from(joined.values()).map(function (e) {
      var raw = [].concat(e.outs || [], e.keys || []); if (isObj(e.outs)) raw = raw.concat(Object.values(e.outs));
      e.feeds = Array.from(new Set(raw.map(function (x) { return cleanFeed(isObj(x) ? (x.key || x.path || x.url || "") : x); }).filter(isFeed)))
        .sort(function (a, b) { return Number(/history|archive|_hist|snapshot/i.test(a)) - Number(/history|archive|_hist|snapshot/i.test(b)); });
      e.short = short(e.name); e.search = (e.short + " " + e.description + " " + e.feeds.join(" ")).toLowerCase();
      return e;
    }).sort(function (a, b) { return a.short.localeCompare(b.short); });
    engineByName = new Map(engines.map(function (e) { return [e.name, e]; }));
    engineByShort = new Map(engines.map(function (e) { return [e.short, e]; }));
    pages = [];
    (nav.categories || []).forEach(function (c) { (c.pages || []).forEach(function (p) { var k = pageKeyOf(p.href); pages.push({ key: k, href: p.href, title: p.title || k, cat: c.name }); }); });
    pageByKey = new Map(pages.map(function (p) { return [p.key, p]; }));
    Object.keys(registry.pages || {}).forEach(function (k) { if (!pageByKey.has(k)) { var p = { key: k, href: pageHref(k), title: registry.pages[k].title || k, cat: "Other" }; pages.push(p); pageByKey.set(k, p); } });
    try { discovered = JSON.parse(localStorage.getItem(LS_SEC) || "{}") || {}; } catch (e) { discovered = {}; }
    $("hd-catalog").textContent = engines.length.toLocaleString() + " engines · " + pages.length.toLocaleString() + " pages · " + Object.keys(registry.pages || {}).length.toLocaleString() + " pages with baked section numbers";
    return defaults;
  }
  function sectionsOf(pageKey) {
    var d = discovered[pageKey], r = registry.pages && registry.pages[pageKey];
    if (d && d.sections && d.sections.length) return d.sections;
    if (r && r.sections) return r.sections;
    return null;
  }
  // live discovery: load the page in a hidden frame, jh-sections posts its numbered list back
  var discoverQueue = new Map();
  function discover(pageKey) {
    if (discoverQueue.has(pageKey)) return discoverQueue.get(pageKey);
    var p = new Promise(function (resolve) {
      var f = document.createElement("iframe"); f.setAttribute("aria-hidden", "true"); f.tabIndex = -1;
      f.style.cssText = "position:absolute;width:1200px;height:900px;left:-3000px;top:0;visibility:hidden;pointer-events:none";
      f.src = pageHref(pageKey) + "?jhdiscover=1";
      var done = false, last = null;
      var finish = function () { if (done) return; done = true; f.remove(); discoverQueue.delete(pageKey); resolve(last); };
      var onMsg = function (ev) {
        if (ev.source !== f.contentWindow || !ev.data || ev.data.jh !== "sections") return;
        last = { title: ev.data.title, sections: ev.data.sections, at: Date.now() };
        discovered[pageKey] = last; try { localStorage.setItem(LS_SEC, JSON.stringify(discovered)); } catch (e) { }
        if (ev.data.settled) { window.removeEventListener("message", onMsg); finish(); }
        else renderPalette();
      };
      window.addEventListener("message", onMsg);
      document.body.appendChild(f);
      setTimeout(function () { window.removeEventListener("message", onMsg); finish(); }, 14000);
    });
    discoverQueue.set(pageKey, p);
    return p;
  }

  // ------------------------------------------------------------------ state
  function normBlock(b) {
    if (!b || typeof b !== "object") return null;
    var t = b.type === "engine" || b.type === "section" || b.type === "page" ? b.type : null;
    if (!t) return null;
    var out = { id: String(b.id || uid()), type: t, w: [1, 2, 3].indexOf(Number(b.w)) >= 0 ? Number(b.w) : (t === "engine" ? 1 : t === "section" ? 2 : 3), title: String(b.title || "").slice(0, 120) };
    if (t === "engine") { out.ref = short(b.ref || b.engine || ""); out.panel = String(b.panel || "").slice(0, 120); out.feed = cleanFeed(b.feed || ""); if (!out.ref) return null; }
    else { out.page = pageKeyOf(b.page || ""); if (!out.page) return null; if (t === "section") { out.sec = String(b.sec || "1"); out.key = String(b.key || ""); } }
    return out;
  }
  function safeState(c) { var blocks = (Array.isArray(c && c.blocks) ? c.blocks : Array.isArray(c && c.cards) ? c.cards : []).map(normBlock).filter(Boolean).slice(0, MAX_BLOCKS); return { schema: SCHEMA, blocks: blocks, revision: Number(c && c.revision) || 0, updatedAt: c && c.updatedAt || null }; }
  function migrateV1(c) {   // the Sep-3 engine-workspace layout → engine blocks
    return { schema: SCHEMA, revision: Number(c.revision) || 0, blocks: (c.cards || []).map(function (card) { return normBlock({ type: "engine", ref: card.engine, feed: card.feed, panel: "", title: card.title, w: card.size === "wide" ? 2 : 1 }); }).filter(Boolean) };
  }
  function fromDefaults(d) { var s = safeState(d || {}); if (!s.blocks.length) s.blocks = [normBlock({ type: "engine", ref: "bond-warroom", panel: "us_rates" }), normBlock({ type: "engine", ref: "bond-warroom", panel: "japan" }), normBlock({ type: "engine", ref: "bond-warroom", panel: "credit" })].filter(Boolean); return s; }
  function loadLocal() { try { var v = JSON.parse(localStorage.getItem(LS_KEY) || "null"); return v ? safeState(v) : null; } catch (e) { return null; } }
  function saveLocal() { try { localStorage.setItem(LS_KEY, JSON.stringify(state)); } catch (e) { } }
  function setStatus(text, kind) { var n = $("hd-save"); n.innerHTML = '<span class="dot ' + (kind || "") + '"></span>' + esc(text); }
  function scheduleSave() {
    state.updatedAt = new Date().toISOString(); saveLocal();
    clearTimeout(saveTimer); saveTimer = setTimeout(cloudSave, 900);
    setStatus(cloudUser ? "saving to your account…" : "saved on this device", cloudUser ? "warn" : "");
  }
  async function cloudRequest(method, payload) {
    var token = await window.JustHodlAuth.getAccessToken(); if (!token) throw new Error("session unavailable");
    var r = await fetch(PROXY + "/workspace/home", { method: method, headers: Object.assign({ Authorization: "Bearer " + token }, payload ? { "Content-Type": "application/json" } : {}), body: payload ? JSON.stringify(payload) : undefined });
    var j = await r.json().catch(function () { return {}; });
    if (!r.ok) { var e = new Error(j.error || ("HTTP " + r.status)); e.status = r.status; e.body = j; throw e; }
    return j;
  }
  async function cloudLoad(user) {
    if (!user) return;
    setStatus("loading your account layout…", "warn");
    try {
      var cloud = await cloudRequest("GET");
      if (cloud && (cloud.schema === SCHEMA || cloud.schema === 1)) {
        var s = cloud.schema === SCHEMA ? safeState(cloud) : migrateV1(cloud);
        cloudRevision = Number(cloud.revision) || 0;
        if (s.blocks.length) { state = s; state.revision = cloudRevision; saveLocal(); render(); }
        else { cloudRevision = Number(cloud.revision) || 0; }
        setStatus("synced · " + user.email, "on");
      } else { cloudRevision = Number(cloud && cloud.revision) || 0; setStatus("account connected · saving this layout", "warn"); await cloudSave(); }
    } catch (e) { setStatus("account layout unavailable (" + e.message + ") · using this device", "warn"); }
  }
  async function cloudSave() {
    if (!cloudUser || !window.JustHodlAuth) return;
    try {
      var payload = { schema: SCHEMA, cards: state.blocks, blocks: state.blocks, updatedAt: state.updatedAt, revision: cloudRevision + 1, baseRevision: cloudRevision };
      var r = await cloudRequest("PUT", payload);
      cloudRevision = Number(r.revision) || cloudRevision + 1; state.revision = cloudRevision;
      setStatus("synced · " + cloudUser.email, "on");
    } catch (e) {
      if (e.status === 409) { cloudRevision = Number(e.body && e.body.revision) || cloudRevision; toast("layout changed elsewhere — reloading it"); await cloudLoad(cloudUser); }
      else setStatus("cloud save failed (" + e.message + ") · kept on this device", "warn");
    }
  }

  // ------------------------------------------------------------------ address grammar
  function parseAddress(raw) {
    var q = String(raw || "").trim().replace(/^https?:\/\/[^/]+/, "").replace(/^\/+/, "").replace(/\.html?(?=[#§/ ]|$)/i, "");
    if (!q) return null;
    var m;
    if ((m = q.match(/^([a-z0-9][a-z0-9_\/-]*?)\s*(?:#|§|\s+§?\s*)(\d+(?:\.\d+)?)$/i))) { var pk = pageKeyOf(m[1]); if (pageByKey.has(pk) || sectionsOf(pk)) return { type: "section", page: pk, sec: m[2] }; }
    if ((m = q.match(/^(?:justhodl-)?([a-z0-9][a-z0-9_-]*)\/([a-z0-9_.\[\]-]+)$/i)) && engineByShort.has(m[1].toLowerCase())) return { type: "engine", ref: m[1].toLowerCase(), panel: m[2] };
    if ((m = q.match(/^(?:justhodl-)?([a-z0-9][a-z0-9_-]*)$/i))) {
      var k = m[1].toLowerCase();
      if (pageByKey.has(k)) return { type: "page", page: k };
      if (engineByShort.has(k)) return { type: "engine", ref: k, panel: "" };
    }
    if ((m = q.match(/^([a-z0-9][a-z0-9_\/-]*)$/i)) && pageByKey.has(pageKeyOf(m[1]))) return { type: "page", page: pageKeyOf(m[1]) };
    return null;
  }
  function addBlock(spec, quiet) {
    var b = normBlock(spec); if (!b) return null;
    if (b.type === "engine") { var e = engineByShort.get(b.ref); if (!b.feed) b.feed = (e && e.feeds[0]) || "data/" + b.ref + ".json"; if (!b.title) b.title = b.ref + (b.panel ? " · " + b.panel : ""); }
    else { var p = pageByKey.get(b.page); if (!b.title) b.title = (p ? p.title : b.page) + (b.type === "section" ? " §" + b.sec : ""); if (b.type === "section") { var s = findSection(b.page, b.sec); if (s) { b.key = s.key; b.title = s.title || b.title; } } }
    if (state.blocks.length >= MAX_BLOCKS) { toast("block limit " + MAX_BLOCKS); return null; }
    state.blocks.push(b); scheduleSave(); render(); if (!quiet) toast("added  " + blockRef(b));
    setTimeout(function () { var n = document.querySelector('[data-bid="' + b.id + '"]'); if (n) n.scrollIntoView({ behavior: "smooth", block: "center" }); }, 60);
    return b;
  }
  function findSection(pageKey, sec) {
    var list = sectionsOf(pageKey) || [];
    for (var i = 0; i < list.length; i++) { if (String(list[i].n) === String(sec)) return list[i]; var sub = list[i].sub || []; for (var j = 0; j < sub.length; j++) if (String(sub[j].n) === String(sec)) return sub[j]; }
    return null;
  }

  // ------------------------------------------------------------------ palette
  function paletteItems(q) {
    q = String(q || "").trim().toLowerCase();
    var items = [], exact = parseAddress(q);
    if (exact) items.push({ grp: "Add exactly", ref: blockRef(normBlock(exact) || exact), title: exact.type === "engine" ? "engine panel" : exact.type === "section" ? "page section" : "whole page", spec: exact, meta: "↵ Enter" });
    var words = q.split(/[\s#§\/]+/).filter(Boolean);
    var hit = function (s) { s = String(s || "").toLowerCase(); return words.every(function (w) { return s.indexOf(w) >= 0; }); };
    // page + its sections
    var pm = q.match(/^([a-z0-9][a-z0-9_\/-]*?)(?:\.html)?\s*(?:#|§|\s)?\s*([\d.]*)$/i);
    var focusPage = pm && pageByKey.get(pageKeyOf(pm[1])) ? pageKeyOf(pm[1]) : null;
    var pageHits = pages.filter(function (p) { return q && (hit(p.key + " " + p.title)); }).slice(0, focusPage ? 1 : 12);
    if (focusPage && pageHits.every(function (p) { return p.key !== focusPage; })) pageHits.unshift(pageByKey.get(focusPage));
    pageHits.forEach(function (p) {
      var secs = sectionsOf(p.key);
      items.push({ grp: "Pages", ref: p.key, title: p.title, spec: { type: "page", page: p.key }, meta: secs ? secs.length + " sections" : "sections not indexed yet", exp: !secs, page: p.key });
      if (secs && (focusPage === p.key || pageHits.length <= 2)) secs.forEach(function (s) {
        if (pm && pm[2] && String(s.n).indexOf(pm[2]) !== 0 && !(s.sub || []).some(function (x) { return String(x.n).indexOf(pm[2]) === 0; })) return;
        items.push({ grp: "Pages", ref: p.key + "#" + s.n, title: s.title, spec: { type: "section", page: p.key, sec: s.n, key: s.key }, meta: (s.sub || []).length ? (s.sub.length + " panels") : "section", sub: false });
        (s.sub || []).forEach(function (x) { if (pm && pm[2] && String(x.n).indexOf(pm[2]) !== 0 && String(s.n).indexOf(pm[2]) !== 0) return; items.push({ grp: "Pages", ref: p.key + "#" + x.n, title: x.title, spec: { type: "section", page: p.key, sec: x.n, key: x.key }, meta: "panel", sub: true }); });
      });
    });
    // sections anywhere (registry + discovered), by words
    if (words.length && !focusPage) {
      var all = Object.assign({}, registry.pages || {}); Object.keys(discovered).forEach(function (k) { if (discovered[k] && discovered[k].sections) all[k] = discovered[k]; });
      var n = 0;
      Object.keys(all).forEach(function (pk) { (all[pk].sections || []).forEach(function (s) {
        if (n < 14 && hit(pk + " " + s.title + " " + s.key)) { items.push({ grp: "Sections", ref: pk + "#" + s.n, title: s.title, spec: { type: "section", page: pk, sec: s.n, key: s.key }, meta: (pageByKey.get(pk) || {}).title || pk }); n++; }
        (s.sub || []).forEach(function (x) { if (n < 14 && hit(pk + " " + x.title + " " + x.key)) { items.push({ grp: "Sections", ref: pk + "#" + x.n, title: x.title, spec: { type: "section", page: pk, sec: x.n, key: x.key }, meta: (pageByKey.get(pk) || {}).title || pk }); n++; } });
      }); });
    }
    // engines (+ panels of the focused engine's feed)
    var em = q.match(/^(?:justhodl-)?([a-z0-9][a-z0-9_-]*)\/?([a-z0-9_.-]*)$/i);
    var focusEngine = em && engineByShort.get(em[1].toLowerCase()) ? em[1].toLowerCase() : null;
    var eHits = engines.filter(function (e) { return q && hit(e.search); }).slice(0, focusEngine ? 1 : 10);
    if (focusEngine && eHits.every(function (e) { return e.short !== focusEngine; })) eHits.unshift(engineByShort.get(focusEngine));
    eHits.forEach(function (e) {
      items.push({ grp: "Engines", ref: e.short, title: e.description || e.name, spec: { type: "engine", ref: e.short, panel: "" }, meta: e.feeds.length ? e.feeds[0].replace(/^data\//, "") : "no output feed" });
      if (e.short === focusEngine && e.feeds[0]) {
        var c = feedCache.get(e.feeds[0]);
        if (c && c.data) panelsOf(c.data, e.feeds[0]).forEach(function (p) { if (!em[2] || p.key.indexOf(em[2].toLowerCase()) >= 0) items.push({ grp: "Engines", ref: e.short + "/" + p.key, title: p.title, spec: { type: "engine", ref: e.short, panel: p.key }, meta: p.meta, sub: true }); });
        else { loadFeed(e.feeds[0]).then(renderPalette); items.push({ grp: "Engines", ref: e.short + "/…", title: "reading panels…", meta: "", spec: null }); }
      }
    });
    return items;
  }
  function panelsOf(data, feed) {
    var out = [];
    if (isObj(data) && isObj(data.panels)) Object.keys(data.panels).forEach(function (k) { var v = data.panels[k]; out.push({ key: k, title: k.replace(/_/g, " "), meta: Array.isArray(v) ? v.length + " rows" : "object" }); });
    if (isObj(data)) Object.keys(data).forEach(function (k) { var v = data[k]; if (k === "panels") return; if (Array.isArray(v) && v.length && isObj(v[0])) out.push({ key: k, title: k.replace(/_/g, " "), meta: v.length + " rows" }); else if (isObj(v) && Object.keys(v).length >= 2 && Object.keys(v).length <= 40 && Object.values(v).every(function (x) { return !isObj(x) || Object.keys(x).length <= 12; })) out.push({ key: k, title: k.replace(/_/g, " "), meta: Object.keys(v).length + " fields" }); });
    var c = contracts[feed]; if (c && Array.isArray(c.rows_path) && c.rows_path.length) { var rp = c.rows_path.join("."); if (!out.some(function (p) { return p.key === rp; })) out.unshift({ key: rp, title: rp.replace(/_/g, " ") + " (contract rows)", meta: "learned rows path" }); }
    return out.slice(0, 40);
  }
  function renderPalette() {
    var box = $("hd-results"), q = $("hd-input").value;
    palette.q = q; palette.items = q.trim() ? paletteItems(q) : [];
    if (!palette.items.length) { box.className = "hd-results" + (q.trim() ? " on" : ""); box.innerHTML = q.trim() ? '<div class="empty">nothing matches — try a page name (bonds), a section (bonds#1.4), an engine (bond-warroom), or words (jgb, auction, liquidity)</div>' : ""; return; }
    box.className = "hd-results on";
    var grp = ""; var html = "";
    palette.items.forEach(function (it, i) {
      if (it.grp !== grp) { grp = it.grp; html += '<div class="grp">' + esc(grp) + "</div>"; }
      html += '<div class="it' + (i === palette.sel ? " sel" : "") + '" data-i="' + i + '" style="' + (it.sub ? "padding-left:26px" : "") + '"><span class="ref">' + esc(it.ref) + '</span><span class="ti">' + esc(it.title || "") + '</span><span class="me">' + esc(it.meta || "") + "</span>" + (it.exp ? '<span class="exp" data-disc="' + esc(it.page) + '">index sections</span>' : "") + "</div>";
    });
    box.innerHTML = html;
  }
  function choose(i) {
    var it = palette.items[i]; if (!it || !it.spec) return;
    if (it.exp && !sectionsOf(it.page)) { $("hd-input").value = it.page + "#"; toast("indexing " + it.page + " sections…"); discover(it.page).then(function () { renderPalette(); }); renderPalette(); return; }
    addBlock(it.spec); $("hd-input").value = ""; renderPalette();
  }

  // ------------------------------------------------------------------ feeds + renderers
  async function loadFeed(feed) {
    var c = feedCache.get(feed);
    if (c && (c.status === "ready" && Date.now() - c.at < 120000 || c.status === "loading")) return c.p || c;
    var p = getJson(["/" + feed, PROXY + "/" + feed]).then(function (data) { var r = { status: "ready", data: data, at: Date.now() }; feedCache.set(feed, r); return r; }, function (e) { var r = { status: "error", error: e.message, at: Date.now() }; feedCache.set(feed, r); return r; });
    feedCache.set(feed, { status: "loading", p: p }); return p;
  }
  function freshness(data, feed) {
    var iso = data && (data.generated_at || data.updated_at || data.timestamp || data.as_of || data.asof || data.date || (data.meta && (data.meta.generated_at || data.meta.updated_at)));
    var t = iso ? Date.parse(iso) : NaN, c = contracts[feed], maxH = c && c.max_age_hours ? Number(c.max_age_hours) : 36;
    if (isNaN(t)) return { cls: "unknown", label: "no stamp", iso: null };
    var ageH = (Date.now() - t) / 36e5;
    return { cls: ageH > maxH ? "stale" : "fresh", label: (ageH > maxH ? "STALE " : "LIVE ") + ago(iso), iso: iso };
  }
  function isWarroomRows(rows) { if (!Array.isArray(rows) || !rows.length || !isObj(rows[0])) return false; var n = 0; rows.forEach(function (r) { if (typeof (r.last != null ? r.last : r.level != null ? r.level : r.value) === "number" && (r.dod != null || r.d5 != null || r.d20 != null || r.flag != null || r.z != null || r.chg != null || r.change != null)) n++; }); return n >= Math.max(1, rows.length * 0.6); }
  function fmtLevel(r) { var v = r.last != null ? r.last : r.level != null ? r.level : r.value; if (typeof v !== "number") return esc(v == null ? "—" : v); if (r.kind === "price") return v.toFixed(2); if (r.kind === "index") return v.toFixed(v >= 100 ? 1 : 2); if (r.unit === "bp" && r.kind === "spread") return (v * 100).toFixed(0) + "bp"; if (r.kind === "yield" || r.unit === "%" || r.unit === "pct") return v.toFixed(3) + "%"; return Math.abs(v) >= 1000 ? v.toLocaleString(undefined, { maximumFractionDigits: 0 }) : v.toFixed(Math.abs(v) >= 100 ? 1 : 2); }
  function fmtChg(v, unit) { if (v == null || typeof v !== "number") return "—"; var s = (v > 0 ? "+" : ""); return unit === "bp" ? s + v.toFixed(1) + "bp" : unit === "pct" || unit === "%" ? s + v.toFixed(2) + "%" : s + (Math.abs(v) >= 100 ? v.toFixed(0) : v.toFixed(Math.abs(v) >= 10 ? 1 : 2)); }
  function cls(v, kind) { return v == null || typeof v !== "number" || Math.abs(v) < 1e-9 ? "flat" : kind === "price" || kind === "level" ? (v > 0 ? "pos" : "neg") : (v > 0 ? "neg" : "pos"); }
  function spark(pts, flag) { pts = Array.isArray(pts) ? pts.filter(function (x) { return typeof x === "number"; }) : []; if (pts.length < 2) return ""; var min = Math.min.apply(null, pts), max = Math.max.apply(null, pts), W = 88, H = 22; var x = function (i) { return 2 + i * (W - 4) / (pts.length - 1); }, y = function (v) { return H - 3 - (H - 6) * (v - min) / Math.max(max - min, 1e-9); }; var d = pts.map(function (v, i) { return (i ? "L" : "M") + x(i).toFixed(1) + "," + y(v).toFixed(1); }).join(" "); var col = flag === "RED" ? "#E07A6A" : flag === "AMBER" ? "#F0B429" : "#c9942e"; return '<svg class="hd-spark" viewBox="0 0 ' + W + " " + H + '" preserveAspectRatio="none"><path d="' + d + '" fill="none" stroke="' + col + '" stroke-width="1.4"/><circle cx="' + x(pts.length - 1).toFixed(1) + '" cy="' + y(pts[pts.length - 1]).toFixed(1) + '" r="1.8" fill="' + col + '"/></svg>'; }
  function flagPill(v) { var s = String(v == null ? "" : v).toUpperCase(); return s ? '<span class="hd-flag ' + esc(s.replace(/[^A-Z]/g, "")) + '">' + esc(s) + "</span>" : ""; }
  function warroomTable(rows, limit) {
    var body = rows.slice(0, limit).map(function (r) {
      var kind = r.kind || "level", unit = r.unit || "", dod = r.dod != null ? r.dod : r.chg != null ? r.chg : r.change, d5 = r.d5, d20 = r.d20;
      var zc = r.z != null ? "<small>z " + (r.z > 0 ? "+" : "") + Number(r.z).toFixed(1) + (kind === "yield" && r.dod_pct != null ? " · " + (r.dod_pct > 0 ? "+" : "") + Number(r.dod_pct).toFixed(1) + "% of level" : "") + "</small>" : (r.history_days != null && r.history_days < 41 ? "<small>z after " + (41 - r.history_days) + " more days</small>" : "");
      var flag = String(r.flag || "").toUpperCase();
      return '<tr class="' + esc(flag) + '" title="' + esc(r.source || "") + (r.asof ? " · as of " + esc(r.asof) : "") + '"><td class="lab">' + esc(r.label || r.name || r.ticker || r.id || "") + "<small>" + esc(r.asof || r.date || "") + (r.pct1y != null ? " · p" + r.pct1y : "") + "</small></td><td>" + fmtLevel(r) + '</td><td class="' + cls(dod, kind) + '"><b>' + fmtChg(dod, unit) + "</b>" + zc + '</td><td class="' + cls(d5, kind) + '">' + fmtChg(d5, unit) + '</td><td class="' + cls(d20, kind) + '">' + fmtChg(d20, unit) + "</td><td>" + spark(r.spark, flag) + "</td><td>" + flagPill(flag) + "</td></tr>";
    }).join("");
    return '<div class="tbl"><table><thead><tr><th>Series</th><th>Level</th><th>DoD</th><th>5d</th><th>20d</th><th>30d</th><th>Flag</th></tr></thead><tbody>' + body + "</tbody></table></div>";
  }
  var LABEL_KEYS = ["label", "name", "ticker", "symbol", "series", "title", "id", "country", "sector", "asset", "term", "date", "key"];
  var NUM_PRI = /^(score|composite|z|zscore|z_score|last|level|value|close|price|dod|chg|change|d1|d5|d20|pct|percentile|pct1y|rank|weight|prob|probability|yield|spread|oas|flow|net|delta|ret|return)/i;
  function genericTable(rows, limit) {
    var keys = {}; rows.slice(0, 60).forEach(function (r) { if (isObj(r)) Object.keys(r).forEach(function (k) { var v = r[k]; if (v == null || isObj(v) || Array.isArray(v)) return; var t = keys[k] || (keys[k] = { n: 0, num: 0, str: 0, flag: 0 }); t.n++; if (typeof v === "number") t.num++; else if (typeof v === "string") { t.str++; if (FLAGS.test(v)) t.flag++; } }); });
    var names = Object.keys(keys);
    var label = LABEL_KEYS.filter(function (k) { return keys[k] && keys[k].str; })[0] || names.filter(function (k) { return keys[k].str >= keys[k].n * 0.8 && !keys[k].flag; })[0];
    var flag = names.filter(function (k) { return keys[k].flag >= keys[k].n * 0.8; })[0];
    var nums = names.filter(function (k) { return k !== label && keys[k].num >= keys[k].n * 0.8; }).sort(function (a, b) { return Number(NUM_PRI.test(b)) - Number(NUM_PRI.test(a)); }).slice(0, 5);
    var extras = names.filter(function (k) { return k !== label && k !== flag && nums.indexOf(k) < 0 && keys[k].str && !/^(source|url|href|note|notes|desc|description|text)$/i.test(k); }).slice(0, Math.max(0, 6 - nums.length));
    var cols = [].concat(nums, extras);
    var fmtV = function (v, k) { if (typeof v === "number") { var s = Math.abs(v) >= 1000 ? v.toLocaleString(undefined, { maximumFractionDigits: 0 }) : Math.abs(v) >= 100 ? v.toFixed(1) : v.toFixed(2); return /pct|percent|ret|return|change|chg|yoy|mom|growth/i.test(k) && Math.abs(v) < 500 ? s + "%" : s; } return esc(String(v)).slice(0, 60); };
    var body = rows.slice(0, limit).map(function (r) {
      var fl = flag ? String(r[flag] || "").toUpperCase() : "";
      return '<tr class="' + esc(fl.replace(/[^A-Z]/g, "")) + '"><td class="lab">' + esc(label ? r[label] : "") + "</td>" + cols.map(function (k) { var v = r[k]; var c = typeof v === "number" && /change|chg|dod|d1|d5|d20|delta|ret|return|z|flow|net/i.test(k) ? cls(v, "price") : ""; return '<td class="' + c + '">' + fmtV(v, k) + "</td>"; }).join("") + (flag ? "<td>" + flagPill(fl) + "</td>" : "") + "</tr>";
    }).join("");
    return '<div class="tbl"><table><thead><tr><th>' + esc(label || "") + "</th>" + cols.map(function (k) { return "<th>" + esc(k.replace(/_/g, " ")) + "</th>"; }).join("") + (flag ? "<th>Flag</th>" : "") + "</tr></thead><tbody>" + body + "</tbody></table></div>";
  }
  function kpiGrid(obj) {
    var items = [];
    Object.keys(obj).forEach(function (k) { var v = obj[k]; if (v == null || Array.isArray(v)) return; if (isObj(v)) { Object.keys(v).slice(0, 8).forEach(function (kk) { var vv = v[kk]; if (vv == null || isObj(vv) || Array.isArray(vv)) return; items.push([k + "." + kk, vv]); }); } else items.push([k, v]); });
    items = items.filter(function (it) { return !/generated|updated|timestamp|version|schema|_at$|source|url/i.test(it[0]); }).slice(0, 24);
    return '<div class="hd-kpis">' + items.map(function (it) { var v = it[1], num = typeof v === "number", flag = typeof v === "string" && FLAGS.test(v); var c = num ? (v > 0 && /change|chg|dod|d1|d5|d20|z|flow|net|delta/i.test(it[0]) ? "pos" : v < 0 && /change|chg|dod|d1|d5|d20|z|flow|net|delta/i.test(it[0]) ? "neg" : "") : "txt"; return '<div class="hd-kpi"><div class="k" title="' + esc(it[0]) + '">' + esc(it[0].replace(/_/g, " ")) + '</div><div class="v ' + c + '">' + (flag ? flagPill(v) : num ? esc(Math.abs(v) >= 1000 ? v.toLocaleString(undefined, { maximumFractionDigits: 0 }) : Math.abs(v) >= 100 ? v.toFixed(1) : v.toFixed(2)) : esc(String(v)).slice(0, 140)) + "</div></div>"; }).join("") + "</div>";
  }
  function resolvePanel(data, b) {
    if (b.panel) { if (isObj(data) && isObj(data.panels) && data.panels[b.panel] != null) return data.panels[b.panel]; var v = get(data, b.panel); if (v !== undefined) return v; return null; }
    var c = contracts[b.feed]; if (c && Array.isArray(c.rows_path) && c.rows_path.length) { var rows = get(data, c.rows_path.join(".")); if (Array.isArray(rows) && rows.length) return rows; }
    if (isObj(data) && isObj(data.panels)) { var first = Object.keys(data.panels)[0]; if (first) return data.panels[first]; }
    if (isObj(data)) { var arr = Object.keys(data).map(function (k) { return data[k]; }).filter(function (v) { return Array.isArray(v) && v.length && isObj(v[0]); }).sort(function (a, z) { return z.length - a.length; })[0]; if (arr) return arr; }
    return data;
  }
  function renderEngine(node, b) {
    var body = node.querySelector(".body"), e = engineByShort.get(b.ref);
    if (!b.feed) b.feed = (e && e.feeds[0]) || "data/" + b.ref + ".json";   // fleet convention: data/<engine>.json
    var c = feedCache.get(b.feed);
    if (!c || c.status === "loading") { body.innerHTML = '<div class="hd-skel"></div>'; loadFeed(b.feed).then(function () { if (document.contains(node)) renderEngine(node, b); }); return; }
    if (c.status === "error") { body.innerHTML = '<div class="msg">feed <b>' + esc(b.feed) + "</b> unavailable: " + esc(c.error) + '</div>'; return; }
    var data = c.data, v = resolvePanel(data, b), fr = freshness(data, b.feed), red = 0, amber = 0, count = "";
    if (v == null) { body.innerHTML = '<div class="msg">no panel <b>' + esc(b.panel) + "</b> in this feed. Panels here: " + panelsOf(data, b.feed).map(function (p) { return '<button class="fix" data-panel="' + esc(p.key) + '">' + esc(p.key) + "</button>"; }).join("") + "</div>"; return; }
    if (Array.isArray(v) && v.length && isObj(v[0])) {
      v.forEach(function (r) { var f = String(r.flag || r.status || r.signal || "").toUpperCase(); if (f === "RED") red++; else if (f === "AMBER") amber++; });
      var lim = b.expand ? v.length : 40;
      body.innerHTML = (isWarroomRows(v) ? warroomTable(v, lim) : genericTable(v, lim)) + (v.length > lim ? '<button class="hd-more">show all ' + v.length + " rows</button>" : "");
      count = v.length + " rows";
    } else if (isObj(v)) { body.innerHTML = kpiGrid(v); count = Object.keys(v).length + " fields"; Object.keys(v).forEach(function (k) { var s = String(v[k]).toUpperCase(); if (s === "RED") red++; else if (s === "AMBER") amber++; }); }
    else body.innerHTML = '<div class="msg">' + esc(String(v)).slice(0, 400) + "</div>";
    node.querySelector(".cnt").innerHTML = esc(count) + (red ? ' · <b class="red">' + red + " RED</b>" : "") + (amber ? ' · <b class="amber">' + amber + " AMBER</b>" : "");
    var fb = node.querySelector(".fresh"); fb.className = "fresh " + fr.cls; fb.textContent = fr.label; fb.title = fr.iso || "";
    node.querySelector(".foot").innerHTML = "<span>" + esc(b.feed) + "</span><span>" + (e ? '<a href="/engine.html?e=' + esc(e.name) + '">engine ↗</a> · ' : "") + '<a href="/' + esc(b.feed) + '" target="_blank" rel="noopener">json ↗</a></span>';
    embedMeta.set(b.id, { red: red, amber: amber, stale: fr.cls === "stale", title: b.title, rows: Array.isArray(v) ? v.length : 0 });
    heartbeat();
  }
  function renderSection(node, b) {
    var body = node.querySelector(".body"); body.innerHTML = '<div class="hd-skel" style="height:120px"></div>';
    var f = document.createElement("iframe"); f.loading = "lazy"; f.title = b.title || blockRef(b); f.dataset.bid = b.id;
    f.src = pageHref(b.page) + (b.type === "section" ? "?embed=" + encodeURIComponent(b.sec) : "?jhframe=1");
    f.addEventListener("load", function () { var sk = body.querySelector(".hd-skel"); if (sk) sk.remove(); });
    body.appendChild(f);
    node.querySelector(".foot").innerHTML = '<span>' + esc(pageHref(b.page)) + (b.type === "section" ? " §" + esc(b.sec) : "") + '</span><span><a href="' + esc(pageHref(b.page)) + (b.key ? "#" + esc(findSection(b.page, b.sec) && findSection(b.page, b.sec).id || "") : "") + '">open page ↗</a></span>';
    var fb = node.querySelector(".fresh"); fb.className = "fresh unknown"; fb.textContent = "live page";
    setTimeout(function () { var sk = body.querySelector(".hd-skel"); if (sk) sk.remove(); }, 9000);
  }
  window.addEventListener("message", function (ev) {
    if (ev.origin !== location.origin || !ev.data || !ev.data.jh) return;
    var f = Array.from(document.querySelectorAll(".hd-block iframe")).find(function (x) { return x.contentWindow === ev.source; }); if (!f) return;
    var node = f.closest(".hd-block"), b = state.blocks.find(function (x) { return x.id === f.dataset.bid; }); if (!node || !b) return;
    if (ev.data.jh === "embed-height") {
      f.style.height = Math.min(Math.max(ev.data.h, 60), 1600) + "px"; var sk = node.querySelector(".hd-skel"); if (sk) sk.remove();
      var red = ev.data.red || 0, amber = ev.data.amber || 0;
      node.querySelector(".cnt").innerHTML = (red ? '<b class="red">' + red + " RED</b>" : "") + (red && amber ? " · " : "") + (amber ? '<b class="amber">' + amber + " AMBER</b>" : "");
      if (ev.data.title && !b.titleLocked) { b.title = ev.data.title; node.querySelector(".t a").textContent = b.title; }
      embedMeta.set(b.id, { red: red, amber: amber, stale: false, title: b.title }); heartbeat();
      var fb = node.querySelector(".fresh"); fb.className = "fresh fresh"; fb.textContent = "LIVE";
    } else if (ev.data.jh === "embed-missing") {
      node.querySelector(".body").innerHTML = '<div class="msg">' + esc(b.page) + " has no section <b>§" + esc(b.sec) + "</b> right now. It has: " + (ev.data.have || []).map(function (n) { return '<button class="fix" data-sec="' + esc(n) + '">§' + esc(n) + "</button>"; }).join("") + "</div>";
    } else if (ev.data.jh === "embed-ready" && ev.data.title && !b.title) { b.title = ev.data.title; node.querySelector(".t a").textContent = b.title; }
  });

  // ------------------------------------------------------------------ layout render
  function blockNode(b, i) {
    var n = document.createElement("section");
    n.className = "hd-block w" + b.w + (b.type === "page" ? " page" : ""); n.dataset.bid = b.id; n.dataset.jhSec = String(i + 1); n.dataset.jhKey = blockRef(b); n.dataset.jhTitle = b.title || blockRef(b); n.draggable = true;
    var href = b.type === "engine" ? "/engine.html?e=" + encodeURIComponent((engineByShort.get(b.ref) || { name: "justhodl-" + b.ref }).name) : pageHref(b.page);
    n.innerHTML = '<div class="ph"><span class="t"><a href="' + esc(href) + '">' + esc(b.title || blockRef(b)) + '</a><span class="ref">' + esc(blockRef(b)) + '</span></span><span class="cnt"></span><span class="fresh unknown">…</span><span class="tools"><button title="narrower" data-act="narrow">−</button><button title="wider" data-act="wide">+</button><button title="move up" data-act="up">↑</button><button title="move down" data-act="down">↓</button><button title="rename" data-act="rename">✎</button><button class="x" title="remove" data-act="remove">×</button></span></div><div class="body"></div><div class="foot"></div>';
    return n;
  }
  function render() {
    var grid = $("hd-grid"); grid.innerHTML = "";
    if (!state.blocks.length) { grid.innerHTML = '<div class="hd-block w3"><div class="msg">Your desk is empty. Type an address above — <b>bonds#1</b>, <b>bond-warroom/japan</b>, <b>auctions</b> — or search words like <b>jgb</b>, <b>auction</b>, <b>liquidity</b>.</div></div>'; heartbeat(); return; }
    state.blocks.forEach(function (b, i) { if (!b.title) addTitle(b); var n = blockNode(b, i); grid.appendChild(n); if (b.type === "engine") renderEngine(n, b); else renderSection(n, b); });
    if (window.JustHodlSections) setTimeout(function () { window.JustHodlSections.rerun(); }, 50);
    heartbeat();
  }
  function heartbeat() {
    var red = 0, amber = 0, stale = 0, names = [], rows = 0;
    state.blocks.forEach(function (b) { var m = embedMeta.get(b.id); if (!m) return; red += m.red; amber += m.amber; if (m.stale) stale++; rows += m.rows || 0; if (m.red) names.push(m.title || blockRef(b)); });
    var score = Math.min(100, Math.round(100 * (3 * red + amber) / Math.max(12, rows / 3 + 6)));
    var regime = score >= 70 ? "ACUTE" : score >= 45 ? "ELEVATED" : score >= 22 ? "WATCH" : "CALM";
    $("hd-score").textContent = String(score); $("hd-arc").setAttribute("stroke-dasharray", (314 * score / 100).toFixed(1) + " 314");
    $("hd-arc").setAttribute("stroke", score >= 70 ? "#E07A6A" : score >= 45 ? "#F0B429" : score >= 22 ? "#c9942e" : "#6fce8a");
    var r = $("hd-regime"); r.textContent = regime; r.className = "hd-regime " + regime;
    $("hd-headline").textContent = !state.blocks.length ? "Add the sections and engines you read first." : red || amber ? (red ? red + " RED" : "") + (red && amber ? " · " : "") + (amber ? amber + " AMBER" : "") + " across your desk" + (names.length ? " — " + names.slice(0, 3).join(", ") + (names.length > 3 ? " +" + (names.length - 3) : "") : "") : "Calm across every series on your desk.";
    $("hd-v1").querySelector(".s").textContent = red + " RED · " + amber + " AMBER"; $("hd-v1").querySelector(".t").textContent = rows ? rows.toLocaleString() + " engine rows scored, plus every embedded page section's own flags" : "Flags aggregate as blocks load";
    $("hd-v2").querySelector(".s").textContent = state.blocks.length + " blocks · " + stale + " stale"; $("hd-v2").querySelector(".t").textContent = stale ? "Stale = older than its engine's learned cadence (engine-contracts)" : "Every engine block is inside its learned cadence";
    $("hd-kick").textContent = "desk · " + state.blocks.length + " blocks · " + (cloudUser ? "account layout" : "device layout") + " · " + new Date().toLocaleTimeString("en-US", { hour12: false, timeZone: "America/New_York" }) + " ET";
  }

  // ------------------------------------------------------------------ interactions
  function idx(id) { return state.blocks.findIndex(function (b) { return b.id === id; }); }
  document.addEventListener("click", function (ev) {
    var btn = ev.target.closest("[data-act]");
    if (btn) {
      var node = btn.closest(".hd-block"), i = idx(node.dataset.bid), b = state.blocks[i]; if (i < 0) return;
      var act = btn.dataset.act;
      if (act === "remove") { state.blocks.splice(i, 1); embedMeta.delete(b.id); toast("removed " + blockRef(b)); }
      else if (act === "up" && i > 0) { state.blocks.splice(i - 1, 0, state.blocks.splice(i, 1)[0]); }
      else if (act === "down" && i < state.blocks.length - 1) { state.blocks.splice(i + 1, 0, state.blocks.splice(i, 1)[0]); }
      else if (act === "wide") b.w = Math.min(3, b.w + 1);
      else if (act === "narrow") b.w = Math.max(1, b.w - 1);
      else if (act === "rename") { var t = prompt("Block title", b.title || blockRef(b)); if (t == null) return; b.title = t.slice(0, 120); b.titleLocked = true; }
      scheduleSave(); render(); return;
    }
    var fix = ev.target.closest(".fix");
    if (fix) { var nd = fix.closest(".hd-block"), bi = idx(nd.dataset.bid); if (bi < 0) return; if (fix.dataset.sec) { state.blocks[bi].sec = fix.dataset.sec; state.blocks[bi].title = ""; } if (fix.dataset.panel) { state.blocks[bi].panel = fix.dataset.panel; state.blocks[bi].title = ""; } addTitle(state.blocks[bi]); scheduleSave(); render(); return; }
    var more = ev.target.closest(".hd-more");
    if (more) { var mn = more.closest(".hd-block"), mi = idx(mn.dataset.bid); if (mi >= 0) { state.blocks[mi].expand = true; renderEngine(mn, state.blocks[mi]); } return; }
    var it = ev.target.closest(".hd-results .it");
    if (it) { var d = ev.target.closest("[data-disc]"); if (d) { toast("indexing " + d.dataset.disc + " sections…"); discover(d.dataset.disc).then(renderPalette); return; } choose(Number(it.dataset.i)); return; }
    var addp = ev.target.closest("[data-add]");
    if (addp) { addBlock({ type: "page", page: addp.dataset.add }); return; }
    if (!ev.target.closest(".hd-palette")) $("hd-results").classList.remove("on");
  });
  function addTitle(b) { if (b.type === "engine") { var e = engineByShort.get(b.ref); b.title = (e ? e.short : b.ref) + (b.panel ? " · " + b.panel : ""); } else { var p = pageByKey.get(b.page), s = b.type === "section" ? findSection(b.page, b.sec) : null; b.title = s ? s.title : (p ? p.title : b.page) + (b.type === "section" ? " §" + b.sec : ""); if (s) b.key = s.key; } }
  var input = $("hd-input");
  input.addEventListener("input", function () { palette.sel = -1; renderPalette(); });
  input.addEventListener("focus", function () { if (input.value.trim()) renderPalette(); });
  input.addEventListener("keydown", function (ev) {
    if (ev.key === "ArrowDown" || ev.key === "ArrowUp") { ev.preventDefault(); if (!palette.items.length) return; palette.sel = (palette.sel + (ev.key === "ArrowDown" ? 1 : -1) + palette.items.length) % palette.items.length; renderPalette(); var s = $("hd-results").querySelector(".it.sel"); if (s) s.scrollIntoView({ block: "nearest" }); }
    else if (ev.key === "Escape") { $("hd-results").classList.remove("on"); input.blur(); }
  });
  $("hd-form").addEventListener("submit", function (ev) {
    ev.preventDefault();
    if (palette.sel >= 0) { choose(palette.sel); return; }
    var spec = parseAddress(input.value);
    if (spec) { if (spec.type === "section" && !sectionsOf(spec.page)) discover(spec.page).then(function () { var b = state.blocks[state.blocks.length - 1]; if (b && b.type === "section" && b.page === spec.page) { addTitle(b); scheduleSave(); render(); } }); addBlock(spec); input.value = ""; renderPalette(); return; }
    if (palette.items.length && palette.items[0].spec) { choose(0); return; }
    toast("no match for " + input.value.trim());
  });
  document.addEventListener("keydown", function (ev) { if (ev.key === "/" && !/input|textarea/i.test(document.activeElement.tagName)) { ev.preventDefault(); input.focus(); } });
  // drag reorder
  var dragId = null;
  document.addEventListener("dragstart", function (ev) { var n = ev.target.closest && ev.target.closest(".hd-block"); if (!n || !n.dataset.bid) return; dragId = n.dataset.bid; n.classList.add("drag"); try { ev.dataTransfer.setData("text/plain", dragId); ev.dataTransfer.effectAllowed = "move"; } catch (e) { } });
  document.addEventListener("dragover", function (ev) { var n = ev.target.closest && ev.target.closest(".hd-block"); if (!n || !dragId || n.dataset.bid === dragId) return; ev.preventDefault(); document.querySelectorAll(".hd-block.over").forEach(function (x) { if (x !== n) x.classList.remove("over"); }); n.classList.add("over"); });
  document.addEventListener("dragleave", function (ev) { var n = ev.target.closest && ev.target.closest(".hd-block"); if (n) n.classList.remove("over"); });
  document.addEventListener("drop", function (ev) { var n = ev.target.closest && ev.target.closest(".hd-block"); if (!n || !dragId) return; ev.preventDefault(); var from = idx(dragId), to = idx(n.dataset.bid); if (from < 0 || to < 0) return; state.blocks.splice(to, 0, state.blocks.splice(from, 1)[0]); dragId = null; scheduleSave(); render(); });
  document.addEventListener("dragend", function () { dragId = null; document.querySelectorAll(".hd-block.drag,.hd-block.over").forEach(function (x) { x.classList.remove("drag", "over"); }); });
  $("hd-reset").addEventListener("click", function () { if (!confirm("Reset the desk to the default layout?")) return; getJson(["/config/home-layout.json"]).catch(function () { return null; }).then(function (d) { state = fromDefaults(d); state.revision = cloudRevision; embedMeta.clear(); scheduleSave(); render(); toast("desk reset"); }); });
  $("hd-export").addEventListener("click", function () { var txt = JSON.stringify({ schema: SCHEMA, blocks: state.blocks.map(function (b) { var o = Object.assign({}, b); delete o.id; delete o.expand; return o; }) }, null, 1); try { navigator.clipboard.writeText(txt); toast("layout JSON copied — paste it to me to make it the site default"); } catch (e) { prompt("layout JSON", txt); } });
  $("hd-refresh").addEventListener("click", function () { feedCache.clear(); embedMeta.clear(); render(); toast("refreshing every block"); });
  setInterval(function () { feedCache.clear(); state.blocks.forEach(function (b) { if (b.type !== "engine") return; var n = document.querySelector('[data-bid="' + b.id + '"]'); if (n) renderEngine(n, b); }); }, 5 * 60000);

  // ------------------------------------------------------------------ directory (every page, nothing hidden)
  function renderDirectory() {
    var box = $("hd-dir-cats"), cats = {};
    pages.forEach(function (p) { (cats[p.cat] = cats[p.cat] || []).push(p); });
    box.innerHTML = Object.keys(cats).sort().map(function (c) {
      return "<details><summary>" + esc(c) + '<span class="c">' + cats[c].length + "</span></summary><ul>" + cats[c].sort(function (a, b) { return a.title.localeCompare(b.title); }).map(function (p) { var s = sectionsOf(p.key); return '<li><span class="n">' + (s ? "§" + s.length : "") + '</span><a href="' + esc(p.href) + '">' + esc(p.title) + '</a><button data-add="' + esc(p.key) + '" title="add the whole page to the desk">+ add</button></li>'; }).join("") + "</ul></details>";
    }).join("");
    $("hd-dir-count").textContent = pages.length + " pages";
  }

  // ------------------------------------------------------------------ boot
  async function boot() {
    var defaults = null;
    try { defaults = await loadCatalogs(); } catch (e) { $("hd-catalog").textContent = "catalog unavailable: " + e.message; }
    state = loadLocal() || fromDefaults(defaults);
    render(); renderDirectory();
    setStatus("saved on this device", "");
    var q = new URLSearchParams(location.search).get("add"); if (q) { var spec = parseAddress(q); if (spec) addBlock(spec); history.replaceState(null, "", location.pathname); }
    if (window.JustHodlAuth && window.JustHodlAuth.onChange) {
      window.JustHodlAuth.onChange(function (user) { cloudUser = user || null; if (user) cloudLoad(user); else setStatus("saved on this device · sign in to sync across devices", ""); heartbeat(); });
      var u = window.JustHodlAuth.getUser && window.JustHodlAuth.getUser(); if (u) { cloudUser = u; cloudLoad(u); }
    }
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
  window.JustHodlHome = { add: function (a) { var s = parseAddress(a); return s ? addBlock(s) : null; }, state: function () { return state; }, discover: discover, render: render };
})();
