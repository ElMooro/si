/* jh-reskin-skip */
/* jh-account-sync — saves chart.html state to the signed-in JustHodl account (2026-10-06).
 * Sign-in: the site's Supabase login (/auth-config.js + supabase-js + /auth.js), the same session every page uses.
 * Storage: data proxy /userstore/* (per-account KV, account id taken only from the verified bearer token).
 *   chart-watchlist  — the whole watchlist document (lists, sections, colour tags, aliases, starred lists, columns)
 *   chart-files      — index of imported CSV/JSON files; chart-file?id= — each file's parsed rows
 * Without a session everything keeps working in this browser only (the previous behaviour).
 */
(function (root) {
  "use strict";
  if (root.JHAccountSync) return;
  var doc = root.document;
  var PROXY = (root.JUSTHODL_AUTH_CONFIG && root.JUSTHODL_AUTH_CONFIG.syncBase) || "https://justhodl-data-proxy.raafouis.workers.dev";
  var user = null, booted = null, listeners = [], status = { state: "local", at: 0, error: null };

  function load(src) {
    return new Promise(function (res, rej) {
      var s = doc.createElement("script"); s.src = src; s.async = false;
      s.onload = function () { res(); }; s.onerror = function () { rej(new Error("could not load " + src)); };
      (doc.head || doc.documentElement).appendChild(s);
    });
  }
  function fire() { listeners.forEach(function (cb) { try { cb(user, status); } catch (e) {} }); }
  function setStatus(state, err) { status = { state: state, at: Date.now(), error: err || null }; fire(); }

  function boot() {
    if (booted) return booted;
    booted = (async function () {
      // auth.js renders into [data-auth-slot]; give it a hidden one so it never floats over the chart toolbar
      if (!doc.querySelector("[data-auth-slot]")) {
        var h = doc.createElement("div"); h.setAttribute("data-auth-slot", ""); h.id = "jh-acct-slot"; h.style.display = "none";
        doc.body.appendChild(h);
      }
      if (!root.JUSTHODL_AUTH_CONFIG) await load("/auth-config.js");
      if (!root.supabase || !root.supabase.createClient) await load("https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2");
      if (!root.JustHodlAuth) await load("/auth.js");
      await root.JustHodlAuth.init();
      user = root.JustHodlAuth.getUser() || null;
      root.JustHodlAuth.onChange(function (u) {
        var before = user && user.id; user = u || null;
        if ((user && user.id) !== before) { setStatus(user ? "signed-in" : "local"); if (user) syncFiles(); }
      });
      setStatus(user ? "signed-in" : "local");
      if (user) syncFiles();
    })().catch(function (e) { setStatus("local", String(e && e.message || e)); });
    return booted;
  }
  function token() {
    if (!user || !root.JustHodlAuth || !root.JustHodlAuth.getAccessToken) return Promise.resolve(null);
    return root.JustHodlAuth.getAccessToken().catch(function () { return null; });
  }
  function call(method, kind, id, body) {
    return token().then(function (t) {
      if (!t) throw new Error("not signed in");
      var u = PROXY + "/userstore/" + kind + (id ? "?id=" + encodeURIComponent(id) : "");
      return root.fetch(u, { method: method, headers: { Authorization: "Bearer " + t, "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined, cache: "no-store" })
        .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { if (!r.ok) throw new Error((j && j.error) || "HTTP " + r.status); return j; }); });
    });
  }
  function get(kind, id) { return call("GET", kind, id); }
  function put(kind, doc_, updatedAt, id) {
    setStatus("saving");
    return call("PUT", kind, id, { doc: doc_, updated_at: updatedAt || Date.now() })
      .then(function (j) { setStatus("saved"); return j; }, function (e) { setStatus("error", e.message); throw e; });
  }

  // ------------------------------------------------------------------ imported files (jh-chart-import.js stores them in IndexedDB "jh-files")
  var IDX_KEY = "jh-files-index";
  function idb() {
    return new Promise(function (res, rej) {
      if (!root.indexedDB) { rej(new Error("no IndexedDB")); return; }
      var rq = root.indexedDB.open("jh-files", 1);
      rq.onupgradeneeded = function () { rq.result.createObjectStore("series", { keyPath: "id" }); };
      rq.onsuccess = function () { res(rq.result); }; rq.onerror = function () { rej(rq.error); };
    });
  }
  function idbGet(id) { return idb().then(function (db) { return new Promise(function (res) { var rq = db.transaction("series").objectStore("series").get(id); rq.onsuccess = function () { res(rq.result || null); }; rq.onerror = function () { res(null); }; }); }).catch(function () { return null; }); }
  function idbPut(rec) { return idb().then(function (db) { return new Promise(function (res) { var tx = db.transaction("series", "readwrite"); tx.objectStore("series").put(rec); tx.oncomplete = function () { res(true); }; tx.onerror = function () { res(false); }; }); }).catch(function () { return false; }); }
  function readIndex() { try { return JSON.parse(root.localStorage.getItem(IDX_KEY) || "{}") || {}; } catch (e) { return {}; } }
  function writeIndex(ix) { try { root.localStorage.setItem(IDX_KEY, JSON.stringify(ix)); } catch (e) {} }
  function syncedKey() { return "jh-files-synced:" + (user ? user.id : "-"); }
  function readSynced() { try { return JSON.parse(root.localStorage.getItem(syncedKey()) || "{}") || {}; } catch (e) { return {}; } }
  function writeSynced(o) { try { root.localStorage.setItem(syncedKey(), JSON.stringify(o)); } catch (e) {} }
  function publishNames(ix) {
    var out = {};
    Object.keys(ix).forEach(function (id) { out[id] = [ix[id].name + " · your file " + (ix[id].file || ""), "file"]; });
    root.JH_FILE_NAMES = out;
    try { root.dispatchEvent(new CustomEvent("jh-tvwl-changed")); } catch (e) {}
  }
  var filesBusy = false;
  function syncFiles() {
    if (!user || filesBusy) return Promise.resolve();
    filesBusy = true;
    var local = readIndex(), synced = readSynced();
    return get("chart-files").then(function (r) {
      var cloud = (r && r.doc) || {}, jobs = [], changed = false;
      // account → this browser: files saved from another device
      Object.keys(cloud).forEach(function (id) {
        if (local[id] && String(local[id].at) === String(cloud[id].at)) { synced[id] = cloud[id].at; return; }
        if (!local[id] && synced[id] !== undefined) return; // deleted here since the last sync; removed below
        jobs.push(get("chart-file", id).then(function (f) {
          if (f && f.doc && f.doc.id) return idbPut(f.doc).then(function (ok) { if (ok) { local[id] = cloud[id]; synced[id] = cloud[id].at; changed = true; } });
        }).catch(function () {}));
      });
      // this browser → account: new or re-imported files
      Object.keys(local).forEach(function (id) {
        if (cloud[id] && String(cloud[id].at) === String(local[id].at)) return;
        jobs.push(idbGet(id).then(function (rec) {
          if (!rec) return;
          return put("chart-file", rec, Date.now(), id).then(function () { cloud[id] = local[id]; synced[id] = local[id].at; });
        }).catch(function () {}));
      });
      // deletions made in this browser after the last sync
      Object.keys(synced).forEach(function (id) {
        if (!local[id] && cloud[id]) { delete cloud[id]; delete synced[id]; jobs.push(call("PUT", "chart-file", id, { delete: true }).catch(function () {})); }
      });
      return Promise.all(jobs).then(function () {
        if (changed) { writeIndex(local); publishNames(local); }
        writeSynced(synced);
        return put("chart-files", cloud, Date.now());
      });
    }).catch(function (e) { setStatus("error", e.message); }).then(function () { filesBusy = false; });
  }
  // files imported while signed in are pushed shortly after the import dialog stores them
  var lastIx = "";
  setInterval(function () {
    if (!user || doc.hidden) return;
    var s = root.localStorage.getItem(IDX_KEY) || "";
    if (s !== lastIx) { lastIx = s; syncFiles(); }
  }, 15000);

  root.JHAccountSync = {
    boot: boot,
    user: function () { return user; },
    status: function () { return status; },
    onChange: function (cb) { listeners.push(cb); },
    get: get, put: put, syncFiles: syncFiles,
    signIn: function () { boot().then(function () { if (root.JustHodlAuth) root.JustHodlAuth.openSignIn(); }); },
    signOut: function () { if (root.JustHodlAuth) root.JustHodlAuth.signOut(); }
  };
  if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", boot); else boot();
})(window);
