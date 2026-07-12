/* JustHodl TV tap — runs in the PAGE's JS world (manifest world:"MAIN").
 *
 * Why this exists: guessing TradingView's internal endpoints failed (v1.1
 * queried /api/v2/lists/ and /api/v1/text_notes/ — dead paths → 0 lists →
 * 0 symbols → 0 notes). The page itself knows the truth. We wrap fetch and
 * XHR, and every TradingView response that looks like watchlists or notes
 * gets forwarded to the content script. Nothing is sent anywhere else —
 * this only listens to traffic the page was already making.
 *
 * It also REPLAYS the endpoints observed in Khalid's own network trace:
 *   /api/v1/symbols_list/custom/?source=web   (all watchlists + symbols)
 *   /api/v1/symbols_list/colored/?source=web  (flagged symbols)
 *   note-manager .../notes/getall/            (note store)
 * Replays run in page context, so session cookies attach exactly as they
 * do for TradingView's own app code.
 */
(function () {
  if (window.__JH_TAP) return;
  window.__JH_TAP = 1;

  var INTEREST = /symbols_list|watchlist|note|getall|text_note|custom\/|colored|drawing/i;
  var SKIP = /google|doubleclick|analytics|collect|sentry|report|savesettings|\.png|\.jpg|\.css/i;

  function post(url, data, kind) {
    try {
      window.postMessage({ __jh: kind || "tap", url: String(url), data: data }, "*");
    } catch (e) {}
  }

  /* ── fetch tap ──────────────────────────────────────────────────── */
  var origFetch = window.fetch;
  window.fetch = function (input, init) {
    var p = origFetch.apply(this, arguments);
    try {
      var u = typeof input === "string" ? input : (input && input.url) || "";
      if (u && INTEREST.test(u) && !SKIP.test(u)) {
        p.then(function (r) {
          try {
            r.clone().json().then(function (j) { post(u, j); }).catch(function () {});
          } catch (e) {}
        }).catch(function () {});
      }
    } catch (e) {}
    return p;
  };

  /* ── XHR tap ────────────────────────────────────────────────────── */
  var oOpen = XMLHttpRequest.prototype.open;
  var oSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function (m, u) {
    try { this.__jhU = u; } catch (e) {}
    return oOpen.apply(this, arguments);
  };
  XMLHttpRequest.prototype.send = function () {
    try {
      var u = this.__jhU || "";
      if (u && INTEREST.test(u) && !SKIP.test(u)) {
        this.addEventListener("load", function () {
          try { post(u, JSON.parse(this.responseText)); } catch (e) {}
        });
      }
    } catch (e) {}
    return oSend.apply(this, arguments);
  };

  /* ── replay on command from the content script ──────────────────── */
  window.addEventListener("message", function (e) {
    var d = e && e.data;
    if (!d || d.__jh !== "replay" || !d.url) return;
    try {
      origFetch(d.url, {
        credentials: "include",
        headers: { "Accept": "application/json" }
      }).then(function (r) {
        return r.json().then(function (j) { post(d.url, j, "tap"); })
          .catch(function () { post(d.url, { __http: r.status }, "tap-err"); });
      }).catch(function (err) {
        post(d.url, { __err: String(err).slice(0, 120) }, "tap-err");
      });
    } catch (err) {
      post(d.url, { __err: String(err).slice(0, 120) }, "tap-err");
    }
  });

  window.postMessage({ __jh: "tap-ready" }, "*");
})();
