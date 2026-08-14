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

  var INTEREST = /symbols_list|watchlist|note|getall|text_note|custom\/|colored|drawing|symbol_search|scanner|snapshot|metainfo|symbol-info|quotes?\//i;
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

  /* ── WebSocket tap (v1.9.0) ─────────────────────────────────────────
   * Khalid: "TradingView has all ICE data since inception — I can see it
   * in my account." He's right, and the fetch/XHR taps above could never
   * capture it: TV streams CHART BARS over wss://data.tradingview.com,
   * not HTTP. That is exactly why the vault only ever held last-values
   * resolved from FRED/Yahoo, never TV's own history.
   *
   * This wraps WebSocket and mines `timescale_update` / `du` frames,
   * which carry {i, v:[unix_ts, open, high, low, close, volume]} arrays.
   * Only bar payloads are forwarded; nothing is injected or requested —
   * we read frames the page already receives when YOU open a chart.
   */
  var SERIES_RX = /timescale_update|"du"|series_completed/;
  var BARS = {};          // symbol -> {ts: [o,h,l,c]}
  var SYMOF = {};         // series id -> resolved symbol

  function noteSymbol(txt) {
    try {
      var m = /"(?:symbol|full_name|pro_name|short_name)"\s*:\s*"([^"]{2,40})"/.exec(txt);
      var s2 = /"(s[0-9]+|sds_sym_[0-9]+)"/.exec(txt);
      if (m && s2) SYMOF[s2[1]] = m[1];
    } catch (e) {}
  }

  function mineBars(txt) {
    try {
      if (!SERIES_RX.test(txt)) return;
      noteSymbol(txt);
      var rx = /"(s[0-9]+|sds_sym_[0-9]+)"\s*:\s*\{\s*"s"\s*:\s*\[([\s\S]*?)\]\s*\}/g, m2;
      while ((m2 = rx.exec(txt)) !== null) {
        var sid = m2[1], blob = m2[2];
        var sym = SYMOF[sid] || sid;
        var brx = /"v"\s*:\s*\[([^\]]+)\]/g, b;
        var store = BARS[sym] || (BARS[sym] = {});
        var n = 0;
        while ((b = brx.exec(blob)) !== null) {
          var p2 = b[1].split(",").map(parseFloat);
          if (p2.length >= 5 && isFinite(p2[0]) && isFinite(p2[4])) {
            store[Math.round(p2[0])] = [p2[1], p2[2], p2[3], p2[4]];
            n++;
          }
        }
        if (n) {
          var keys = Object.keys(store);
          post("ws:" + sym, { symbol: sym, n_total: keys.length, added: n },
               "bars-progress");
        }
      }
    } catch (e) {}
  }

  window.__JH_BARS = BARS;   // popup/content can flush this
  window.addEventListener("message", function (e) {
    try {
      if (!e.data || e.data.__jh_cmd !== "flush-bars") return;
      var out = [];
      Object.keys(BARS).forEach(function (sym) {
        var st = BARS[sym], ks = Object.keys(st).sort(function (a, b) { return a - b; });
        if (!ks.length) return;
        out.push({ symbol: sym, n: ks.length,
                   first: ks[0], last: ks[ks.length - 1],
                   bars: ks.map(function (k) { return [Number(k)].concat(st[k]); }) });
      });
      window.postMessage({ __jh: "bars-flush", data: out }, "*");
    } catch (e2) {}
  }, false);

  var OrigWS = window.WebSocket;
  if (OrigWS) {
    window.WebSocket = function (url, protos) {
      var ws = protos === undefined ? new OrigWS(url) : new OrigWS(url, protos);
      try {
        if (/tradingview|widgetdata/i.test(String(url))) {
          ws.addEventListener("message", function (ev) {
            try {
              if (typeof ev.data === "string" && ev.data.length > 60) mineBars(ev.data);
            } catch (e) {}
          });
        }
      } catch (e) {}
      return ws;
    };
    window.WebSocket.prototype = OrigWS.prototype;
    window.WebSocket.CONNECTING = OrigWS.CONNECTING;
    window.WebSocket.OPEN = OrigWS.OPEN;
    window.WebSocket.CLOSING = OrigWS.CLOSING;
    window.WebSocket.CLOSED = OrigWS.CLOSED;
  }

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

  /* ── v1.5.0 sources: DOM fallback on symbol pages ─────────────────
   * Network taps catch most attribution (symbol_search/scanner carry
   * source fields), but server-rendered symbol pages may not refetch.
   * On /symbols/ paths, after settle, read the page's own source line. */
  function domSrc() {
    try {
      if (!/\/symbols\//.test(location.pathname)) return;
      var sym = (location.pathname.match(/\/symbols\/([^\/]+)/) || [])[1] || "";
      var el = document.querySelector('[class*="source" i], [data-name*="source" i]');
      var t = el ? el.textContent.trim().slice(0, 160) : "";
      post(location.href, { __dom: 1, symbol: sym.replace(/-/g, ":"),
                            source_text: t, title: document.title.slice(0, 160) },
           "tap");
    } catch (e) {}
  }
  setTimeout(domSrc, 2500);
  var _push = history.pushState;
  history.pushState = function () {
    _push.apply(this, arguments);
    setTimeout(domSrc, 2500);
  };

  /* ── v1.6.0 ACTIVE HARVEST — walks EVERY watchlist symbol on command,
   * in page context (TV session + CORS), so its own APIs answer; every
   * response flows through the existing tap and the content-script
   * sniffer extracts whatever source fields exist. ~3 symbols/sec. */
  var HARVESTING = false;
  function harvest(symbols) {
    if (HARVESTING) return;
    HARVESTING = true;
    var i = 0, total = symbols.length;
    function step() {
      if (i >= total) {
        HARVESTING = false;
        post("harvest", { __progress: 1, done: total, total: total,
                          finished: 1 }, "tap");
        return;
      }
      var sym = symbols[i++];
      var bare = String(sym).split(":").pop();
      try {
        origFetch("https://symbol-search.tradingview.com/symbol_search/v3/?text=" +
                  encodeURIComponent(bare) + "&hl=0&lang=en&domain=production",
                  { credentials: "include" })
          .then(function (r) { return r.json(); })
          .then(function (j) { post("symsearch:" + sym, j); })
          .catch(function () {});
      } catch (e) {}
      try {
        origFetch("https://www.tradingview.com/symbols/" +
                  String(sym).replace(":", "-") + "/",
                  { credentials: "include" })
          .then(function (r) { return r.text(); })
          .then(function (t) {
            post("htmlsrc", { __htmlsrc: 1, symbol: sym,
                              text: String(t).slice(0, 60000) }, "tap");
          }).catch(function () {});
      } catch (e) {}
      if (i % 5 === 0)
        post("harvest", { __progress: 1, done: i, total: total }, "tap");
      setTimeout(step, 340);
    }
    step();
  }
  window.addEventListener("message", function (e) {
    var d = e && e.data;
    if (d && d.__jh_cmd === "harvest" && Array.isArray(d.symbols))
      harvest(d.symbols.slice(0, 12000));
  });
})();