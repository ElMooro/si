/* jh-book-fuse.js — join Backlog Radar + Company Order Book + Forward Orders
   onto any ticker desk that needs locked-demand context.
   Does not replace those three pages. Reuses jhOrderbook math when present. */
(function (root) {
  if (root.__jhBookFuseV1) return;
  root.__jhBookFuseV1 = true;

  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com";

  var STATE = { ready: false, rows: {}, radar: {}, fo: {} };

  function up(t) { return String(t == null ? "" : t).toUpperCase().trim(); }

  function foList(doc) {
    if (!doc || typeof doc !== "object") return [];
    if (Array.isArray(doc.all_results)) return doc.all_results;
    if (Array.isArray(doc.results)) return doc.results;
    if (Array.isArray(doc.scored)) return doc.scored;
    if (Array.isArray(doc.all)) return doc.all;
    if (doc.by_ticker && typeof doc.by_ticker === "object") {
      return Object.keys(doc.by_ticker).map(function (k) {
        var v = doc.by_ticker[k] || {};
        if (!v.ticker) v = Object.assign({ ticker: k }, v);
        return v;
      });
    }
    return [];
  }

  function foScore(row) {
    if (!row) return null;
    var d = row.data || row;
    var keys = ["composite", "score", "fwd_score", "forward_score", "rpo_score"];
    for (var i = 0; i < keys.length; i++) {
      var v = row[keys[i]] != null ? row[keys[i]] : d[keys[i]];
      if (typeof v === "number" && isFinite(v)) return v;
    }
    return null;
  }

  function loadFromDocs(backlog, mined, fo, mom, now) {
    var rows = [];
    if (root.jhOrderbook && typeof root.jhOrderbook.buildRows === "function") {
      rows = root.jhOrderbook.buildRows(backlog || {}, mined || {}, fo || {}, now || Date.now());
      if (typeof root.jhOrderbook.attachPrices === "function") {
        root.jhOrderbook.attachPrices(rows, mom || {});
      }
    }
    var byRow = {};
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      if (r && r.ticker) byRow[up(r.ticker)] = r;
    }
    var radar = {};
    var src = (backlog && backlog.by_ticker) || {};
    Object.keys(src).forEach(function (k) {
      radar[up(k)] = src[k];
    });
    var foIdx = {};
    foList(fo).forEach(function (row) {
      var t = up(row.ticker || (row.data && row.data.ticker));
      if (t) foIdx[t] = row;
    });
    STATE = { ready: true, rows: byRow, radar: radar, fo: foIdx };
    return STATE;
  }

  function lookup(ticker) {
    var t = up(ticker);
    if (!t) return null;
    var book = STATE.rows[t] || null;
    var radar = STATE.radar[t] || null;
    var fo = STATE.fo[t] || null;
    if (!book && !radar && !fo) return null;
    var usd = book && book.book_usd != null ? book.book_usd : (radar && radar.rpo);
    var qoq = book && book.qoq != null ? book.qoq : (radar && radar.rpo_qoq);
    var yoy = book && book.yoy != null ? book.yoy : (radar && radar.rpo_yoy);
    var mom = book ? book.mom : null;
    return {
      ticker: t,
      book_usd: usd,
      mom: mom,
      qoq: qoq,
      yoy: yoy,
      price_mom: book ? book.price_mom : null,
      price_qoq: book ? book.price_qoq : null,
      price_yoy: book ? book.price_yoy : null,
      asof: (book && book.asof) || (radar && (radar.rpo_asof || radar.deferred_asof)) || null,
      src: (book && (book.book_src || book.book_kind)) || (radar ? "backlog" : (fo ? "forward" : null)),
      accelerating: !!(radar && (radar.demand_accelerating || radar.deferred_accelerating)) || !!(book && book.accelerating),
      ev_to_rpo: radar && radar.ev_to_rpo != null ? radar.ev_to_rpo : null,
      rev_yoy: radar && radar.rev_yoy != null ? radar.rev_yoy : null,
      divergence: radar && radar.rpo_minus_rev_growth != null ? radar.rpo_minus_rev_growth : null,
      deferred_yoy: radar && radar.deferred_yoy != null ? radar.deferred_yoy : null,
      fo_score: foScore(fo),
      has_book: usd != null,
      has_radar: !!radar,
      has_fo: !!fo
    };
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      if (c === "&") return "\u0026amp;";
      if (c === "<") return "\u0026lt;";
      if (c === ">") return "\u0026gt;";
      return "\u0026quot;";
    });
  }

  function pct(v) {
    if (v == null || !isFinite(v)) return "";
    return (v >= 0 ? "+" : "") + Number(v).toFixed(1) + "%";
  }

  function bn(v) {
    if (v == null || !isFinite(v)) return "";
    var a = Math.abs(v);
    if (a >= 1e9) return "$" + (v / 1e9).toFixed(1) + "B";
    if (a >= 1e6) return "$" + (v / 1e6).toFixed(0) + "M";
    return "$" + Math.round(v).toLocaleString("en-US");
  }

  function stripHtml(hit) {
    if (!hit) return "";
    var bits = [];
    if (hit.book_usd != null) bits.push("<b>" + esc(bn(hit.book_usd)) + "</b>");
    if (hit.qoq != null) bits.push("QoQ " + esc(pct(hit.qoq)));
    if (hit.yoy != null) bits.push("YoY " + esc(pct(hit.yoy)));
    if (hit.mom != null) bits.push("MoM " + esc(pct(hit.mom)));
    if (hit.accelerating) bits.push('<span class="jh-book-accel">ACCEL</span>');
    if (hit.ev_to_rpo != null) bits.push("EV/RPO " + esc(String(hit.ev_to_rpo)) + "x");
    if (hit.fo_score != null) bits.push("FO " + esc(Number(hit.fo_score).toFixed(0)));
    if (hit.src) bits.push(esc(String(hit.src)));
    var t = encodeURIComponent(hit.ticker);
    var links = '<a href="/orderbook.html">Order Book</a> · <a href="/backlog.html">Backlog Radar</a> · <a href="/forward-orders.html">Forward Orders</a> · <a href="/chart.html?s=' + t + '">Chart</a>';
    return '<div class="jh-book-fuse" data-ticker="' + esc(hit.ticker) + '">'
      + '<div class="jh-book-fuse-line">' + bits.join(" · ") + "</div>"
      + '<div class="jh-book-fuse-links">' + links + "</div>"
      + "</div>";
  }

  function mount(el, ticker) {
    if (!el) return null;
    var hit = lookup(ticker);
    if (!hit) {
      el.innerHTML = "";
      el.setAttribute("data-book-empty", "1");
      return null;
    }
    el.innerHTML = stripHtml(hit);
    el.removeAttribute("data-book-empty");
    return hit;
  }

  function tickerFromPage() {
    try {
      var u = new URL(root.location && root.location.href || "https://justhodl.ai/");
      var q = u.searchParams.get("s") || u.searchParams.get("symbol") || u.searchParams.get("ticker") || "";
      if (q) return up(q);
    } catch (e) {}
    var marked = root.document && root.document.querySelector("[data-book-fuse-ticker]");
    if (marked) return up(marked.getAttribute("data-book-fuse-ticker"));
    var hdr = root.document && (root.document.getElementById("symbolHeader") || root.document.getElementById("tk"));
    if (hdr && hdr.textContent && hdr.textContent.replace(/[^\w.]/g, "")) return up(hdr.textContent);
    return "";
  }

  async function pull(path) {
    var urls = [
      path + "?t=" + Date.now(),
      PROXY + path + "?t=" + Date.now(),
      S3 + path + "?t=" + Date.now()
    ];
    for (var i = 0; i < urls.length; i++) {
      try {
        var r = await fetch(urls[i], { cache: "no-store" });
        if (r.ok) return await r.json();
      } catch (e) {}
    }
    return null;
  }

  async function boot() {
    var pack = await Promise.all([
      pull("/data/backlog.json"),
      pull("/data/backlog-mined.json"),
      pull("/data/forward-orders.json"),
      pull("/data/momentum-scanner.json")
    ]);
    loadFromDocs(pack[0] || {}, pack[1] || {}, pack[2] || {}, pack[3] || {}, Date.now());
    autoMount();
    return STATE;
  }

  function ensureStyle() {
    if (!root.document || root.document.getElementById("jh-book-fuse-css")) return;
    var s = root.document.createElement("style");
    s.id = "jh-book-fuse-css";
    s.textContent = ".jh-book-fuse{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:#a8b3c7;border:1px solid #1c2433;background:#0c1018;border-radius:8px;padding:10px 12px;margin:10px 0}"
      + ".jh-book-fuse-line{color:#e1e8f4;margin-bottom:4px}"
      + ".jh-book-fuse a{color:#22d3ee;text-decoration:none}"
      + ".jh-book-accel{color:#26ffaf;border:1px solid #26ffaf;border-radius:4px;padding:0 6px;font-size:10px;margin-left:4px}";
    root.document.head.appendChild(s);
  }

  function autoMount() {
    if (!root.document) return;
    ensureStyle();
    var nodes = root.document.querySelectorAll("[data-book-fuse]");
    var t = tickerFromPage();
    if (!nodes.length && t) {
      var host = root.document.getElementById("jh-book-fuse-slot")
        || root.document.getElementById("results")
        || root.document.getElementById("content")
        || root.document.querySelector(".hero")
        || root.document.querySelector(".wrap");
      if (host && !host.querySelector(".jh-book-fuse")) {
        var box = root.document.createElement("div");
        box.setAttribute("data-book-fuse", "1");
        if (host.firstChild) host.insertBefore(box, host.firstChild);
        else host.appendChild(box);
        nodes = [box];
      }
    }
    for (var i = 0; i < nodes.length; i++) {
      var el = nodes[i];
      var spec = el.getAttribute("data-book-fuse-ticker") || t;
      if (spec) mount(el, spec);
    }
  }

  var api = {
    loadFromDocs: loadFromDocs,
    lookup: lookup,
    mount: mount,
    stripHtml: stripHtml,
    boot: boot,
    autoMount: autoMount,
    tickerFromPage: tickerFromPage,
    state: function () { return STATE; }
  };
  root.jhBookFuse = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;

  if (typeof root.addEventListener === "function") {
    root.addEventListener("DOMContentLoaded", function () {
      if (root.document && (root.document.querySelector("[data-book-fuse]") || tickerFromPage())) boot();
    });
  }
})(typeof window !== "undefined" ? window : globalThis);
