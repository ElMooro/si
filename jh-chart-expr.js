/* jh-reskin-skip */
/* jh-chart-expr — TradingView-style spread / ratio symbols on chart.html.
 * Charts expressions such as "AMEX:XLY/AMEX:XLP", "1-FRED:NFCI", "(TVC:US10Y-TVC:US02Y)*100",
 * "FRED:WALCL-FRED:WTREGEN-FRED:RRPONTSYD" exactly as they appear in the user's TradingView watchlists.
 * Each leg is read in full from PROXY /series (entire stored history). Legs are aligned as-of: every date of the
 * densest leg, each other leg carries its most recent observation, starting when all legs have data.
 * Nothing is substituted: a leg that the warehouse cannot serve fails the whole expression with its reason.
 */
(function (root) {
  "use strict";
  if (root.JHChartExpr) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var LEG = /[A-Za-z][A-Za-z0-9_]*:[A-Za-z0-9_.!^=&-]*[A-Za-z0-9_!]/;

  function tokenize(s) {
    var out = [], i = 0, m;
    s = String(s || "").replace(/^expr:/i, "");
    while (i < s.length) {
      var c = s[i];
      if (c === " ") { i++; continue; }
      if ("+-*/()".indexOf(c) >= 0) { out.push({ t: "op", v: c }); i++; continue; }
      var rest = s.slice(i);
      if ((m = rest.match(/^\d+(\.\d+)?(e[+-]?\d+)?/i)) && !/^[\d.]+[A-Za-z_]*:/.test(rest)) { out.push({ t: "num", v: parseFloat(m[0]) }); i += m[0].length; continue; }
      // a leg: EXCH:SYMBOL — a '-' inside a symbol is only kept when followed by a letter run and no operator context (e.g. BRK-B is rare in TV ids)
      if ((m = rest.match(/^[A-Za-z0-9_]+:[A-Za-z0-9_.!^=&]+/))) { out.push({ t: "leg", v: m[0] }); i += m[0].length; continue; }
      throw new Error("Unsupported character in expression: " + c);
    }
    return out;
  }
  // recursive descent: expr := term (('+'|'-') term)* ; term := factor (('*'|'/') factor)* ; factor := num | leg | '(' expr ')' | '-' factor
  function parse(tokens) {
    var p = 0;
    function peek() { return tokens[p]; }
    function eat(v) { var t = tokens[p]; if (!t || t.t !== "op" || t.v !== v) throw new Error("Expected " + v); p++; }
    function factor() {
      var t = tokens[p++];
      if (!t) throw new Error("Unexpected end of expression");
      if (t.t === "num") return { k: "num", v: t.v };
      if (t.t === "leg") return { k: "leg", v: t.v };
      if (t.t === "op" && t.v === "(") { var e = expr(); eat(")"); return e; }
      if (t.t === "op" && t.v === "-") return { k: "neg", a: factor() };
      throw new Error("Unexpected " + t.v);
    }
    function term() { var a = factor(); while (peek() && peek().t === "op" && (peek().v === "*" || peek().v === "/")) { var o = tokens[p++].v; a = { k: o, a: a, b: factor() }; } return a; }
    function expr() { var a = term(); while (peek() && peek().t === "op" && (peek().v === "+" || peek().v === "-")) { var o = tokens[p++].v; a = { k: o, a: a, b: term() }; } return a; }
    var tree = expr(); if (p !== tokens.length) throw new Error("Trailing tokens in expression"); return tree;
  }
  function legs(tree, out) {
    out = out || [];
    if (!tree) return out;
    if (tree.k === "leg") { if (out.indexOf(tree.v) < 0) out.push(tree.v); }
    else { legs(tree.a, out); legs(tree.b, out); }
    return out;
  }
  function isExpr(s) {
    s = String(s || "");
    if (/^expr:/i.test(s)) return true;
    if (!/[()+*\/]|[A-Za-z0-9!)]\s*-\s*[A-Za-z0-9(]|^\s*-/.test(s)) return false;
    if (!LEG.test(s)) return false;
    try { var t = parse(tokenize(s)); return t.k !== "leg"; } catch (e) { return false; }
  }
  function evalAt(tree, vals) {
    switch (tree.k) {
      case "num": return tree.v;
      case "leg": return vals[tree.v];
      case "neg": return -evalAt(tree.a, vals);
      case "+": return evalAt(tree.a, vals) + evalAt(tree.b, vals);
      case "-": return evalAt(tree.a, vals) - evalAt(tree.b, vals);
      case "*": return evalAt(tree.a, vals) * evalAt(tree.b, vals);
      case "/": var d = evalAt(tree.b, vals); return d === 0 ? NaN : evalAt(tree.a, vals) / d;
    }
    return NaN;
  }
  var cache = {};
  function loadLeg(id) {
    if (cache[id]) return cache[id];
    cache[id] = root.fetch(PROXY + "/series?id=" + encodeURIComponent(id)).then(function (r) { return r.json(); }).then(function (d) {
      var obs = (d && d.obs || []).filter(function (o) { return o && o[1] != null && isFinite(o[1]); });
      if (!obs.length) throw new Error(id + ": " + (d && d.error ? String(d.error).split("\n")[0].slice(0, 160) : "no stored history"));
      return { id: id, name: d.name || id, freq: d.freq || "", obs: obs };
    });
    cache[id].catch(function () { delete cache[id]; });
    return cache[id];
  }
  function evaluate(sym) {
    var tree = parse(tokenize(sym)), L = legs(tree);
    if (!L.length) return Promise.reject(new Error("Expression has no data legs"));
    return Promise.all(L.map(loadLeg)).then(function (packs) {
      var dense = packs.slice().sort(function (a, b) { return b.obs.length - a.obs.length; })[0];
      var start = packs.reduce(function (m, p) { return p.obs[0][0] > m ? p.obs[0][0] : m; }, "");
      var idx = {}; packs.forEach(function (p) { idx[p.id] = 0; });
      var out = [];
      dense.obs.forEach(function (o) {
        var dt = o[0]; if (dt < start) return;
        var vals = {};
        for (var i = 0; i < packs.length; i++) {
          var p = packs[i], j = idx[p.id];
          while (j + 1 < p.obs.length && p.obs[j + 1][0] <= dt) j++;
          idx[p.id] = j;
          if (p.obs[j][0] > dt) return;
          vals[p.id] = p.obs[j][1];
        }
        var v = evalAt(tree, vals);
        if (isFinite(v)) { var t = Math.floor(Date.parse(dt.slice(0, 10) + "T00:00:00Z") / 1000); out.push({ time: t, open: v, high: v, low: v, close: v, volume: 0 }); }
      });
      var src = "Spread " + L.length + " leg" + (L.length > 1 ? "s" : "") + " · /series full history · as-of aligned on " + dense.id + " · " + packs.map(function (p) { return p.id + " " + p.obs[0][0].slice(0, 4) + "→"; }).join(", ");
      var legInfo = packs.map(function (p) { return { id: p.id, name: p.name, first: p.obs[0][0], last: p.obs[p.obs.length - 1][0], n: p.obs.length }; });
      var H = root.JHObservationSeries;
      if (H && typeof H.warehouse === "function" && out.length) {
        // Same observation contract the chart uses for warehouse series, so scalar (FRED/macro) spreads plot on any interval.
        try {
          var pkt = { id: sym, name: "Spread: " + packs.map(function (p) { return p.name; }).join(" ⋄ ").slice(0, 240), unit: "expression", freq: dense.freq || null,
            obs: out.map(function (b) { return [new Date(b.time * 1000).toISOString().slice(0, 10), b.close]; }) };
          var r = H.warehouse(pkt, sym, PROXY + "/series?id=" + encodeURIComponent(dense.id));
          if (r && r.d && r.d.length) { r.src = src + " · " + r.src; r.legs = legInfo; if (r.evidence) r.evidence.expression_legs = legInfo; return r; }
        } catch (e) {}
      }
      return {
        d: out,
        src: "Spread " + L.length + " leg" + (L.length > 1 ? "s" : "") + " · /series full history · as-of aligned on " + dense.id + " · " + packs.map(function (p) { return p.id + " " + p.obs[0][0].slice(0, 4) + "→"; }).join(", "),
        legs: packs.map(function (p) { return { id: p.id, name: p.name, first: p.obs[0][0], last: p.obs[p.obs.length - 1][0], n: p.obs.length }; })
      };
    });
  }
  function hook() {
    var C = root.JHChartCatalog;
    if (!C || typeof C.klines !== "function" || C.__exprHooked) return !!(C && C.__exprHooked);
    var orig = C.klines;
    C.klines = function (sym) {
      if (isExpr(sym)) return evaluate(sym).catch(function (e) { return { d: [], src: "Spread unavailable — " + e.message }; });
      return orig.apply(this, arguments);
    };
    if (typeof C.go === "function") {
      var go = C.go;
      C.go = function (s) { if (isExpr(s)) return false; return go.apply(this, arguments); };
    }
    C.__exprHooked = true;
    return true;
  }
  root.JHChartExpr = { isExpr: isExpr, parse: function (s) { return parse(tokenize(s)); }, legs: function (s) { return legs(parse(tokenize(s))); }, evaluate: evaluate, hook: hook };
  if (!hook()) { var n = 0, t = setInterval(function () { if (hook() || ++n > 80) clearInterval(t); }, 250); }
})(typeof window !== "undefined" ? window : globalThis);
