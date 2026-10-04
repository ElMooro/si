/* Chart clicks strip the namespace. Read the tvwatch route table as text and hand jhGoSymbol the mapped id. */
(function () {
  if (window.__jhRowHandoff) return;
  window.__jhRowHandoff = 1;
  var map = null, econ = null, fut = null, pending = null;
  var Y = {"TVC:US01Y":"FRED:DGS1","TVC:US02Y":"FRED:DGS2","TVC:US03Y":"FRED:DGS3","TVC:US05Y":"FRED:DGS5","TVC:US10Y":"FRED:DGS10","TVC:US30Y":"FRED:DGS30","TVC:US01MY":"FRED:DGS1MO","TVC:US03MY":"FRED:DGS3MO","TVC:US06MY":"FRED:DGS6MO"};
  function addRaw(text, into) {
    var re = /raw = "([^"]*)", lines = raw\.split/g, m, i, line, cut;
    while ((m = re.exec(text))) {
      var lines = m[1].split("\\n");
      for (i = 0; i < lines.length; i++) {
        line = lines[i]; cut = line.indexOf("|");
        if (cut > 0) into[line.slice(0, cut)] = line.slice(cut + 1);
      }
    }
  }
  function grab(text, marker) {
    var i = text.indexOf(marker), b, depth = 0, j, body, out = Object.create(null), re = /"?([A-Z0-9]{1,16})"?\s*:\s*"([^"]*)"/g, m;
    if (i < 0) return out;
    b = text.indexOf("{", i); if (b < 0) return out;
    for (j = b; j < text.length; j++) {
      if (text.charAt(j) === "{") depth++;
      else if (text.charAt(j) === "}") { depth--; if (!depth) { j++; break; } }
    }
    body = text.slice(b, j);
    while ((m = re.exec(body))) out[m[1]] = m[2];
    return out;
  }
  function load() {
    if (map) return Promise.resolve(map);
    if (pending) return pending;
    pending = fetch("/jh-chart-tvwatch.js?handoff=20261004a", {cache:"force-cache"}).then(function (r) {
      if (!r.ok) throw new Error("map " + r.status);
      return r.text();
    }).then(function (text) {
      map = Object.create(null);
      addRaw(text, map);
      econ = grab(text, "var econ = ");
      fut = grab(text, "var fut = ");
      return map;
    }).catch(function () { map = Object.create(null); econ = {}; fut = {}; return map; });
    return pending;
  }
  function route(sym) {
    var u = String(sym || "").trim(), up, cut, venue, bare, hit, pair, cc, fm;
    if (!u || !map) return "";
    hit = map[u] || map[u.toUpperCase()] || Y[u.toUpperCase()] || "";
    if (hit) return hit;
    up = u.toUpperCase(); cut = up.indexOf(":");
    venue = cut > 0 ? up.slice(0, cut) : ""; bare = cut > 0 ? up.slice(cut + 1) : up;
    if (venue === "ECONOMICS" && econ && econ[bare]) return "FRED:" + econ[bare];
    if (/^(FX|FX_IDC|OANDA)$/.test(venue)) {
      pair = bare.replace(/[^A-Z]/g, "");
      cc = ["USD","EUR","GBP","JPY","CHF","CAD","AUD","NZD","CNY","CNH","HKD","SEK","NOK","DKK","MXN","ZAR","SGD"];
      if (pair.length === 6 && cc.indexOf(pair.slice(0, 3)) >= 0 && cc.indexOf(pair.slice(3)) >= 0) return pair + "=X";
    }
    fm = /^([A-Z0-9]{1,3})1!$/.exec(bare);
    if (fm && fut && fut[fm[1]] && /^(CME|CME_MINI|CBOT|NYMEX|COMEX)$/.test(venue)) return fut[fm[1]];
    return "";
  }
  function blocked(id) { return /^(DATA|provider|DESK|CQSNAP|CQARM|CQDOC):/i.test(id); }
  document.addEventListener("click", function (ev) {
    var t = ev.target, row, sym, fromHits;
    if (!t || !t.closest || t.closest(".w-cmp,.grip,.tvflag,.wx")) return;
    row = t.closest("#wlist [data-s], #hits [data-s]");
    if (!row) return;
    sym = row.getAttribute("data-s");
    if (!sym) return;
    ev.preventDefault();
    ev.stopPropagation();
    fromHits = !!row.closest("#hits");
    load().then(function () {
      var next = route(sym);
      if (!next || blocked(next)) next = sym;
      if (fromHits) {
        var q = document.getElementById("q");
        if (q) { q.value = ""; try { q.dispatchEvent(new Event("input")); } catch (e) {} }
      }
      if (typeof window.jhGoSymbol === "function") window.jhGoSymbol(next);
    });
  }, true);
  load();
})();
