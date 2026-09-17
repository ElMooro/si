/* jh-reskin-skip */
/* Red on-chart issuer warnings from share-flows + forensic-screen. chart.html only. */
(function () {
  var path = location.pathname || "";
  if (!/chart\.html?$|\/chart\/?$/i.test(path)) return;
  if (window.__jhChartRisk) return;
  window.__jhChartRisk = true;

  var sf = null;
  var fo = null;
  var lastSym = "";

  function el() {
    var n = document.getElementById("jh-risk-banner");
    if (n) return n;
    n = document.createElement("div");
    n.id = "jh-risk-banner";
    n.setAttribute("role", "status");
    n.style.cssText = [
      "display:none", "position:absolute", "left:12px", "top:36px", "z-index:12",
      "max-width:min(420px,70%)", "padding:8px 10px", "border-radius:6px",
      "background:rgba(242,54,69,.16)", "border:1px solid #f23645", "color:#f23645",
      "font:12px/1.35 'IBM Plex Mono',ui-monospace,monospace", "pointer-events:none"
    ].join(";");
    var stage = document.getElementById("stage") || document.getElementById("app") || document.body;
    stage.style.position = stage.style.position || "relative";
    stage.appendChild(n);
    return n;
  }

  function norm(s) {
    s = String(s || "").toUpperCase().trim();
    var i = s.lastIndexOf(":");
    if (i >= 0) s = s.slice(i + 1);
    return s.replace(/[^A-Z0-9.\-]/g, "");
  }

  function currentSym() {
    var p = new URLSearchParams(location.search);
    var h = (location.hash || "").replace(/^#/, "");
    var inp = document.getElementById("symin");
    return norm(p.get("s") || p.get("symbol") || h || (inp && inp.value) || window.jhSymbol || "");
  }

  function load() {
    return Promise.all([
      fetch("/data/share-flows.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
      fetch("/data/forensic-screen.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
    ]).then(function (a) {
      sf = (a[0] && (a[0].tickers || a[0].by_ticker)) || {};
      var raw = a[1];
      fo = {};
      if (raw && raw.rows) raw.rows.forEach(function (row) { if (row && row.symbol) fo[row.symbol] = row; });
      else if (raw && raw.tickers) fo = raw.tickers;
      else if (raw && typeof raw === "object") fo = raw;
    });
  }

  function paint(sym) {
    var n = el();
    var row = sf[sym] || {};
    var fr = fo[sym] || {};
    var read = String(row.read || "");
    var yoy = row.sh_yoy_pct;
    var flags = row.flags || [];
    var bits = [];
    var hot = false;
    if (read === "EXTREME_DILUTION" || row.extreme) {
      hot = true;
      bits.push("EXTREME DILUTION" + (yoy != null ? " share count +" + yoy + "% YoY" : ""));
    } else if (read === "HEAVY_DILUTION") {
      hot = true;
      bits.push("HEAVY DILUTION" + (yoy != null ? " +" + yoy + "% YoY" : ""));
    } else if (read === "DILUTING" && yoy != null && yoy >= 5) {
      hot = true;
      bits.push("DILUTING +" + yoy + "% YoY");
    }
    if (flags.indexOf("ATM_SHELF_ACTIVE") >= 0) {
      hot = true;
      bits.push("ATM shelf active" + (row.atm_shelf_date ? " " + row.atm_shelf_date : ""));
    }
    if (fr.m_flag || fr.beneish_flag || (fr.flags && String(fr.flags).indexOf("BENEISH") >= 0)) {
      hot = true;
      bits.push("Beneish manipulation-pattern flag");
    }
    var concern = fr.flags || fr.careful || fr.risk_flags || [];
    if (Array.isArray(concern)) {
      if (concern.indexOf("DILUTION_SEVERE") >= 0) { hot = true; bits.push("census: DILUTION_SEVERE"); }
      if (concern.indexOf("HIGH_CONCERN") >= 0) { hot = true; bits.push("census: HIGH_CONCERN"); }
    }
    if (!hot || !bits.length) {
      n.style.display = "none";
      n.textContent = "";
      return;
    }
    n.style.display = "block";
    n.textContent = sym + " — " + bits.join(" · ");
  }

  function tick() {
    var sym = currentSym();
    if (!sym || !sf) return;
    if (sym === lastSym && document.getElementById("jh-risk-banner")) {
      paint(sym);
      return;
    }
    lastSym = sym;
    paint(sym);
  }

  load().then(function () {
    tick();
    setInterval(tick, 1500);
    var inp = document.getElementById("symin");
    if (inp) inp.addEventListener("change", tick);
    window.addEventListener("hashchange", tick);
  });
})();
