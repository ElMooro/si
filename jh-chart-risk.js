/* jh-reskin-skip */
/* Red issuer warnings from share-flows + forensic-screen.
   chart.html and chart-pro.html. */
(function () {
  var path = location.pathname || "";
  if (!/chart(\.html)?$|chart-pro\.html$/i.test(path)) return;
  if (window.__jhChartRiskV2) return;
  window.__jhChartRiskV2 = true;

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
      "display:none", "position:fixed", "left:72px", "top:92px", "z-index:80",
      "max-width:min(520px,72vw)", "padding:8px 12px", "border-radius:6px",
      "background:#3a1014", "border:1px solid #f23645", "color:#ff6b6b",
      "font:12px/1.35 'IBM Plex Mono',ui-monospace,monospace", "pointer-events:none",
      "box-shadow:0 8px 24px rgba(0,0,0,.45)"
    ].join(";");
    document.body.appendChild(n);
    return n;
  }

  function norm(s) {
    s = String(s || "").toUpperCase().trim();
    var i = s.lastIndexOf(":");
    if (i >= 0) s = s.slice(i + 1);
    return s.replace(/[^A-Z0-9.\-]/g, "");
  }

  function currentSym() {
    try {
      if (window.State && State.activeTicker) return norm(State.activeTicker);
    } catch (e) {}
    var p = new URLSearchParams(location.search);
    var h = (location.hash || "").replace(/^#/, "");
    var inp = document.getElementById("symin") || document.querySelector(".tv-search input, input[placeholder*='Symbol']");
    var tab = document.querySelector(".chart-tab.active, .chart-tabs .on, [data-ticker].on");
    var fromTab = tab && (tab.getAttribute("data-ticker") || tab.textContent);
    return norm(p.get("s") || p.get("symbol") || h || (inp && inp.value) || fromTab || window.jhSymbol || "");
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
    if (!sym || !sf) { n.style.display = "none"; return; }
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
      bits.push("ATM shelf" + (row.atm_shelf_date ? " " + row.atm_shelf_date : ""));
    }
    if (fr.m_flag || fr.beneish_flag || (fr.flags && String(fr.flags).indexOf("BENEISH") >= 0)) {
      hot = true;
      bits.push("Beneish flag");
    }
    var concern = fr.flags || fr.careful || fr.risk_flags || [];
    if (Array.isArray(concern)) {
      if (concern.indexOf("DILUTION_SEVERE") >= 0) { hot = true; bits.push("DILUTION_SEVERE"); }
      if (concern.indexOf("HIGH_CONCERN") >= 0) { hot = true; bits.push("HIGH_CONCERN"); }
    }
    if (!hot || !bits.length) {
      n.style.display = "none";
      n.textContent = "";
      return;
    }
    n.style.display = "block";
    n.textContent = "⚠ " + sym + " — " + bits.join(" · ");
  }

  function tick() {
    paint(currentSym());
  }

  load().then(function () {
    tick();
    setInterval(tick, 1200);
  });
})();
