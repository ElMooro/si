/* ops 5404 pattern: data/research/{SYMBOL}.json */
(function () {
  var BUCKET = "https://justhodl-dashboard-live.s3.amazonaws.com";
  function esc(s) {
    return String(s == null ? "" : s).replace(/[<>&]/g, function (c) {
      return { "<": "<", ">": ">", "&": "&" }[c];
    });
  }
  function keys(obj) {
    if (!obj || typeof obj !== "object") return [];
    return Object.keys(obj).slice(0, 12);
  }
  async function mount(sym) {
    if (!sym || sym === "—") return;
    var host = document.getElementById("jh-research-slot");
    if (!host) {
      host = document.createElement("div");
      host.id = "jh-research-slot";
      var results = document.getElementById("results");
      if (!results) return;
      results.insertBefore(host, results.firstChild);
    }
    host.innerHTML = "<div class=\"empty\">research warehouse…</div>";
    try {
      var r = await fetch(BUCKET + "/data/research/" + encodeURIComponent(sym) + ".json?t=" + Date.now());
      if (!r.ok) {
        host.innerHTML = "<div class=\"section\"><div class=\"section-eyebrow\">Warehouse research</div><div class=\"empty\">no data/research/" + esc(sym) + ".json</div></div>";
        return;
      }
      var d = await r.json();
      var bits = keys(d).map(function (k) {
        var v = d[k];
        if (v && typeof v === "object") return esc(k) + " [obj]";
        return esc(k) + ": " + esc(v).slice(0, 80);
      }).join("<br>");
      host.innerHTML = "<div class=\"section\"><div class=\"section-eyebrow\">Warehouse research</div><h2 class=\"section-title\">" + esc(sym) + " note</h2><div class=\"card\">" + bits + "</div></div>";
    } catch (e) {
      host.innerHTML = "";
    }
  }
  function current() {
    var el = document.getElementById("symbolHeader");
    return el ? String(el.textContent || "").trim().toUpperCase() : "";
  }
  var last = "";
  setInterval(function () {
    var s = current();
    if (s && s !== last && s !== "—") {
      last = s;
      mount(s);
    }
  }, 400);
})();
