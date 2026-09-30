/* jh-quote-health.js — per-source quote health cards (8/10).
 *
 * Renders data/quote-health.json (published by justhodl-price-redundancy)
 * as one card per feed source: badge pill, last-ok age, median latency,
 * and 24h success rate.
 *
 *   <script src="/jh-quote-health.js"></script>
 *   <div id="quote-health"></div>
 *   <script>JHQuoteHealth.render("quote-health");</script>
 *
 * Custom URL: JHQuoteHealth.render("quote-health", "/data/quote-health.json")
 */
(function () {
  if (window.JHQuoteHealth) return;

  var BADGE_CLS = {
    LIVE: "fresh", DELAYED: "aging", SESSION: "session",
    STALE: "stale", OFFLINE: "unknown",
  };

  function injectCSS() {
    if (document.getElementById("qh-css")) return;
    var s = document.createElement("style"); s.id = "qh-css";
    s.textContent = [
      ".qh-wrap{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}",
      ".qh-card{background:#0d1420;border:1px solid #1c2942;border-radius:10px;padding:12px 14px;font-family:ui-monospace,Menlo,monospace}",
      ".qh-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}",
      ".qh-name{font-size:13px;font-weight:700;color:#e6edf7;text-transform:capitalize}",
      ".qh-pill{display:inline-flex;align-items:center;gap:6px;font-size:10.5px;padding:3px 9px;border-radius:12px;border:1px solid}",
      ".qh-pill .dot{width:6px;height:6px;border-radius:50%}",
      ".qh-fresh{color:#26ffaf;border-color:rgba(38,255,175,0.4)}.qh-fresh .dot{background:#26ffaf}",
      ".qh-aging{color:#fbbf24;border-color:rgba(251,191,36,0.4)}.qh-aging .dot{background:#fbbf24}",
      ".qh-stale{color:#ff5577;border-color:rgba(255,85,119,0.5)}.qh-stale .dot{background:#ff5577}",
      ".qh-unknown{color:#6f7b91;border-color:#2a3550}.qh-unknown .dot{background:#6f7b91}",
      ".qh-session{color:#7cc7ff;border-color:rgba(124,199,255,0.4)}.qh-session .dot{background:#7cc7ff}",
      ".qh-row{display:flex;justify-content:space-between;font-size:11px;color:#8fa0bd;padding:2px 0}",
      ".qh-row b{color:#d5deee;font-weight:600}",
      ".qh-err{color:#6f7b91;font-size:12px;font-family:ui-monospace,Menlo,monospace}",
    ].join("");
    document.head.appendChild(s);
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function ago(iso) {
    if (!iso) return "never";
    var t = Date.parse(iso);
    if (isNaN(t)) return "unknown";
    var s = Math.max(0, (Date.now() - t) / 1000);
    if (s < 60) return Math.round(s) + "s ago";
    if (s < 3600) return Math.round(s / 60) + "m ago";
    if (s < 86400) return Math.round(s / 3600) + "h ago";
    return Math.round(s / 86400) + "d ago";
  }

  function card(name, h) {
    h = h || {};
    var badge = h.badge || "OFFLINE";
    var cls = BADGE_CLS[badge] || "unknown";
    var lat = h.median_latency_ms != null ? Math.round(h.median_latency_ms) + " ms" : "—";
    var ok = h.success_24h != null ? Math.round(h.success_24h * 100) + "%" : "—";
    return (
      '<div class="qh-card">' +
      '<div class="qh-head"><span class="qh-name">' + esc(name) + "</span>" +
      '<span class="qh-pill qh-' + cls + '"><span class="dot"></span>' + esc(badge) + "</span></div>" +
      '<div class="qh-row"><span>last ok</span><b>' + esc(ago(h.last_ok)) + "</b></div>" +
      '<div class="qh-row"><span>median latency</span><b>' + esc(lat) + "</b></div>" +
      '<div class="qh-row"><span>24h success</span><b>' + esc(ok) + "</b></div>" +
      "</div>"
    );
  }

  function render(targetId, url) {
    var host = typeof targetId === "string" ? document.getElementById(targetId) : targetId;
    if (!host) return null;
    injectCSS();
    var full = (url || "/data/quote-health.json") + "?t=" + Date.now();
    return fetch(full)
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (doc) {
        var sources = (doc && doc.sources) || {};
        var names = Object.keys(sources);
        if (!names.length) {
          host.innerHTML = '<div class="qh-err">quote health unavailable</div>';
          return null;
        }
        host.innerHTML =
          '<div class="qh-wrap">' +
          names.map(function (n) { return card(n, sources[n]); }).join("") +
          "</div>";
        return names.length;
      })
      .catch(function () {
        host.innerHTML = '<div class="qh-err">quote health unavailable</div>';
        return null;
      });
  }

  window.JHQuoteHealth = { render: render };
})();
