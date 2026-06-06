/* freshness.js — data staleness gate. Given a data file's generated_at (or any
 * ISO timestamp), shows a clear FRESH / AGING / STALE badge so a silently-failed
 * Lambda never presents old numbers as current.
 *
 *   <script src="/freshness.js"></script>
 *   Freshness.badge('el-id', generatedAtIso, { freshHrs: 26, staleHrs: 72, label: 'Signals' });
 *   Freshness.fromUrl('el-id', '/data/best-setups.json', { field: 'generated_at', ... });
 *
 * Thresholds default to: FRESH < 26h, AGING 26–72h, STALE > 72h. Tune per feed
 * (a daily engine is fine at 26h; an hourly one should use much tighter bounds).
 */
(function () {
  if (window.Freshness) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function injectCSS() {
    if (document.getElementById("fresh-css")) return;
    var s = document.createElement("style"); s.id = "fresh-css";
    s.textContent = [
      ".fresh-badge{display:inline-flex;align-items:center;gap:6px;font-family:ui-monospace,Menlo,monospace;font-size:10.5px;padding:3px 9px;border-radius:12px;border:1px solid}",
      ".fresh-badge .dot{width:6px;height:6px;border-radius:50%}",
      ".fresh-fresh{color:#26ffaf;border-color:rgba(38,255,175,0.4)}.fresh-fresh .dot{background:#26ffaf}",
      ".fresh-aging{color:#fbbf24;border-color:rgba(251,191,36,0.4)}.fresh-aging .dot{background:#fbbf24}",
      ".fresh-stale{color:#ff5577;border-color:rgba(255,85,119,0.5)}.fresh-stale .dot{background:#ff5577;animation:freshpulse 1.6s infinite}",
      ".fresh-unknown{color:#6f7b91;border-color:#2a3550}.fresh-unknown .dot{background:#6f7b91}",
      "@keyframes freshpulse{0%,100%{opacity:1}50%{opacity:.4}}",
    ].join("");
    document.head.appendChild(s);
  }

  function ageHours(iso) {
    if (!iso) return null;
    var t = Date.parse(iso);
    if (isNaN(t)) return null;
    return (Date.now() - t) / 36e5;
  }

  function fmtAge(h) {
    if (h == null) return "unknown age";
    if (h < 1) return Math.round(h * 60) + "m ago";
    if (h < 48) return Math.round(h) + "h ago";
    return Math.round(h / 24) + "d ago";
  }

  function classify(h, freshHrs, staleHrs) {
    if (h == null) return { cls: "unknown", word: "no timestamp" };
    if (h <= freshHrs) return { cls: "fresh", word: "live" };
    if (h <= staleHrs) return { cls: "aging", word: "aging" };
    return { cls: "stale", word: "STALE" };
  }

  function badge(targetId, iso, opts) {
    opts = opts || {};
    var host = typeof targetId === "string" ? document.getElementById(targetId) : targetId;
    if (!host) return null;
    injectCSS();
    var h = ageHours(iso);
    var c = classify(h, opts.freshHrs || 26, opts.staleHrs || 72);
    var lbl = opts.label ? opts.label + " " : "";
    host.innerHTML =
      '<span class="fresh-badge fresh-' + c.cls + '" title="' + lbl + 'data ' + fmtAge(h) +
      (c.cls === "stale" ? " — an engine may have failed; treat with caution" : "") + '">' +
      '<span class="dot"></span>' + lbl + c.word + ' · ' + fmtAge(h) + '</span>';
    return c.cls;
  }

  function fromUrl(targetId, url, opts) {
    opts = opts || {};
    var full = (url.indexOf("http") === 0 ? url : PROXY + (url[0] === "/" ? url : "/" + url)) + "?t=" + Date.now();
    return fetch(full).then(function (r) { return r.ok ? r.json() : null; }).then(function (d) {
      var iso = d ? (d[opts.field || "generated_at"] || d.generated_at || d.updated_at || d.as_of) : null;
      return badge(targetId, iso, opts);
    }).catch(function () { return badge(targetId, null, opts); });
  }

  window.Freshness = { badge: badge, fromUrl: fromUrl, ageHours: ageHours };
})();
