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
      ".fresh-session{color:#7cc7ff;border-color:rgba(124,199,255,0.4)}.fresh-session .dot{background:#7cc7ff}",
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
    // ops 4401: self-origin fetch — workers.dev PROXY is CSP-blocked
    // (connect-src), silently failing the freshness badge. Same-origin
    // and S3 are CSP-allowed; /data/ is served from justhodl.ai directly.
    var self_origin_base = url.indexOf("http") === 0 ? url : (url[0] === "/" ? url : "/" + url);
    var full = self_origin_base + "?t=" + Date.now();
    return fetch(full).then(function (r) { return r.ok ? r.json() : null; }).then(function (d) {
      var iso = d ? (d[opts.field || "generated_at"] || d.generated_at || d.updated_at || d.as_of) : null;
      return badge(targetId, iso, opts);
    }).catch(function () { return badge(targetId, null, opts); });
  }

  /* ---- 8/10: intraday quote badges (mirrors aws/shared/quote_meta.py) ----
   *
   *   Freshness.quoteBadge('el-id', quoteAsOfIso, { source: 'polygon-realtime', latencyMs: 182 });
   *
   * Renders e.g. "DELAYED · 4m ago · Polygon". Vocabulary: LIVE (<60s,
   * realtime-entitled, market open), DELAYED (<15m intraday), SESSION
   * (market closed, last session close), STALE, OFFLINE (no quote).
   */
  var REALTIME_KINDS = { "polygon-realtime": 1, "fmp-realtime": 1 };

  function prettySource(kind) {
    var k = String(kind || "").toLowerCase();
    if (k.indexOf("polygon") === 0) return "Polygon";
    if (k === "fmp" || k.indexOf("fmp") === 0) return "FMP";
    if (k === "yahoo") return "Yahoo";
    if (k === "stooq") return "Stooq";
    return kind || "";
  }

  function nyParts(d) {
    // Heuristic ET clock via the platform tz database; throws -> caller fails soft.
    var s = d.toLocaleString("en-US", { timeZone: "America/New_York" });
    var p = new Date(s);
    if (isNaN(p.getTime())) throw new Error("tz-parse");
    return { dow: p.getDay(), mins: p.getHours() * 60 + p.getMinutes() };
  }

  function marketOpenNY(d) {
    var p = nyParts(d);
    return p.dow >= 1 && p.dow <= 5 && p.mins >= 570 && p.mins < 960;
  }

  function fmtQuoteAge(sec) {
    if (sec == null) return "unknown age";
    if (sec < 0) sec = 0;
    if (sec < 60) return Math.round(sec) + "s ago";
    if (sec < 3600) return Math.round(sec / 60) + "m ago";
    return fmtAge(sec / 3600);
  }

  function classifyQuote(iso, source) {
    if (!iso) return { cls: "unknown", word: "OFFLINE" };
    var t = Date.parse(iso);
    if (isNaN(t)) return { cls: "stale", word: "STALE" };
    var ageS = (Date.now() - t) / 1000;
    if (ageS < -300) return { cls: "stale", word: "STALE" };
    if (ageS < 0) ageS = 0;
    var realtime = !!REALTIME_KINDS[String(source || "").toLowerCase()];
    var open = false;
    try { open = marketOpenNY(new Date()); } catch (e) { open = false; }
    if (open) {
      if (realtime && ageS <= 60) return { cls: "fresh", word: "LIVE" };
      if (ageS <= 900) return { cls: "aging", word: "DELAYED" };
      return { cls: "stale", word: "STALE" };
    }
    if (ageS <= 86400) return { cls: "session", word: "SESSION" };
    return { cls: "stale", word: "STALE" };
  }

  function quoteBadge(targetId, asOfIso, opts) {
    opts = opts || {};
    var host = typeof targetId === "string" ? document.getElementById(targetId) : targetId;
    if (!host) return null;
    injectCSS();
    var c = classifyQuote(asOfIso, opts.source);
    var t = Date.parse(asOfIso);
    var ageS = isNaN(t) ? null : (Date.now() - t) / 1000;
    var src = prettySource(opts.source);
    var text = c.word + " · " + fmtQuoteAge(ageS) + (src ? " · " + src : "");
    if (opts.latencyMs != null && !isNaN(opts.latencyMs)) {
      text += " · " + Math.round(opts.latencyMs) + "ms";
    }
    var tip = "quote as of " + (asOfIso || "unknown") +
      (opts.latencyMs != null ? "; fetch latency " + Math.round(opts.latencyMs) + "ms" : "") +
      (c.cls === "stale" ? " — treat with caution" : "");
    host.innerHTML =
      '<span class="fresh-badge fresh-' + c.cls + '" title="' + tip + '">' +
      '<span class="dot"></span>' + text + "</span>";
    return c.cls;
  }

  window.Freshness = { badge: badge, fromUrl: fromUrl, ageHours: ageHours, quoteBadge: quoteBadge };
})();
