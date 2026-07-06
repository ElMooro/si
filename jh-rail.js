/* jh-rail.js — shared desk-detail right rail (design-audit §8-C).
   Mounts ONLY on pages that resolve to a real engine-registry entry via
   output-path matching — hub/document/composite pages get nothing rather
   than a fabricated rail. Every field is either real (fetched) or an
   explicit "not yet published" placeholder; nothing here is invented copy. */
(function () {
  "use strict";
  if (window.__jhRail) return; window.__jhRail = true;
  var path = location.pathname.replace(/^\//, "");
  if (!path || path === "index.html" || path.indexOf("screener") === 0) return;

  function j(u) { return fetch(u, { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }); }

  Promise.all([j("/nav-manifest.json"), j("/data/engine-registry.json")]).then(function (res) {
    var man = res[0], reg = res[1];
    if (!man || !reg) return;

    // ── this page's own referenced data/*.json outputs (same extraction the server-side audit uses) ──
    var refs = {};
    document.querySelectorAll("script:not([src])").forEach(function (s) {
      var m = (s.textContent || "").match(/["'/](data\/[a-z0-9_\-./]+?\.json)/gi) || [];
      m.forEach(function (x) { refs[x.replace(/^["'/]/, "")] = 1; });
    });
    var refList = Object.keys(refs);
    if (!refList.length) return;

    // ── resolve the engine: any registry entry whose outs[] intersects this page's refs ──
    var entries = Array.isArray(reg.engines) ? reg.engines
                : (reg.engines && typeof reg.engines === "object")
                  ? Object.keys(reg.engines).map(function (k) { var v = reg.engines[k] || {}; v.name = v.name || k; return v; })
                  : (Array.isArray(reg) ? reg : []);
    function stem(s) { return s.replace(/^data\//, "").replace(/\.json$/, "").toLowerCase(); }
    function toks(s) { return stem(s).split(/[-_]+/).filter(Boolean); }
    var pageStem = stem(path.replace(/\.html?$/, ""));
    var pageToks = toks(path.replace(/\.html?$/, ""));
    var best = null, bestScore = 0;
    for (var i = 0; i < entries.length; i++) {
      var outs = entries[i].outs || [];
      for (var j2 = 0; j2 < outs.length; j2++) {
        if (refList.indexOf(outs[j2]) === -1) continue;
        var ot = toks(outs[j2]);
        var shared = pageToks.filter(function (t) { return ot.indexOf(t) !== -1; }).length;
        var initials = ot.map(function (t) { return t[0]; }).join("");
        var sc = shared / Math.max(pageToks.length, ot.length, 1);
        if (initials === pageStem || stem(outs[j2]) === pageStem) sc = 1;
        if (sc > bestScore) { bestScore = sc; best = entries[i]; }
      }
    }
    var engine = bestScore > 0 ? best : null;
    if (!engine) return;                      // no confidently-matched engine — render nothing rather than guess

    // ── related desks: real siblings from this page's manifest category ──
    var cat = null;
    man.categories.forEach(function (c) {
      (c.pages || []).forEach(function (p) {
        if ((p.href || "").replace(/^\//, "") === path) cat = c;
      });
    });
    var siblings = cat ? (cat.pages || [])
      .filter(function (p) { return (p.href || "").replace(/^\//, "") !== path; })
      .slice(0, 5) : [];

    var label = (engine.name || "").replace(/^justhodl-/, "").replace(/-/g, " ")
      .replace(/\b\w/g, function (c) { return c.toUpperCase(); });
    var outPath = (engine.outs && engine.outs[0]) || refList[0];
    var interp = (engine.doc && engine.doc.trim())
      ? engine.doc.trim()
      : "Interpretation notes not yet published for this engine — read the chart and table above for the current signal.";

    fetch("/" + outPath, { cache: "no-store", method: "HEAD" }).then(function (r) {
      var lm = r && r.headers.get("Last-Modified");
      var age = lm ? (Date.now() - new Date(lm).getTime()) / 36e5 : null;
      render(age);
    }).catch(function () { render(null); });

    function render(ageHours) {
      var freshness = ageHours == null ? "freshness unavailable"
        : ageHours < 1 ? "updated " + Math.round(ageHours * 60) + "m ago"
        : ageHours < 48 ? "updated " + Math.round(ageHours) + "h ago"
        : "stale — last update " + Math.round(ageHours) + "h ago";
      var relHtml = siblings.length
        ? siblings.map(function (p) {
            return '<a class="jh-rail-link" href="' + p.href + '">' + (p.name || p.href) + " →</a>";
          }).join("")
        : '<span class="jh-rail-dim">No related pages found in this desk.</span>';

      var html =
        '<aside class="jh-rail" aria-label="Engine detail">' +
          '<div class="jh-rail-sec"><h4>INTERPRETATION</h4><p>' + escapeHtml(interp) + "</p></div>" +
          '<div class="jh-rail-sec"><h4>FEEDS INTO</h4><p>Desk-level reads across the platform roll into ' +
            '<a class="jh-rail-link" href="/signal-board.html">Signal Board</a> and the ' +
            '<a class="jh-rail-link" href="/index.html">KA Index</a>.' +
            (cat ? " Grouped under <b>" + escapeHtml(cat.name || cat.category || "") + "</b>." : "") + "</p></div>" +
          '<div class="jh-rail-sec"><h4>RELATED DESKS</h4><div class="jh-rail-links">' + relHtml + "</div></div>" +
          '<div class="jh-rail-sec"><h4>DATA PROVENANCE</h4><p><b>' + escapeHtml(label) + "</b><br>" +
            "Output: <code>" + escapeHtml(outPath) + "</code><br>" + escapeHtml(freshness) + "</p></div>" +
        "</aside>";

      var mount = document.createElement("div");
      mount.innerHTML = html;
      document.body.appendChild(mount.firstChild);
      document.body.classList.add("jh-has-rail");
    }
  });

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
})();
