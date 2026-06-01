/**
 * ai-name-brief-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Per-name brief renderer. Used for pages with a top-N ticker list
 * (baggers, screeners, eps-velocity, insider clusters, smart money, etc).
 *
 *   <script src="/ai-name-brief-kit.js"></script>
 *   <div id="ai-names"></div>
 *   <script>JHAINameBrief.mount('ai-names', 'baggers-names');</script>
 *
 * The 2nd arg is the basename of the JSON in s3://justhodl-dashboard-live/data/.
 * Reads via the CF data proxy.
 *
 * Expected schema:
 *   { title, regime_note, generated_at,
 *     names: [
 *       {ticker, rank, primary_score, regime_fit, one_liner, thesis,
 *        catalyst, primary_risk,
 *        historical_analog: {ticker, period, what_happened},
 *        confidence, asymmetric_estimate}
 *     ]
 *   }
 */
(function () {
  if (window.JHAINameBrief) return;

  var S3_BASE = "https://justhodl-dashboard-live.s3.amazonaws.com/data";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function jhFetch(slug, suffix) {
    var ts = Date.now();
    var p = S3_BASE + "/" + slug + ".json" + (suffix || "") + (suffix && suffix.indexOf("?") >= 0 ? "&" : "?") + "t=" + ts;
    var f = PROXY   + "/" + slug + ".json" + (suffix || "") + (suffix && suffix.indexOf("?") >= 0 ? "&" : "?") + "t=" + ts;
    return fetch(p).then(function (r) {
      if (r.ok) return r;
      return fetch(f).then(function (r2) {
        if (!r2.ok) throw new Error("Both endpoints failed (S3=" + r.status + ", proxy=" + r2.status + ")");
        return r2;
      });
    });
  }


  function injectCSS() {
    if (document.getElementById("jh-ai-name-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-name-css";
    s.textContent = "\n.jhnb-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.55}\n.jhnb-banner{background:linear-gradient(135deg,rgba(167,139,250,.08),rgba(0,212,255,.04));border:1px solid rgba(167,139,250,.22);border-left:4px solid #a78bfa;border-radius:10px;padding:16px 20px;margin-bottom:14px}\n.jhnb-banner-head{display:flex;align-items:center;gap:12px;margin-bottom:8px;flex-wrap:wrap}\n.jhnb-label{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#a78bfa;font-weight:700}\n.jhnb-title{font-family:ui-monospace,monospace;font-size:14px;font-weight:700;color:#e6eaf2}\n.jhnb-age{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-left:auto}\n.jhnb-regime-note{font-size:13.5px;color:#cbd2dc;line-height:1.55;font-style:italic}\n\n.jhnb-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:11px}\n.jhnb-card{background:#11161f;border:1px solid #1c2433;border-radius:8px;padding:13px 15px;position:relative;transition:border-color 0.15s}\n.jhnb-card:hover{border-color:#2c3a52}\n.jhnb-card.STRONG_FIT{border-left:4px solid #26ffaf}\n.jhnb-card.NEUTRAL{border-left:4px solid #ffd266}\n.jhnb-card.POOR_FIT{border-left:4px solid #ff5577}\n\n.jhnb-head{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:7px;gap:10px}\n.jhnb-ticker{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:16px;font-weight:800;color:#e6eaf2;letter-spacing:0.5px}\n.jhnb-rank{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;background:#1c2433;padding:2px 7px;border-radius:3px;font-weight:600}\n.jhnb-score{font-family:ui-monospace,monospace;font-size:13px;font-weight:700;color:#00d4ff;margin-left:auto}\n.jhnb-fit{font-family:ui-monospace,monospace;font-size:9px;font-weight:700;padding:2px 6px;border-radius:3px;letter-spacing:0.5px;text-transform:uppercase}\n.jhnb-fit.STRONG_FIT{background:rgba(38,255,175,.14);color:#26ffaf}\n.jhnb-fit.NEUTRAL{background:rgba(255,210,102,.14);color:#ffd266}\n.jhnb-fit.POOR_FIT{background:rgba(255,85,119,.14);color:#ff5577}\n\n.jhnb-oneliner{font-size:13px;font-weight:600;color:#e6eaf2;margin-bottom:8px;line-height:1.45}\n.jhnb-thesis{font-size:12.5px;color:#cbd2dc;line-height:1.55;margin-bottom:9px}\n\n.jhnb-row{font-size:11.5px;line-height:1.55;margin:4px 0;display:flex;gap:6px}\n.jhnb-row .l{color:#6f7b91;font-family:ui-monospace,monospace;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;flex-shrink:0;width:55px;padding-top:1px}\n.jhnb-row .v{color:#cbd2dc;flex:1}\n.jhnb-row.cat .v{color:#26ffaf}\n.jhnb-row.risk .v{color:#ff7a18}\n\n.jhnb-analog{margin-top:8px;padding-top:7px;border-top:1px dashed rgba(28,36,51,.8);font-size:11px;color:#a8b3c7}\n.jhnb-analog .ticker{color:#a78bfa;font-family:ui-monospace,monospace;font-weight:700;font-size:11.5px}\n.jhnb-analog .period{color:#6f7b91;font-family:ui-monospace,monospace;font-size:10.5px}\n.jhnb-analog .what{display:block;margin-top:3px;color:#cbd2dc;font-style:italic}\n\n.jhnb-foot{margin-top:9px;padding-top:7px;border-top:1px dashed rgba(28,36,51,.8);display:flex;align-items:center;justify-content:space-between;gap:8px}\n.jhnb-conf{font-family:ui-monospace,monospace;font-size:10px;font-weight:700;padding:2px 6px;border-radius:3px}\n.jhnb-conf.HIGH{background:rgba(38,255,175,.13);color:#26ffaf}\n.jhnb-conf.MEDIUM{background:rgba(255,210,102,.13);color:#ffd266}\n.jhnb-conf.LOW{background:rgba(168,179,199,.10);color:#a8b3c7}\n.jhnb-asym{font-family:ui-monospace,monospace;font-size:11px;color:#00d4ff;font-weight:600;text-align:right;flex:1}\n\n.jhnb-loading{padding:24px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhnb-err{padding:14px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function ageStr(iso) {
    if (!iso) return "";
    var ms = Date.now() - new Date(iso).getTime();
    var m = Math.round(ms / 60000);
    if (m < 1) return "just now";
    if (m < 60) return m + "m ago";
    if (m < 1440) return Math.round(m / 60) + "h ago";
    return Math.round(m / 1440) + "d ago";
  }

  function renderCard(n, idx) {
    var fit = (n.regime_fit || "NEUTRAL").toUpperCase();
    var conf = (n.confidence || "MEDIUM").toUpperCase();
    var analog = n.historical_analog || {};
    return '<div class="jhnb-card ' + fit + '">' +
      '<div class="jhnb-head">' +
        '<span class="jhnb-ticker">' + esc(n.ticker || "?") + '</span>' +
        (n.rank != null ? '<span class="jhnb-rank">#' + esc(n.rank) + '</span>' : '') +
        (n.primary_score != null ? '<span class="jhnb-score">' + esc(n.primary_score) + '</span>' : '') +
        '<span class="jhnb-fit ' + fit + '">' + esc(fit.replace(/_/g, " ")) + '</span>' +
      '</div>' +
      (n.one_liner ? '<div class="jhnb-oneliner">' + esc(n.one_liner) + '</div>' : '') +
      (n.thesis ? '<div class="jhnb-thesis">' + esc(n.thesis) + '</div>' : '') +
      (n.catalyst ? '<div class="jhnb-row cat"><span class="l">catalyst</span><span class="v">' + esc(n.catalyst) + '</span></div>' : '') +
      (n.primary_risk ? '<div class="jhnb-row risk"><span class="l">risk</span><span class="v">' + esc(n.primary_risk) + '</span></div>' : '') +
      (analog.ticker ? '<div class="jhnb-analog">📐 <span class="ticker">' + esc(analog.ticker) + '</span> <span class="period">' + esc(analog.period || '') + '</span>' +
        (analog.what_happened ? '<span class="what">' + esc(analog.what_happened) + '</span>' : '') + '</div>' : '') +
      '<div class="jhnb-foot">' +
        '<span class="jhnb-conf ' + conf + '">conf: ' + esc(conf) + '</span>' +
        (n.asymmetric_estimate ? '<span class="jhnb-asym">' + esc(n.asymmetric_estimate) + '</span>' : '') +
      '</div>' +
    '</div>';
  }

  function mount(elId, contextSlug, opts) {
    injectCSS();
    var el = document.getElementById(elId);
    if (!el) return;
    el.classList.add("jhnb-wrap");
    el.innerHTML = '<div class="jhnb-loading">⚡ generating per-name AI briefs…</div>';

    var url = PROXY + "/" + contextSlug + ".json?t=" + Date.now();
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (b) {
      var names = (b.names || []);
      var html = '';

      html += '<div class="jhnb-banner">' +
                '<div class="jhnb-banner-head">' +
                  '<span class="jhnb-label">⚡ AI Per-Name Briefs</span>' +
                  '<span class="jhnb-title">' + esc(b.title || contextSlug) + '</span>' +
                  '<span class="jhnb-age">' + esc(ageStr(b.generated_at)) + ' · ' + names.length + ' names</span>' +
                '</div>' +
                (b.regime_note ? '<div class="jhnb-regime-note">' + esc(b.regime_note) + '</div>' : '') +
              '</div>';

      if (!names.length) {
        html += '<div class="jhnb-loading">No per-name briefs in this universe yet.</div>';
      } else {
        html += '<div class="jhnb-grid">' + names.map(renderCard).join("") + '</div>';
      }

      el.innerHTML = html;
      return b;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhnb-err">Per-name AI briefs unavailable: ' + esc(e.message || e) + '</div>';
      if (opts && typeof opts.onError === "function") opts.onError(e);
      throw e;
    });
  }

  window.JHAINameBrief = { mount: mount, version: "1.0.0" };
})();
