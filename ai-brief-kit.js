/**
 * ai-brief-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * One-line adoption of the AI Decisive Brief on any page.
 *
 *   <script src="/ai-brief-kit.js"></script>
 *   <div id="ai-brief"></div>
 *   <script>JHAIBrief.mount('ai-brief', 'auction-decisive-call');</script>
 *
 * The 2nd arg is the basename of the JSON in s3://justhodl-dashboard-live/data/.
 * Reads via the CF data proxy for edge cache.
 *
 * Designed for the rich schema produced by justhodl-auction-interpreter and
 * future siblings: regime / confidence / one_liner / thesis /
 * supporting_evidence / historical_analogs / cross_asset / trade_ideas /
 * tripwires / next_auctions_to_watch.
 *
 * Gracefully degrades if any section is missing.
 */
(function () {
  if (window.JHAIBrief) return;

  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  /* ── Styles (injected once) ──────────────────────────────────── */
  function injectCSS() {
    if (document.getElementById("jh-ai-brief-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-brief-css";
    s.textContent = "\n.jhab-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.55}\n.jhab-mono{font-family:ui-monospace,Menlo,Consolas,monospace}\n.jhab-banner{background:linear-gradient(135deg,rgba(0,212,255,.08),rgba(167,139,250,.05));border:1px solid rgba(0,212,255,.22);border-left:4px solid #00d4ff;border-radius:10px;padding:18px 22px;margin-bottom:16px}\n.jhab-banner.RISK_ON_AGGRESSIVE{border-left-color:#26ffaf;background:linear-gradient(135deg,rgba(38,255,175,.10),rgba(38,255,175,.03))}\n.jhab-banner.RISK_ON{border-left-color:#26ffaf;background:linear-gradient(135deg,rgba(38,255,175,.07),rgba(0,212,255,.03))}\n.jhab-banner.NEUTRAL{border-left-color:#ffd266}\n.jhab-banner.RISK_OFF{border-left-color:#ff7a18;background:linear-gradient(135deg,rgba(255,122,24,.08),rgba(255,85,119,.03))}\n.jhab-banner.CRISIS_PREP{border-left-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.10),rgba(255,85,119,.03))}\n.jhab-banner-head{display:flex;align-items:center;gap:12px;margin-bottom:8px;flex-wrap:wrap}\n.jhab-label{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#00d4ff;font-weight:700}\n.jhab-regime-pill{font-family:ui-monospace,monospace;font-size:11px;font-weight:700;padding:3px 10px;border-radius:5px;letter-spacing:0.5px}\n.jhab-conf-pill{font-family:ui-monospace,monospace;font-size:10px;font-weight:600;padding:2px 7px;border-radius:4px;background:rgba(168,179,199,.10);color:#a8b3c7}\n.jhab-conf-pill.HIGH{background:rgba(38,255,175,.14);color:#26ffaf}\n.jhab-conf-pill.MEDIUM{background:rgba(255,210,102,.14);color:#ffd266}\n.jhab-conf-pill.LOW{background:rgba(255,122,24,.14);color:#ff7a18}\n.jhab-one-liner{font-size:16px;font-weight:600;color:#e6eaf2;margin-bottom:10px;line-height:1.4}\n.jhab-thesis{color:#cbd2dc;font-size:13.5px;line-height:1.65}\n.jhab-age{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;margin-left:auto}\n\n.jhab-section{background:#0f131a;border:1px solid #1c2433;border-radius:8px;padding:14px 18px;margin-bottom:12px}\n.jhab-section-head{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.2px;text-transform:uppercase;font-weight:700;color:#a78bfa;margin-bottom:10px;display:flex;justify-content:space-between;align-items:baseline}\n.jhab-section-head .count{color:#6f7b91;font-weight:400;font-size:10px}\n.jhab-list{list-style:none;padding:0;margin:0}\n.jhab-list li{padding:6px 0;border-bottom:1px solid rgba(28,36,51,.6);font-size:13px;line-height:1.55}\n.jhab-list li:last-child{border-bottom:none}\n.jhab-list li .lbl{color:#e6eaf2;font-weight:500}\n.jhab-list li .dat{color:#00d4ff;font-family:ui-monospace,monospace;font-size:11.5px;margin-left:6px}\n\n.jhab-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px}\n.jhab-card{background:#11161f;border:1px solid #1c2433;border-radius:7px;padding:11px 13px}\n.jhab-card .head{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px}\n.jhab-card .name{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;font-weight:700;color:#e6eaf2}\n.jhab-dir{font-family:ui-monospace,monospace;font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px}\n.jhab-dir.BULLISH{background:rgba(38,255,175,.13);color:#26ffaf}\n.jhab-dir.BEARISH{background:rgba(255,85,119,.13);color:#ff5577}\n.jhab-dir.CAUTION{background:rgba(255,122,24,.13);color:#ff7a18}\n.jhab-dir.MIXED{background:rgba(255,210,102,.13);color:#ffd266}\n.jhab-dir.NEUTRAL{background:rgba(168,179,199,.10);color:#a8b3c7}\n.jhab-card .why{color:#a8b3c7;font-size:11.5px;line-height:1.5;margin-bottom:6px}\n.jhab-card .inst{color:#00d4ff;font-family:ui-monospace,monospace;font-size:11px;border-top:1px dashed rgba(28,36,51,.8);padding-top:5px}\n\n.jhab-trade{background:#11161f;border:1px solid #1c2433;border-left:3px solid #a78bfa;border-radius:6px;padding:10px 13px;margin-bottom:8px}\n.jhab-trade .setup{font-size:13px;color:#e6eaf2;margin-bottom:4px;font-weight:500}\n.jhab-trade .meta{font-family:ui-monospace,monospace;font-size:11px;color:#a8b3c7;line-height:1.65}\n.jhab-trade .meta .k{color:#6f7b91;text-transform:uppercase;letter-spacing:0.8px;font-size:10px;margin-right:5px}\n.jhab-trade .meta .v{color:#00d4ff}\n.jhab-trade .meta .rr{color:#26ffaf;font-weight:700}\n\n.jhab-tripwire{background:#11161f;border:1px solid #1c2433;border-radius:6px;padding:10px 13px;margin-bottom:7px;display:flex;gap:10px;align-items:center;font-size:12.5px;line-height:1.5}\n.jhab-sev{font-family:ui-monospace,monospace;font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;flex-shrink:0;align-self:flex-start;margin-top:2px}\n.jhab-sev.LOW{background:rgba(168,179,199,.10);color:#a8b3c7}\n.jhab-sev.MEDIUM{background:rgba(255,210,102,.14);color:#ffd266}\n.jhab-sev.HIGH{background:rgba(255,85,119,.14);color:#ff5577}\n.jhab-tripwire .cond{flex:1;color:#e6eaf2}\n.jhab-tripwire .act{color:#a8b3c7;font-size:11.5px;display:block;margin-top:3px}\n\n.jhab-analog{background:#11161f;border:1px solid #1c2433;border-radius:6px;padding:10px 13px;margin-bottom:7px}\n.jhab-analog .head{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}\n.jhab-analog .period{font-family:ui-monospace,monospace;font-size:12px;font-weight:700;color:#a78bfa}\n.jhab-analog .sim{font-family:ui-monospace,monospace;font-size:10.5px;color:#00d4ff;font-weight:700}\n.jhab-analog .happened, .jhab-analog .expect{font-size:12px;color:#cbd2dc;line-height:1.55}\n.jhab-analog .expect{color:#a8b3c7;margin-top:4px;border-top:1px dashed rgba(28,36,51,.8);padding-top:5px}\n.jhab-analog .expect b{color:#26ffaf;font-weight:600}\n\n.jhab-auction{background:#11161f;border:1px solid #1c2433;border-radius:6px;padding:11px 13px;margin-bottom:7px}\n.jhab-auction .head{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-bottom:5px}\n.jhab-auction .date{font-family:ui-monospace,monospace;font-size:12px;font-weight:700;color:#e6eaf2}\n.jhab-auction .tenor{font-size:11.5px;color:#a78bfa;font-weight:600}\n.jhab-auction .thr{font-family:ui-monospace,monospace;font-size:11px;color:#00d4ff;margin-bottom:5px}\n.jhab-auction .clean, .jhab-auction .dirty{font-size:11.5px;line-height:1.5;color:#a8b3c7}\n.jhab-auction .clean b{color:#26ffaf}.jhab-auction .dirty b{color:#ff5577}\n\n.jhab-pred-section{background:linear-gradient(135deg,rgba(167,139,250,.06),rgba(0,212,255,.03));border:1px solid rgba(167,139,250,.22);border-left:4px solid #a78bfa;border-radius:10px;padding:16px 20px;margin-bottom:14px}\n.jhab-pred-section .jhab-section-head{color:#a78bfa;font-size:12px;margin-bottom:14px}\n.jhab-pred-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(310px,1fr));gap:12px}\n.jhab-pred{background:#11161f;border:1px solid #1c2433;border-radius:8px;padding:13px 15px;position:relative;overflow:hidden}\n.jhab-pred.UPSIDE{border-left:4px solid #26ffaf}\n.jhab-pred.DOWNSIDE{border-left:4px solid #ff5577}\n.jhab-pred.SIDEWAYS{border-left:4px solid #ffd266}\n.jhab-pred-head{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:9px}\n.jhab-pred-asset{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px;font-weight:700;color:#e6eaf2}\n.jhab-pred-ticker{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;background:#1c2433;padding:2px 6px;border-radius:3px;margin-left:6px;font-weight:600}\n.jhab-pred-arrow{font-size:18px;line-height:1;font-weight:700}\n.jhab-pred-arrow.UPSIDE{color:#26ffaf}.jhab-pred-arrow.DOWNSIDE{color:#ff5577}.jhab-pred-arrow.SIDEWAYS{color:#ffd266}\n.jhab-pred-range{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:22px;font-weight:700;line-height:1.2;margin-bottom:3px}\n.jhab-pred-range.UPSIDE{color:#26ffaf}\n.jhab-pred-range.DOWNSIDE{color:#ff5577}\n.jhab-pred-range.SIDEWAYS{color:#ffd266}\n.jhab-pred-horizon{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;letter-spacing:0.5px;text-transform:uppercase;margin-bottom:9px}\n.jhab-pred-meta{display:flex;gap:10px;align-items:center;margin-bottom:9px;font-size:11px;font-family:ui-monospace,monospace}\n.jhab-pred-prob{flex:1;background:#0c1018;border-radius:3px;height:5px;overflow:hidden;position:relative}\n.jhab-pred-prob-fill{height:100%;background:linear-gradient(90deg,#a78bfa,#00d4ff)}\n.jhab-pred-prob-lbl{color:#a8b3c7;font-weight:700;white-space:nowrap}\n.jhab-pred-analog{font-size:11px;color:#a78bfa;font-family:ui-monospace,monospace;margin-bottom:5px;padding-bottom:5px;border-bottom:1px dashed rgba(28,36,51,.8)}\n.jhab-pred-outcome{font-size:11.5px;color:#cbd2dc;line-height:1.5;margin-bottom:8px}\n.jhab-pred-trigs{font-size:11px;line-height:1.5;border-top:1px dashed rgba(28,36,51,.8);padding-top:7px}\n.jhab-pred-trig-up{color:#26ffaf}.jhab-pred-trig-dn{color:#ff5577}\n.jhab-pred-trig-lbl{color:#6f7b91;font-family:ui-monospace,monospace;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;margin-right:4px}\n.jhab-pred-reason{font-size:11px;color:#6f7b91;font-style:italic;margin-top:6px;padding-top:5px;border-top:1px dashed rgba(28,36,51,.8);line-height:1.5}\n\n.jhab-loading{padding:24px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhab-err{padding:14px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }

  /* ── Helpers ─────────────────────────────────────────────────── */
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
  function regimeColor(r) {
    var rr = (r || "").toUpperCase();
    if (rr.indexOf("RISK_ON") === 0) return "background:rgba(38,255,175,.13);color:#26ffaf";
    if (rr === "CRISIS_PREP") return "background:rgba(255,85,119,.13);color:#ff5577";
    if (rr === "RISK_OFF") return "background:rgba(255,122,24,.13);color:#ff7a18";
    return "background:rgba(255,210,102,.13);color:#ffd266";  // NEUTRAL
  }

  /* ── Section renderers ───────────────────────────────────────── */
  function renderEvidence(items) {
    if (!items || !items.length) return "";
    return '<section class="jhab-section">' +
      '<div class="jhab-section-head">Supporting Evidence<span class="count">' + items.length + ' points</span></div>' +
      '<ul class="jhab-list">' +
      items.map(function (e) {
        return '<li><span class="lbl">' + esc(e.point) + '</span>' +
               (e.data ? '<span class="dat">' + esc(e.data) + '</span>' : "") + '</li>';
      }).join("") +
      '</ul></section>';
  }
  function renderAnalogs(items) {
    if (!items || !items.length) return "";
    return '<section class="jhab-section">' +
      '<div class="jhab-section-head">Historical Analogs<span class="count">' + items.length + '</span></div>' +
      items.map(function (a) {
        return '<div class="jhab-analog">' +
               '<div class="head">' +
                  '<span class="period">' + esc(a.period) + '</span>' +
                  '<span class="sim">' + (a.similarity_pct != null ? a.similarity_pct + '% match' : '') + '</span>' +
               '</div>' +
               '<div class="happened">' + esc(a.what_happened) + '</div>' +
               (a.expectation ? '<div class="expect"><b>What to expect →</b> ' + esc(a.expectation) + '</div>' : '') +
               '</div>';
      }).join("") + '</section>';
  }
  function renderCrossAsset(items) {
    if (!items || !items.length) return "";
    return '<section class="jhab-section">' +
      '<div class="jhab-section-head">Cross-Asset Reads<span class="count">' + items.length + ' assets</span></div>' +
      '<div class="jhab-grid">' +
      items.map(function (c) {
        var dir = (c.direction || "NEUTRAL").toUpperCase();
        return '<div class="jhab-card">' +
               '<div class="head"><span class="name">' + esc(c.asset) + '</span>' +
               '<span class="jhab-dir ' + dir + '">' + dir + '</span></div>' +
               '<div class="why">' + esc(c.why) + '</div>' +
               (c.instruments ? '<div class="inst">→ ' + esc(c.instruments) + '</div>' : '') +
               '</div>';
      }).join("") + '</div></section>';
  }
  function renderTrades(items) {
    if (!items || !items.length) return "";
    return '<section class="jhab-section">' +
      '<div class="jhab-section-head">Actionable Trade Ideas<span class="count">' + items.length + '</span></div>' +
      items.map(function (t) {
        return '<div class="jhab-trade">' +
               '<div class="setup">' + esc(t.setup) + '</div>' +
               '<div class="meta">' +
                  '<span class="k">instrument</span><span class="v">' + esc(t.instrument) + '</span>' +
                  ' &nbsp; <span class="k">level</span><span class="v">' + esc(t.level) + '</span>' +
                  (t.risk_reward ? ' &nbsp; <span class="k">R:R</span><span class="rr">' + esc(t.risk_reward) + '</span>' : '') +
                  (t.thesis_link ? '<br><span class="k">why</span>' + esc(t.thesis_link) : '') +
               '</div></div>';
      }).join("") + '</section>';
  }
  function renderTripwires(items) {
    if (!items || !items.length) return "";
    return '<section class="jhab-section">' +
      '<div class="jhab-section-head">Tripwires<span class="count">' + items.length + ' triggers</span></div>' +
      items.map(function (t) {
        var sev = (t.severity || "MEDIUM").toUpperCase();
        return '<div class="jhab-tripwire">' +
               '<span class="jhab-sev ' + sev + '">' + sev + '</span>' +
               '<div><span class="cond">' + esc(t.condition) + '</span>' +
               (t.action ? '<span class="act">→ ' + esc(t.action) + '</span>' : '') +
               '</div></div>';
      }).join("") + '</section>';
  }
  function renderNextAuctions(items) {
    if (!items || !items.length) return "";
    return '<section class="jhab-section">' +
      '<div class="jhab-section-head">Next Auctions To Watch<span class="count">' + items.length + '</span></div>' +
      items.map(function (a) {
        return '<div class="jhab-auction">' +
               '<div class="head"><span class="date">' + esc(a.date) + '</span>' +
               '<span class="tenor">' + esc(a.tenor) + '</span></div>' +
               (a.watch_thresholds ? '<div class="thr">Watch: ' + esc(a.watch_thresholds) + '</div>' : '') +
               (a.clean_signal_means ? '<div class="clean"><b>Clean →</b> ' + esc(a.clean_signal_means) + '</div>' : '') +
               (a.dirty_signal_means ? '<div class="dirty"><b>Dirty →</b> ' + esc(a.dirty_signal_means) + '</div>' : '') +
               '</div>';
      }).join("") + '</section>';
  }

  function fmtRange(lo, hi) {
    function s(n) { return (n > 0 ? "+" : "") + n + "%"; }
    if (lo == null || hi == null) return "—";
    if (lo === hi) return s(lo);
    return s(lo) + " to " + s(hi);
  }
  function renderHistoricalPredictions(items) {
    if (!items || !items.length) return "";
    return '<section class="jhab-pred-section">' +
      '<div class="jhab-section-head">🔮 AI Forward Predictions — Risk Assets &amp; Crypto<span class="count">' +
      items.length + ' assets · based on closest historical analog</span></div>' +
      '<div class="jhab-pred-grid">' +
      items.map(function (p) {
        var dir = (p.prediction_direction || "SIDEWAYS").toUpperCase();
        var arrow = dir === "UPSIDE" ? "▲" : dir === "DOWNSIDE" ? "▼" : "◆";
        var prob = Math.max(0, Math.min(100, Number(p.probability_pct) || 50));
        return '<div class="jhab-pred ' + dir + '">' +
          '<div class="jhab-pred-head">' +
            '<div><span class="jhab-pred-asset">' + esc(p.asset) + '</span>' +
            (p.ticker ? '<span class="jhab-pred-ticker">' + esc(p.ticker) + '</span>' : '') + '</div>' +
            '<div class="jhab-pred-arrow ' + dir + '">' + arrow + '</div>' +
          '</div>' +
          '<div class="jhab-pred-range ' + dir + '">' + fmtRange(p.prediction_range_low_pct, p.prediction_range_high_pct) + '</div>' +
          '<div class="jhab-pred-horizon">' + (p.prediction_horizon_weeks != null ? ('over ' + p.prediction_horizon_weeks + ' weeks') : 'horizon —') +
            ' · conf ' + esc(p.confidence || "—") + '</div>' +
          '<div class="jhab-pred-meta">' +
            '<div class="jhab-pred-prob"><div class="jhab-pred-prob-fill" style="width:' + prob + '%"></div></div>' +
            '<span class="jhab-pred-prob-lbl">' + prob + '% prob</span>' +
          '</div>' +
          (p.best_analog_period ? '<div class="jhab-pred-analog">📐 Analog: ' + esc(p.best_analog_period) + '</div>' : '') +
          (p.analog_outcome_summary ? '<div class="jhab-pred-outcome">' + esc(p.analog_outcome_summary) + '</div>' : '') +
          '<div class="jhab-pred-trigs">' +
            (p.upside_trigger ? '<div><span class="jhab-pred-trig-lbl">upside if</span><span class="jhab-pred-trig-up">' + esc(p.upside_trigger) + '</span></div>' : '') +
            (p.downside_trigger ? '<div style="margin-top:3px"><span class="jhab-pred-trig-lbl">downside ' + (p.downside_scenario_pct != null ? '(' + (p.downside_scenario_pct > 0 ? '+' : '') + p.downside_scenario_pct + '%)' : '') + ' if</span><span class="jhab-pred-trig-dn">' + esc(p.downside_trigger) + '</span></div>' : '') +
          '</div>' +
          (p.key_reasoning ? '<div class="jhab-pred-reason">' + esc(p.key_reasoning) + '</div>' : '') +
        '</div>';
      }).join("") +
      '</div></section>';
  }

  /* ── Main mount ──────────────────────────────────────────────── */
  function mount(elId, contextSlug, opts) {
    injectCSS();
    var el = document.getElementById(elId);
    if (!el) return;
    el.classList.add("jhab-wrap");
    el.innerHTML = '<div class="jhab-loading">⚡ generating institutional brief…</div>';

    var url = PROXY + "/" + contextSlug + ".json?t=" + Date.now();
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (b) {
      var regime = (b.regime || "NEUTRAL").toUpperCase();
      var html = '';

      // Banner
      html += '<div class="jhab-banner ' + regime + '">' +
                '<div class="jhab-banner-head">' +
                  '<span class="jhab-label">⚡ AI Decisive Call</span>' +
                  '<span class="jhab-regime-pill" style="' + regimeColor(regime) + '">' + esc(regime.replace(/_/g, " ")) + '</span>' +
                  '<span class="jhab-conf-pill ' + (b.confidence || "") + '">conf: ' + esc(b.confidence || "—") + '</span>' +
                  '<span class="jhab-age">' + esc(ageStr(b.generated_at)) + '</span>' +
                '</div>' +
                '<div class="jhab-one-liner">' + esc(b.one_liner || "") + '</div>' +
                (b.thesis ? '<div class="jhab-thesis">' + esc(b.thesis) + '</div>' : '') +
              '</div>';

      html += renderHistoricalPredictions(b.historical_predictions);
      html += renderEvidence(b.supporting_evidence);
      html += renderCrossAsset(b.cross_asset);
      html += renderTrades(b.trade_ideas);
      html += renderTripwires(b.tripwires);
      html += renderAnalogs(b.historical_analogs);
      html += renderNextAuctions(b.next_auctions_to_watch);

      el.innerHTML = html;
      return b;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhab-err">AI brief unavailable: ' + esc(e.message || e) +
                      ' &nbsp;<small style="color:#6f7b91">(falling back to static call)</small></div>';
      if (opts && typeof opts.onError === "function") opts.onError(e);
      throw e;
    });
  }

  window.JHAIBrief = { mount: mount, version: "1.0.0" };
})();
