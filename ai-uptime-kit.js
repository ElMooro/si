/**
 * ai-uptime-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Renders the system-uptime status grid. Reads
 * data/_alerts/uptime-status.json (maintained by the router's hourly
 * uptime check). One traffic-light row per monitored brief:
 *   FRESH    = green
 *   WARNING  = yellow (>75% of max_age, approaching threshold)
 *   STALE    = red (> max_age)
 *   MISSING  = red striped (file not found)
 *
 *   <script src="/ai-uptime-kit.js"></script>
 *   <div id="ai-uptime"></div>
 *   <script>JHAIUptime.mount('ai-uptime');</script>
 */
(function () {
  if (window.JHAIUptime) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function injectCSS() {
    if (document.getElementById("jh-ai-uptime-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-uptime-css";
    s.textContent = "\n.jhup-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jhup-overall{background:#0a0f17;border:1px solid #1c2433;border-radius:10px;padding:16px 20px;margin-bottom:14px;display:flex;align-items:center;gap:18px;flex-wrap:wrap;border-left:6px solid #6f7b91}\n.jhup-overall.HEALTHY{border-left-color:#26ffaf;background:linear-gradient(135deg,rgba(38,255,175,.04),transparent 40%)}\n.jhup-overall.WARNING{border-left-color:#fbbf24;background:linear-gradient(135deg,rgba(251,191,36,.06),transparent 40%)}\n.jhup-overall.STALE{border-left-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.06),transparent 40%);animation:jhup-pulse 2s ease-in-out infinite}\n@keyframes jhup-pulse{0%,100%{box-shadow:0 0 0 0 rgba(255,85,119,.0)}50%{box-shadow:0 0 22px 3px rgba(255,85,119,.18)}}\n.jhup-overall-light{width:22px;height:22px;border-radius:50%;background:#6f7b91;flex-shrink:0;position:relative}\n.jhup-overall.HEALTHY .jhup-overall-light{background:#26ffaf;box-shadow:0 0 14px rgba(38,255,175,.45)}\n.jhup-overall.WARNING .jhup-overall-light{background:#fbbf24;box-shadow:0 0 14px rgba(251,191,36,.45)}\n.jhup-overall.STALE .jhup-overall-light{background:#ff5577;box-shadow:0 0 14px rgba(255,85,119,.55)}\n.jhup-overall-status{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:15px;font-weight:800;letter-spacing:0.6px;color:#e6eaf2}\n.jhup-overall.HEALTHY .jhup-overall-status{color:#26ffaf}\n.jhup-overall.WARNING .jhup-overall-status{color:#fbbf24}\n.jhup-overall.STALE .jhup-overall-status{color:#ff5577}\n.jhup-overall-detail{font-family:ui-monospace,monospace;font-size:12px;color:#a8b3c7;flex:1;min-width:0}\n.jhup-overall-detail b{color:#e6eaf2}\n.jhup-overall-check{font-family:ui-monospace,monospace;font-size:11px;color:#6f7b91;text-align:right;flex-shrink:0}\n.jhup-overall-check b{color:#cbd2dc}\n\n.jhup-counts{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:9px;margin-bottom:14px}\n.jhup-count-cell{background:#0a0f17;border:1px solid #1c2433;border-radius:7px;padding:9px 14px;border-left:4px solid #6f7b91}\n.jhup-count-cell.fresh{border-left-color:#26ffaf}\n.jhup-count-cell.warning{border-left-color:#fbbf24}\n.jhup-count-cell.stale{border-left-color:#ff5577}\n.jhup-count-cell.missing{border-left-color:#ff5577}\n.jhup-count-cell .lbl{font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:3px}\n.jhup-count-cell .val{font-family:ui-monospace,monospace;font-size:22px;font-weight:800;color:#e6eaf2;line-height:1.1}\n.jhup-count-cell.fresh .val{color:#26ffaf}\n.jhup-count-cell.warning .val{color:#fbbf24}\n.jhup-count-cell.stale .val{color:#ff5577}\n.jhup-count-cell.missing .val{color:#ff5577}\n\n.jhup-brief-grid{background:#0a0f17;border:1px solid #1c2433;border-radius:10px;overflow:hidden}\n.jhup-row{display:grid;grid-template-columns:14px 1fr 100px 80px 100px;gap:14px;align-items:center;padding:12px 16px;border-bottom:1px solid #1c2433;font-family:ui-monospace,monospace;font-size:12.5px}\n.jhup-row:last-child{border-bottom:none}\n.jhup-row:hover{background:#11161f}\n@media (max-width:680px){.jhup-row{grid-template-columns:14px 1fr auto;gap:9px;font-size:11.5px}.jhup-row .age,.jhup-row .threshold,.jhup-row .gen-at{display:none}}\n.jhup-dot{width:11px;height:11px;border-radius:50%;flex-shrink:0;box-shadow:0 0 0 2px #0a0f17}\n.jhup-dot.FRESH{background:#26ffaf;box-shadow:0 0 0 2px #0a0f17,0 0 6px rgba(38,255,175,.5)}\n.jhup-dot.WARNING{background:#fbbf24;box-shadow:0 0 0 2px #0a0f17,0 0 6px rgba(251,191,36,.5)}\n.jhup-dot.STALE{background:#ff5577;box-shadow:0 0 0 2px #0a0f17,0 0 8px rgba(255,85,119,.65);animation:jhup-dot-pulse 1.6s ease-in-out infinite}\n.jhup-dot.MISSING{background:#ff5577;box-shadow:0 0 0 2px #0a0f17,0 0 0 3px #ff5577 inset}\n@keyframes jhup-dot-pulse{0%,100%{box-shadow:0 0 0 2px #0a0f17,0 0 6px rgba(255,85,119,.4)}50%{box-shadow:0 0 0 2px #0a0f17,0 0 14px rgba(255,85,119,.85)}}\n.jhup-label{color:#e6eaf2;font-weight:700;display:flex;align-items:center;gap:7px;min-width:0}\n.jhup-label .key{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;letter-spacing:0.3px;font-weight:400;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n.jhup-age{text-align:right;font-weight:800}\n.jhup-age.FRESH{color:#26ffaf}\n.jhup-age.WARNING{color:#fbbf24}\n.jhup-age.STALE{color:#ff5577;animation:jhup-pulse 1.6s ease-in-out infinite}\n.jhup-age.MISSING{color:#ff5577;font-style:italic}\n.jhup-threshold{color:#6f7b91;text-align:right;font-size:11px}\n.jhup-threshold b{color:#a8b3c7}\n.jhup-gen-at{color:#6f7b91;text-align:right;font-size:10.5px}\n\n.jhup-header{display:grid;grid-template-columns:14px 1fr 100px 80px 100px;gap:14px;padding:8px 16px;font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;border-bottom:1px solid #1c2433;background:#11161f}\n@media (max-width:680px){.jhup-header .col-age,.jhup-header .col-thresh,.jhup-header .col-gen{display:none}}\n.jhup-header .col-thresh,.jhup-header .col-age,.jhup-header .col-gen{text-align:right}\n\n.jhup-meta-block{margin-top:14px;font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;padding:10px 14px;background:#0d1219;border:1px dashed #1c2433;border-radius:5px;line-height:1.65}\n.jhup-meta-block b{color:#cbd2dc}\n.jhup-meta-block code{font-family:ui-monospace,monospace;font-size:10.5px;color:#22d3ee;background:#11161f;padding:1px 6px;border-radius:3px}\n\n.jhup-empty{padding:25px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:11.5px;background:#11161f;border:1px dashed #1c2433;border-radius:6px}\n.jhup-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhup-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");}
  function fmtAge(h) {
    if (h == null) return "missing";
    if (h < 1) return Math.round(h * 60) + "m";
    if (h < 24) return h.toFixed(1) + "h";
    return (h / 24).toFixed(1) + "d";
  }
  function ageRelative(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      if (isNaN(d.getTime())) return "—";
      var ms = Date.now() - d.getTime();
      if (ms < 60000) return "just now";
      if (ms < 3600000) return Math.round(ms/60000) + "m ago";
      if (ms < 86400000) return Math.round(ms/3600000) + "h ago";
      return Math.round(ms/86400000) + "d ago";
    } catch (e) { return "—"; }
  }
  function fetchJSON(path) {
    var url = PROXY + "/" + path + "?t=" + Date.now();
    return fetch(url).then(function (r) { return r.ok ? r.json() : null; })
                     .catch(function () { return null; });
  }

  function mount(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhup-wrap");
    el.innerHTML = '<div class="jhup-loading">🔌 loading uptime status…</div>';

    return fetchJSON("_alerts/uptime-status.json").then(function (doc) {
      if (!doc) {
        el.innerHTML = '<div class="jhup-empty">No uptime status yet — populates on the first hourly check. ' +
                       'EventBridge rule fires <code>cron(0 * * * ? *)</code>.</div>';
        return null;
      }

      var overall = (doc.overall_status || "HEALTHY").toUpperCase();
      var checkedAt = doc.checked_at;
      var briefs = doc.briefs || [];

      var html = "";

      // Overall status banner
      var overallLabel = overall === "HEALTHY" ? "🟢 ALL SYSTEMS HEALTHY"
                        : overall === "WARNING" ? "🟡 WARNING — approaching stale threshold"
                        : "🔴 STALE — briefs not refreshing";
      html += '<div class="jhup-overall ' + overall + '">' +
                '<div class="jhup-overall-light"></div>' +
                '<div>' +
                  '<div class="jhup-overall-status">' + esc(overallLabel) + '</div>' +
                  '<div class="jhup-overall-detail">' +
                    esc(doc.n_fresh || 0) + ' fresh · ' +
                    esc(doc.n_warning || 0) + ' warning · ' +
                    esc(doc.n_stale || 0) + ' stale · ' +
                    esc(doc.n_missing || 0) + ' missing &nbsp;·&nbsp; ' +
                    esc(doc.n_monitored || 0) + ' briefs monitored' +
                  '</div>' +
                '</div>' +
                '<div class="jhup-overall-check">' +
                  'last check<br><b>' + esc(ageRelative(checkedAt)) + '</b>' +
                '</div>' +
              '</div>';

      // Count cells
      html += '<div class="jhup-counts">' +
                '<div class="jhup-count-cell fresh"><div class="lbl">fresh</div><div class="val">' + esc(doc.n_fresh || 0) + '</div></div>' +
                '<div class="jhup-count-cell warning"><div class="lbl">warning</div><div class="val">' + esc(doc.n_warning || 0) + '</div></div>' +
                '<div class="jhup-count-cell stale"><div class="lbl">stale</div><div class="val">' + esc(doc.n_stale || 0) + '</div></div>' +
                '<div class="jhup-count-cell missing"><div class="lbl">missing</div><div class="val">' + esc(doc.n_missing || 0) + '</div></div>' +
              '</div>';

      // Per-brief detail grid
      html += '<div class="jhup-brief-grid">';
      html += '<div class="jhup-header">' +
                '<div></div>' +
                '<div>brief</div>' +
                '<div class="col-age">age</div>' +
                '<div class="col-thresh">threshold</div>' +
                '<div class="col-gen">last seen</div>' +
              '</div>';

      // Stale first (urgency), then warning, then fresh
      var sorted = briefs.slice().sort(function (a, b) {
        var pri = {"MISSING":0, "STALE":1, "WARNING":2, "FRESH":3};
        return (pri[a.status] || 9) - (pri[b.status] || 9);
      });

      sorted.forEach(function (b) {
        var status = b.status || "FRESH";
        var ageStr = status === "MISSING" ? "missing" : fmtAge(b.age_hours);
        html += '<div class="jhup-row">' +
                  '<div class="jhup-dot ' + status + '"></div>' +
                  '<div class="jhup-label">' +
                    '<span>' + esc(b.label) + '</span>' +
                    '<span class="key">' + esc(b.key) + '</span>' +
                  '</div>' +
                  '<div class="jhup-age ' + status + ' age">' + esc(ageStr) + '</div>' +
                  '<div class="jhup-threshold threshold">≤ <b>' + esc(b.max_age_hours) + 'h</b></div>' +
                  '<div class="jhup-gen-at gen-at">' +
                    (b.generated_at ? esc(ageRelative(b.generated_at)) : '—') +
                  '</div>' +
                '</div>';
      });
      html += '</div>';

      // Legend / methodology
      html += '<div class="jhup-meta-block">' +
                '<b>Status thresholds (per brief):</b>' +
                ' 🟢 FRESH if age &lt; 75% of max_age_hours · 🟡 WARNING if 75-100% · 🔴 STALE if &gt; 100% · MISSING if file not found.' +
                '<br><b>Alert rules:</b> STALE_NEW fires on any brief transitioning FRESH→STALE. ' +
                'STALE_REMINDER fires every 4h while staleness persists. ALL_CLEAR fires when all briefs return to FRESH.' +
                '<br><b>Heartbeat cadence:</b> hourly <code>cron(0 * * * ? *)</code> via EventBridge → router with <code>{"contexts":["system-uptime-monitor"]}</code>.' +
                '<br><b>State:</b> <code>data/_alerts/uptime-status.json</code> (display) · <code>data/_alerts/uptime-state.json</code> (alert dedup).' +
              '</div>';

      el.innerHTML = html;
      return doc;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhup-err">Uptime status unavailable: ' + esc(e.message || e) + '</div>';
      throw e;
    });
  }

  window.JHAIUptime = { mount: mount, version: "1.0.0" };
})();
