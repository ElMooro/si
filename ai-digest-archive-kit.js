/**
 * ai-digest-archive-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Daily digest archive renderer. Reads the rolling index file and
 * renders one card per past digest with activity-level badge + score
 * pills + expand-to-see-message-preview interaction.
 *
 *   <script src="/ai-digest-archive-kit.js"></script>
 *   <div id="ai-digest-archive"></div>
 *   <script>JHAIDigest.mount('ai-digest-archive');</script>
 */
(function () {
  if (window.JHAIDigest) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function injectCSS() {
    if (document.getElementById("jh-ai-digest-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-digest-css";
    s.textContent = "\n.jhdg-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jhdg-summary{background:#0a0f17;border:1px solid #1c2433;border-radius:10px;padding:14px 18px;margin-bottom:14px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}\n.jhdg-summary-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#a78bfa;font-weight:800}\n.jhdg-summary .stat{font-family:ui-monospace,monospace;font-size:11px;color:#a8b3c7;padding:3px 9px;background:#11161f;border:1px solid #1c2433;border-radius:4px}\n.jhdg-summary .stat b{color:#e6eaf2}\n.jhdg-summary .stat.extreme b{color:#ff5577}\n.jhdg-summary .stat.active b{color:#ff7a18}\n.jhdg-summary .stat.quiet b{color:#26ffaf}\n\n.jhdg-card{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:14px 18px;margin-bottom:9px;border-left:5px solid #6f7b91;transition:all 0.15s}\n.jhdg-card.QUIET{border-left-color:#26ffaf}\n.jhdg-card.ACTIVE{border-left-color:#ff7a18}\n.jhdg-card.EXTREME{border-left-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.05),transparent 40%)}\n.jhdg-card:hover{border-color:#2a3548}\n\n.jhdg-card-hdr{display:flex;align-items:baseline;gap:10px;cursor:pointer;flex-wrap:wrap}\n.jhdg-card-date{font-family:ui-monospace,monospace;font-size:14px;font-weight:800;color:#e6eaf2;letter-spacing:0.3px}\n.jhdg-card-dow{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;letter-spacing:0.5px;text-transform:uppercase}\n.jhdg-card-activity{font-family:ui-monospace,monospace;font-size:10px;font-weight:800;padding:3px 9px;border-radius:3px;letter-spacing:0.8px}\n.jhdg-card-activity.QUIET{background:rgba(38,255,175,.13);color:#26ffaf}\n.jhdg-card-activity.ACTIVE{background:rgba(255,122,24,.14);color:#ff7a18}\n.jhdg-card-activity.EXTREME{background:rgba(255,85,119,.18);color:#ff5577;animation:jhdg-pulse 1.6s ease-in-out infinite}\n@keyframes jhdg-pulse{0%,100%{box-shadow:0 0 0 0 rgba(255,85,119,.3)}50%{box-shadow:0 0 12px 2px rgba(255,85,119,.3)}}\n.jhdg-card-scores{margin-left:auto;display:flex;gap:8px;font-family:ui-monospace,monospace;font-size:11px;color:#a8b3c7;flex-wrap:wrap}\n.jhdg-card-scores .pill{padding:2px 8px;background:#11161f;border:1px solid #1c2433;border-radius:3px;display:inline-flex;gap:5px;align-items:center}\n.jhdg-card-scores .pill .lbl{color:#6f7b91;font-size:9.5px;letter-spacing:0.5px;text-transform:uppercase}\n.jhdg-card-scores .pill .val{color:#e6eaf2;font-weight:800}\n.jhdg-card-scores .pill.eq{border-left:2px solid #ff7a18}\n.jhdg-card-scores .pill.mc{border-left:2px solid #22d3ee}\n.jhdg-card-scores .pill .regime{font-size:9.5px;font-weight:800;padding:0 4px;border-radius:2px;letter-spacing:0.3px}\n.jhdg-card-scores .pill .regime.NORMAL{background:rgba(38,255,175,.12);color:#26ffaf}\n.jhdg-card-scores .pill .regime.ELEVATED{background:rgba(255,122,24,.13);color:#ff7a18}\n.jhdg-card-scores .pill .regime.EXTREME{background:rgba(255,85,119,.18);color:#ff5577}\n.jhdg-card-alerts{font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;padding:2px 8px;background:#11161f;border:1px solid #1c2433;border-radius:3px}\n.jhdg-card-alerts b{color:#ff7a18}\n.jhdg-card-alerts.quiet{color:#26ffaf;background:rgba(38,255,175,.06);border-color:rgba(38,255,175,.18)}\n.jhdg-card-toggle{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;margin-left:6px}\n\n.jhdg-card-body{display:none;margin-top:11px;padding-top:11px;border-top:1px solid #1c2433}\n.jhdg-card.open .jhdg-card-body{display:block}\n.jhdg-card.open .jhdg-card-toggle::after{content:' ▾'}\n.jhdg-card:not(.open) .jhdg-card-toggle::after{content:' ▸'}\n.jhdg-card-msg{font-family:ui-monospace,monospace;font-size:11px;color:#cbd2dc;line-height:1.6;white-space:pre-wrap;background:#11161f;border:1px solid #1c2433;border-radius:5px;padding:11px 13px;max-height:340px;overflow-y:auto}\n.jhdg-card-msg b{color:#e6eaf2;font-weight:700}\n.jhdg-card-meta{margin-top:7px;font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;display:flex;gap:14px;flex-wrap:wrap}\n.jhdg-card-meta a{color:#22d3ee;text-decoration:none}\n.jhdg-card-meta a:hover{text-decoration:underline}\n\n.jhdg-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhdg-empty{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px;background:#0a0f17;border:1px dashed #1c2433;border-radius:9px}\n.jhdg-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");}
  function dow(dateStr) {
    if (!dateStr) return "";
    var parts = dateStr.split("-");
    if (parts.length !== 3) return "";
    var d = new Date(Date.UTC(+parts[0], +parts[1]-1, +parts[2]));
    if (isNaN(d.getTime())) return "";
    return ["sun","mon","tue","wed","thu","fri","sat"][d.getUTCDay()];
  }
  function fetchJSON(path) {
    var url = PROXY + "/" + path + "?t=" + Date.now();
    return fetch(url).then(function (r) { return r.ok ? r.json() : null; })
                     .catch(function () { return null; });
  }

  // Convert Telegram-flavored Markdown to plain text for the expand panel
  function markdownToText(md) {
    if (!md) return "";
    return md.replace(/\*([^*]+)\*/g, function (m, p1) { return p1; }) // strip bold *...*
             .replace(/`([^`]+)`/g, function (m, p1) { return p1; }); // strip inline `code`
  }

  function renderCard(entry, fullAudit) {
    var act = (entry.activity_level || "QUIET").toUpperCase();
    var n_alerts = (entry.n_equity_alerts_today || 0) + (entry.n_macro_alerts_today || 0);
    var quietClass = n_alerts === 0 ? " quiet" : "";

    var eqRegime = (entry.equity_regime || "—").toUpperCase();
    var mcRegime = (entry.macro_regime  || "—").toUpperCase();

    var html = '<div class="jhdg-card ' + act + '" data-key="' + esc(entry.key) + '">' +
                 '<div class="jhdg-card-hdr">' +
                   '<span class="jhdg-card-date">' + esc(entry.date) + '</span>' +
                   '<span class="jhdg-card-dow">' + esc(dow(entry.date)) + '</span>' +
                   '<span class="jhdg-card-activity ' + act + '">' + esc(act) + '</span>' +
                   '<div class="jhdg-card-scores">' +
                     '<span class="pill eq">' +
                       '<span class="lbl">🎯 EQ</span>' +
                       '<span class="val">' + esc(entry.equity_score == null ? "—" : entry.equity_score) + '</span>' +
                       '<span class="regime ' + eqRegime + '">' + esc(eqRegime) + '</span>' +
                     '</span>' +
                     '<span class="pill mc">' +
                       '<span class="lbl">🏛 MC</span>' +
                       '<span class="val">' + esc(entry.macro_score == null ? "—" : entry.macro_score) + '</span>' +
                       '<span class="regime ' + mcRegime + '">' + esc(mcRegime) + '</span>' +
                     '</span>' +
                     '<span class="jhdg-card-alerts' + quietClass + '">' +
                       (n_alerts === 0 ? '✓ no alerts' : '<b>' + n_alerts + '</b> alert' + (n_alerts === 1 ? '' : 's')) +
                     '</span>' +
                   '</div>' +
                   '<span class="jhdg-card-toggle"></span>' +
                 '</div>' +
                 '<div class="jhdg-card-body">' +
                   '<div class="jhdg-card-msg" data-loaded="0">loading message…</div>' +
                   '<div class="jhdg-card-meta">' +
                     '<span>generated: ' + esc(entry.generated_at) + '</span>' +
                     '<span>telegram: ' + (entry.telegram_ok ? '✓ delivered' : '✗ failed') + '</span>' +
                     '<span>chars: ' + esc(entry.message_chars || '—') + '</span>' +
                   '</div>' +
                 '</div>' +
               '</div>';
    return html;
  }

  function attachClicks(el) {
    el.querySelectorAll(".jhdg-card-hdr").forEach(function (hdr) {
      hdr.addEventListener("click", function () {
        var card = hdr.parentElement;
        var was_open = card.classList.contains("open");
        card.classList.toggle("open");
        if (!was_open) {
          // Lazy-load the message preview when expanding
          var msgEl = card.querySelector(".jhdg-card-msg");
          if (msgEl && msgEl.getAttribute("data-loaded") === "0") {
            var key = card.getAttribute("data-key"); // e.g. data/_alerts/digest-2026-05-30.json
            if (key && key.indexOf("data/") === 0) {
              var path = key.substring(5); // strip 'data/' since proxy prepends it
              fetchJSON(path).then(function (audit) {
                if (audit && audit.message_preview) {
                  msgEl.textContent = markdownToText(audit.message_preview);
                  msgEl.setAttribute("data-loaded", "1");
                } else {
                  msgEl.textContent = "(no message preview available)";
                }
              });
            }
          }
        }
      });
    });
  }

  function mount(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhdg-wrap");
    el.innerHTML = '<div class="jhdg-loading">📂 loading digest archive…</div>';

    return fetchJSON("_alerts/digests-index.json").then(function (idx) {
      if (!idx || !idx.entries || !idx.entries.length) {
        el.innerHTML = '<div class="jhdg-empty">No digests yet — the archive will populate as the daily 9am UTC cycle runs. ' +
                        'Today\'s test digest should appear here once it completes. Hit refresh in a minute.</div>';
        return idx;
      }
      var n = idx.entries.length;
      var bd = idx.activity_breakdown || {};
      var html = '<div class="jhdg-summary">' +
                   '<span class="jhdg-summary-h">📂 ' + esc(n) + ' digest' + (n === 1 ? '' : 's') + ' archived</span>' +
                   '<span class="stat">range: <b>' + esc(idx.earliest_date) + ' → ' + esc(idx.latest_date) + '</b></span>' +
                   '<span class="stat extreme">extreme: <b>' + esc(bd.extreme || 0) + '</b></span>' +
                   '<span class="stat active">active: <b>' + esc(bd.active || 0) + '</b></span>' +
                   '<span class="stat quiet">quiet: <b>' + esc(bd.quiet || 0) + '</b></span>' +
                 '</div>';

      idx.entries.forEach(function (e) {
        html += renderCard(e);
      });
      el.innerHTML = html;
      attachClicks(el);
      return idx;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhdg-err">Archive unavailable: ' + esc(e.message || e) + '</div>';
      throw e;
    });
  }

  window.JHAIDigest = { mount: mount, version: "1.0.0" };
})();
