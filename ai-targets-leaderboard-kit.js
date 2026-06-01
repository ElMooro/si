/**
 * ai-targets-leaderboard-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Sustained-target leaderboard. Reads data/_alerts/targets-index.json
 * (30-day rolling aggregation built by the digest function from each
 * sniffer's 7-day stats) and renders two side-by-side leaderboards:
 *   - 🎯 EQUITY: top assets by n_digest_appearances DESC
 *   - 🏛 MACRO:  top instruments by n_digest_appearances DESC
 *
 * Each row shows: rank, name, appearance bar, max events/window,
 * first-seen, last-seen, recent indicator (last 7d).
 *
 *   <script src="/ai-targets-leaderboard-kit.js"></script>
 *   <div id="ai-targets"></div>
 *   <script>JHAITargets.mount('ai-targets');</script>
 */
(function () {
  if (window.JHAITargets) return;
  var S3_BASE = "https://justhodl-dashboard-live.s3.amazonaws.com/data";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function jhFetch(path) {
    var ts = "?t=" + Date.now();
    var sep = path.indexOf("?") >= 0 ? "&t=" + Date.now() : ts;
    var p = S3_BASE + "/" + path + sep;
    var f = PROXY   + "/" + path + sep;
    return fetch(p).then(function (r) {
      if (r.ok) return r;
      return fetch(f).then(function (r2) {
        if (!r2.ok) throw new Error("Both endpoints failed (S3=" + r.status + ", proxy=" + r2.status + ")");
        return r2;
      });
    });
  }


  function injectCSS() {
    if (document.getElementById("jh-ai-targets-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-targets-css";
    s.textContent = "\n.jhtg-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jhtg-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#a78bfa;margin:22px 0 10px;font-weight:700;padding-bottom:5px;border-bottom:1px solid rgba(167,139,250,.18)}\n\n.jhtg-summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:18px}\n.jhtg-stat{background:#0a0f17;border:1px solid #1c2433;border-left:4px solid #6f7b91;border-radius:7px;padding:10px 14px}\n.jhtg-stat.equity{border-left-color:#ff7a18}\n.jhtg-stat.macro{border-left-color:#22d3ee}\n.jhtg-stat.recent{border-left-color:#26ffaf}\n.jhtg-stat.purple{border-left-color:#a78bfa}\n.jhtg-stat .lbl{font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:4px}\n.jhtg-stat .val{font-family:ui-monospace,monospace;font-size:22px;font-weight:800;color:#e6eaf2;line-height:1.1}\n.jhtg-stat.equity .val{color:#ff7a18}\n.jhtg-stat.macro .val{color:#22d3ee}\n.jhtg-stat.recent .val{color:#26ffaf}\n.jhtg-stat.purple .val{color:#a78bfa}\n.jhtg-stat .sub{font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;margin-top:3px}\n\n.jhtg-cols{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}\n@media (max-width:880px){.jhtg-cols{grid-template-columns:1fr}}\n\n.jhtg-board{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:14px 16px}\n.jhtg-board.equity{border-left:5px solid #ff7a18}\n.jhtg-board.macro{border-left:5px solid #22d3ee}\n.jhtg-board-hdr{display:flex;align-items:baseline;gap:11px;margin-bottom:11px;flex-wrap:wrap;padding-bottom:7px;border-bottom:1px solid #1c2433}\n.jhtg-board-title{font-family:ui-monospace,monospace;font-size:12px;font-weight:800;letter-spacing:0.4px}\n.jhtg-board.equity .jhtg-board-title{color:#ff7a18}\n.jhtg-board.macro  .jhtg-board-title{color:#22d3ee}\n.jhtg-board-count{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;margin-left:auto;letter-spacing:0.4px}\n\n.jhtg-row{display:grid;grid-template-columns:28px 1fr auto;gap:9px;align-items:center;padding:7px 0;border-bottom:1px solid rgba(28,36,51,.5);font-family:ui-monospace,monospace;font-size:12px;position:relative;cursor:default;transition:background 0.1s}\n.jhtg-row:last-child{border-bottom:none}\n.jhtg-row:hover{background:rgba(167,139,250,.04)}\n.jhtg-rank{font-weight:800;color:#a78bfa;text-align:right}\n.jhtg-rank.first{color:#ff5577}\n.jhtg-rank.second{color:#ff7a18}\n.jhtg-rank.third{color:#fbbf24}\n.jhtg-name{color:#e6eaf2;font-weight:700;display:flex;align-items:center;gap:7px;min-width:0}\n.jhtg-name .nm{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n.jhtg-name .rec{font-size:8.5px;font-weight:800;padding:1px 5px;border-radius:2px;background:rgba(38,255,175,.13);color:#26ffaf;letter-spacing:0.5px;flex-shrink:0}\n.jhtg-name .stale{font-size:8.5px;font-weight:800;padding:1px 5px;border-radius:2px;background:rgba(111,123,145,.13);color:#6f7b91;letter-spacing:0.5px;flex-shrink:0}\n.jhtg-row.stale .jhtg-name .nm{color:#a8b3c7}\n.jhtg-row.stale{opacity:0.65}\n.jhtg-bar-cell{display:flex;align-items:center;gap:8px;font-size:11px;color:#a8b3c7;min-width:0}\n.jhtg-bar{flex:1;height:6px;background:#11161f;border-radius:3px;overflow:hidden;min-width:30px;max-width:90px}\n.jhtg-bar-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,#a78bfa,#ff7a18);transition:width 0.3s}\n.jhtg-board.macro .jhtg-bar-fill{background:linear-gradient(90deg,#a78bfa,#22d3ee)}\n.jhtg-row.stale .jhtg-bar-fill{background:#3a4458}\n.jhtg-bar-val{font-weight:800;color:#e6eaf2;font-size:11px;min-width:18px;text-align:right}\n.jhtg-meta{font-size:10px;color:#6f7b91;display:flex;flex-direction:column;gap:1px;text-align:right;line-height:1.3;flex-shrink:0}\n.jhtg-meta b{color:#cbd2dc}\n\n.jhtg-empty{padding:25px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:11.5px;background:#11161f;border:1px dashed #1c2433;border-radius:6px}\n.jhtg-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhtg-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n\n.jhtg-meta-block{margin-top:6px;font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;padding:9px 12px;background:#0d1219;border:1px dashed #1c2433;border-radius:5px;line-height:1.6}\n.jhtg-meta-block b{color:#cbd2dc}\n";
    document.head.appendChild(s);
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");}
  function fetchJSON(path) {
    return jhFetch(path).then(function (r) { return r.ok ? r.json() : null; })
                     .catch(function () { return null; });
  }
  function daysBetween(dateStr) {
    if (!dateStr) return 0;
    var parts = dateStr.split("-"); if (parts.length !== 3) return 0;
    var d = new Date(Date.UTC(+parts[0], +parts[1]-1, +parts[2]));
    var ms = Date.now() - d.getTime();
    return Math.max(0, Math.round(ms / 86400000));
  }

  function renderBoard(boardClass, title, entries, keyField, maxN) {
    if (!entries || !entries.length) {
      return '<div class="jhtg-board ' + boardClass + '">' +
               '<div class="jhtg-board-hdr">' +
                 '<div class="jhtg-board-title">' + esc(title) + '</div>' +
                 '<div class="jhtg-board-count">0 targets tracked</div>' +
               '</div>' +
               '<div class="jhtg-empty">No targets accumulated yet — the leaderboard fills in as digests run.<br>Each daily digest contributes one observation per top-targeted asset/instrument.</div>' +
             '</div>';
    }

    var html = '<div class="jhtg-board ' + boardClass + '">';
    html += '<div class="jhtg-board-hdr">' +
              '<div class="jhtg-board-title">' + esc(title) + '</div>' +
              '<div class="jhtg-board-count">' + esc(entries.length) + ' tracked · top ' + esc(Math.min(entries.length, 20)) + '</div>' +
            '</div>';

    var topN = entries.slice(0, 20);
    topN.forEach(function (e, i) {
      var rank = i + 1;
      var rankClass = rank === 1 ? "first" : (rank === 2 ? "second" : (rank === 3 ? "third" : ""));
      var name = e[keyField] || "?";
      var nApp = e.n_digest_appearances || 0;
      var pct = maxN > 0 ? Math.round((nApp / maxN) * 100) : 0;
      var staleClass = e.is_recent === false ? " stale" : "";
      var dSince = daysBetween(e.last_seen);
      var lastSeenLbl = dSince === 0 ? "today" : (dSince + "d ago");

      html += '<div class="jhtg-row' + staleClass + '">' +
                '<span class="jhtg-rank ' + rankClass + '">#' + esc(rank) + '</span>' +
                '<span class="jhtg-name">' +
                  '<span class="nm">' + esc(name) + '</span>' +
                  (e.is_recent ? '<span class="rec">●</span>' : '<span class="stale">○</span>') +
                '</span>' +
                '<div style="display:flex;align-items:center;gap:11px">' +
                  '<div class="jhtg-bar-cell">' +
                    '<div class="jhtg-bar"><div class="jhtg-bar-fill" style="width:' + pct + '%"></div></div>' +
                    '<span class="jhtg-bar-val">' + esc(nApp) + '×</span>' +
                  '</div>' +
                  '<div class="jhtg-meta" title="max events in any 7d window">' +
                    '<span>max <b>' + esc(e.max_n_times_in_window || 0) + '</b>/win</span>' +
                    '<span>' + esc(lastSeenLbl) + '</span>' +
                  '</div>' +
                '</div>' +
              '</div>';
    });

    html += '</div>';
    return html;
  }

  function mount(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhtg-wrap");
    el.innerHTML = '<div class="jhtg-loading">🎯 loading 30-day targets leaderboard…</div>';

    return fetchJSON("_alerts/targets-index.json").then(function (idx) {
      if (!idx) {
        el.innerHTML = '<div class="jhtg-empty">No targets index yet — page will populate after the next digest run. ' +
                       'Each daily 09:00 + 21:00 UTC digest contributes observations.</div>';
        return null;
      }

      var eq = idx.equity_targets || [];
      var mc = idx.macro_targets  || [];
      var maxEq = eq.length ? Math.max.apply(null, eq.map(function(e){return e.n_digest_appearances || 0;})) : 0;
      var maxMc = mc.length ? Math.max.apply(null, mc.map(function(e){return e.n_digest_appearances || 0;})) : 0;

      var html = "";

      // Summary
      html += '<div class="jhtg-section-h">📊 30-Day Rolling Summary · updated each digest</div>';
      html += '<div class="jhtg-summary">' +
                '<div class="jhtg-stat purple"><div class="lbl">lookback window</div><div class="val">' + esc(idx.lookback_days || 30) + 'd</div><div class="sub">rolling</div></div>' +
                '<div class="jhtg-stat equity"><div class="lbl">🎯 equity targets</div><div class="val">' + esc(idx.n_equity_targets || 0) + '</div><div class="sub">' + esc(idx.n_equity_recent || 0) + ' active in last 7d</div></div>' +
                '<div class="jhtg-stat macro"><div class="lbl">🏛 macro targets</div><div class="val">' + esc(idx.n_macro_targets || 0) + '</div><div class="sub">' + esc(idx.n_macro_recent || 0) + ' active in last 7d</div></div>' +
                '<div class="jhtg-stat recent"><div class="lbl">total active</div><div class="val">' + esc((idx.n_equity_recent || 0) + (idx.n_macro_recent || 0)) + '</div><div class="sub">named in last 7d</div></div>' +
              '</div>';

      // Two leaderboards side-by-side
      html += '<div class="jhtg-section-h">🏆 Sustained-Target Leaderboards · ranked by appearance count</div>';
      html += '<div class="jhtg-cols">';
      html += renderBoard("equity", "🎯 EQUITY · most-flagged assets", eq, "asset", maxEq);
      html += renderBoard("macro",  "🏛 MACRO · most-flagged instruments", mc, "instrument", maxMc);
      html += '</div>';

      // Legend
      html += '<div class="jhtg-meta-block">' +
                '<b>How to read this:</b><br>' +
                '<b>●</b> green = name appeared in a digest within the last 7 days (active pattern) · ' +
                '<b>○</b> grey = last seen 8-30 days ago (cooling off)<br>' +
                '<b>N×</b> = number of separate digests where this name showed up in the top-targeted list · ' +
                '<b>max N/win</b> = peak event count in any single 7-day rolling window<br>' +
                'A name appearing 8× with high max/win is the strongest sustained front-running signal. ' +
                'A name appearing 1-2× was a one-off flag.<br>' +
                'Updated automatically by the 09:00 + 21:00 UTC digests (2 observations per name per day max).<br>' +
                'Auto-prunes entries with last_seen older than 30 days.' +
              '</div>';

      el.innerHTML = html;
      return idx;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhtg-err">Targets leaderboard unavailable: ' + esc(e.message || e) + '</div>';
      throw e;
    });
  }

  window.JHAITargets = { mount: mount, version: "1.0.0" };
})();
