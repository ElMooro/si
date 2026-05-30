/**
 * ai-digest-trends-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Longitudinal analytics for the digest index. Renders:
 *   1. Summary KPI grid (totals, percentages, streaks)
 *   2. 60-day score trajectory chart (equity + macro overlaid as SVG)
 *   3. Daily activity strip (one cell per day, color = activity level)
 *   4. Noisiest days leaderboard (top 10 ranked by composite badness)
 *
 *   <script src="/ai-digest-trends-kit.js"></script>
 *   <div id="ai-digest-trends"></div>
 *   <script>JHAITrends.mount('ai-digest-trends');</script>
 */
(function () {
  if (window.JHAITrends) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function injectCSS() {
    if (document.getElementById("jh-ai-trends-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-trends-css";
    s.textContent = "\n.jhtr-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jhtr-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#a78bfa;margin:22px 0 10px;font-weight:700;padding-bottom:5px;border-bottom:1px solid rgba(167,139,250,.18)}\n\n.jhtr-summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:14px}\n.jhtr-stat{background:#0a0f17;border:1px solid #1c2433;border-left:4px solid #6f7b91;border-radius:7px;padding:10px 14px}\n.jhtr-stat.quiet{border-left-color:#26ffaf}\n.jhtr-stat.active{border-left-color:#ff7a18}\n.jhtr-stat.extreme{border-left-color:#ff5577}\n.jhtr-stat.total{border-left-color:#a78bfa}\n.jhtr-stat .lbl{font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:4px}\n.jhtr-stat .val{font-family:ui-monospace,monospace;font-size:22px;font-weight:800;color:#e6eaf2;line-height:1.1}\n.jhtr-stat.quiet .val{color:#26ffaf}\n.jhtr-stat.active .val{color:#ff7a18}\n.jhtr-stat.extreme .val{color:#ff5577}\n.jhtr-stat.total .val{color:#a78bfa}\n.jhtr-stat .sub{font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;margin-top:3px}\n\n.jhtr-chart-wrap{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:14px;position:relative}\n.jhtr-chart{width:100%;display:block}\n.jhtr-band-extreme{fill:rgba(255,85,119,.05)}\n.jhtr-band-elevated{fill:rgba(255,210,102,.04)}\n.jhtr-band-normal{fill:rgba(38,255,175,.04)}\n.jhtr-grid-line{stroke:#1c2433;stroke-width:1;stroke-dasharray:1,3;fill:none}\n.jhtr-axis-label{font-family:ui-monospace,monospace;font-size:9px;fill:#6f7b91;letter-spacing:0.4px}\n.jhtr-band-label{font-family:ui-monospace,monospace;font-size:9.5px;fill:#6f7b91;letter-spacing:0.4px;font-weight:600}\n.jhtr-line-eq{fill:none;stroke:#ff7a18;stroke-width:2;vector-effect:non-scaling-stroke}\n.jhtr-line-mc{fill:none;stroke:#22d3ee;stroke-width:2;vector-effect:non-scaling-stroke}\n.jhtr-dot-eq{fill:#ff7a18;stroke:#0a0f17;stroke-width:1.5}\n.jhtr-dot-mc{fill:#22d3ee;stroke:#0a0f17;stroke-width:1.5}\n.jhtr-chart-legend{display:flex;gap:14px;font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;margin-bottom:9px;flex-wrap:wrap}\n.jhtr-chart-legend .swatch{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px;vertical-align:middle}\n.jhtr-chart-legend .swatch.eq{background:#ff7a18}\n.jhtr-chart-legend .swatch.mc{background:#22d3ee}\n\n.jhtr-strip-wrap{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:13px 16px;margin-bottom:14px}\n.jhtr-strip-h{font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:8px}\n.jhtr-strip{display:flex;gap:2px;flex-wrap:wrap}\n.jhtr-day{width:18px;height:24px;border-radius:3px;background:#1c2433;cursor:pointer;position:relative;transition:transform 0.1s,box-shadow 0.1s}\n.jhtr-day[data-session=\"close\"]{box-shadow:inset 0 -3px 0 rgba(251,191,36,.55)}\n.jhtr-day.QUIET{background:rgba(38,255,175,.30)}\n.jhtr-day.ACTIVE{background:rgba(255,122,24,.45)}\n.jhtr-day.EXTREME{background:rgba(255,85,119,.65);animation:jhtr-pulse 1.6s ease-in-out infinite}\n.jhtr-day:hover{transform:scale(1.12);box-shadow:0 0 8px rgba(167,139,250,.4);z-index:2}\n@keyframes jhtr-pulse{0%,100%{box-shadow:0 0 0 0 rgba(255,85,119,.0)}50%{box-shadow:0 0 8px 1px rgba(255,85,119,.4)}}\n.jhtr-day-tt{position:absolute;bottom:100%;left:50%;transform:translateX(-50%);background:#11161f;border:1px solid #2a3548;border-radius:5px;padding:7px 10px;font-family:ui-monospace,monospace;font-size:10.5px;color:#e6eaf2;white-space:nowrap;pointer-events:none;opacity:0;transition:opacity 0.12s;z-index:20;line-height:1.5;margin-bottom:5px}\n.jhtr-day:hover .jhtr-day-tt{opacity:1}\n.jhtr-day-tt b{color:#a78bfa}\n\n.jhtr-leaderboard{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:13px 16px}\n.jhtr-lb-row{display:grid;grid-template-columns:32px 1fr auto auto auto;gap:11px;align-items:center;padding:7px 10px;border-radius:5px;font-family:ui-monospace,monospace;font-size:12px;transition:background 0.1s}\n.jhtr-lb-row:nth-child(odd){background:#0d1219}\n.jhtr-lb-row:hover{background:#11161f}\n.jhtr-lb-rank{font-weight:800;color:#a78bfa}\n.jhtr-lb-rank.first{color:#ff5577}\n.jhtr-lb-rank.second{color:#ff7a18}\n.jhtr-lb-rank.third{color:#ffd266}\n.jhtr-lb-date{color:#e6eaf2;font-weight:700}\n.jhtr-lb-date .dow{color:#6f7b91;font-weight:400;font-size:10.5px;margin-left:5px}\n.jhtr-lb-scores{font-size:11px;color:#a8b3c7;display:flex;gap:9px;align-items:center}\n.jhtr-lb-scores .eq{color:#ff7a18;font-weight:700}\n.jhtr-lb-scores .mc{color:#22d3ee;font-weight:700}\n.jhtr-lb-alerts{font-size:11px;color:#cbd2dc}\n.jhtr-lb-alerts b{color:#ff5577}\n.jhtr-lb-act{font-size:9.5px;font-weight:800;padding:2px 7px;border-radius:3px;letter-spacing:0.5px}\n.jhtr-lb-act.QUIET{background:rgba(38,255,175,.13);color:#26ffaf}\n.jhtr-lb-act.ACTIVE{background:rgba(255,122,24,.14);color:#ff7a18}\n.jhtr-lb-act.EXTREME{background:rgba(255,85,119,.18);color:#ff5577}\n\n.jhtr-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhtr-empty{padding:20px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px;background:#0a0f17;border:1px dashed #1c2433;border-radius:7px}\n.jhtr-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
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

  function computeStats(entries) {
    var n = entries.length;
    var nq = entries.filter(function (e) { return e.activity_level === "QUIET"; }).length;
    var na = entries.filter(function (e) { return e.activity_level === "ACTIVE"; }).length;
    var nx = entries.filter(function (e) { return e.activity_level === "EXTREME"; }).length;
    var totalAlerts = entries.reduce(function (acc, e) { return acc + (e.n_equity_alerts_today || 0) + (e.n_macro_alerts_today || 0); }, 0);
    var avgAlerts = n ? (totalAlerts / n).toFixed(2) : "0";

    // Longest quiet streak
    var maxStreak = 0, cur = 0;
    // entries are newest first; reverse to compute chronologically
    var chrono = entries.slice().reverse();
    chrono.forEach(function (e) {
      if (e.activity_level === "QUIET") { cur++; maxStreak = Math.max(maxStreak, cur); }
      else cur = 0;
    });
    var currentQuietStreak = 0;
    for (var i = 0; i < chrono.length; i++) {
      var idx = chrono.length - 1 - i; // walk from newest
      if (chrono[idx].activity_level === "QUIET") currentQuietStreak++;
      else break;
    }

    // Score stats
    var eqScores = entries.map(function (e) { return e.equity_score; }).filter(function (v) { return typeof v === "number"; });
    var mcScores = entries.map(function (e) { return e.macro_score;  }).filter(function (v) { return typeof v === "number"; });
    function mean(arr) { return arr.length ? (arr.reduce(function (a, b) { return a + b; }, 0) / arr.length) : null; }
    function maxV(arr) { return arr.length ? Math.max.apply(null, arr) : null; }

    return {
      n: n, nq: nq, na: na, nx: nx,
      totalAlerts: totalAlerts, avgAlerts: avgAlerts,
      maxStreak: maxStreak, currentQuietStreak: currentQuietStreak,
      eqMean: mean(eqScores), mcMean: mean(mcScores),
      eqMax: maxV(eqScores), mcMax: maxV(mcScores),
    };
  }

  function renderSummary(stats, idx) {
    var pctQ = stats.n ? Math.round(stats.nq / stats.n * 100) : 0;
    var pctA = stats.n ? Math.round(stats.na / stats.n * 100) : 0;
    var pctX = stats.n ? Math.round(stats.nx / stats.n * 100) : 0;
    var html = '<div class="jhtr-summary">' +
                 '<div class="jhtr-stat total"><div class="lbl">days archived</div><div class="val">' + esc(stats.n) + '</div><div class="sub">' + esc(idx.earliest_date || '—') + ' → ' + esc(idx.latest_date || '—') + '</div></div>' +
                 '<div class="jhtr-stat quiet"><div class="lbl">quiet days</div><div class="val">' + esc(stats.nq) + '</div><div class="sub">' + esc(pctQ) + '% of total</div></div>' +
                 '<div class="jhtr-stat active"><div class="lbl">active days</div><div class="val">' + esc(stats.na) + '</div><div class="sub">' + esc(pctA) + '% of total</div></div>' +
                 '<div class="jhtr-stat extreme"><div class="lbl">extreme days</div><div class="val">' + esc(stats.nx) + '</div><div class="sub">' + esc(pctX) + '% of total</div></div>' +
                 '<div class="jhtr-stat total"><div class="lbl">total alerts</div><div class="val">' + esc(stats.totalAlerts) + '</div><div class="sub">avg ' + esc(stats.avgAlerts) + '/day</div></div>' +
                 '<div class="jhtr-stat quiet"><div class="lbl">quiet streak</div><div class="val">' + esc(stats.currentQuietStreak) + 'd</div><div class="sub">longest: ' + esc(stats.maxStreak) + 'd</div></div>' +
                 '<div class="jhtr-stat" style="border-left-color:#ff7a18"><div class="lbl">🎯 eq mean</div><div class="val" style="color:#ff7a18">' + esc(stats.eqMean == null ? '—' : stats.eqMean.toFixed(1)) + '</div><div class="sub">max ' + esc(stats.eqMax == null ? '—' : stats.eqMax) + '</div></div>' +
                 '<div class="jhtr-stat" style="border-left-color:#22d3ee"><div class="lbl">🏛 mc mean</div><div class="val" style="color:#22d3ee">' + esc(stats.mcMean == null ? '—' : stats.mcMean.toFixed(1)) + '</div><div class="sub">max ' + esc(stats.mcMax == null ? '—' : stats.mcMax) + '</div></div>' +
               '</div>';
    return html;
  }

  function renderChart(entries) {
    if (!entries.length) return '<div class="jhtr-empty">No data yet — the chart will populate as daily digests accumulate.</div>';
    // Sort chronologically (oldest first) for the time axis
    var sorted = entries.slice().sort(function (a, b) { return (a.date || "").localeCompare(b.date || ""); });

    var W = 1000, H = 240;
    var padL = 38, padR = 12, padT = 14, padB = 24;
    var innerW = W - padL - padR;
    var innerH = H - padT - padB;

    var n = sorted.length;
    function xOf(i) { return padL + (n <= 1 ? innerW / 2 : (innerW * i / (n - 1))); }
    function yOf(v) {
      var s = Math.max(0, Math.min(100, v || 0));
      return padT + ((100 - s) / 100) * innerH;
    }

    // Regime bands
    var bands =
      '<rect class="jhtr-band-normal"   x="'+padL+'" y="'+yOf(30)+'"  width="'+innerW+'" height="'+(yOf(0)-yOf(30))+'"/>' +
      '<rect class="jhtr-band-elevated" x="'+padL+'" y="'+yOf(60)+'"  width="'+innerW+'" height="'+(yOf(30)-yOf(60))+'"/>' +
      '<rect class="jhtr-band-extreme"  x="'+padL+'" y="'+yOf(100)+'" width="'+innerW+'" height="'+(yOf(60)-yOf(100))+'"/>';

    // Y axis labels + grid
    var grid = '';
    [0, 30, 60, 100].forEach(function (v) {
      var y = yOf(v);
      grid += '<line class="jhtr-grid-line" x1="'+padL+'" y1="'+y+'" x2="'+(W-padR)+'" y2="'+y+'"/>' +
              '<text class="jhtr-axis-label" x="'+(padL-4)+'" y="'+(y+3)+'" text-anchor="end">'+v+'</text>';
    });
    // Band labels
    grid += '<text class="jhtr-band-label" x="'+(padL+5)+'" y="'+(yOf(15)+3)+'">NORMAL</text>' +
            '<text class="jhtr-band-label" x="'+(padL+5)+'" y="'+(yOf(45)+3)+'">ELEVATED</text>' +
            '<text class="jhtr-band-label" x="'+(padL+5)+'" y="'+(yOf(80)+3)+'">EXTREME</text>';

    // X axis ticks — show up to ~6 dates evenly spaced
    var nTicks = Math.min(6, n);
    for (var i = 0; i < nTicks; i++) {
      var idx = nTicks <= 1 ? 0 : Math.round(i * (n - 1) / (nTicks - 1));
      var x = xOf(idx);
      var dateStr = sorted[idx].date || "";
      var parts = dateStr.split("-");
      var lbl = parts.length === 3 ? (parts[1] + "/" + parts[2]) : dateStr;
      grid += '<text class="jhtr-axis-label" x="'+x+'" y="'+(H-padB+12)+'" text-anchor="middle">'+lbl+'</text>';
    }

    // Lines for equity + macro
    var eqPath = "", mcPath = "";
    sorted.forEach(function (e, i) {
      if (typeof e.equity_score === "number") eqPath += (eqPath ? "L" : "M") + xOf(i) + "," + yOf(e.equity_score);
      if (typeof e.macro_score  === "number") mcPath += (mcPath ? "L" : "M") + xOf(i) + "," + yOf(e.macro_score);
    });

    // Dots
    var dots = "";
    sorted.forEach(function (e, i) {
      if (typeof e.equity_score === "number") {
        dots += '<circle class="jhtr-dot-eq" cx="'+xOf(i)+'" cy="'+yOf(e.equity_score)+'" r="3.5"><title>'+esc(e.date)+' · equity '+esc(e.equity_score)+' '+esc(e.equity_regime)+'</title></circle>';
      }
      if (typeof e.macro_score === "number") {
        dots += '<circle class="jhtr-dot-mc" cx="'+xOf(i)+'" cy="'+yOf(e.macro_score)+'" r="3.5"><title>'+esc(e.date)+' · macro '+esc(e.macro_score)+' '+esc(e.macro_regime)+'</title></circle>';
      }
    });

    var svg = '<svg class="jhtr-chart" viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none">' +
                bands + grid +
                '<path class="jhtr-line-eq" d="'+eqPath+'"/>' +
                '<path class="jhtr-line-mc" d="'+mcPath+'"/>' +
                dots +
              '</svg>';

    var legend = '<div class="jhtr-chart-legend">' +
                   '<span><span class="swatch eq"></span><b style="color:#ff7a18">🎯 Equity sniffer</b> (overall_anomaly_score)</span>' +
                   '<span><span class="swatch mc"></span><b style="color:#22d3ee">🏛 Macro sniffer</b> (overall_macro_score)</span>' +
                   '<span style="margin-left:auto;color:#6f7b91">1 datapoint per daily digest (09:00 UTC)</span>' +
                 '</div>';

    return '<div class="jhtr-chart-wrap">' + legend + svg + '</div>';
  }

  function renderStrip(entries) {
    if (!entries.length) return '<div class="jhtr-empty">Strip will populate as digests accumulate.</div>';
    var sorted = entries.slice().sort(function (a, b) {
      var dc = (a.date || "").localeCompare(b.date || "");
      if (dc !== 0) return dc;
      // open before close within same date (chronological order)
      var sa = (a.session || "open") === "close" ? 1 : 0;
      var sb = (b.session || "open") === "close" ? 1 : 0;
      return sa - sb;
    });
    var html = '<div class="jhtr-strip-wrap">' +
                 '<div class="jhtr-strip-h">Activity strip · oldest → newest · ' + esc(sorted.length) + ' digest' + (sorted.length === 1 ? '' : 's') + ' (📅 open + 🔔 close)</div>' +
                 '<div class="jhtr-strip">';
    sorted.forEach(function (e) {
      var act = (e.activity_level || "QUIET").toUpperCase();
      var n_alerts = (e.n_equity_alerts_today || 0) + (e.n_macro_alerts_today || 0);
      var session = (e.session || "open").toLowerCase();
      var sessionEmoji = session === "close" ? "🔔" : "📅";
      html += '<div class="jhtr-day ' + act + '" data-session="' + session + '">' +
                '<div class="jhtr-day-tt">' +
                  esc(e.date) + ' ' + sessionEmoji + ' <b>' + esc(session) + '</b> · <b>' + esc(act) + '</b><br>' +
                  '🎯 eq ' + esc(e.equity_score == null ? '—' : e.equity_score) + ' ' + esc(e.equity_regime || '') + '<br>' +
                  '🏛 mc ' + esc(e.macro_score  == null ? '—' : e.macro_score)  + ' ' + esc(e.macro_regime  || '') + '<br>' +
                  (n_alerts > 0 ? n_alerts + ' alert' + (n_alerts === 1 ? '' : 's') : '✓ no alerts') +
                '</div>' +
              '</div>';
    });
    html += '</div></div>';
    return html;
  }

  function renderLeaderboard(entries) {
    // Composite badness score: alerts × 10 + (eq + mc) / 2 + extreme bonus
    function badness(e) {
      var nA = (e.n_equity_alerts_today || 0) + (e.n_macro_alerts_today || 0);
      var avgScore = ((e.equity_score || 0) + (e.macro_score || 0)) / 2;
      var extremeBonus = (e.equity_regime === "EXTREME" || e.macro_regime === "EXTREME") ? 30 : 0;
      return nA * 10 + avgScore + extremeBonus;
    }
    var ranked = entries.slice().sort(function (a, b) { return badness(b) - badness(a); }).slice(0, 10);

    if (!ranked.length || badness(ranked[0]) === 0) {
      return '<div class="jhtr-empty">No noisy days yet — every digest so far classified as QUIET.</div>';
    }

    var html = '<div class="jhtr-leaderboard">';
    ranked.forEach(function (e, i) {
      var rank = i + 1;
      var rankClass = rank === 1 ? "first" : (rank === 2 ? "second" : (rank === 3 ? "third" : ""));
      var n_alerts = (e.n_equity_alerts_today || 0) + (e.n_macro_alerts_today || 0);
      var act = (e.activity_level || "QUIET").toUpperCase();
      html += '<div class="jhtr-lb-row">' +
                '<span class="jhtr-lb-rank ' + rankClass + '">#' + esc(rank) + '</span>' +
                '<span class="jhtr-lb-date">' + esc(e.date) + ' <span class="dow">' + esc(dow(e.date)) + '</span></span>' +
                '<span class="jhtr-lb-scores">' +
                  '<span class="eq">🎯 ' + esc(e.equity_score == null ? '—' : e.equity_score) + '</span>' +
                  '<span class="mc">🏛 ' + esc(e.macro_score  == null ? '—' : e.macro_score)  + '</span>' +
                '</span>' +
                '<span class="jhtr-lb-alerts">' +
                  (n_alerts > 0 ? '<b>' + n_alerts + '</b> alert' + (n_alerts === 1 ? '' : 's') : '0 alerts') +
                '</span>' +
                '<span class="jhtr-lb-act ' + act + '">' + esc(act) + '</span>' +
              '</div>';
    });
    html += '</div>';
    return html;
  }

  function mount(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhtr-wrap");
    el.innerHTML = '<div class="jhtr-loading">📊 loading digest trends…</div>';

    return fetchJSON("_alerts/digests-index.json").then(function (idx) {
      if (!idx || !idx.entries || !idx.entries.length) {
        el.innerHTML = '<div class="jhtr-empty">No digests yet — trends will populate as the daily 09:00 UTC cycle runs. ' +
                       'After 7+ days the chart and leaderboard will have enough data to be meaningful.</div>';
        return idx;
      }
      var entries = idx.entries;
      var stats = computeStats(entries);

      var html = "";
      html += '<div class="jhtr-section-h">📊 Summary · ' + esc(idx.earliest_date) + ' → ' + esc(idx.latest_date) + '</div>';
      html += renderSummary(stats, idx);

      html += '<div class="jhtr-section-h">📈 60-Day Score Trajectory · both sniffers overlaid</div>';
      html += renderChart(entries);

      html += '<div class="jhtr-section-h">🗓 Daily Activity Strip · color = activity level (hover for details)</div>';
      html += renderStrip(entries);

      html += '<div class="jhtr-section-h">🏆 Noisiest Days · top 10 ranked by alerts × 10 + avg score + extreme bonus</div>';
      html += renderLeaderboard(entries);

      el.innerHTML = html;
      return idx;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhtr-err">Trends unavailable: ' + esc(e.message || e) + '</div>';
      throw e;
    });
  }

  window.JHAITrends = { mount: mount, version: "1.0.0" };
})();
