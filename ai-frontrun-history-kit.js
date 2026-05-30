/**
 * ai-frontrun-history-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * 7-day history panel for the front-run sniffer. Renders an SVG line
 * chart of anomaly scores over time with regime bands + stats panel
 * + extreme-events table.
 *
 *   <script src="/ai-frontrun-history-kit.js"></script>
 *   <div id="ai-frontrun-history"></div>
 *   <script>JHAIFrontHist.mount('ai-frontrun-history');</script>
 */
(function () {
  if (window.JHAIFrontHist) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var DEFAULT_KEY = "frontrun-sniffer-history";

  function injectCSS() {
    if (document.getElementById("jh-ai-fronthist-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-fronthist-css";
    s.textContent = "\n.jhfh-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.55;margin-bottom:22px}\n.jhfh-card{background:#0a0f17;border:1px solid #1c2433;border-radius:10px;padding:18px 22px}\n.jhfh-head{display:flex;align-items:center;gap:11px;margin-bottom:12px;flex-wrap:wrap}\n.jhfh-title{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#ff7a18;font-weight:800}\n.jhfh-meta{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-left:auto}\n.jhfh-meta b{color:#cbd2dc}\n\n.jhfh-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:14px}\n.jhfh-stat{background:#11161f;border:1px solid #1c2433;border-radius:6px;padding:9px 12px}\n.jhfh-stat .lbl{font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:3px}\n.jhfh-stat .val{font-family:ui-monospace,monospace;font-size:18px;font-weight:800;color:#e6eaf2}\n.jhfh-stat .sub{font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;margin-top:2px}\n.jhfh-stat.up .val{color:#26ffaf}\n.jhfh-stat.down .val{color:#ff5577}\n.jhfh-stat.warn .val{color:#ff7a18}\n\n.jhfh-chart-wrap{background:#0d1219;border:1px solid #1c2433;border-radius:7px;padding:12px;margin-bottom:14px;position:relative}\n.jhfh-chart{width:100%;display:block}\n.jhfh-band-normal{fill:rgba(38,255,175,.04)}\n.jhfh-band-elevated{fill:rgba(255,210,102,.04)}\n.jhfh-band-extreme{fill:rgba(255,85,119,.05)}\n.jhfh-band-line{stroke:#1c2433;stroke-width:1;stroke-dasharray:2,4;fill:none}\n.jhfh-grid-line{stroke:#1c2433;stroke-width:1;stroke-dasharray:1,3;fill:none}\n.jhfh-axis-label{font-family:ui-monospace,monospace;font-size:9px;fill:#6f7b91;letter-spacing:0.4px}\n.jhfh-line{fill:none;stroke:#ff7a18;stroke-width:1.8;vector-effect:non-scaling-stroke}\n.jhfh-line-fill{fill:url(#jhfh-grad);opacity:0.18}\n.jhfh-dot{fill:#ff7a18;stroke:#0d1219;stroke-width:1;cursor:pointer;transition:r 0.15s,opacity 0.15s}\n.jhfh-dot.EXTREME{fill:#ff5577}\n.jhfh-dot.ELEVATED{fill:#ff7a18}\n.jhfh-dot.NORMAL{fill:#26ffaf}\n.jhfh-dot:hover{r:6;opacity:1}\n.jhfh-marker{stroke:#ff5577;stroke-width:1;stroke-dasharray:2,2;fill:none;opacity:0.6}\n.jhfh-current{stroke:#00d4ff;stroke-width:1.5;stroke-dasharray:3,2;fill:none}\n.jhfh-band-label{font-family:ui-monospace,monospace;font-size:9.5px;fill:#6f7b91;letter-spacing:0.4px;font-weight:600}\n\n.jhfh-tooltip{position:absolute;background:#11161f;border:1px solid #2a3548;border-radius:5px;padding:7px 10px;font-family:ui-monospace,monospace;font-size:11px;color:#e6eaf2;pointer-events:none;opacity:0;transition:opacity 0.12s;z-index:10;max-width:300px;line-height:1.5;box-shadow:0 4px 14px rgba(0,0,0,.4)}\n.jhfh-tooltip.show{opacity:1}\n.jhfh-tooltip .tt-ts{color:#6f7b91;font-size:10px;margin-bottom:3px}\n.jhfh-tooltip .tt-score{color:#ff7a18;font-weight:800;font-size:13px}\n.jhfh-tooltip .tt-score.EXTREME{color:#ff5577}\n.jhfh-tooltip .tt-score.NORMAL{color:#26ffaf}\n.jhfh-tooltip .tt-headline{color:#cbd2dc;font-size:10.5px;margin-top:3px;font-family:-apple-system,sans-serif;font-style:italic}\n\n.jhfh-targets-h{font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1.2px;text-transform:uppercase;color:#a78bfa;margin:10px 0 6px;font-weight:700}\n.jhfh-targets{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px}\n.jhfh-target-pill{font-family:ui-monospace,monospace;font-size:11px;background:#11161f;border:1px solid #1c2433;border-left:3px solid #a78bfa;border-radius:4px;padding:4px 10px;color:#e6eaf2}\n.jhfh-target-pill .n{color:#a78bfa;font-weight:700;margin-left:5px}\n\n.jhfh-events-h{font-family:ui-monospace,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#ff5577;margin:14px 0 8px;font-weight:700}\n.jhfh-event{background:#11161f;border:1px solid #1c2433;border-left:3px solid #ff5577;border-radius:6px;padding:10px 13px;margin-bottom:6px;font-size:12px}\n.jhfh-event.NORMAL{border-left-color:#26ffaf}\n.jhfh-event.ELEVATED{border-left-color:#ff7a18}\n.jhfh-event-head{display:flex;align-items:baseline;gap:11px;margin-bottom:4px;flex-wrap:wrap}\n.jhfh-event-ts{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91}\n.jhfh-event-score{font-family:ui-monospace,monospace;font-size:13px;font-weight:800;color:#ff5577}\n.jhfh-event-score.ELEVATED{color:#ff7a18}\n.jhfh-event-regime{font-family:ui-monospace,monospace;font-size:9.5px;font-weight:700;padding:1px 6px;border-radius:3px;letter-spacing:0.5px;background:rgba(255,85,119,.12);color:#ff5577}\n.jhfh-event-regime.ELEVATED{background:rgba(255,122,24,.13);color:#ff7a18}\n.jhfh-event-target{font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;margin-left:auto}\n.jhfh-event-target b{color:#00d4ff}\n.jhfh-event-headline{color:#cbd2dc;font-size:11.5px;line-height:1.5}\n\n.jhfh-empty{padding:24px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhfh-loading{padding:24px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhfh-err{padding:13px 16px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:6px;color:#ff5577;font-size:12px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");}
  function tsShort(iso){
    if(!iso) return "—";
    var d = new Date(iso);
    var now = new Date();
    var sameDay = d.toDateString() === now.toDateString();
    var sameYear = d.getFullYear() === now.getFullYear();
    var hh = d.getHours().toString().padStart(2,"0");
    var mm = d.getMinutes().toString().padStart(2,"0");
    if (sameDay) return hh+":"+mm;
    var mo = (d.getMonth()+1).toString().padStart(2,"0");
    var da = d.getDate().toString().padStart(2,"0");
    return mo+"-"+da+" "+hh+":"+mm + (sameYear?"":" "+d.getFullYear());
  }

  function renderChart(snaps) {
    // SVG chart geometry
    var W = 1000, H = 240;
    var padL = 38, padR = 12, padT = 12, padB = 22;
    var innerW = W - padL - padR;
    var innerH = H - padT - padB;

    if (!snaps.length) return '<div class="jhfh-empty">No history yet — chart will populate as the sniffer runs (every 4h).</div>';

    // X is time. Use min/max ts in data, or fix to last 7 days?
    var now = Date.now();
    var weekAgo = now - 7 * 86400 * 1000;
    var times = snaps.map(function(s){ return new Date(s.ts).getTime(); }).filter(function(t){ return !isNaN(t); });
    var tMin = Math.min(weekAgo, Math.min.apply(null, times));
    var tMax = Math.max(now, Math.max.apply(null, times));
    var span = Math.max(tMax - tMin, 1);

    function xOf(ts) { return padL + ((new Date(ts).getTime() - tMin) / span) * innerW; }
    function yOf(score) {
      var s = Math.max(0, Math.min(100, score || 0));
      return padT + ((100 - s) / 100) * innerH;
    }

    // Regime bands
    var yNormalTop = yOf(30);
    var yElevatedTop = yOf(60);
    var yExtremeTop = yOf(100);

    var bands =
      '<rect class="jhfh-band-normal"  x="'+padL+'" y="'+yOf(30)+'" width="'+innerW+'" height="'+(yOf(0)-yOf(30))+'"/>' +
      '<rect class="jhfh-band-elevated" x="'+padL+'" y="'+yOf(60)+'" width="'+innerW+'" height="'+(yOf(30)-yOf(60))+'"/>' +
      '<rect class="jhfh-band-extreme" x="'+padL+'" y="'+yOf(100)+'" width="'+innerW+'" height="'+(yOf(60)-yOf(100))+'"/>';

    // Grid lines + Y axis labels (0, 30, 60, 100)
    var grid = '';
    [0, 30, 60, 100].forEach(function(v) {
      var y = yOf(v);
      grid += '<line class="jhfh-band-line" x1="'+padL+'" y1="'+y+'" x2="'+(W-padR)+'" y2="'+y+'"/>' +
              '<text class="jhfh-axis-label" x="'+(padL-4)+'" y="'+(y+3)+'" text-anchor="end">'+v+'</text>';
    });
    // Band labels (only on left side)
    grid += '<text class="jhfh-band-label" x="'+(padL+5)+'" y="'+(yOf(15)+3)+'">NORMAL</text>' +
            '<text class="jhfh-band-label" x="'+(padL+5)+'" y="'+(yOf(45)+3)+'">ELEVATED</text>' +
            '<text class="jhfh-band-label" x="'+(padL+5)+'" y="'+(yOf(80)+3)+'">EXTREME</text>';

    // X axis time labels — 5 ticks evenly spaced across 7-day window
    var nTicks = 5;
    for (var i = 0; i < nTicks; i++) {
      var t = tMin + (span * i / (nTicks - 1));
      var x = padL + (innerW * i / (nTicks - 1));
      var d = new Date(t);
      var lbl = (d.getMonth()+1) + "/" + d.getDate();
      grid += '<line class="jhfh-grid-line" x1="'+x+'" y1="'+padT+'" x2="'+x+'" y2="'+(H-padB)+'"/>' +
              '<text class="jhfh-axis-label" x="'+x+'" y="'+(H-padB+12)+'" text-anchor="middle">'+lbl+'</text>';
    }

    // Build path d-string for the line + area
    var d = '';
    var validSnaps = snaps.filter(function(s){ return s.score != null && !isNaN(new Date(s.ts).getTime()); })
                          .sort(function(a,b){ return new Date(a.ts) - new Date(b.ts); });
    if (!validSnaps.length) return '<div class="jhfh-empty">No valid snapshots yet.</div>';

    validSnaps.forEach(function(s, idx) {
      d += (idx === 0 ? "M" : "L") + xOf(s.ts) + "," + yOf(s.score);
    });
    var areaD = d + " L" + xOf(validSnaps[validSnaps.length-1].ts) + "," + yOf(0) +
                " L" + xOf(validSnaps[0].ts) + "," + yOf(0) + " Z";

    var line = '<path class="jhfh-line-fill" d="'+areaD+'"/>' +
               '<path class="jhfh-line" d="'+d+'"/>';

    // Dots — each snapshot
    var dots = '';
    validSnaps.forEach(function(s, idx) {
      var rg = (s.regime || "NORMAL").toUpperCase();
      var r = (s.score >= 60 || rg === "EXTREME") ? 4.5 : 3;
      dots += '<circle class="jhfh-dot '+rg+'" cx="'+xOf(s.ts)+'" cy="'+yOf(s.score)+'" r="'+r+'" ' +
              'data-ts="'+esc(s.ts)+'" data-score="'+esc(s.score)+'" data-regime="'+esc(rg)+'" ' +
              'data-headline="'+esc(s.headline||"")+'" data-target="'+esc(s.top_setup_asset||"")+'" ' +
              'data-loudest="'+esc(s.loudest_signal||"")+'"/>';
    });

    // "NOW" vertical line
    var nowX = xOf(new Date().toISOString());
    var nowLine = '<line class="jhfh-current" x1="'+nowX+'" y1="'+padT+'" x2="'+nowX+'" y2="'+(H-padB)+'"/>' +
                  '<text class="jhfh-axis-label" x="'+(nowX+3)+'" y="'+(padT+10)+'" style="fill:#00d4ff">NOW</text>';

    var svg = '<svg class="jhfh-chart" viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none">' +
                '<defs><linearGradient id="jhfh-grad" x1="0" y1="0" x2="0" y2="1">' +
                  '<stop offset="0%" stop-color="#ff7a18" stop-opacity="0.55"/>' +
                  '<stop offset="100%" stop-color="#ff7a18" stop-opacity="0"/>' +
                '</linearGradient></defs>' +
                bands + grid + line + nowLine + dots +
              '</svg>';

    return svg;
  }

  function attachHover(el) {
    var dots = el.querySelectorAll(".jhfh-dot");
    var tt = el.querySelector(".jhfh-tooltip");
    if (!tt || !dots.length) return;
    dots.forEach(function(dot) {
      dot.addEventListener("mouseenter", function(ev) {
        var ts = dot.getAttribute("data-ts");
        var score = dot.getAttribute("data-score");
        var regime = dot.getAttribute("data-regime");
        var headline = dot.getAttribute("data-headline");
        var target = dot.getAttribute("data-target");
        var loudest = dot.getAttribute("data-loudest");
        tt.innerHTML = '<div class="tt-ts">'+esc(tsShort(ts))+' UTC</div>' +
                       '<div class="tt-score '+regime+'">score: '+esc(score)+' · '+esc(regime)+'</div>' +
                       (target ? '<div style="color:#a8b3c7;font-size:10.5px;margin-top:2px">top target: <b style="color:#00d4ff">'+esc(target)+'</b></div>' : '') +
                       (headline ? '<div class="tt-headline">'+esc(headline.substring(0,200))+'</div>' : '') +
                       (loudest ? '<div class="tt-headline" style="color:#ff7a18">⚠ '+esc(loudest.substring(0,150))+'</div>' : '');
        tt.classList.add("show");
        var bb = dot.getBoundingClientRect();
        var pb = el.getBoundingClientRect();
        var x = bb.left - pb.left + 10;
        var y = bb.top - pb.top - 10;
        tt.style.left = Math.min(x, pb.width - 320) + "px";
        tt.style.top = Math.max(y, 0) + "px";
      });
      dot.addEventListener("mouseleave", function() {
        tt.classList.remove("show");
      });
    });
  }

  function mount(elId, contextSlug, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhfh-wrap");
    el.innerHTML = '<div class="jhfh-loading">📈 loading 7-day anomaly history…</div>';
    var url = PROXY + "/" + (contextSlug || DEFAULT_KEY) + ".json?t=" + Date.now();
    return fetch(url).then(function(r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function(b) {
      var snaps = b.snapshots || [];
      var stats = b.stats_7d || {};
      var events = b.events || [];

      var deltaSign = stats.score_delta_vs_mean_7d == null ? null : (stats.score_delta_vs_mean_7d >= 0 ? "+" : "");
      var deltaClass = stats.score_delta_vs_mean_7d == null ? "" : (stats.score_delta_vs_mean_7d > 5 ? "warn" : (stats.score_delta_vs_mean_7d < -5 ? "down" : ""));

      var html = '<div class="jhfh-card">' +
                  '<div class="jhfh-head">' +
                    '<span class="jhfh-title">📈 7-Day Anomaly History · Score Trajectory</span>' +
                    '<span class="jhfh-meta">snapshots: <b>' + esc(stats.n_snapshots_7d || 0) + '</b> over 7d · ' +
                      'extreme: <b>' + esc(stats.n_extreme_7d || 0) + '</b> · ' +
                      'elevated: <b>' + esc(stats.n_elevated_7d || 0) + '</b> · ' +
                      'normal: <b>' + esc(stats.n_normal_7d || 0) + '</b>' +
                    '</span>' +
                  '</div>' +

                  '<div class="jhfh-stats">' +
                    '<div class="jhfh-stat"><div class="lbl">Now</div><div class="val">' + esc(stats.score_latest != null ? stats.score_latest : "—") + '</div><div class="sub">' + esc((b.snapshots && b.snapshots.length ? (b.snapshots[b.snapshots.length-1].regime || "—") : "—")) + '</div></div>' +
                    '<div class="jhfh-stat ' + deltaClass + '"><div class="lbl">vs 7d mean</div><div class="val">' + (deltaSign != null ? deltaSign + esc(stats.score_delta_vs_mean_7d) : "—") + '</div><div class="sub">' + (deltaSign != null && Math.abs(stats.score_delta_vs_mean_7d) > 5 ? (stats.score_delta_vs_mean_7d > 0 ? "above avg" : "below avg") : "near avg") + '</div></div>' +
                    '<div class="jhfh-stat"><div class="lbl">7d mean</div><div class="val">' + esc(stats.score_mean != null ? stats.score_mean : "—") + '</div></div>' +
                    '<div class="jhfh-stat"><div class="lbl">7d range</div><div class="val">' + esc(stats.score_min != null ? stats.score_min : "—") + '–' + esc(stats.score_max != null ? stats.score_max : "—") + '</div></div>' +
                    '<div class="jhfh-stat"><div class="lbl">peak setups</div><div class="val">' + esc(stats.max_setups_7d || 0) + '</div><div class="sub">in one cycle</div></div>' +
                  '</div>' +

                  '<div class="jhfh-chart-wrap">' + renderChart(snaps) +
                    '<div class="jhfh-tooltip"></div>' +
                  '</div>';

      if (stats.most_targeted_assets && stats.most_targeted_assets.length) {
        html += '<div class="jhfh-targets-h">🎯 Most-targeted assets (last 7 days)</div>' +
                '<div class="jhfh-targets">' +
                  stats.most_targeted_assets.map(function(t) {
                    return '<span class="jhfh-target-pill">' + esc(t.asset) + '<span class="n">×' + esc(t.n_times) + '</span></span>';
                  }).join('') +
                '</div>';
      }

      if (events.length) {
        html += '<div class="jhfh-events-h">⚠ Anomaly events (score ≥ 60 or EXTREME)</div>';
        events.forEach(function(e) {
          var rg = (e.regime || "ELEVATED").toUpperCase();
          html += '<div class="jhfh-event ' + rg + '">' +
                    '<div class="jhfh-event-head">' +
                      '<span class="jhfh-event-ts">' + esc(tsShort(e.ts)) + ' UTC</span>' +
                      '<span class="jhfh-event-score ' + rg + '">score ' + esc(e.score) + '</span>' +
                      '<span class="jhfh-event-regime ' + rg + '">' + esc(rg) + '</span>' +
                      (e.top_setup_asset ? '<span class="jhfh-event-target">target: <b>' + esc(e.top_setup_asset) + '</b> ' + esc(e.top_setup_dir || '') + '</span>' : '') +
                    '</div>' +
                    (e.headline ? '<div class="jhfh-event-headline">' + esc(e.headline) + '</div>' : '') +
                  '</div>';
        });
      } else if (snaps.length) {
        html += '<div class="jhfh-events-h">⚠ Anomaly events (score ≥ 60 or EXTREME)</div>' +
                '<div class="jhfh-empty" style="padding:14px">No high-anomaly events in the last 7 days. The institutional flow tape has been within normal-to-elevated range.</div>';
      }

      html += '</div>';
      el.innerHTML = html;
      attachHover(el);
      return b;
    }).catch(function(e) {
      el.innerHTML = '<div class="jhfh-err">History panel unavailable: ' + esc(e.message || e) + '. The chart populates as the sniffer runs (first snapshot lands after the next 4h cycle).</div>';
      if (opts && typeof opts.onError === "function") opts.onError(e);
      throw e;
    });
  }
  window.JHAIFrontHist = { mount: mount, version: "1.0.0" };
})();
