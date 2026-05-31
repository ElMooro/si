/**
 * ai-skill-cockpit-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Renders the closed-loop accuracy dashboard. Reads
 * data/_skill/frontrun-skill-index.json (maintained by router's
 * skill_aggregator). Five sections:
 *   1. Headline KPIs (overall hit rate, scored %, total predictions)
 *   2. Per-engine accuracy leaderboard
 *   3. By-regime breakdown (do calls do better in EXTREME vs NORMAL?)
 *   4. Confidence calibration plot (claimed prob vs actual hit rate)
 *   5. Recent graded predictions timeline
 *
 *   <script src="/ai-skill-cockpit-kit.js"></script>
 *   <div id="ai-skill"></div>
 *   <script>JHAISkill.mount('ai-skill');</script>
 */
(function () {
  if (window.JHAISkill) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function injectCSS() {
    if (document.getElementById("jh-ai-skill-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-skill-css";
    s.textContent = "\n.jhsk-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n.jhsk-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#a78bfa;margin:22px 0 10px;font-weight:700;padding-bottom:5px;border-bottom:1px solid rgba(167,139,250,.18)}\n\n.jhsk-headline{background:#0a0f17;border:1px solid #1c2433;border-radius:11px;padding:18px 22px;margin-bottom:14px;border-left:6px solid #a78bfa;display:flex;gap:22px;flex-wrap:wrap;align-items:center}\n.jhsk-headline .big{font-family:ui-monospace,monospace;font-size:38px;font-weight:800;line-height:1;letter-spacing:-0.02em}\n.jhsk-headline .big.good{color:#26ffaf}\n.jhsk-headline .big.bad{color:#ff5577}\n.jhsk-headline .big.neutral{color:#fbbf24}\n.jhsk-headline .lbl{font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:4px;font-weight:700}\n.jhsk-headline .sub{font-family:ui-monospace,monospace;font-size:11.5px;color:#a8b3c7;margin-top:3px}\n.jhsk-headline-stats{display:flex;gap:18px;flex-wrap:wrap}\n.jhsk-headline-stat{padding-left:18px;border-left:1px solid #1c2433;min-width:90px}\n.jhsk-headline-stat:first-child{padding-left:0;border-left:none}\n.jhsk-headline-stat .v{font-family:ui-monospace,monospace;font-size:18px;font-weight:800;color:#e6eaf2;line-height:1.2}\n\n.jhsk-engines{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:11px;margin-bottom:14px}\n.jhsk-engine{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:13px 16px;border-left:5px solid #6f7b91;position:relative}\n.jhsk-engine.good{border-left-color:#26ffaf}\n.jhsk-engine.neutral{border-left-color:#fbbf24}\n.jhsk-engine.bad{border-left-color:#ff5577}\n.jhsk-engine.pending{border-left-color:#6f7b91;opacity:0.65}\n.jhsk-engine .name{font-family:ui-monospace,monospace;font-size:12px;font-weight:800;color:#e6eaf2;margin-bottom:8px;letter-spacing:0.3px}\n.jhsk-engine .hit{font-family:ui-monospace,monospace;font-size:30px;font-weight:800;line-height:1;display:flex;align-items:baseline;gap:6px}\n.jhsk-engine.good .hit{color:#26ffaf}\n.jhsk-engine.neutral .hit{color:#fbbf24}\n.jhsk-engine.bad .hit{color:#ff5577}\n.jhsk-engine .hit small{font-size:13px;color:#6f7b91;font-weight:400}\n.jhsk-engine .row{display:flex;justify-content:space-between;font-family:ui-monospace,monospace;font-size:11px;color:#a8b3c7;margin-top:7px;line-height:1.45}\n.jhsk-engine .row b{color:#e6eaf2;font-weight:700}\n\n.jhsk-row-grid{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:12px 16px;margin-bottom:14px}\n.jhsk-row-grid .h{font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:8px;font-weight:700}\n.jhsk-row-grid table{width:100%;border-collapse:collapse;font-family:ui-monospace,monospace;font-size:12px}\n.jhsk-row-grid td{padding:5px 9px;border-bottom:1px solid rgba(28,36,51,.4)}\n.jhsk-row-grid tr:last-child td{border-bottom:none}\n.jhsk-row-grid .lbl{color:#6f7b91;width:130px}\n.jhsk-row-grid .bar-cell{position:relative;width:60%}\n.jhsk-row-grid .bar-bg{height:6px;background:#11161f;border-radius:3px;overflow:hidden}\n.jhsk-row-grid .bar-fill{height:100%;background:linear-gradient(90deg,#26ffaf,#22d3ee);border-radius:3px;transition:width 0.3s}\n.jhsk-row-grid .v{text-align:right;color:#e6eaf2;font-weight:700;min-width:60px}\n\n.jhsk-calibration{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;padding:14px 18px;margin-bottom:14px}\n.jhsk-cal-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:11px;margin-top:9px}\n@media(max-width:580px){.jhsk-cal-grid{grid-template-columns:repeat(2,1fr)}}\n.jhsk-cal-cell{background:#11161f;border:1px solid #1c2433;border-radius:6px;padding:9px 12px;text-align:center}\n.jhsk-cal-cell .b{font-family:ui-monospace,monospace;font-size:10px;color:#a78bfa;letter-spacing:0.5px;margin-bottom:5px}\n.jhsk-cal-cell .a{font-family:ui-monospace,monospace;font-size:22px;font-weight:800;color:#e6eaf2;line-height:1}\n.jhsk-cal-cell .n{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;margin-top:5px}\n.jhsk-cal-cell.calibrated .a{color:#26ffaf}\n.jhsk-cal-cell.overconfident .a{color:#ff5577}\n.jhsk-cal-cell.underconfident .a{color:#fbbf24}\n\n.jhsk-recent{background:#0a0f17;border:1px solid #1c2433;border-radius:9px;overflow:hidden}\n.jhsk-rec-hdr{padding:8px 16px;background:#11161f;font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;border-bottom:1px solid #1c2433}\n.jhsk-rec-row{display:grid;grid-template-columns:14px 90px 1fr auto 60px;gap:12px;align-items:center;padding:8px 16px;border-bottom:1px solid rgba(28,36,51,.5);font-family:ui-monospace,monospace;font-size:11.5px}\n.jhsk-rec-row:last-child{border-bottom:none}\n.jhsk-rec-row:hover{background:#11161f}\n.jhsk-rec-dot{width:10px;height:10px;border-radius:50%}\n.jhsk-rec-dot.correct{background:#26ffaf;box-shadow:0 0 5px rgba(38,255,175,.5)}\n.jhsk-rec-dot.wrong{background:#ff5577;box-shadow:0 0 5px rgba(255,85,119,.5)}\n.jhsk-rec-ts{color:#6f7b91}\n.jhsk-rec-detail{color:#cbd2dc;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n.jhsk-rec-detail b{color:#e6eaf2;font-weight:700}\n.jhsk-rec-conf{color:#a78bfa;text-align:right}\n.jhsk-rec-ret{text-align:right;font-weight:800}\n.jhsk-rec-ret.pos{color:#26ffaf}\n.jhsk-rec-ret.neg{color:#ff5577}\n\n.jhsk-empty{padding:25px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:11.5px;background:#11161f;border:1px dashed #1c2433;border-radius:6px}\n.jhsk-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhsk-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n.jhsk-meta{margin-top:14px;font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;padding:10px 14px;background:#0d1219;border:1px dashed #1c2433;border-radius:5px;line-height:1.65}\n.jhsk-meta b{color:#cbd2dc}\n.jhsk-meta code{font-family:ui-monospace,monospace;font-size:10.5px;color:#22d3ee;background:#11161f;padding:1px 6px;border-radius:3px}\n";
    document.head.appendChild(s);
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");}
  function pct(v){return v==null ? '—' : (v * 100).toFixed(1) + '%';}
  function fmtRet(v){if(v==null)return '—';var s=v>=0?'+':'';return s + v.toFixed(2) + '%';}
  function ageOf(iso){if(!iso)return '—';try{var d=new Date(iso);var ms=Date.now()-d.getTime();if(ms<3600000)return Math.round(ms/60000)+'m';if(ms<86400000)return Math.round(ms/3600000)+'h';return Math.round(ms/86400000)+'d';}catch(e){return '—';}}
  function fetchJSON(path){var url=PROXY+'/'+path+'?t='+Date.now();return fetch(url).then(function(r){return r.ok?r.json():null;}).catch(function(){return null;});}

  function classifyHitRate(hr){
    if (hr == null) return 'pending';
    if (hr >= 0.60) return 'good';
    if (hr >= 0.45) return 'neutral';
    return 'bad';
  }
  function classifyCalibration(bucket, actualHr){
    if (actualHr == null) return '';
    // Expected hit rate ≈ midpoint of bucket
    var mids = {"0.30-0.50":0.40, "0.50-0.65":0.575, "0.65-0.80":0.725, "0.80-0.95":0.875};
    var mid = mids[bucket];
    if (!mid) return '';
    var delta = actualHr - mid;
    if (Math.abs(delta) <= 0.08) return 'calibrated';
    if (delta < 0) return 'overconfident';
    return 'underconfident';
  }

  function renderHeadline(idx){
    var totalScored = idx.n_scored || 0;
    var totalCorrect = 0;
    for (var k in (idx.by_engine||{})) totalCorrect += idx.by_engine[k].n_correct || 0;
    var overallHr = totalScored > 0 ? (totalCorrect / totalScored) : null;
    var hrClass = classifyHitRate(overallHr);
    var hrLabel = overallHr == null ? 'collecting…' : pct(overallHr);

    return '<div class="jhsk-headline">' +
             '<div>' +
               '<div class="lbl">overall hit rate (' + esc(idx.lookback_days || 90) + 'd)</div>' +
               '<div class="big ' + hrClass + '">' + esc(hrLabel) + '</div>' +
               '<div class="sub">' + esc(totalCorrect) + ' correct / ' + esc(totalScored) + ' scored predictions</div>' +
             '</div>' +
             '<div class="jhsk-headline-stats">' +
               '<div class="jhsk-headline-stat"><div class="lbl">scored</div><div class="v">' + esc(totalScored) + '</div></div>' +
               '<div class="jhsk-headline-stat"><div class="lbl">pending</div><div class="v">' + esc(idx.n_pending || 0) + '</div></div>' +
               '<div class="jhsk-headline-stat"><div class="lbl">total</div><div class="v">' + esc(idx.n_total_predictions || 0) + '</div></div>' +
               '<div class="jhsk-headline-stat"><div class="lbl">engines</div><div class="v">' + esc(Object.keys(idx.by_engine || {}).length) + '</div></div>' +
               '<div class="jhsk-headline-stat"><div class="lbl">scored %</div><div class="v">' + esc((idx.scored_pct || 0).toFixed ? idx.scored_pct.toFixed(0) : idx.scored_pct) + '%</div></div>' +
             '</div>' +
           '</div>';
  }

  function renderEngines(byEngine){
    var keys = Object.keys(byEngine || {});
    if (!keys.length) return '<div class="jhsk-empty">No engines tracked yet — predictions need to be logged + graded first.</div>';
    // Sort by hit_rate DESC, pending last
    keys.sort(function(a,b){
      var ha = byEngine[a].hit_rate;
      var hb = byEngine[b].hit_rate;
      if (ha == null && hb == null) return 0;
      if (ha == null) return 1;
      if (hb == null) return -1;
      return hb - ha;
    });
    var html = '<div class="jhsk-engines">';
    keys.forEach(function(k){
      var e = byEngine[k];
      var cls = classifyHitRate(e.hit_rate);
      var hrLabel = e.hit_rate == null ? 'n/a' : pct(e.hit_rate);
      var roll = e.rolling_30d_hit_rate == null ? 'n/a' : pct(e.rolling_30d_hit_rate);
      var ar   = e.avg_return_pct == null ? '—' : fmtRet(e.avg_return_pct);
      var pf   = e.profit_factor == null ? '—' : e.profit_factor.toFixed(2);
      html += '<div class="jhsk-engine ' + cls + '">' +
                '<div class="name">' + esc(k.replace(/_/g,' ').toUpperCase()) + '</div>' +
                '<div class="hit">' + esc(hrLabel) + '<small>hit rate</small></div>' +
                '<div class="row"><span>scored</span><b>' + esc(e.n_scored || 0) + '/' + esc(e.n_total || 0) + '</b></div>' +
                '<div class="row"><span>30d rolling</span><b>' + esc(roll) + '</b></div>' +
                '<div class="row"><span>avg return</span><b>' + esc(ar) + '</b></div>' +
                '<div class="row"><span>profit factor</span><b>' + esc(pf) + '</b></div>' +
                '<div class="row"><span>pending grade</span><b>' + esc(e.n_pending_grade || 0) + '</b></div>' +
              '</div>';
    });
    html += '</div>';
    return html;
  }

  function renderRegimes(byRegime){
    var html = '<div class="jhsk-row-grid">' +
                 '<div class="h">Hit rate by regime at time of prediction</div>' +
                 '<table>';
    ['EXTREME','ELEVATED','NORMAL'].forEach(function(rg){
      var rb = (byRegime || {})[rg] || {n:0,correct:0,hit_rate:null};
      var w = rb.hit_rate == null ? 0 : Math.round(rb.hit_rate * 100);
      html += '<tr>' +
                '<td class="lbl">' + esc(rg) + '</td>' +
                '<td class="bar-cell"><div class="bar-bg"><div class="bar-fill" style="width:' + w + '%"></div></div></td>' +
                '<td class="v">' + esc(rb.hit_rate == null ? 'n/a' : pct(rb.hit_rate)) + ' <small style="color:#6f7b91">(' + esc(rb.n) + ')</small></td>' +
              '</tr>';
    });
    html += '</table></div>';
    return html;
  }

  function renderCalibration(byBucket){
    var buckets = ["0.30-0.50","0.50-0.65","0.65-0.80","0.80-0.95"];
    var html = '<div class="jhsk-calibration">' +
                 '<div style="font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;font-weight:700">Confidence Calibration · claimed probability vs actual hit rate</div>' +
                 '<div class="jhsk-cal-grid">';
    buckets.forEach(function(b){
      var cb = (byBucket || {})[b];
      var hr = cb ? cb.hit_rate : null;
      var n = cb ? cb.n : 0;
      var cls = classifyCalibration(b, hr);
      html += '<div class="jhsk-cal-cell ' + cls + '">' +
                '<div class="b">claimed ' + esc(b) + '</div>' +
                '<div class="a">' + esc(hr == null ? '—' : pct(hr)) + '</div>' +
                '<div class="n">n=' + esc(n) + ' · ' + esc(cls || 'pending') + '</div>' +
              '</div>';
    });
    html += '</div></div>';
    return html;
  }

  function renderRecent(recent){
    if (!recent || !recent.length) return '<div class="jhsk-empty">No graded predictions yet — they\'ll appear here after the outcome-checker scores them (typically 1-30 days after logging).</div>';
    var html = '<div class="jhsk-recent">' +
                 '<div class="jhsk-rec-hdr">Last ' + esc(recent.length) + ' graded predictions</div>';
    recent.forEach(function(r){
      var cls = r.is_correct ? 'correct' : 'wrong';
      var retCls = (r.return_pct == null) ? '' : (r.return_pct >= 0 ? 'pos' : 'neg');
      html += '<div class="jhsk-rec-row">' +
                '<span class="jhsk-rec-dot ' + cls + '"></span>' +
                '<span class="jhsk-rec-ts">' + esc(ageOf(r.logged_at)) + ' ago</span>' +
                '<span class="jhsk-rec-detail">' +
                  '<b>' + esc(r.engine || r.signal_type || '?') + '</b> · ' +
                  '<b>' + esc(r.asset || '?') + '</b> ' + esc(r.direction || '') + ' ' +
                  '<span style="color:#6f7b91">(' + esc(r.window || '?') + ')</span>' +
                '</span>' +
                '<span class="jhsk-rec-conf">conf ' + esc(((r.confidence||0)*100).toFixed(0)) + '%</span>' +
                '<span class="jhsk-rec-ret ' + retCls + '">' + esc(fmtRet(r.return_pct)) + '</span>' +
              '</div>';
    });
    html += '</div>';
    return html;
  }

  function renderImprovementLog(cfg){
    if (!cfg) return '<div class="jhsk-empty">No calibration config yet — populates on the first self-improvement-calibrator run.</div>';
    var overrides = cfg.engine_overrides || {};
    var history = cfg.history || [];
    var pending = cfg.pending_proposals || [];
    var nEngines = Object.keys(overrides).length;

    var html = '';

    // Current active calibration scales
    html += '<div class="jhsk-row-grid">' +
              '<div class="h">Active calibration overrides · version ' + esc(cfg.version || '1.0') + '</div>';
    if (nEngines === 0) {
      html += '<div style="padding:10px 4px;color:#6f7b91;font-size:11.5px;font-family:ui-monospace,monospace">No engine has crossed the significance threshold yet (need n_scored ≥ ' + esc((cfg.thresholds||{}).min_n_scored || 20) + '). All engines running at confidence_scale = 1.00 (no calibration).</div>';
    } else {
      html += '<table>';
      Object.keys(overrides).forEach(function(eng){
        var ovr = overrides[eng] || {};
        var scale = ovr.confidence_scale || 1.0;
        var direction = scale > 1.0 ? 'upweight' : (scale < 1.0 ? 'downweight' : 'neutral');
        var color = scale > 1.05 ? '#26ffaf' : (scale < 0.95 ? '#fbbf24' : '#a8b3c7');
        html += '<tr>' +
                  '<td class="lbl">' + esc(eng.replace(/_/g,' ')) + '</td>' +
                  '<td class="bar-cell"><div style="display:flex;align-items:center;gap:9px;font-size:11px;color:#6f7b91"><span style="color:' + color + ';font-weight:700">' + esc(direction) + '</span><span>n=' + esc(ovr.n_predictions_at_tweak || 0) + '</span></div></td>' +
                  '<td class="v" style="color:' + color + '">×' + esc(scale.toFixed(2)) + '</td>' +
                '</tr>';
      });
      html += '</table>';
    }
    html += '</div>';

    // Recent tweaks log
    if (history.length > 0) {
      html += '<div class="jhsk-recent">' +
                '<div class="jhsk-rec-hdr">Recent calibration tweaks (last ' + esc(Math.min(history.length, 10)) + ')</div>';
      history.slice(-10).reverse().forEach(function(t){
        var statusColor = t.status === 'AUTO_APPLIED' ? '#26ffaf' : (t.status === 'PENDING_REVIEW' ? '#fbbf24' : '#6f7b91');
        var dirIcon = t.calibration_type === 'underconfident' ? '↑' : '↓';
        html += '<div class="jhsk-rec-row" style="grid-template-columns:14px 90px 1fr auto 80px">' +
                  '<span class="jhsk-rec-dot ' + (t.status === 'AUTO_APPLIED' ? 'correct' : 'wrong') + '" style="background:' + statusColor + '"></span>' +
                  '<span class="jhsk-rec-ts">' + esc(ageOf(t.proposed_at || t.applied_at)) + ' ago</span>' +
                  '<span class="jhsk-rec-detail">' +
                    '<b>' + esc(t.engine) + '</b> ' + esc(dirIcon) + ' ' +
                    esc((t.current_scale||1).toFixed(2)) + ' → <b>' + esc((t.proposed_scale||1).toFixed(2)) + '</b> · ' +
                    '<span style="color:#6f7b91">' + esc((t.calibration_type||'')) + ' by ' + esc((Math.abs(t.calibration_error||0)*100).toFixed(0)) + '% (n=' + esc(t.n_predictions||0) + ')</span>' +
                  '</span>' +
                  '<span class="jhsk-rec-conf" style="color:' + statusColor + '">' + esc(t.status || '?') + '</span>' +
                  '<span class="jhsk-rec-ret">v' + esc((t.tweak_id||'').split('-')[1] || '?') + '</span>' +
                '</div>';
      });
      html += '</div>';
    } else {
      html += '<div class="jhsk-empty" style="margin-top:11px">No tweaks yet — calibrator runs daily but only acts when an engine accumulates ≥' + esc((cfg.thresholds||{}).min_n_scored || 20) + ' scored predictions with calibration error ≥' + esc(((cfg.thresholds||{}).err_threshold || 0.10)*100) + '%.</div>';
    }

    // Pending proposals (require human review)
    if (pending && pending.length > 0) {
      html += '<div class="jhsk-row-grid" style="margin-top:11px;border-left:3px solid #fbbf24">' +
                '<div class="h" style="color:#fbbf24">⏸ Pending review · ' + esc(pending.length) + ' proposals</div>' +
                '<table>';
      pending.forEach(function(p){
        html += '<tr>' +
                  '<td class="lbl">' + esc(p.engine) + '</td>' +
                  '<td colspan="2" style="color:#cbd2dc">' + esc((p.current_scale||1).toFixed(2)) + ' → ' + esc((p.proposed_scale||1).toFixed(2)) + ' · <span style="color:#6f7b91">' + esc(p.reason_pending || 'step too large') + '</span></td>' +
                '</tr>';
      });
      html += '</table></div>';
    }

    return html;
  }

  function renderOpportunities(rank){
    if (!rank || !rank.ranked_opportunities) return '<div class="jhsk-empty">No opportunity rankings yet — populates after the next opportunity-ranker run (every 4h).</div>';
    var opps = rank.ranked_opportunities || [];
    if (opps.length === 0) return '<div class="jhsk-empty">No active opportunities right now — sniffers have no setups in this cycle.</div>';

    var html = '<div style="font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-bottom:10px;line-height:1.55">' +
                 '<b style="color:#cbd2dc">Formula:</b> <code style="background:#11161f;padding:1px 6px;border-radius:3px;color:#22d3ee">score = engine_hit_rate × claimed_conf × recency × (0.5 + 0.5×richness)</code><br>' +
                 'Engines with strong track records get higher scores. Newer setups score higher than stale ones. More confluence (categories/pillars) = more weight.' +
               '</div>';

    html += '<div class="jhsk-recent">' +
              '<div class="jhsk-rec-hdr">Top ' + esc(Math.min(opps.length, 12)) + ' opportunities · ranked by closed-loop score</div>';
    opps.slice(0, 12).forEach(function(o){
      var scoreColor = o.score >= 0.45 ? '#26ffaf' : (o.score >= 0.30 ? '#fbbf24' : '#a8b3c7');
      var sourceColor = o.source === 'macro_sniffer' ? '#22d3ee' : '#ff7a18';
      html += '<div class="jhsk-rec-row" style="grid-template-columns:36px 70px 1fr 100px 60px">' +
                '<span style="font-family:ui-monospace,monospace;font-size:13px;font-weight:800;color:#a78bfa">#' + esc(o.rank) + '</span>' +
                '<span style="font-family:ui-monospace,monospace;font-size:11px;color:' + sourceColor + ';font-weight:700">' + esc(o.engine.indexOf('macro')>-1?'🏛 MACRO':'🎯 EQUITY') + '</span>' +
                '<span class="jhsk-rec-detail">' +
                  '<b>' + esc(o.asset) + '</b> ' + esc(o.direction) + ' · ' +
                  '<span style="color:#6f7b91">hit_rate ' + esc((o.engine_hit_rate*100).toFixed(0)) + '% × conf ' + esc((o.claimed_confidence*100).toFixed(0)) + '%</span>' +
                '</span>' +
                '<span style="text-align:right;font-family:ui-monospace,monospace;font-size:11px;color:#6f7b91">' + esc(o.age_hours.toFixed(1)) + 'h old</span>' +
                '<span style="text-align:right;font-family:ui-monospace,monospace;font-weight:800;font-size:13px;color:' + scoreColor + '">' + esc(o.score.toFixed(3)) + '</span>' +
              '</div>';
    });
    html += '</div>';

    if (rank.generated_at) {
      html += '<div style="margin-top:8px;font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;text-align:right">refreshed ' + esc(ageOf(rank.generated_at)) + ' ago · ' + esc(opps.length) + ' total ranked</div>';
    }
    return html;
  }

  function mount(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhsk-wrap");
    el.innerHTML = '<div class="jhsk-loading">🧠 loading skill + calibration + opportunities…</div>';

    return Promise.all([
      fetchJSON("_skill/frontrun-skill-index.json"),
      fetchJSON("_skill/calibration-config.json"),
      fetchJSON("_skill/opportunity-rankings.json"),
    ]).then(function (results) {
      var idx = results[0], cal = results[1], opps = results[2];
      if (!idx) {
        el.innerHTML = '<div class="jhsk-empty">' +
                       'No skill index yet — populates on the first skill-aggregator run.<br>' +
                       'The router needs to scan DynamoDB for graded predictions from signal-logger + outcome-checker. ' +
                       'After 7-30 days of normal operation the cockpit becomes meaningful.' +
                       '</div>';
        return null;
      }
      var html = "";

      // ─── Section 1: Best Opportunities (top of cockpit — most actionable) ───
      html += '<div class="jhsk-section-h">🏆 Best Opportunities — closed-loop ranked</div>';
      html += renderOpportunities(opps);

      // ─── Section 2: Self-Improvement Log (system applying what it learned) ───
      html += '<div class="jhsk-section-h">🔧 Self-Improvement · per-engine calibration overrides</div>';
      html += renderImprovementLog(cal);

      // ─── Section 3: Overall System Skill ───
      html += '<div class="jhsk-section-h">🧠 System Skill — overall accuracy</div>';
      html += renderHeadline(idx);
      html += '<div class="jhsk-section-h">⚙️ Per-Engine Leaderboard</div>';
      html += renderEngines(idx.by_engine);
      html += '<div class="jhsk-section-h">📊 By Regime</div>';
      html += renderRegimes(idx.by_regime);
      html += '<div class="jhsk-section-h">🎯 Calibration</div>';
      html += renderCalibration(idx.by_confidence_bucket);
      html += '<div class="jhsk-section-h">🗓 Recent Graded Predictions</div>';
      html += renderRecent(idx.recent_calls);

      html += '<div class="jhsk-meta">' +
                '<b>The closed loop:</b>' +
                ' Sniffers produce setups → signal-logger writes predictions to DynamoDB (with calibration_version stamped) → outcome-checker grades them → skill aggregator computes per-engine accuracy → self-improvement-calibrator detects miscalibration and writes calibration-config.json → signal-logger reads that config on next cycle and scales confidence claims accordingly. The system applies what it has learned.' +
                '<br><b>Auto-application rules:</b> bounded step size ≤ 0.15 per cycle · scale clamped to [0.5, 1.5] · n_scored ≥ 20 required for any tweak · |error| ≥ 10% required to trigger. Half-step convergence prevents oscillation.' +
                '<br><b>Verification:</b> every tweak gets a calibration_version stamped onto subsequent predictions, so before/after hit rates per version can be measured. The system can prove its own improvement.' +
                '<br><b>Coming next:</b> Claude-driven prompt rewrites for systematically wrong engines (Phase 3 of the loop — beyond mere scaling).' +
              '</div>';

      el.innerHTML = html;
      return {idx: idx, cal: cal, opps: opps};
    }).catch(function (e) {
      el.innerHTML = '<div class="jhsk-err">Skill cockpit unavailable: ' + esc(e.message || e) + '</div>';
      throw e;
    });
  }

  window.JHAISkill = { mount: mount, version: "1.0.0" };
})();
