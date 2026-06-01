/**
 * ai-frontrun-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Front-run sniffer renderer. Designed for high-contrast urgency.
 *
 *   <script src="/ai-frontrun-kit.js"></script>
 *   <div id="ai-frontrun"></div>
 *   <script>JHAIFront.mount('ai-frontrun');</script>
 */
(function () {
  if (window.JHAIFront) return;
  var PROXY = "https://justhodl-dashboard-live.s3.amazonaws.com/data";
  var DEFAULT_KEY = "frontrun-sniffer";

  function injectCSS() {
    if (document.getElementById("jh-ai-front-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-front-css";
    s.textContent = "\n.jhfr-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jhfr-headline{background:linear-gradient(135deg,rgba(255,122,24,.10),rgba(255,85,119,.06));border:1px solid rgba(255,122,24,.32);border-left:4px solid #ff7a18;border-radius:11px;padding:20px 24px;margin-bottom:14px;position:relative}\n.jhfr-headline.EXTREME{border-color:rgba(255,85,119,.55);border-left-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.14),rgba(255,122,24,.06));animation:jhfr-pulse 2s ease-in-out infinite}\n.jhfr-headline.NORMAL{border-color:rgba(255,210,102,.32);border-left-color:#ffd266;background:linear-gradient(135deg,rgba(255,210,102,.06),rgba(0,212,255,.04))}\n@keyframes jhfr-pulse{0%,100%{box-shadow:0 0 0 0 rgba(255,85,119,.20)}50%{box-shadow:0 0 30px 5px rgba(255,85,119,.18)}}\n\n.jhfr-meta{display:flex;align-items:center;gap:11px;margin-bottom:11px;flex-wrap:wrap}\n.jhfr-badge{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#ff7a18;font-weight:800}\n.jhfr-regime-pill{font-family:ui-monospace,monospace;font-size:13px;font-weight:800;padding:5px 12px;border-radius:5px;letter-spacing:0.5px}\n.jhfr-regime-pill.NORMAL{background:rgba(255,210,102,.16);color:#ffd266;border:1px solid rgba(255,210,102,.32)}\n.jhfr-regime-pill.ELEVATED{background:rgba(255,122,24,.18);color:#ff7a18;border:1px solid rgba(255,122,24,.42)}\n.jhfr-regime-pill.EXTREME{background:rgba(255,85,119,.22);color:#ff5577;border:1px solid rgba(255,85,119,.55)}\n.jhfr-score-box{font-family:ui-monospace,monospace;font-size:14px;font-weight:700;color:#e6eaf2;padding:5px 12px;background:#11161f;border:1px solid #1c2433;border-radius:4px}\n.jhfr-score-box .lbl{font-size:9.5px;color:#6f7b91;letter-spacing:1px;text-transform:uppercase;margin-right:6px}\n.jhfr-age{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-left:auto}\n\n.jhfr-headline-text{font-size:17px;font-weight:700;color:#e6eaf2;line-height:1.45;margin-bottom:10px}\n.jhfr-thesis{font-size:13.5px;color:#cbd2dc;line-height:1.65}\n\n.jhfr-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#ff7a18;margin:18px 0 8px;padding-bottom:5px;border-bottom:1px solid rgba(255,122,24,.18)}\n\n.jhfr-loudest{background:#11161f;border:1px solid #1c2433;border-left:3px solid #ff5577;border-radius:7px;padding:12px 15px;margin-bottom:11px}\n.jhfr-loudest .hdr{font-family:ui-monospace,monospace;font-size:10.5px;color:#ff5577;letter-spacing:1.2px;text-transform:uppercase;font-weight:700;margin-bottom:6px}\n.jhfr-loudest .sig{font-size:14px;font-weight:700;color:#e6eaf2;line-height:1.4;margin-bottom:5px}\n.jhfr-loudest .vals{display:flex;gap:14px;font-family:ui-monospace,monospace;font-size:11px;color:#a8b3c7;margin-bottom:6px;flex-wrap:wrap}\n.jhfr-loudest .vals b{color:#ff7a18}\n.jhfr-loudest .interp{font-size:12.5px;color:#cbd2dc;line-height:1.55;font-style:italic}\n\n.jhfr-setup{background:#11161f;border:1px solid #1c2433;border-left:5px solid #ff7a18;border-radius:9px;padding:15px 18px;margin-bottom:11px}\n.jhfr-setup.HIGH{border-left-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.04),transparent 40%)}\n.jhfr-setup.MEDIUM{border-left-color:#ff7a18}\n.jhfr-setup.LOW{border-left-color:#ffd266}\n.jhfr-setup-head{display:flex;align-items:baseline;gap:10px;margin-bottom:8px;flex-wrap:wrap}\n.jhfr-setup-rank{font-family:ui-monospace,monospace;font-size:11px;color:#6f7b91;font-weight:700}\n.jhfr-setup-target{font-family:ui-monospace,monospace;font-size:17px;font-weight:800;color:#e6eaf2;letter-spacing:0.5px}\n.jhfr-setup-dir{font-family:ui-monospace,monospace;font-size:11px;font-weight:700;padding:3px 9px;border-radius:3px;letter-spacing:0.5px}\n.jhfr-setup-dir.UPSIDE{background:rgba(38,255,175,.16);color:#26ffaf}\n.jhfr-setup-dir.DOWNSIDE{background:rgba(255,85,119,.18);color:#ff5577}\n.jhfr-setup-dir.SIDEWAYS_SQUEEZE{background:rgba(167,139,250,.16);color:#a78bfa}\n.jhfr-setup-conf{font-family:ui-monospace,monospace;font-size:10.5px;font-weight:700;padding:3px 8px;border-radius:3px;color:#a8b3c7;background:#1c2433}\n.jhfr-setup-conf.HIGH{color:#26ffaf;background:rgba(38,255,175,.14)}\n.jhfr-setup-conf.MEDIUM{color:#ffd266;background:rgba(255,210,102,.13)}\n.jhfr-setup-prob{font-family:ui-monospace,monospace;font-size:11.5px;color:#00d4ff;font-weight:700;margin-left:auto}\n.jhfr-setup-meta{display:flex;flex-wrap:wrap;gap:14px;font-family:ui-monospace,monospace;font-size:11.5px;color:#a8b3c7;margin-bottom:9px}\n.jhfr-setup-meta b{color:#e6eaf2}\n.jhfr-who{font-size:12.5px;color:#cbd2dc;margin-bottom:9px}\n.jhfr-who b{color:#ff7a18}\n\n.jhfr-guns{margin:9px 0;background:#0d1219;border:1px dashed #1c2433;border-radius:6px;padding:9px 12px}\n.jhfr-guns-hdr{font-family:ui-monospace,monospace;font-size:9.5px;color:#ff7a18;letter-spacing:1.2px;text-transform:uppercase;font-weight:700;margin-bottom:5px}\n.jhfr-gun{font-size:12px;color:#cbd2dc;line-height:1.5;margin:3px 0;padding-left:6px;border-left:2px solid #1c2433}\n.jhfr-gun .cat{display:inline-block;font-family:ui-monospace,monospace;font-size:9.5px;color:#a78bfa;letter-spacing:0.5px;font-weight:700;padding:1px 5px;background:rgba(167,139,250,.10);border-radius:2px;margin-right:5px}\n.jhfr-gun .cat.rates{color:#fbbf24;background:rgba(251,191,36,.10)}\n.jhfr-gun .cat.auction{color:#22d3ee;background:rgba(34,211,238,.10)}\n.jhfr-gun .cat.funding{color:#ef4444;background:rgba(239,68,68,.10)}\n.jhfr-gun .cat.macro{color:#22c55e;background:rgba(34,197,94,.10)}\n.jhfr-gun .pctile{font-family:ui-monospace,monospace;font-size:10px;color:#ff5577;font-weight:700;margin-left:5px}\n\n.jhfr-catalyst{display:flex;gap:10px;align-items:baseline;font-size:12px;color:#a8b3c7;margin:7px 0;padding:7px 11px;background:rgba(0,212,255,.05);border-left:2px solid #00d4ff;border-radius:4px}\n.jhfr-catalyst .lbl{font-family:ui-monospace,monospace;font-size:9.5px;color:#00d4ff;letter-spacing:0.8px;text-transform:uppercase;font-weight:700;flex-shrink:0}\n.jhfr-catalyst b{color:#e6eaf2}\n\n.jhfr-analog{font-size:11.5px;color:#a78bfa;margin:7px 0;font-style:italic;line-height:1.5}\n.jhfr-analog b{color:#a78bfa;font-style:normal;font-weight:700}\n\n.jhfr-actions{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:9px}\n@media (max-width:700px){.jhfr-actions{grid-template-columns:1fr}}\n.jhfr-ride,.jhfr-fade{background:#0d1219;border:1px solid #1c2433;border-radius:6px;padding:9px 11px;font-size:11.5px;line-height:1.55}\n.jhfr-ride{border-left:3px solid #26ffaf}\n.jhfr-ride .hdr{font-family:ui-monospace,monospace;font-size:9.5px;color:#26ffaf;letter-spacing:1px;text-transform:uppercase;font-weight:700;margin-bottom:4px}\n.jhfr-fade{border-left:3px solid #ff5577}\n.jhfr-fade .hdr{font-family:ui-monospace,monospace;font-size:9.5px;color:#ff5577;letter-spacing:1px;text-transform:uppercase;font-weight:700;margin-bottom:4px}\n\n.jhfr-trip{margin-top:8px;font-size:11.5px;color:#ffd266;padding:7px 10px;background:rgba(255,210,102,.06);border-left:2px solid #ffd266;border-radius:4px}\n.jhfr-trip b{font-family:ui-monospace,monospace;font-size:9.5px;color:#ffd266;text-transform:uppercase;letter-spacing:0.8px;font-weight:700;margin-right:6px}\n\n.jhfr-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:9px;margin-bottom:6px}\n.jhfr-card{background:#11161f;border:1px solid #1c2433;border-left:3px solid #a78bfa;border-radius:7px;padding:10px 13px;font-size:12px;line-height:1.5}\n.jhfr-card .top{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:4px;gap:8px}\n.jhfr-card .asset{font-family:ui-monospace,monospace;font-size:14px;font-weight:800;color:#e6eaf2;letter-spacing:0.3px}\n.jhfr-card .channel{font-family:ui-monospace,monospace;font-size:9.5px;color:#a78bfa;letter-spacing:0.6px;font-weight:700;padding:1px 5px;background:rgba(167,139,250,.10);border-radius:2px}\n.jhfr-card .actor{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-bottom:4px}\n.jhfr-card .actor b{color:#e6eaf2}\n.jhfr-card .impl{color:#cbd2dc;font-size:11.5px;line-height:1.5}\n.jhfr-card.dealer{border-left-color:#ff7a18}\n.jhfr-card.dealer .channel{color:#ff7a18;background:rgba(255,122,24,.10)}\n.jhfr-card.insider{border-left-color:#26ffaf}\n.jhfr-card.insider .channel{color:#26ffaf;background:rgba(38,255,175,.10)}\n\n.jhfr-actionable{background:linear-gradient(135deg,rgba(255,122,24,.08),rgba(38,255,175,.04));border:1px solid rgba(255,122,24,.35);border-left:5px solid #ff7a18;border-radius:9px;padding:14px 17px;margin-top:14px}\n.jhfr-actionable .hdr{font-family:ui-monospace,monospace;font-size:11px;color:#ff7a18;font-weight:800;letter-spacing:1.4px;text-transform:uppercase;margin-bottom:7px}\n.jhfr-actionable .txt{font-size:14px;color:#e6eaf2;font-weight:600;line-height:1.5}\n\n.jhfr-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhfr-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function ageStr(iso) {
    if (!iso) return "";
    var m = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
    if (m < 1) return "just now"; if (m < 60) return m + "m ago";
    if (m < 1440) return Math.round(m / 60) + "h ago"; return Math.round(m / 1440) + "d ago";
  }

  function mount(elId, contextSlug, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhfr-wrap");
    el.innerHTML = '<div class="jhfr-loading">🎯 sniffing 25+ flow feeds for institutional front-running…</div>';
    var url = PROXY + "/" + (contextSlug || DEFAULT_KEY) + ".json?t=" + Date.now();
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (b) {
      var regime = (b.anomaly_regime || "NORMAL").toUpperCase();
      var html = '';
      // HEADLINE
      html += '<div class="jhfr-headline ' + regime + '">' +
                '<div class="jhfr-meta">' +
                  '<span class="jhfr-badge">🎯 FRONT-RUN SNIFFER · 25-FEED INSTITUTIONAL FLOW SCAN</span>' +
                  '<span class="jhfr-regime-pill ' + regime + '">' + esc(regime) + '</span>' +
                  '<span class="jhfr-score-box"><span class="lbl">ANOM</span>' + esc(b.overall_anomaly_score == null ? '—' : b.overall_anomaly_score) + '/100</span>' +
                  '<span class="jhfr-age">' + esc(ageStr(b.generated_at)) + '</span>' +
                '</div>' +
                (b.headline ? '<div class="jhfr-headline-text">' + esc(b.headline) + '</div>' : '') +
                (b.thesis ? '<div class="jhfr-thesis">' + esc(b.thesis) + '</div>' : '') +
              '</div>';

      // LOUDEST ANOMALY
      var la = b.loudest_anomaly;
      if (la && la.signal) {
        html += '<div class="jhfr-loudest">' +
                  '<div class="hdr">⚠ Loudest single-feed anomaly right now</div>' +
                  '<div class="sig">' + esc(la.signal) + '</div>' +
                  '<div class="vals">' +
                    '<span>feed: <b>' + esc(la.from_feed) + '</b></span>' +
                    '<span>value: <b>' + esc(la.value) + '</b></span>' +
                    '<span>pctile: <b>' + esc(la.anomaly_pctile) + '</b></span>' +
                  '</div>' +
                  (la.interpretation ? '<div class="interp">' + esc(la.interpretation) + '</div>' : '') +
                '</div>';
      }

      // SUSPECTED SETUPS — main payload
      if (b.suspected_setups && b.suspected_setups.length) {
        html += '<div class="jhfr-section-h">🎯 Suspected Front-Run Setups — convergent across 3+ flow categories</div>';
        b.suspected_setups.forEach(function (sx) {
          var conf = (sx.confidence || "MEDIUM").toUpperCase();
          var dir = (sx.target_direction || "UPSIDE").toUpperCase();
          html += '<div class="jhfr-setup ' + conf + '">' +
                    '<div class="jhfr-setup-head">' +
                      '<span class="jhfr-setup-rank">#' + esc(sx.rank) + '</span>' +
                      '<span class="jhfr-setup-target">' + esc(sx.target_asset) + '</span>' +
                      '<span class="jhfr-setup-dir ' + dir + '">' + esc(dir.replace(/_/g, " ")) + '</span>' +
                      '<span class="jhfr-setup-conf ' + conf + '">conf: ' + esc(conf) + '</span>' +
                      '<span class="jhfr-setup-prob">' + esc(sx.probability_pct || '?') + '% prob</span>' +
                    '</div>' +
                    '<div class="jhfr-setup-meta">' +
                      '<span>magnitude: <b>' + esc(sx.magnitude_pct) + '</b></span>' +
                      '<span>horizon: <b>' + esc(sx.horizon) + '</b></span>' +
                    '</div>' +
                    (sx.who_is_positioning ? '<div class="jhfr-who">👥 <b>Who:</b> ' + esc(sx.who_is_positioning) + '</div>' : '') +

                    (sx.smoking_gun_signals && sx.smoking_gun_signals.length ?
                      '<div class="jhfr-guns">' +
                        '<div class="jhfr-guns-hdr">🔫 Smoking Gun Signals (' + sx.smoking_gun_signals.length + ' convergent)</div>' +
                        sx.smoking_gun_signals.map(function (g) {
                          var cat = String(g.category || '').toUpperCase();
                          var catClass = '';
                          if (/AUCTION|TIC|PRIMARY_DEALER|DEALER_SURVEY|SOVEREIGN/.test(cat)) catClass = ' auction';
                          else if (/FUNDING|EURODOLLAR|PLUMBING|SOFR|FRA_OIS|BTFP|SWAP_LINE/.test(cat)) catClass = ' funding';
                          else if (/BOND_VOL|YIELD|MOVE|DURATION|CURVE|RATES|BREAKEVEN/.test(cat)) catClass = ' rates';
                          else if (/NET_LIQUIDITY|WALCL|TGA|RRP|MACRO|NOWCAST|FED_SPEAK|FED_NLP|FED_PATH/.test(cat)) catClass = ' macro';
                          return '<div class="jhfr-gun"><span class="cat' + catClass + '">' + esc(g.category) + '</span>' +
                                 esc(g.signal) + (g.anomaly_pctile != null ? '<span class="pctile">pctile ' + esc(g.anomaly_pctile) + '</span>' : '') +
                                 '</div>';
                        }).join('') +
                      '</div>' : '') +

                    (sx.catalyst_being_front_run ? '<div class="jhfr-catalyst"><span class="lbl">CATALYST</span>' + esc(sx.catalyst_being_front_run) + (sx.catalyst_date ? ' · <b>' + esc(sx.catalyst_date) + '</b>' : '') + '</div>' : '') +

                    (sx.historical_analog && sx.historical_analog.period ?
                      '<div class="jhfr-analog">📐 Analog: <b>' + esc(sx.historical_analog.period) + '</b> — ' + esc(sx.historical_analog.what_happened || '') + (sx.historical_analog.similarity_pct ? ' · similarity <b>' + esc(sx.historical_analog.similarity_pct) + '%</b>' : '') + '</div>' : '') +

                    '<div class="jhfr-actions">' +
                      (sx.ride_this_flow ? '<div class="jhfr-ride"><div class="hdr">▲ Ride this flow</div>' + esc(sx.ride_this_flow) + '</div>' : '') +
                      (sx.fade_this_flow ? '<div class="jhfr-fade"><div class="hdr">▼ Fade this flow</div>' + esc(sx.fade_this_flow) + '</div>' : '') +
                    '</div>' +

                    (sx.invalidation_tripwire ? '<div class="jhfr-trip"><b>Invalidation:</b> ' + esc(sx.invalidation_tripwire) + '</div>' : '') +
                  '</div>';
        });
      }

      // WHALE ALERTS
      if (b.whale_alerts && b.whale_alerts.length) {
        html += '<div class="jhfr-section-h">🐋 Whale Alerts — unusual position changes</div>';
        html += '<div class="jhfr-grid">';
        b.whale_alerts.forEach(function (w) {
          html += '<div class="jhfr-card">' +
                    '<div class="top"><span class="asset">' + esc(w.asset) + '</span>' +
                      '<span class="channel">' + esc(w.channel) + '</span></div>' +
                    '<div class="actor"><b>' + esc(w.actor || '?') + '</b> · ' + esc(w.direction || '') + ' · ' + esc(w.size || '') + '</div>' +
                    (w.implication ? '<div class="impl">' + esc(w.implication) + '</div>' : '') +
                  '</div>';
        });
        html += '</div>';
      }

      // DEALER HEDGING FLOWS
      if (b.dealer_hedging_flows && b.dealer_hedging_flows.length) {
        html += '<div class="jhfr-section-h">🎩 Dealer Hedging Flows — GEX shifts, put walls, gamma stress</div>';
        html += '<div class="jhfr-grid">';
        b.dealer_hedging_flows.forEach(function (d) {
          html += '<div class="jhfr-card dealer">' +
                    '<div class="top"><span class="asset">' + esc(d.asset) + '</span>' +
                      '<span class="channel">' + esc(d.flow_type) + '</span></div>' +
                    '<div class="actor">' + esc(d.signal || '') + '</div>' +
                    (d.implication ? '<div class="impl">' + esc(d.implication) + '</div>' : '') +
                  '</div>';
        });
        html += '</div>';
      }

      // INSIDER CAPITULATION
      if (b.insider_capitulation_alerts && b.insider_capitulation_alerts.length) {
        html += '<div class="jhfr-section-h">👁 Insider Capitulation Alerts</div>';
        html += '<div class="jhfr-grid">';
        b.insider_capitulation_alerts.forEach(function (i) {
          html += '<div class="jhfr-card insider">' +
                    '<div class="top"><span class="asset">' + esc(i.ticker) + '</span>' +
                      '<span class="channel">' + esc(i.pattern) + '</span></div>' +
                    (i.implication ? '<div class="impl">' + esc(i.implication) + '</div>' : '') +
                  '</div>';
        });
        html += '</div>';
      }

      // MOST ACTIONABLE
      if (b.most_actionable_setup) {
        html += '<div class="jhfr-actionable">' +
                  '<div class="hdr">⚡ Most Actionable Setup Right Now</div>' +
                  '<div class="txt">' + esc(b.most_actionable_setup) + '</div>' +
                '</div>';
      }

      el.innerHTML = html;
      return b;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhfr-err">Front-run sniffer unavailable: ' + esc(e.message || e) + '</div>';
      if (opts && typeof opts.onError === "function") opts.onError(e);
      throw e;
    });
  }
  window.JHAIFront = { mount: mount, version: "1.0.0" };
})();
