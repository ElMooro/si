/**
 * ai-macro-frontrun-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Macro front-run sniffer renderer. Pillar-organized layout with
 * trade-specific setup cards (DV01-aware sizing).
 *
 *   <script src="/ai-macro-frontrun-kit.js"></script>
 *   <div id="ai-macro-frontrun"></div>
 *   <script>JHAIMacroFront.mount('ai-macro-frontrun');</script>
 */
(function () {
  if (window.JHAIMacroFront) return;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var DEFAULT_KEY = "macro-frontrun-sniffer";

  function injectCSS() {
    if (document.getElementById("jh-ai-macrofr-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-macrofr-css";
    s.textContent = "\n.jhmf-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jhmf-headline{background:linear-gradient(135deg,rgba(34,211,238,.08),rgba(167,139,250,.06));border:1px solid rgba(34,211,238,.32);border-left:4px solid #22d3ee;border-radius:11px;padding:20px 24px;margin-bottom:14px}\n.jhmf-headline.EXTREME{border-color:rgba(255,85,119,.55);border-left-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.14),rgba(255,122,24,.06));animation:jhmf-pulse 2s ease-in-out infinite}\n.jhmf-headline.NORMAL{border-color:rgba(38,255,175,.32);border-left-color:#26ffaf;background:linear-gradient(135deg,rgba(38,255,175,.06),rgba(34,211,238,.04))}\n@keyframes jhmf-pulse{0%,100%{box-shadow:0 0 0 0 rgba(255,85,119,.20)}50%{box-shadow:0 0 30px 5px rgba(255,85,119,.18)}}\n\n.jhmf-meta{display:flex;align-items:center;gap:11px;margin-bottom:11px;flex-wrap:wrap}\n.jhmf-badge{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#22d3ee;font-weight:800}\n.jhmf-regime-pill{font-family:ui-monospace,monospace;font-size:13px;font-weight:800;padding:5px 12px;border-radius:5px;letter-spacing:0.5px}\n.jhmf-regime-pill.NORMAL{background:rgba(38,255,175,.16);color:#26ffaf;border:1px solid rgba(38,255,175,.32)}\n.jhmf-regime-pill.ELEVATED{background:rgba(255,210,102,.18);color:#ffd266;border:1px solid rgba(255,210,102,.42)}\n.jhmf-regime-pill.EXTREME{background:rgba(255,85,119,.22);color:#ff5577;border:1px solid rgba(255,85,119,.55)}\n.jhmf-score-box{font-family:ui-monospace,monospace;font-size:14px;font-weight:700;color:#e6eaf2;padding:5px 12px;background:#11161f;border:1px solid #1c2433;border-radius:4px}\n.jhmf-score-box .lbl{font-size:9.5px;color:#6f7b91;letter-spacing:1px;text-transform:uppercase;margin-right:6px}\n.jhmf-age{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-left:auto}\n\n.jhmf-headline-text{font-size:17px;font-weight:700;color:#e6eaf2;line-height:1.45;margin-bottom:10px}\n.jhmf-thesis{font-size:13.5px;color:#cbd2dc;line-height:1.65}\n\n.jhmf-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#22d3ee;margin:18px 0 8px;padding-bottom:5px;border-bottom:1px solid rgba(34,211,238,.18)}\n\n.jhmf-pillars{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:9px;margin-bottom:12px}\n.jhmf-pillar{background:#11161f;border:1px solid #1c2433;border-radius:8px;padding:11px 13px;border-left:4px solid #6f7b91}\n.jhmf-pillar.auction_tape{border-left-color:#22d3ee}\n.jhmf-pillar.primary_dealer_positioning{border-left-color:#22d3ee}\n.jhmf-pillar.funding_plumbing{border-left-color:#ef4444}\n.jhmf-pillar.net_liquidity{border-left-color:#22c55e}\n.jhmf-pillar.fed_path{border-left-color:#a78bfa}\n.jhmf-pillar.yield_curve_velocity{border-left-color:#fbbf24}\n.jhmf-pillar.tic_flows{border-left-color:#22d3ee}\n.jhmf-pillar-hdr{display:flex;align-items:baseline;gap:7px;margin-bottom:5px}\n.jhmf-pillar-name{font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#a8b3c7;font-weight:700}\n.jhmf-pillar-state{font-family:ui-monospace,monospace;font-size:11px;font-weight:800;padding:1px 7px;border-radius:3px;letter-spacing:0.5px;margin-left:auto}\n.jhmf-pillar-state.CALM,.jhmf-pillar-state.HEALED,.jhmf-pillar-state.EXPANDING,.jhmf-pillar-state.NEUTRAL,.jhmf-pillar-state.INFLOW{background:rgba(38,255,175,.14);color:#26ffaf}\n.jhmf-pillar-state.NORMAL,.jhmf-pillar-state.FLATTENING,.jhmf-pillar-state.STEEPENING{background:rgba(34,211,238,.14);color:#22d3ee}\n.jhmf-pillar-state.TIGHT,.jhmf-pillar-state.STRESSED,.jhmf-pillar-state.HAWKISH_TILT,.jhmf-pillar-state.DOVISH_TILT,.jhmf-pillar-state.DURATION_BID,.jhmf-pillar-state.DURATION_SHORT,.jhmf-pillar-state.CONTRACTING,.jhmf-pillar-state.BEAR_FLATTENER,.jhmf-pillar-state.BULL_STEEPENER,.jhmf-pillar-state.INVERTED{background:rgba(255,210,102,.16);color:#ffd266}\n.jhmf-pillar-state.STRESS,.jhmf-pillar-state.CRISIS,.jhmf-pillar-state.OUTFLOW{background:rgba(255,85,119,.18);color:#ff5577}\n.jhmf-pillar-pctile{font-family:ui-monospace,monospace;font-size:9.5px;color:#6f7b91;font-weight:700}\n.jhmf-pillar-signal{font-size:11.5px;color:#e6eaf2;line-height:1.45;margin-bottom:4px;font-weight:600}\n.jhmf-pillar-mean{font-size:11px;color:#a8b3c7;line-height:1.45;font-style:italic}\n\n.jhmf-loudest{background:#11161f;border:1px solid #1c2433;border-left:3px solid #ff5577;border-radius:7px;padding:12px 15px;margin-bottom:11px}\n.jhmf-loudest .hdr{font-family:ui-monospace,monospace;font-size:10.5px;color:#ff5577;letter-spacing:1.2px;text-transform:uppercase;font-weight:700;margin-bottom:6px}\n.jhmf-loudest .pill{font-family:ui-monospace,monospace;font-size:9.5px;color:#22d3ee;background:rgba(34,211,238,.10);padding:2px 7px;border-radius:3px;font-weight:700;margin-right:8px}\n.jhmf-loudest .sig{font-size:14px;font-weight:700;color:#e6eaf2;line-height:1.4;margin-bottom:5px}\n.jhmf-loudest .vals{display:flex;gap:14px;font-family:ui-monospace,monospace;font-size:11px;color:#a8b3c7;margin-bottom:6px;flex-wrap:wrap}\n.jhmf-loudest .vals b{color:#ff5577}\n.jhmf-loudest .interp{font-size:12.5px;color:#cbd2dc;line-height:1.55;font-style:italic}\n\n.jhmf-setup{background:#11161f;border:1px solid #1c2433;border-left:5px solid #22d3ee;border-radius:9px;padding:15px 18px;margin-bottom:11px}\n.jhmf-setup.HIGH{border-left-color:#ff7a18;background:linear-gradient(135deg,rgba(255,122,24,.04),transparent 40%)}\n.jhmf-setup.MEDIUM{border-left-color:#22d3ee}\n.jhmf-setup.LOW{border-left-color:#ffd266}\n.jhmf-setup-head{display:flex;align-items:baseline;gap:10px;margin-bottom:8px;flex-wrap:wrap}\n.jhmf-setup-rank{font-family:ui-monospace,monospace;font-size:11px;color:#6f7b91;font-weight:700}\n.jhmf-setup-type{font-family:ui-monospace,monospace;font-size:12px;font-weight:800;color:#22d3ee;letter-spacing:0.5px;padding:2px 8px;background:rgba(34,211,238,.10);border-radius:3px}\n.jhmf-setup-conf{font-family:ui-monospace,monospace;font-size:10.5px;font-weight:700;padding:3px 8px;border-radius:3px;color:#a8b3c7;background:#1c2433}\n.jhmf-setup-conf.HIGH{color:#ff7a18;background:rgba(255,122,24,.14)}\n.jhmf-setup-conf.MEDIUM{color:#22d3ee;background:rgba(34,211,238,.13)}\n.jhmf-setup-headline{font-size:15px;font-weight:700;color:#e6eaf2;line-height:1.4;margin-bottom:7px}\n.jhmf-setup-thesis{font-size:12.5px;color:#cbd2dc;line-height:1.55;margin-bottom:11px}\n\n.jhmf-trade{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:8px;background:#0d1219;border:1px dashed #1c2433;border-radius:6px;padding:10px 12px;margin:9px 0}\n.jhmf-trade-cell{display:flex;flex-direction:column;gap:3px}\n.jhmf-trade-cell .lbl{font-family:ui-monospace,monospace;font-size:9.5px;color:#6f7b91;letter-spacing:0.8px;text-transform:uppercase;font-weight:700}\n.jhmf-trade-cell .val{font-family:ui-monospace,monospace;font-size:13px;color:#e6eaf2;font-weight:700}\n.jhmf-trade-cell .val.instr{color:#22d3ee;font-size:14px}\n.jhmf-trade-cell .val.dir{color:#fbbf24}\n.jhmf-trade-cell .val.entry{color:#26ffaf}\n.jhmf-trade-cell .val.target{color:#26ffaf}\n.jhmf-trade-cell .val.stop{color:#ff5577}\n.jhmf-trade-dv01{grid-column:1/-1;font-size:11px;color:#a78bfa;font-style:italic;line-height:1.45;padding-top:7px;border-top:1px solid #1c2433;margin-top:4px}\n.jhmf-trade-dv01 b{color:#a78bfa;font-style:normal;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;font-family:ui-monospace,monospace;font-size:9.5px;margin-right:6px}\n\n.jhmf-guns{margin:9px 0;background:#0d1219;border:1px dashed #1c2433;border-radius:6px;padding:9px 12px}\n.jhmf-guns-hdr{font-family:ui-monospace,monospace;font-size:9.5px;color:#22d3ee;letter-spacing:1.2px;text-transform:uppercase;font-weight:700;margin-bottom:5px}\n.jhmf-gun{font-size:12px;color:#cbd2dc;line-height:1.5;margin:3px 0;padding-left:6px;border-left:2px solid #1c2433}\n.jhmf-gun .pillar{display:inline-block;font-family:ui-monospace,monospace;font-size:9.5px;color:#22d3ee;letter-spacing:0.5px;font-weight:700;padding:1px 5px;background:rgba(34,211,238,.10);border-radius:2px;margin-right:5px}\n.jhmf-gun .pillar.funding_plumbing,.jhmf-gun .pillar.funding{color:#ef4444;background:rgba(239,68,68,.10)}\n.jhmf-gun .pillar.net_liquidity{color:#22c55e;background:rgba(34,197,94,.10)}\n.jhmf-gun .pillar.fed_path,.jhmf-gun .pillar.fed_speak,.jhmf-gun .pillar.fed_nlp{color:#a78bfa;background:rgba(167,139,250,.10)}\n.jhmf-gun .pillar.yield_curve,.jhmf-gun .pillar.yield_curve_velocity,.jhmf-gun .pillar.bond_vol{color:#fbbf24;background:rgba(251,191,36,.10)}\n.jhmf-gun .pctile{font-family:ui-monospace,monospace;font-size:10px;color:#ff5577;font-weight:700;margin-left:5px}\n\n.jhmf-catalyst{margin:7px 0;padding:9px 12px;background:rgba(167,139,250,.04);border-left:2px solid #a78bfa;border-radius:4px;font-size:12px;color:#cbd2dc;line-height:1.55}\n.jhmf-catalyst .lbl{font-family:ui-monospace,monospace;font-size:9.5px;color:#a78bfa;letter-spacing:1px;text-transform:uppercase;font-weight:700;display:block;margin-bottom:3px}\n.jhmf-catalyst b{color:#e6eaf2;font-weight:600}\n.jhmf-catalyst .row{display:flex;flex-wrap:wrap;gap:11px;margin-top:4px;font-size:11.5px}\n.jhmf-catalyst .row span{color:#a8b3c7}\n.jhmf-catalyst .row b{color:#e6eaf2}\n\n.jhmf-analog{font-size:11.5px;color:#a78bfa;margin:7px 0;font-style:italic;line-height:1.5}\n.jhmf-analog b{color:#a78bfa;font-style:normal;font-weight:700}\n\n.jhmf-trip{margin-top:8px;font-size:11.5px;color:#ffd266;padding:7px 10px;background:rgba(255,210,102,.06);border-left:2px solid #ffd266;border-radius:4px}\n.jhmf-trip b{font-family:ui-monospace,monospace;font-size:9.5px;color:#ffd266;text-transform:uppercase;letter-spacing:0.8px;font-weight:700;margin-right:6px}\n\n.jhmf-cal{background:#11161f;border:1px solid #1c2433;border-left:3px solid #a78bfa;border-radius:7px;padding:11px 14px;margin-bottom:7px;font-size:12px;line-height:1.55}\n.jhmf-cal-hdr{display:flex;align-items:baseline;gap:11px;flex-wrap:wrap;margin-bottom:5px}\n.jhmf-cal-event{font-family:ui-monospace,monospace;font-size:13px;font-weight:800;color:#e6eaf2;letter-spacing:0.3px}\n.jhmf-cal-date{font-family:ui-monospace,monospace;font-size:11px;color:#22d3ee;font-weight:700}\n.jhmf-cal-strength{font-family:ui-monospace,monospace;font-size:9.5px;font-weight:800;padding:1px 6px;border-radius:3px;letter-spacing:0.5px;margin-left:auto}\n.jhmf-cal-strength.STRONG{background:rgba(255,85,119,.16);color:#ff5577}\n.jhmf-cal-strength.MODERATE{background:rgba(255,210,102,.13);color:#ffd266}\n.jhmf-cal-strength.WEAK,.jhmf-cal-strength.NONE{background:rgba(167,176,200,.10);color:#a8b3c7}\n.jhmf-cal-body{color:#cbd2dc;font-size:11.5px;line-height:1.55}\n.jhmf-cal-body .lbl{font-family:ui-monospace,monospace;font-size:10px;color:#6f7b91;letter-spacing:0.5px;margin-right:5px}\n\n.jhmf-actionable{background:linear-gradient(135deg,rgba(34,211,238,.08),rgba(167,139,250,.04));border:1px solid rgba(34,211,238,.35);border-left:5px solid #22d3ee;border-radius:9px;padding:14px 17px;margin-top:14px}\n.jhmf-actionable .hdr{font-family:ui-monospace,monospace;font-size:11px;color:#22d3ee;font-weight:800;letter-spacing:1.4px;text-transform:uppercase;margin-bottom:7px}\n.jhmf-actionable .txt{font-size:14px;color:#e6eaf2;font-weight:600;line-height:1.5}\n\n.jhmf-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhmf-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");}
  function ageStr(iso) {
    if (!iso) return "";
    var m = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
    if (m < 1) return "just now"; if (m < 60) return m + "m ago";
    if (m < 1440) return Math.round(m / 60) + "h ago"; return Math.round(m / 1440) + "d ago";
  }

  var PILLAR_LABELS = {
    auction_tape:              "🏛 Auction Tape",
    primary_dealer_positioning:"🤝 Primary Dealer",
    funding_plumbing:          "🔧 Funding Plumbing",
    net_liquidity:             "💧 Net Liquidity",
    fed_path:                  "🎤 Fed Path",
    yield_curve_velocity:      "📉 Yield Curve",
    tic_flows:                 "🌐 TIC Flows"
  };

  function renderPillars(pillars) {
    if (!pillars || typeof pillars !== "object") return "";
    var html = '<div class="jhmf-pillars">';
    var order = ["auction_tape","primary_dealer_positioning","funding_plumbing","net_liquidity","fed_path","yield_curve_velocity","tic_flows"];
    order.forEach(function (k) {
      var p = pillars[k]; if (!p) return;
      var stateRaw = (p.state || p.shape || "—");
      var state = String(stateRaw).toUpperCase().replace(/[^A-Z_]/g, "");
      html += '<div class="jhmf-pillar ' + k + '">' +
                '<div class="jhmf-pillar-hdr">' +
                  '<span class="jhmf-pillar-name">' + esc(PILLAR_LABELS[k] || k) + '</span>' +
                  '<span class="jhmf-pillar-state ' + state + '">' + esc(stateRaw) + '</span>' +
                  (p.anomaly_pctile != null ? '<span class="jhmf-pillar-pctile">p' + esc(p.anomaly_pctile) + '</span>' : '') +
                '</div>' +
                (p.key_signal ? '<div class="jhmf-pillar-signal">' + esc(p.key_signal) + '</div>' : '') +
                (p.what_it_means ? '<div class="jhmf-pillar-mean">' + esc(p.what_it_means) + '</div>' : '') +
                (p.hawk_dove_score != null ? '<div class="jhmf-pillar-mean">hawk-dove score: <b style="color:#a78bfa">' + esc(p.hawk_dove_score) + '</b></div>' : '') +
                (p.delta_direction ? '<div class="jhmf-pillar-mean">delta: <b style="color:#22c55e">' + esc(p.delta_direction) + '</b></div>' : '') +
              '</div>';
    });
    html += '</div>';
    return html;
  }

  function renderTradeSpecifics(ts) {
    if (!ts) return "";
    var cells = [
      ["instrument", "instr", ts.primary_instrument],
      ["direction",  "dir",   ts.direction],
      ["entry",      "entry", ts.entry_level],
      ["target",     "target",ts.target_level],
      ["stop",       "stop",  ts.stop_level],
      ["size",       "",      ts.size_pct_of_portfolio],
      ["horizon",    "",      ts.horizon],
      ["est P&L",    "",      ts.expected_pnl_pct]
    ].filter(function (c) { return c[2] != null && c[2] !== ""; });
    var inner = cells.map(function (c) {
      return '<div class="jhmf-trade-cell">' +
               '<div class="lbl">' + esc(c[0]) + '</div>' +
               '<div class="val ' + c[1] + '">' + esc(c[2]) + '</div>' +
             '</div>';
    }).join("");
    if (ts.dv01_aware_note) {
      inner += '<div class="jhmf-trade-dv01"><b>DV01:</b> ' + esc(ts.dv01_aware_note) + '</div>';
    }
    return '<div class="jhmf-trade">' + inner + '</div>';
  }

  function mount(elId, contextSlug, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jhmf-wrap");
    el.innerHTML = '<div class="jhmf-loading">🏛 sniffing 20 macro/rates/auction pillars for dealer front-running…</div>';
    var url = PROXY + "/" + (contextSlug || DEFAULT_KEY) + ".json?t=" + Date.now();
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (b) {
      var regime = (b.macro_regime || "NORMAL").toUpperCase();
      var html = '';

      // HEADLINE
      html += '<div class="jhmf-headline ' + regime + '">' +
                '<div class="jhmf-meta">' +
                  '<span class="jhmf-badge">🏛 MACRO FRONT-RUN · 20-PILLAR RATES/AUCTIONS/BONDS DEEP DIVE</span>' +
                  '<span class="jhmf-regime-pill ' + regime + '">' + esc(regime) + '</span>' +
                  '<span class="jhmf-score-box"><span class="lbl">MACRO</span>' + esc(b.overall_macro_score == null ? '—' : b.overall_macro_score) + '/100</span>' +
                  '<span class="jhmf-age">' + esc(ageStr(b.generated_at)) + '</span>' +
                '</div>' +
                (b.headline ? '<div class="jhmf-headline-text">' + esc(b.headline) + '</div>' : '') +
                (b.thesis ? '<div class="jhmf-thesis">' + esc(b.thesis) + '</div>' : '') +
              '</div>';

      // 7-PILLAR DASHBOARD
      if (b.pillars) {
        html += '<div class="jhmf-section-h">🏛 7-Pillar Macro Dashboard</div>';
        html += renderPillars(b.pillars);
      }

      // LOUDEST MACRO ANOMALY
      var la = b.loudest_macro_anomaly;
      if (la && la.signal) {
        html += '<div class="jhmf-loudest">' +
                  '<div class="hdr">⚠ Loudest single-pillar macro anomaly</div>' +
                  '<div class="sig">' + (la.pillar ? '<span class="pill">' + esc(la.pillar) + '</span>' : '') + esc(la.signal) + '</div>' +
                  '<div class="vals">' +
                    '<span>value: <b>' + esc(la.value) + '</b></span>' +
                    '<span>pctile: <b>' + esc(la.anomaly_pctile) + '</b></span>' +
                  '</div>' +
                  (la.interpretation ? '<div class="interp">' + esc(la.interpretation) + '</div>' : '') +
                '</div>';
      }

      // MACRO SETUPS
      if (b.macro_setups && b.macro_setups.length) {
        html += '<div class="jhmf-section-h">🎯 Macro Setups — convergent across 3+ rates/macro pillars</div>';
        b.macro_setups.forEach(function (sx) {
          var conf = (sx.confidence || "MEDIUM").toUpperCase();
          html += '<div class="jhmf-setup ' + conf + '">' +
                    '<div class="jhmf-setup-head">' +
                      '<span class="jhmf-setup-rank">#' + esc(sx.rank) + '</span>' +
                      '<span class="jhmf-setup-type">' + esc(sx.setup_type || '—') + '</span>' +
                      '<span class="jhmf-setup-conf ' + conf + '">conf: ' + esc(conf) + '</span>' +
                    '</div>' +
                    (sx.headline ? '<div class="jhmf-setup-headline">' + esc(sx.headline) + '</div>' : '') +
                    (sx.thesis ? '<div class="jhmf-setup-thesis">' + esc(sx.thesis) + '</div>' : '') +
                    renderTradeSpecifics(sx.trade_specifics) +
                    (sx.smoking_guns && sx.smoking_guns.length ?
                      '<div class="jhmf-guns">' +
                        '<div class="jhmf-guns-hdr">🔫 Smoking Guns (' + sx.smoking_guns.length + ' convergent pillars)</div>' +
                        sx.smoking_guns.map(function (g) {
                          var pillarClass = String(g.pillar || '').toLowerCase().replace(/[^a-z_]/g, '');
                          return '<div class="jhmf-gun"><span class="pillar ' + pillarClass + '">' + esc(g.pillar) + '</span>' +
                                 esc(g.signal) + (g.anomaly_pctile != null ? '<span class="pctile">pctile ' + esc(g.anomaly_pctile) + '</span>' : '') +
                                 '</div>';
                        }).join('') +
                      '</div>' : '') +
                    (sx.front_running_catalyst ?
                      '<div class="jhmf-catalyst">' +
                        '<span class="lbl">📅 FRONT-RUNNING CATALYST</span>' +
                        '<b>' + esc(sx.front_running_catalyst.event) + '</b>' +
                        (sx.front_running_catalyst.date ? ' · <b>' + esc(sx.front_running_catalyst.date) + '</b>' : '') +
                        '<div class="row">' +
                          (sx.front_running_catalyst.consensus ? '<span>consensus: <b>' + esc(sx.front_running_catalyst.consensus) + '</b></span>' : '') +
                          (sx.front_running_catalyst.what_dealers_are_pricing ? '<span>dealers pricing: <b>' + esc(sx.front_running_catalyst.what_dealers_are_pricing) + '</b></span>' : '') +
                        '</div>' +
                        (sx.front_running_catalyst.consensus_vs_dealer_position ? '<div style="margin-top:5px">→ ' + esc(sx.front_running_catalyst.consensus_vs_dealer_position) + '</div>' : '') +
                      '</div>' : '') +
                    (sx.historical_analog && sx.historical_analog.period ?
                      '<div class="jhmf-analog">📐 Analog: <b>' + esc(sx.historical_analog.period) + '</b> — ' + esc(sx.historical_analog.what_happened || '') + (sx.historical_analog.similarity_pct ? ' · similarity <b>' + esc(sx.historical_analog.similarity_pct) + '%</b>' : '') + '</div>' : '') +
                    (sx.invalidation_tripwire ? '<div class="jhmf-trip"><b>Invalidation:</b> ' + esc(sx.invalidation_tripwire) + '</div>' : '') +
                  '</div>';
        });
      }

      // UPCOMING MACRO CATALYSTS
      if (b.upcoming_macro_catalysts && b.upcoming_macro_catalysts.length) {
        html += '<div class="jhmf-section-h">📅 Upcoming Macro Catalysts — what dealers are positioning for</div>';
        b.upcoming_macro_catalysts.forEach(function (c) {
          var strength = (c.front_run_signal_strength || "NONE").toUpperCase();
          html += '<div class="jhmf-cal">' +
                    '<div class="jhmf-cal-hdr">' +
                      '<span class="jhmf-cal-event">' + esc(c.event) + '</span>' +
                      (c.date ? '<span class="jhmf-cal-date">' + esc(c.date) + '</span>' : '') +
                      '<span class="jhmf-cal-strength ' + strength + '">' + esc(strength) + '</span>' +
                    '</div>' +
                    '<div class="jhmf-cal-body">' +
                      (c.consensus ? '<div><span class="lbl">consensus:</span><b>' + esc(c.consensus) + '</b></div>' : '') +
                      (c.what_to_watch ? '<div style="margin-top:4px"><span class="lbl">watch:</span>' + esc(c.what_to_watch) + '</div>' : '') +
                    '</div>' +
                  '</div>';
        });
      }

      // MOST ACTIONABLE
      if (b.most_actionable_macro_trade) {
        html += '<div class="jhmf-actionable">' +
                  '<div class="hdr">⚡ Most Actionable Macro Trade Right Now</div>' +
                  '<div class="txt">' + esc(b.most_actionable_macro_trade) + '</div>' +
                '</div>';
      }

      el.innerHTML = html;
      return b;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhmf-err">Macro front-run sniffer unavailable: ' + esc(e.message || e) + '</div>';
      if (opts && typeof opts.onError === "function") opts.onError(e);
      throw e;
    });
  }
  window.JHAIMacroFront = { mount: mount, version: "1.0.0" };
})();
