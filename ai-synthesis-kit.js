/**
 * ai-synthesis-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Master CIO synthesis brief renderer. The integration of all 30 desk views.
 * Designed to be the HEADLINE block on today.html and index.html.
 *
 *   <script src="/ai-synthesis-kit.js"></script>
 *   <div id="ai-synthesis"></div>
 *   <script>JHAISynth.mount('ai-synthesis');</script>
 */
(function () {
  if (window.JHAISynth) return;

  var PROXY = "https://justhodl-dashboard-live.s3.amazonaws.com/data";
  var DEFAULT_KEY = "desk-consensus";

  function injectCSS() {
    if (document.getElementById("jh-ai-synth-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-synth-css";
    s.textContent = "\n.jhsy-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jhsy-headline{background:linear-gradient(135deg,rgba(0,212,255,.08),rgba(167,139,250,.04));border:1px solid rgba(0,212,255,.25);border-radius:12px;padding:22px 26px;margin-bottom:14px;position:relative;overflow:hidden}\n.jhsy-headline.RISK_OFF{border-color:rgba(255,85,119,.4);background:linear-gradient(135deg,rgba(255,85,119,.10),rgba(255,122,24,.04))}\n.jhsy-headline.RISK_ON{border-color:rgba(38,255,175,.35);background:linear-gradient(135deg,rgba(38,255,175,.09),rgba(0,212,255,.04))}\n.jhsy-headline.NEUTRAL{border-color:rgba(255,210,102,.35);background:linear-gradient(135deg,rgba(255,210,102,.08),rgba(0,212,255,.04))}\n.jhsy-headline.RISK_OFF_AGGRESSIVE{border-color:rgba(255,85,119,.55);background:linear-gradient(135deg,rgba(255,85,119,.16),rgba(255,122,24,.06))}\n.jhsy-headline.RISK_ON_AGGRESSIVE{border-color:rgba(38,255,175,.55);background:linear-gradient(135deg,rgba(38,255,175,.14),rgba(0,212,255,.05))}\n\n.jhsy-meta-row{display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap}\n.jhsy-badge{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#a78bfa;font-weight:700}\n.jhsy-regime-pill{font-family:ui-monospace,monospace;font-size:13px;font-weight:800;padding:6px 14px;border-radius:5px;letter-spacing:0.5px}\n.jhsy-regime-pill.RISK_OFF{background:rgba(255,85,119,.18);color:#ff5577;border:1px solid rgba(255,85,119,.35)}\n.jhsy-regime-pill.RISK_ON{background:rgba(38,255,175,.16);color:#26ffaf;border:1px solid rgba(38,255,175,.32)}\n.jhsy-regime-pill.NEUTRAL{background:rgba(255,210,102,.14);color:#ffd266;border:1px solid rgba(255,210,102,.32)}\n.jhsy-regime-pill.RISK_OFF_AGGRESSIVE{background:rgba(255,85,119,.26);color:#ff7799;border:1px solid rgba(255,85,119,.5)}\n.jhsy-regime-pill.RISK_ON_AGGRESSIVE{background:rgba(38,255,175,.22);color:#5dffc1;border:1px solid rgba(38,255,175,.5)}\n.jhsy-conf{font-family:ui-monospace,monospace;font-size:11.5px;font-weight:700;color:#a8b3c7;padding:3px 9px;background:#1c2433;border-radius:3px;letter-spacing:0.5px}\n.jhsy-support{font-family:ui-monospace,monospace;font-size:11.5px;color:#6f7b91}\n.jhsy-support b{color:#26ffaf;font-weight:700}\n.jhsy-age{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-left:auto}\n\n.jhsy-oneliner{font-size:17px;font-weight:700;color:#e6eaf2;line-height:1.45;margin-bottom:10px}\n.jhsy-thesis{font-size:13.5px;color:#cbd2dc;line-height:1.65}\n\n.jhsy-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#00d4ff;margin:18px 0 8px;padding-bottom:5px;border-bottom:1px solid #1c2433}\n\n.jhsy-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px;margin-bottom:6px}\n.jhsy-card{background:#11161f;border:1px solid #1c2433;border-radius:7px;padding:12px 14px;font-size:12.5px}\n.jhsy-card .lbl{font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1px;color:#6f7b91;text-transform:uppercase;margin-bottom:4px}\n.jhsy-card .val{font-size:13.5px;font-weight:600;color:#e6eaf2;line-height:1.5;margin-bottom:3px}\n.jhsy-card .sub{font-size:11px;color:#a8b3c7;line-height:1.5}\n\n.jhsy-dissent .jhsy-card{border-left:3px solid #ff7a18}\n.jhsy-dissent .desk{font-family:ui-monospace,monospace;font-size:11.5px;color:#ff7a18;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px}\n.jhsy-dissent .call{font-size:12px;color:#a8b3c7;margin-bottom:6px}\n.jhsy-dissent .signal{font-size:12.5px;color:#e6eaf2;line-height:1.55;margin-bottom:6px}\n.jhsy-dissent .why{font-size:11.5px;color:#cbd2dc;line-height:1.55;font-style:italic;border-top:1px dashed #1c2433;padding-top:6px}\n\n.jhsy-asym .jhsy-card{border-left:3px solid #a78bfa;display:flex;flex-direction:column;gap:4px}\n.jhsy-asym .asset-row{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:3px}\n.jhsy-asym .asset{font-family:ui-monospace,monospace;font-size:14px;font-weight:800;color:#e6eaf2}\n.jhsy-asym .direction{font-family:ui-monospace,monospace;font-size:10.5px;font-weight:700;padding:2px 7px;border-radius:3px;letter-spacing:0.5px}\n.jhsy-asym .direction.STRONG_UPSIDE,.jhsy-asym .direction.MILDLY_UPSIDE{background:rgba(38,255,175,.16);color:#26ffaf}\n.jhsy-asym .direction.SIDEWAYS{background:rgba(168,179,199,.12);color:#a8b3c7}\n.jhsy-asym .direction.STRONG_DOWNSIDE,.jhsy-asym .direction.MILDLY_DOWNSIDE{background:rgba(255,85,119,.18);color:#ff5577}\n.jhsy-asym .direction.SPLIT{background:rgba(255,210,102,.16);color:#ffd266}\n.jhsy-asym .votes{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-bottom:4px}\n.jhsy-asym .voicebull{font-size:11.5px;color:#26ffaf;line-height:1.45;margin-top:3px}\n.jhsy-asym .voicebear{font-size:11.5px;color:#ff5577;line-height:1.45;margin-top:3px}\n.jhsy-asym .trade-impl{font-size:12px;color:#cbd2dc;border-top:1px dashed #1c2433;padding-top:6px;margin-top:4px;font-style:italic}\n\n.jhsy-conv .jhsy-card{border-left:3px solid #26ffaf}\n.jhsy-conv .ticker{font-family:ui-monospace,monospace;font-size:15px;font-weight:800;color:#26ffaf}\n.jhsy-conv .meta{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin:3px 0}\n.jhsy-conv .desks{display:flex;flex-wrap:wrap;gap:4px;margin:5px 0}\n.jhsy-conv .desk-pill{font-family:ui-monospace,monospace;font-size:10px;background:#1c2433;color:#a8b3c7;padding:1px 6px;border-radius:3px}\n.jhsy-conv .syn{font-size:12px;color:#cbd2dc;line-height:1.5;margin-top:5px}\n\n.jhsy-tripwire{background:rgba(255,85,119,.06);border:1px solid rgba(255,85,119,.25);border-left:4px solid #ff5577;border-radius:7px;padding:13px 16px;margin-bottom:10px}\n.jhsy-tripwire .head{font-family:ui-monospace,monospace;font-size:11px;color:#ff5577;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;margin-bottom:5px}\n.jhsy-tripwire .cond{font-size:13px;color:#e6eaf2;font-weight:600;line-height:1.5;margin-bottom:4px}\n.jhsy-tripwire .act{font-size:12px;color:#cbd2dc;line-height:1.5}\n.jhsy-tripwire .act b{color:#ff7a18}\n\n.jhsy-action{background:linear-gradient(135deg,rgba(38,255,175,.05),rgba(0,212,255,.04));border:1px solid rgba(38,255,175,.32);border-left:4px solid #26ffaf;border-radius:9px;padding:15px 18px;margin-bottom:10px}\n.jhsy-action .head{font-family:ui-monospace,monospace;font-size:11px;color:#26ffaf;font-weight:700;letter-spacing:1.4px;text-transform:uppercase;margin-bottom:7px}\n.jhsy-action .trade{font-size:14px;color:#e6eaf2;font-weight:600;line-height:1.55;margin-bottom:7px}\n.jhsy-action .hedge{font-size:12.5px;color:#cbd2dc;line-height:1.55;margin-bottom:6px}\n.jhsy-action .hedge b{color:#a78bfa}\n.jhsy-action .sizing{font-size:11.5px;color:#a8b3c7;line-height:1.5;border-top:1px dashed rgba(38,255,175,.18);padding-top:7px;font-style:italic}\n\n.jhsy-loading{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhsy-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
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

  function mount(elId, contextSlug, opts) {
    injectCSS();
    var el = document.getElementById(elId);
    if (!el) return;
    el.classList.add("jhsy-wrap");
    el.innerHTML = '<div class="jhsy-loading">⚡ synthesizing 30-desk consensus…</div>';

    var url = PROXY + "/" + (contextSlug || DEFAULT_KEY) + ".json?t=" + Date.now();
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (b) {
      var cs = b.consensus || {};
      var regime = (cs.regime || "NEUTRAL").toUpperCase();
      var html = '';

      // HEADLINE
      html += '<div class="jhsy-headline ' + regime + '">' +
                '<div class="jhsy-meta-row">' +
                  '<span class="jhsy-badge">⚡ CIO Synthesis · 30-Desk Consensus</span>' +
                  '<span class="jhsy-regime-pill ' + regime + '">' + esc(regime.replace(/_/g, " ")) + '</span>' +
                  '<span class="jhsy-conf">conf: ' + esc(cs.confidence || "—") + '</span>' +
                  (cs.n_supporting_desks ? '<span class="jhsy-support"><b>' + esc(cs.n_supporting_desks) + '</b> desks aligned</span>' : '') +
                  '<span class="jhsy-age">' + esc(ageStr(b.generated_at)) + '</span>' +
                '</div>' +
                (cs.one_liner ? '<div class="jhsy-oneliner">' + esc(cs.one_liner) + '</div>' : '') +
                (cs.thesis ? '<div class="jhsy-thesis">' + esc(cs.thesis) + '</div>' : '') +
              '</div>';

      // TODAY'S ACTION
      var ta = b.today_action;
      if (ta && ta.primary_trade) {
        html += '<div class="jhsy-action">' +
                  '<div class="head">⚡ Today\'s primary trade — most multi-desk support</div>' +
                  '<div class="trade">' + esc(ta.primary_trade) + '</div>' +
                  (ta.primary_hedge ? '<div class="hedge"><b>Hedge:</b> ' + esc(ta.primary_hedge) + '</div>' : '') +
                  (ta.position_sizing_note ? '<div class="sizing">' + esc(ta.position_sizing_note) + '</div>' : '') +
                '</div>';
      }

      // LOUDEST TRIPWIRE
      var tw = b.loudest_tripwire;
      if (tw && tw.condition) {
        html += '<div class="jhsy-tripwire">' +
                  '<div class="head">⚠ Loudest tripwire — [' + esc(tw.severity) + '] from ' + esc(tw.from_desk) + '</div>' +
                  '<div class="cond">' + esc(tw.condition) + '</div>' +
                  (tw.action ? '<div class="act"><b>Action:</b> ' + esc(tw.action) + '</div>' : '') +
                '</div>';
      }

      // ASYMMETRIC SETUPS
      if (b.asymmetric_setups && b.asymmetric_setups.length) {
        html += '<div class="jhsy-section-h">Cross-Asset Vote — Where Desks Agree</div>';
        html += '<div class="jhsy-grid jhsy-asym">';
        b.asymmetric_setups.forEach(function (a) {
          var dir = (a.consensus_direction || "SIDEWAYS").toUpperCase();
          html += '<div class="jhsy-card">' +
                    '<div class="asset-row">' +
                      '<span class="asset">' + esc(a.asset) + '</span>' +
                      '<span class="direction ' + dir + '">' + esc(dir.replace(/_/g, " ")) + '</span>' +
                    '</div>' +
                    '<div class="votes">▲ ' + esc(a.n_bullish || 0) + ' bull · ◆ ' + esc(a.n_sideways || 0) + ' side · ▼ ' + esc(a.n_bearish || 0) + ' bear · ' + esc(a.best_horizon_weeks || '?') + 'wk</div>' +
                    (a.loudest_bull ? '<div class="voicebull">▲ ' + esc(a.loudest_bull) + '</div>' : '') +
                    (a.loudest_bear ? '<div class="voicebear">▼ ' + esc(a.loudest_bear) + '</div>' : '') +
                    (a.trade_implication ? '<div class="trade-impl">' + esc(a.trade_implication) + '</div>' : '') +
                  '</div>';
        });
        html += '</div>';
      }

      // DISSENT
      if (b.dissent && b.dissent.length) {
        html += '<div class="jhsy-section-h">Credible Dissent — Desks Disagreeing With Consensus</div>';
        html += '<div class="jhsy-grid jhsy-dissent">';
        b.dissent.forEach(function (d) {
          html += '<div class="jhsy-card">' +
                    '<div class="desk">' + esc(d.desk) + '</div>' +
                    '<div class="call">' + esc(d.their_call || '') + '</div>' +
                    '<div class="signal">' + esc(d.key_signal || '') + '</div>' +
                    (d.why_credible ? '<div class="why">' + esc(d.why_credible) + '</div>' : '') +
                  '</div>';
        });
        html += '</div>';
      }

      // CONVERGENT NAMES
      if (b.convergent_names && b.convergent_names.length) {
        html += '<div class="jhsy-section-h">Multi-Desk Convergent Tickers — Tickers Strong-Fit on 2+ Screener Desks</div>';
        html += '<div class="jhsy-grid jhsy-conv">';
        b.convergent_names.forEach(function (n) {
          html += '<div class="jhsy-card">' +
                    '<div class="ticker">' + esc(n.ticker) + '</div>' +
                    '<div class="meta">score: ' + esc(n.strongest_score || '—') + (n.best_analog ? ' · analog: <b>' + esc(n.best_analog) + '</b>' : '') + '</div>' +
                    '<div class="desks">' + (n.appearing_in || []).map(function (d) {
                      return '<span class="desk-pill">' + esc(d) + '</span>';
                    }).join("") + '</div>' +
                    (n.synthesis_one_liner ? '<div class="syn">' + esc(n.synthesis_one_liner) + '</div>' : '') +
                  '</div>';
        });
        html += '</div>';
      }

      el.innerHTML = html;
      return b;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhsy-err">Synthesis brief unavailable: ' + esc(e.message || e) + '</div>';
      if (opts && typeof opts.onError === "function") opts.onError(e);
      throw e;
    });
  }

  window.JHAISynth = { mount: mount, version: "1.0.0" };
})();
