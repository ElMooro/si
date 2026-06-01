/**
 * ai-alerts-cockpit-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Unified alerts cockpit. Reads BOTH sniffer states + BOTH history
 * files + BOTH current briefs in parallel. Renders:
 *   1. Live status grid (per-sniffer scores, regimes, alerts-today)
 *   2. Macro convergence-fingerprint live state (3 pillars)
 *   3. Chronological merged timeline of every alert event
 *
 *   <script src="/ai-alerts-cockpit-kit.js"></script>
 *   <div id="ai-alerts-cockpit"></div>
 *   <script>JHAIAlerts.mount('ai-alerts-cockpit');</script>
 */
(function () {
  if (window.JHAIAlerts) return;
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
    if (document.getElementById("jh-ai-alerts-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-alerts-css";
    s.textContent = "\n.jha-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:24px}\n\n.jha-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:#22d3ee;margin:20px 0 9px;font-weight:700;padding-bottom:5px;border-bottom:1px solid rgba(34,211,238,.18)}\n\n.jha-status-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px}\n@media (max-width:780px){.jha-status-grid{grid-template-columns:1fr}}\n.jha-sniffer{background:#0a0f17;border:1px solid #1c2433;border-radius:11px;padding:16px 20px;position:relative;overflow:hidden}\n.jha-sniffer.equity{border-left:5px solid #ff7a18}\n.jha-sniffer.macro {border-left:5px solid #22d3ee}\n.jha-sniffer-hdr{display:flex;align-items:baseline;gap:11px;margin-bottom:11px;flex-wrap:wrap}\n.jha-sniffer-title{font-family:ui-monospace,monospace;font-size:12.5px;font-weight:800;letter-spacing:0.5px}\n.jha-sniffer.equity .jha-sniffer-title{color:#ff7a18}\n.jha-sniffer.macro  .jha-sniffer-title{color:#22d3ee}\n.jha-sniffer-link{margin-left:auto;font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;text-decoration:none;border:1px solid #1c2433;padding:2px 8px;border-radius:3px;transition:all .15s}\n.jha-sniffer-link:hover{color:#e6eaf2;border-color:#2a3548}\n\n.jha-kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(78px,1fr));gap:7px;margin-bottom:11px}\n.jha-kpi{background:#11161f;border:1px solid #1c2433;border-radius:5px;padding:6px 9px}\n.jha-kpi .lbl{font-family:ui-monospace,monospace;font-size:9px;letter-spacing:0.8px;text-transform:uppercase;color:#6f7b91;margin-bottom:2px}\n.jha-kpi .val{font-family:ui-monospace,monospace;font-size:14px;font-weight:800;color:#e6eaf2}\n.jha-kpi .val.NORMAL{color:#26ffaf}\n.jha-kpi .val.ELEVATED{color:#ff7a18}\n.jha-kpi .val.EXTREME{color:#ff5577;animation:jha-pulse 1.6s ease-in-out infinite}\n.jha-kpi .val.cooldown{color:#a78bfa;font-size:11px}\n@keyframes jha-pulse{0%,100%{text-shadow:0 0 0 transparent}50%{text-shadow:0 0 12px rgba(255,85,119,0.6)}}\n\n.jha-headline{font-size:12.5px;color:#cbd2dc;line-height:1.55;padding:8px 11px;background:#0d1219;border:1px solid #1c2433;border-radius:5px;font-style:italic}\n\n.jha-state-row{display:flex;align-items:center;gap:9px;margin-top:9px;font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;flex-wrap:wrap}\n.jha-state-row .pill{padding:1px 7px;background:#1c2433;border-radius:3px}\n.jha-state-row .pill b{color:#e6eaf2}\n.jha-state-row .pill.alert-recent{background:rgba(255,85,119,.12);color:#ff5577}\n.jha-state-row .pill.alert-quiet{background:rgba(38,255,175,.10);color:#26ffaf}\n\n.jha-conv{background:#0a0f17;border:1px solid #1c2433;border-radius:10px;padding:14px 18px;margin-bottom:14px;border-left:5px solid #22d3ee}\n.jha-conv.FIRED{border-left-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.10),rgba(255,122,24,.05));animation:jha-conv-pulse 2s ease-in-out infinite}\n@keyframes jha-conv-pulse{0%,100%{box-shadow:0 0 0 0 rgba(255,85,119,.18)}50%{box-shadow:0 0 22px 4px rgba(255,85,119,.20)}}\n.jha-conv-hdr{display:flex;align-items:baseline;gap:11px;margin-bottom:10px;flex-wrap:wrap}\n.jha-conv-title{font-family:ui-monospace,monospace;font-size:11px;font-weight:800;letter-spacing:1px;text-transform:uppercase;color:#22d3ee}\n.jha-conv.FIRED .jha-conv-title{color:#ff5577}\n.jha-conv-status{font-family:ui-monospace,monospace;font-size:12.5px;font-weight:800;letter-spacing:0.5px;padding:3px 10px;border-radius:4px;background:rgba(38,255,175,.12);color:#26ffaf}\n.jha-conv.FIRED .jha-conv-status{background:rgba(255,85,119,.18);color:#ff5577}\n.jha-conv-desc{font-size:12.5px;color:#cbd2dc;line-height:1.55;margin-bottom:11px}\n.jha-conv-desc b{color:#e6eaf2;font-weight:600}\n.jha-pillars{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin-bottom:9px}\n@media (max-width:580px){.jha-pillars{grid-template-columns:1fr}}\n.jha-pillar{background:#11161f;border:1px solid #1c2433;border-radius:7px;padding:10px 13px;border-top:3px solid #6f7b91}\n.jha-pillar.stressed{border-top-color:#ff5577;background:linear-gradient(135deg,rgba(255,85,119,.06),transparent 50%)}\n.jha-pillar.calm{border-top-color:#26ffaf}\n.jha-pillar-name{font-family:ui-monospace,monospace;font-size:9.5px;letter-spacing:1px;text-transform:uppercase;color:#6f7b91;margin-bottom:4px;font-weight:700}\n.jha-pillar-state{font-family:ui-monospace,monospace;font-size:13.5px;font-weight:800;color:#e6eaf2;margin-bottom:3px;letter-spacing:0.3px}\n.jha-pillar.stressed .jha-pillar-state{color:#ff5577}\n.jha-pillar.calm .jha-pillar-state{color:#26ffaf}\n.jha-pillar-mean{font-size:11px;color:#a8b3c7;line-height:1.45}\n.jha-conv-count{font-family:ui-monospace,monospace;font-size:11.5px;color:#cbd2dc;padding-top:8px;border-top:1px solid #1c2433;margin-top:7px}\n.jha-conv-count b{color:#22d3ee}\n.jha-conv.FIRED .jha-conv-count b{color:#ff5577}\n\n.jha-timeline{background:#0a0f17;border:1px solid #1c2433;border-radius:10px;padding:16px 18px}\n.jha-timeline-empty{padding:30px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jha-event{display:flex;align-items:flex-start;gap:11px;padding:11px 0;border-bottom:1px solid rgba(28,36,51,.6);position:relative}\n.jha-event:last-child{border-bottom:none}\n.jha-event-dot{flex-shrink:0;width:10px;height:10px;border-radius:50%;margin-top:6px;background:#6f7b91;box-shadow:0 0 0 2px #0a0f17,0 0 0 3px #1c2433}\n.jha-event-dot.equity{background:#ff7a18;box-shadow:0 0 0 2px #0a0f17,0 0 0 3px #ff7a18}\n.jha-event-dot.macro{background:#22d3ee;box-shadow:0 0 0 2px #0a0f17,0 0 0 3px #22d3ee}\n.jha-event-dot.EXTREME{background:#ff5577;box-shadow:0 0 0 2px #0a0f17,0 0 0 3px #ff5577,0 0 12px rgba(255,85,119,.5);animation:jha-pulse 1.6s ease-in-out infinite}\n.jha-event-body{flex:1;min-width:0}\n.jha-event-head{display:flex;align-items:baseline;gap:9px;margin-bottom:3px;flex-wrap:wrap}\n.jha-event-ts{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91}\n.jha-event-source{font-family:ui-monospace,monospace;font-size:9.5px;font-weight:700;padding:1px 6px;border-radius:3px;letter-spacing:0.5px}\n.jha-event-source.equity{background:rgba(255,122,24,.13);color:#ff7a18}\n.jha-event-source.macro{background:rgba(34,211,238,.13);color:#22d3ee}\n.jha-event-score{font-family:ui-monospace,monospace;font-size:13px;font-weight:800;color:#e6eaf2}\n.jha-event-regime{font-family:ui-monospace,monospace;font-size:9.5px;font-weight:800;padding:1px 6px;border-radius:3px;letter-spacing:0.5px}\n.jha-event-regime.NORMAL{background:rgba(38,255,175,.12);color:#26ffaf}\n.jha-event-regime.ELEVATED{background:rgba(255,122,24,.14);color:#ff7a18}\n.jha-event-regime.EXTREME{background:rgba(255,85,119,.18);color:#ff5577}\n.jha-event-target{font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;margin-left:auto}\n.jha-event-target b{color:#cbd2dc}\n.jha-event-headline{color:#cbd2dc;font-size:12px;line-height:1.5}\n.jha-event-conv-tag{display:inline-block;font-family:ui-monospace,monospace;font-size:9.5px;font-weight:800;padding:1px 6px;border-radius:3px;background:rgba(255,85,119,.18);color:#ff5577;letter-spacing:0.5px;margin-left:4px}\n\n.jha-legend{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;padding:8px 12px;background:#0d1219;border:1px dashed #1c2433;border-radius:5px;margin-top:11px;line-height:1.6}\n.jha-legend b{color:#cbd2dc}\n.jha-legend .equity-dot,.jha-legend .macro-dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px;vertical-align:middle}\n.jha-legend .equity-dot{background:#ff7a18}\n.jha-legend .macro-dot{background:#22d3ee}\n\n.jha-loading{padding:40px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jha-err{padding:15px 18px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/\"/g,"&quot;").replace(/'/g,"&#39;");}
  function tsShort(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return "—";
    var now = new Date();
    var sameDay = d.toDateString() === now.toDateString();
    var hh = d.getHours().toString().padStart(2,"0");
    var mm = d.getMinutes().toString().padStart(2,"0");
    if (sameDay) return "today " + hh + ":" + mm;
    var mo = (d.getMonth()+1).toString().padStart(2,"0");
    var da = d.getDate().toString().padStart(2,"0");
    return mo + "-" + da + " " + hh + ":" + mm;
  }
  function ageStr(iso) {
    if (!iso) return "never";
    var ms = Date.now() - new Date(iso).getTime();
    if (isNaN(ms) || ms < 0) return "never";
    var m = Math.round(ms / 60000);
    if (m < 1) return "just now";
    if (m < 60) return m + "m ago";
    if (m < 1440) return Math.round(m/60) + "h ago";
    return Math.round(m/1440) + "d ago";
  }
  function fetchJSON(slug) {
    return jhFetch(slug).then(function (r) {
      if (!r.ok) return null;
      return r.json();
    }).catch(function () { return null; });
  }

  function renderSnifferStatus(label, sniffClass, brief, state, pageUrl, scoreField, regimeField) {
    if (!brief && !state) {
      return '<div class="jha-sniffer ' + sniffClass + '"><div class="jha-sniffer-hdr"><div class="jha-sniffer-title">' + esc(label) + '</div><a class="jha-sniffer-link" href="' + esc(pageUrl) + '">open →</a></div><div style="color:#6f7b91;font-size:11.5px;padding:10px 0">No data yet — first cycle pending.</div></div>';
    }
    var score = (brief || {})[scoreField];
    var regime = ((brief || {})[regimeField] || "NORMAL").toUpperCase();
    var headline = (brief || {}).headline || "";
    var alertsToday = state ? (state.alerts_today || 0) : 0;
    var lastKind = state ? (state.last_alert_kind || "—") : "—";
    var lastAlertAt = state ? state.last_alert_at : null;
    var lastConv = state ? state.last_convergence_fingerprint_at : null;

    var ageOfBrief = ageStr((brief || {}).generated_at);
    var ageOfAlert = ageStr(lastAlertAt);
    var recentAlert = false;
    if (lastAlertAt) {
      try { recentAlert = (Date.now() - new Date(lastAlertAt).getTime()) < 6 * 3600 * 1000; } catch (e) {}
    }

    var html = '<div class="jha-sniffer ' + sniffClass + '">' +
                 '<div class="jha-sniffer-hdr">' +
                   '<div class="jha-sniffer-title">' + esc(label) + '</div>' +
                   '<a class="jha-sniffer-link" href="' + esc(pageUrl) + '">open →</a>' +
                 '</div>' +
                 '<div class="jha-kpi-grid">' +
                   '<div class="jha-kpi"><div class="lbl">score</div><div class="val ' + regime + '">' + (score == null ? '—' : esc(score)) + '</div></div>' +
                   '<div class="jha-kpi"><div class="lbl">regime</div><div class="val ' + regime + '">' + esc(regime) + '</div></div>' +
                   '<div class="jha-kpi"><div class="lbl">last alert</div><div class="val cooldown">' + esc(ageOfAlert) + '</div></div>' +
                   '<div class="jha-kpi"><div class="lbl">today</div><div class="val">' + esc(alertsToday) + '/8</div></div>' +
                 '</div>' +
                 (headline ? '<div class="jha-headline">' + esc(headline.substring(0, 220)) + '</div>' : '') +
                 '<div class="jha-state-row">' +
                   '<span class="pill">brief: <b>' + esc(ageOfBrief) + '</b></span>' +
                   (lastKind && lastKind !== '—' ? '<span class="pill">last kind: <b>' + esc(lastKind) + '</b></span>' : '') +
                   (lastConv && sniffClass === 'macro' ? '<span class="pill alert-recent">conv. last fired: <b>' + esc(ageStr(lastConv)) + '</b></span>' : '') +
                   (recentAlert ? '<span class="pill alert-recent">⚠ alerting</span>' : '<span class="pill alert-quiet">✓ quiet</span>') +
                 '</div>' +
               '</div>';
    return html;
  }

  function renderConvergence(macroBrief, macroState) {
    var pillars = (macroBrief || {}).pillars || {};
    var auc = pillars.auction_tape || {};
    var pd  = pillars.primary_dealer_positioning || {};
    var fp  = pillars.funding_plumbing || {};

    function isStressed(state, calmList) {
      if (!state) return null;
      var s = String(state).toUpperCase();
      return !calmList.includes(s);
    }
    var aucStressed = isStressed(auc.state, ["CALM"]);
    var pdStressed  = isStressed(pd.state,  ["NEUTRAL"]);
    var fpStressed  = isStressed(fp.state,  ["HEALED"]);

    var nStressed = 0;
    [aucStressed, pdStressed, fpStressed].forEach(function (v) { if (v === true) nStressed++; });
    var fired = nStressed >= 3;
    var firedClass = fired ? "FIRED" : "";
    var status = fired ? "🚨 FINGERPRINT FIRED" : (nStressed > 0 ? "⚠ PARTIAL — " + nStressed + "/3 stressed" : "✅ ALL CALM");

    var lastConv = macroState ? macroState.last_convergence_fingerprint_at : null;

    var html = '<div class="jha-conv ' + firedClass + '">' +
                 '<div class="jha-conv-hdr">' +
                   '<div class="jha-conv-title">🏛 MACRO CONVERGENCE FINGERPRINT — Aug 2007 / Sep 2019 / Mar 2020 / Mar 2023 signature</div>' +
                   '<div class="jha-conv-status">' + esc(status) + '</div>' +
                 '</div>' +
                 '<div class="jha-conv-desc">' +
                   'When <b>auction tape</b>, <b>primary dealer positioning</b>, and <b>funding plumbing</b> ' +
                   'all stress simultaneously, that\'s the highest-conviction macro front-running pattern in the history book.' +
                 '</div>' +
                 '<div class="jha-pillars">' +
                   pillarCell("auction tape", auc, aucStressed) +
                   pillarCell("primary dealer", pd, pdStressed) +
                   pillarCell("funding plumbing", fp, fpStressed) +
                 '</div>' +
                 (lastConv ? '<div class="jha-conv-count">Last convergence fingerprint fire: <b>' + esc(tsShort(lastConv)) + ' (' + esc(ageStr(lastConv)) + ')</b></div>' :
                   '<div class="jha-conv-count">Last convergence fingerprint fire: <b>never</b> (since alerting deployed)</div>') +
               '</div>';
    return html;
  }


  function detectEquityConvergence(equityBrief) {
    var setups = (equityBrief || {}).suspected_setups || [];
    if (!setups.length) return { fired: false, cardinals: { DEALER_GEX:false, OPTIONS_FLOW_GAMMA:false, SKEW_VOL:false, CFTC_SHORT:false }, n_present: 0, sample: [] };

    var cards = { DEALER_GEX:false, OPTIONS_FLOW_GAMMA:false, SKEW_VOL:false, CFTC_SHORT:false };
    var sample = [];
    setups.slice(0, 2).forEach(function (sx) {
      (sx.smoking_gun_signals || []).forEach(function (g) {
        var cat = String(g.category || "").toUpperCase();
        if ((cat.includes("DEALER_GEX") || cat.includes("GEX")) && !cards.DEALER_GEX) {
          cards.DEALER_GEX = true;
          sample.push({ cardinal:"DEALER_GEX", signal:(g.signal||"").substring(0,120) });
        } else if ((cat.includes("OPTIONS_FLOW") || cat.includes("OPTIONS_GAMMA") || cat === "GAMMA") && !cards.OPTIONS_FLOW_GAMMA) {
          cards.OPTIONS_FLOW_GAMMA = true;
          sample.push({ cardinal:"OPTIONS_FLOW_GAMMA", signal:(g.signal||"").substring(0,120) });
        } else if ((cat.includes("SKEW") || cat.includes("IV_CRUSH") || cat.includes("TAIL") || cat === "VOL" || cat.includes("CATALYST_SKEW")) && !cards.SKEW_VOL) {
          cards.SKEW_VOL = true;
          sample.push({ cardinal:"SKEW_VOL", signal:(g.signal||"").substring(0,120) });
        } else if ((cat.includes("CFTC") || cat.includes("SHORT_INTEREST") || cat === "SHORT" || cat.includes("SQUEEZE")) && !cards.CFTC_SHORT) {
          cards.CFTC_SHORT = true;
          sample.push({ cardinal:"CFTC_SHORT", signal:(g.signal||"").substring(0,120) });
        }
      });
    });
    var n_present = Object.values(cards).filter(function(v){return v;}).length;
    return { fired: n_present >= 3, cardinals: cards, n_present: n_present, sample: sample,
              top_target: setups[0] ? setups[0].target_asset : null,
              top_direction: setups[0] ? setups[0].target_direction : null };
  }


  function renderEquityConvergence(equityBrief, equityState) {
    var c = detectEquityConvergence(equityBrief);
    var fired = c.fired;
    var firedClass = fired ? "FIRED" : "";
    var status = fired ? "🚨 FINGERPRINT FIRED" : (c.n_present > 0 ? "⚠ PARTIAL — " + c.n_present + "/4 stressed" : "✅ ALL CALM");
    var lastConv = equityState ? equityState.last_convergence_fingerprint_at : null;

    function card(name, label, sub) {
      var stressed = c.cardinals[name];
      var cls = stressed ? "stressed" : "calm";
      var state = stressed ? "FIRED" : "QUIET";
      // Find the contributing signal if fired
      var contrib = c.sample.find(function (s) { return s.cardinal === name; });
      return '<div class="jha-pillar ' + cls + '">' +
               '<div class="jha-pillar-name">' + esc(label) + '</div>' +
               '<div class="jha-pillar-state">' + esc(state) + '</div>' +
               '<div class="jha-pillar-mean">' + esc(sub) +
                 (contrib && contrib.signal ? '<br><span style="color:#a8b3c7">↳ ' + esc(contrib.signal) + '</span>' : '') +
               '</div>' +
             '</div>';
    }

    var html = '<div class="jha-conv ' + firedClass + '" style="border-left-color:' + (fired ? '#ff5577' : '#ff7a18') + '">' +
                 '<div class="jha-conv-hdr">' +
                   '<div class="jha-conv-title" style="color:' + (fired ? '#ff5577' : '#ff7a18') + '">🎯 EQUITY CONVERGENCE FINGERPRINT — Jan 2021 GME / Feb 2018 vol unwind / Dec 2018 dealer cascade signature</div>' +
                   '<div class="jha-conv-status">' + esc(status) + '</div>' +
                 '</div>' +
                 '<div class="jha-conv-desc">' +
                   'When 3+ of these 4 cardinal equity-microstructure categories fire together in the top setup\'s smoking guns: <b>Dealer GEX</b>, <b>Options Flow / Gamma</b>, <b>Skew / Vol / Tail Hedge</b>, <b>CFTC / Short Interest</b>.' +
                 '</div>' +
                 '<div class="jha-pillars" style="grid-template-columns:repeat(4,1fr)">' +
                   card("DEALER_GEX",         "dealer gex",         "Dealer gamma exposure shift") +
                   card("OPTIONS_FLOW_GAMMA", "options flow/gamma", "Unusual call/put activity") +
                   card("SKEW_VOL",           "skew / vol",         "Tail-hedge bidding, IV moves") +
                   card("CFTC_SHORT",         "cftc / short int",   "Commercials flip, squeeze setup") +
                 '</div>' +
                 (c.top_target ? '<div class="jha-conv-count">Top target on this fingerprint: <b>' + esc(c.top_target) + ' ' + esc(c.top_direction || '') + '</b></div>' : '') +
                 (lastConv ? '<div class="jha-conv-count">Last convergence fire: <b>' + esc(tsShort(lastConv)) + ' (' + esc(ageStr(lastConv)) + ')</b></div>' :
                   '<div class="jha-conv-count">Last convergence fire: <b>never</b> (since alerting deployed)</div>') +
               '</div>';
    return html;
  }


  function pillarCell(name, p, stressed) {
    var cls = stressed === true ? "stressed" : (stressed === false ? "calm" : "");
    var state = (p && p.state) ? p.state : "—";
    var mean = (p && p.what_it_means) ? p.what_it_means : "";
    return '<div class="jha-pillar ' + cls + '">' +
             '<div class="jha-pillar-name">' + esc(name) + '</div>' +
             '<div class="jha-pillar-state">' + esc(state) + '</div>' +
             (mean ? '<div class="jha-pillar-mean">' + esc(mean.substring(0, 140)) + '</div>' : '') +
           '</div>';
  }

  function buildTimeline(equityHist, macroHist) {
    var items = [];
    function pushFromHistory(hist, source) {
      var events = (hist || {}).events || [];
      events.forEach(function (e) {
        items.push({
          ts: e.ts,
          source: source,
          score: e.score,
          regime: e.regime,
          headline: e.headline || "",
          target: source === "equity" ? e.top_setup_asset : e.top_setup_instr,
          direction: e.top_setup_dir,
          isExtreme: (e.regime || "").toUpperCase() === "EXTREME",
        });
      });
    }
    pushFromHistory(equityHist, "equity");
    pushFromHistory(macroHist,  "macro");

    // Sort newest first
    items.sort(function (a, b) {
      return new Date(b.ts).getTime() - new Date(a.ts).getTime();
    });
    return items;
  }

  function renderTimeline(items) {
    if (!items.length) {
      return '<div class="jha-timeline-empty">No alert events in the last 7 days. ' +
             'The institutional flow tape has been within normal-to-elevated range and ' +
             'no convergence fingerprint has fired. The timeline will populate the moment ' +
             'either sniffer crosses score 60 or regime EXTREME.</div>';
    }
    var html = "";
    items.forEach(function (it) {
      var rg = (it.regime || "ELEVATED").toUpperCase();
      var sourceLabel = it.source === "equity" ? "EQUITY FRONT-RUN" : "MACRO FRONT-RUN";
      html += '<div class="jha-event">' +
                '<div class="jha-event-dot ' + it.source + ' ' + rg + '"></div>' +
                '<div class="jha-event-body">' +
                  '<div class="jha-event-head">' +
                    '<span class="jha-event-ts">' + esc(tsShort(it.ts)) + ' UTC</span>' +
                    '<span class="jha-event-source ' + it.source + '">' + esc(sourceLabel) + '</span>' +
                    '<span class="jha-event-score">score ' + esc(it.score) + '</span>' +
                    '<span class="jha-event-regime ' + rg + '">' + esc(rg) + '</span>' +
                    (it.target ? '<span class="jha-event-target">target: <b>' + esc(it.target) + '</b>' + (it.direction ? ' ' + esc(it.direction) : '') + '</span>' : '') +
                  '</div>' +
                  (it.headline ? '<div class="jha-event-headline">' + esc(it.headline) + '</div>' : '') +
                '</div>' +
              '</div>';
    });
    return html;
  }

  function mount(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    el.classList.add("jha-wrap");
    el.innerHTML = '<div class="jha-loading">📡 loading alerts cockpit · syncing 6 data sources…</div>';

    return Promise.all([
      fetchJSON("frontrun-sniffer.json"),
      fetchJSON("macro-frontrun-sniffer.json"),
      fetchJSON("_alerts/frontrun-sniffer-alert-state.json"),
      fetchJSON("_alerts/macro-frontrun-sniffer-alert-state.json"),
      fetchJSON("frontrun-sniffer-history.json"),
      fetchJSON("macro-frontrun-sniffer-history.json"),
    ]).then(function (results) {
      var equityBrief = results[0];
      var macroBrief  = results[1];
      var equityState = results[2];
      var macroState  = results[3];
      var equityHist  = results[4];
      var macroHist   = results[5];

      var timeline = buildTimeline(equityHist, macroHist);

      var html = "";

      // Section 1: live status of both sniffers
      html += '<div class="jha-section-h">📡 Live Sniffer Status — both engines + alert state</div>';
      html += '<div class="jha-status-grid">';
      html += renderSnifferStatus(
        "🎯 EQUITY FRONT-RUN SNIFFER",
        "equity", equityBrief, equityState,
        "/frontrun.html",
        "overall_anomaly_score", "anomaly_regime"
      );
      html += renderSnifferStatus(
        "🏛 MACRO FRONT-RUN SNIFFER",
        "macro", macroBrief, macroState,
        "/macro-frontrun.html",
        "overall_macro_score", "macro_regime"
      );
      html += '</div>';

      // Section 2a: equity convergence fingerprint live state
      html += '<div class="jha-section-h">🎯 Equity Convergence Fingerprint — live 4-cardinal state</div>';
      html += renderEquityConvergence(equityBrief, equityState);

      // Section 2b: macro convergence fingerprint live state
      html += '<div class="jha-section-h">🏛 Macro Convergence Fingerprint — live 3-pillar state</div>';
      html += renderConvergence(macroBrief, macroState);

      // Section 3: chronological alert event timeline
      html += '<div class="jha-section-h">📊 Alert Event Timeline — score ≥ 60 OR regime EXTREME, last 7d</div>';
      html += '<div class="jha-timeline">' + renderTimeline(timeline) + '</div>';

      // Legend
      html += '<div class="jha-legend">' +
                '<span class="equity-dot"></span><b>equity</b> = unified sniffer (39 feeds across 11 categories) &nbsp;·&nbsp; ' +
                '<span class="macro-dot"></span><b>macro</b> = rates/auctions/bonds deep-dive (20 pillars) &nbsp;·&nbsp; ' +
                'Telegram alerts via @Justhodl_bot, capped at 8/day/sniffer, 60-min cooldown except convergence-fingerprint and regime-transition (bypass).' +
              '</div>';

      el.innerHTML = html;
    }).catch(function (e) {
      el.innerHTML = '<div class="jha-err">Alerts cockpit unavailable: ' + esc(e.message || e) + '</div>';
      throw e;
    });
  }

  window.JHAIAlerts = { mount: mount, version: "1.0.0" };
})();
