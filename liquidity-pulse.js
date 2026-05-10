/* liquidity-pulse.js — JustHodl liquidity & credit pulse overlay.
   Reads data/liquidity-pulse.json and injects a status pill on every page,
   plus a full panel when an element with id="liquidity-pulse-panel" exists.

   Usage:
     <script src="/liquidity-pulse.js" defer></script>
     // optional panel target:
     <div id="liquidity-pulse-panel"></div>
     // optional opt-out:
     <script>window.JUSTHODL_LIQ_NO_PILL = true;</script>
*/
(function() {
  const ENDPOINT = "https://justhodl-dashboard-live.s3.amazonaws.com/data/liquidity-pulse.json";
  const REFRESH_MS = 5 * 60 * 1000;

  const SIGNAL_COLORS = {
    NORMAL:           { bg: "rgba(34,197,94,0.10)",  fg: "#22c55e", icon: "🟢" },
    EXPANDING:        { bg: "rgba(34,197,94,0.16)",  fg: "#22c55e", icon: "🟢" },
    TIGHT_EUPHORIA:   { bg: "rgba(167,139,250,0.10)",fg: "#a78bfa", icon: "🟣" },
    TIGHTENING:       { bg: "rgba(245,158,11,0.10)", fg: "#f59e0b", icon: "🟡" },
    WATCH:            { bg: "rgba(245,158,11,0.14)", fg: "#f59e0b", icon: "🟡" },
    DRAINING:         { bg: "rgba(249,115,22,0.14)", fg: "#f97316", icon: "🟠" },
    ELEVATED:         { bg: "rgba(249,115,22,0.18)", fg: "#f97316", icon: "🟠" },
    ACUTE_DRAIN:      { bg: "rgba(239,68,68,0.16)",  fg: "#ef4444", icon: "🔴" },
    CRISIS:           { bg: "rgba(239,68,68,0.20)",  fg: "#ef4444", icon: "🔴" },
    UNKNOWN:          { bg: "rgba(120,145,180,0.10)",fg: "#6b7a92", icon: "⚪" },
  };
  const sigColor = (s) => SIGNAL_COLORS[s] || SIGNAL_COLORS.UNKNOWN;

  const GROUP_META = {
    balance:  { icon: "📊", label: "balance sheet" },
    facility: { icon: "🚨", label: "emergency facility" },
    credit:   { icon: "💸", label: "credit spread" },
  };

  function injectStyles() {
    if (document.getElementById("jhLiqStyles")) return;
    const s = document.createElement("style");
    s.id = "jhLiqStyles";
    s.textContent = `
      .jh-liq-pill {
        position: fixed; right: 12px; bottom: 100px; z-index: 9997;
        display: flex; align-items: center; gap: 6px;
        padding: 6px 10px 6px 8px;
        background: rgba(8,9,15,0.92); border: 1px solid #2e2e2e;
        border-radius: 16px; font-size: 11px; line-height: 1;
        font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
        box-shadow: 0 4px 12px rgba(0,0,0,0.35);
        cursor: pointer; transition: all 0.15s;
        backdrop-filter: blur(6px);
      }
      .jh-liq-pill:hover { transform: translateY(-1px); border-color: #facc15; }
      .jh-liq-pill .label { color: #a8b3c7; font-weight: 500; }
      .jh-liq-pill .num { color: #e6ecf5; font-weight: 700;
                          padding: 0 4px; border-radius: 3px; }
      .jh-liq-pill .num.credit { background: rgba(239,68,68,0.10); color: #ef4444; }
      .jh-liq-pill .num.liq    { background: rgba(59,130,246,0.10); color: #3b82f6; }
      .jh-liq-panel { background: #0e1120; border: 1px solid rgba(120,145,180,0.12);
                      border-radius: 8px; padding: 0; overflow: hidden;
                      font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif; }
      .jh-liq-panel .head { padding: 12px 16px; border-bottom: 1px solid rgba(120,145,180,0.12);
                            background: #141828; display: flex; justify-content: space-between;
                            align-items: center; gap: 12px; flex-wrap: wrap; }
      .jh-liq-panel .head h3 { margin: 0; font-size: 14px; color: #e6ecf5; font-weight: 600; }
      .jh-liq-panel .head .meta { font-size: 11px; color: #6b7a92; }
      .jh-liq-panel .summary {
        padding: 12px 18px; background: rgba(59,130,246,0.04);
        border-bottom: 1px solid rgba(120,145,180,0.10);
        font-size: 13px; color: #a8b3c7; line-height: 1.55;
      }
      .jh-liq-panel .composites { display: grid; grid-template-columns: repeat(2,1fr); gap: 12px;
                                    padding: 14px 16px; border-bottom: 1px solid rgba(120,145,180,0.10); }
      @media(max-width: 700px) { .jh-liq-panel .composites { grid-template-columns: 1fr; } }
      .jh-liq-panel .composite-card {
        background: #141828; border: 1px solid rgba(120,145,180,0.12);
        border-radius: 6px; padding: 14px 16px;
      }
      .jh-liq-panel .composite-card .label { font-size: 10px; color: #6b7a92;
                                               text-transform: uppercase; letter-spacing: 0.06em;
                                               font-weight: 600; }
      .jh-liq-panel .composite-card .v { font-size: 28px; font-weight: 700; line-height: 1;
                                           margin: 6px 0; font-variant-numeric: tabular-nums; }
      .jh-liq-panel .composite-card .regime { font-size: 11px; font-weight: 700;
                                                 text-transform: uppercase; letter-spacing: 0.04em;
                                                 padding: 3px 10px; border-radius: 3px;
                                                 display: inline-block; }

      .jh-liq-table { width: 100%; border-collapse: collapse; font-size: 12px; }
      .jh-liq-table th { padding: 8px 10px; text-align: left; font-size: 10px; color: #6b7a92;
                          text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600;
                          border-bottom: 1px solid rgba(120,145,180,0.12); background: #141828; }
      .jh-liq-table th.r, .jh-liq-table td.r { text-align: right; font-variant-numeric: tabular-nums; }
      .jh-liq-table td { padding: 8px 10px; border-bottom: 1px solid rgba(120,145,180,0.06);
                          color: #a8b3c7; }
      .jh-liq-table tr.balance { background: rgba(59,130,246,0.02); }
      .jh-liq-table tr.facility { background: rgba(239,68,68,0.04); }
      .jh-liq-table tr.credit { background: rgba(245,158,11,0.02); }
      .jh-liq-table .sid { color: #6b7a92; font-family: monospace; font-size: 10px; }
      .jh-liq-table .label-cell { color: #e6ecf5; font-weight: 500; }
      .jh-liq-table .pill { padding: 2px 7px; border-radius: 3px; font-size: 10px; font-weight: 700;
                              text-transform: uppercase; letter-spacing: 0.04em; display: inline-block; }
      .jh-liq-table .interp { font-size: 11px; color: #6b7a92; padding: 4px 10px 10px 14px;
                                line-height: 1.5; max-width: 480px; }
      .jh-liq-table .delta-pos { color: #22c55e; font-weight: 600; }
      .jh-liq-table .delta-neg { color: #ef4444; font-weight: 600; }
      .jh-liq-table .delta-flat { color: #6b7a92; }
    `;
    document.head.appendChild(s);
  }

  function fmtAge(iso) {
    if (!iso) return "—";
    const t = new Date(iso);
    const mins = Math.round((Date.now() - t.getTime()) / 60000);
    if (mins < 60) return mins + "m ago";
    if (mins < 60*24) return Math.round(mins / 60) + "h ago";
    return Math.round(mins / 1440) + "d ago";
  }

  function fmtPct(v, dp=2) {
    if (v == null || isNaN(v)) return "—";
    const n = Number(v);
    return (n >= 0 ? "+" : "") + n.toFixed(dp) + "%";
  }
  function fmtNum(v, dp=2) {
    if (v == null || isNaN(v)) return "—";
    return Number(v).toFixed(dp);
  }
  function deltaClass(v) {
    if (v == null) return "delta-flat";
    return v > 0 ? "delta-pos" : v < 0 ? "delta-neg" : "delta-flat";
  }

  function buildPill(d) {
    const c = (d.composites || {});
    let pill = document.querySelector(".jh-liq-pill");
    if (!pill) {
      pill = document.createElement("div");
      pill.className = "jh-liq-pill";
      pill.title = "Liquidity & credit pulse — click for full breakdown";
      pill.onclick = () => { window.location.href = "/liquidity.html#pulse"; };
      document.body.appendChild(pill);
    }
    const credScore = c.credit_stress_score != null ? Math.round(c.credit_stress_score) : "—";
    const liqScore = c.liquidity_score != null ? Math.round(c.liquidity_score) : "—";
    pill.innerHTML = `
      <span style="font-size:13px">💧</span>
      <span class="label">liq</span>
      <span class="num liq" title="Liquidity drain score (higher = tighter)">${liqScore}</span>
      <span class="label" style="margin-left:4px">cr</span>
      <span class="num credit" title="Credit stress score (higher = stress)">${credScore}</span>
    `;
  }

  function fmtValue(latest, sid) {
    if (latest == null) return "—";
    // Heuristic format by magnitude
    if (Math.abs(latest) >= 1e9) return "$" + (latest/1e9).toFixed(2) + "T";
    if (Math.abs(latest) >= 1e6) return "$" + (latest/1e6).toFixed(2) + "B";
    if (Math.abs(latest) >= 1e3) return "$" + (latest/1e3).toFixed(1) + "M";
    if (sid && (sid.startsWith("BAML") || sid.startsWith("HQM"))) return latest.toFixed(2) + "%";
    return latest.toFixed(2);
  }

  function buildPanel(target, d) {
    const c = (d.composites || {});
    const series = d.series || {};

    // Composite cards
    const credColor = sigColor(c.credit_regime || "UNKNOWN");
    const liqColor = sigColor(c.liquidity_regime || "UNKNOWN");

    // Group rows by category
    const groups = { balance: [], facility: [], credit: [] };
    for (const [sid, s] of Object.entries(series)) {
      const g = s.group || "balance";
      if (groups[g]) groups[g].push([sid, s]);
    }

    const rowsHtml = [];
    for (const [groupKey, label] of [["balance", "FED BALANCE SHEET"],
                                       ["facility", "EMERGENCY FACILITIES"],
                                       ["credit", "CREDIT STRESS SPREADS"]]) {
      const arr = groups[groupKey] || [];
      if (arr.length === 0) continue;
      rowsHtml.push(`<tr><th colspan="7" style="padding-top:14px; color:#888; font-size:10px;
                          background:transparent; border-bottom:1px solid rgba(120,145,180,0.18)">
        ${label}</th></tr>`);
      for (const [sid, s] of arr) {
        const sig = s.signal || "UNKNOWN";
        const col = sigColor(sig);
        const d_wow = (s.deltas || {}).wow_pct;
        const d_mom = (s.deltas || {}).mom_pct;
        const d_qoq = (s.deltas || {}).qoq_pct;
        const d_yoy = (s.deltas || {}).yoy_pct;
        rowsHtml.push(`<tr class="${groupKey}">
          <td><span class="sid">${sid}</span><br/>
              <span class="label-cell">${s.label || sid}</span></td>
          <td class="r"><strong style="color:#e6ecf5">${fmtValue(s.latest_value, sid)}</strong>
              <br/><span style="font-size:10px;color:#6b7a92">${s.latest_date || ""}</span></td>
          <td class="r ${deltaClass(d_wow)}">${fmtPct(d_wow, 1)}</td>
          <td class="r ${deltaClass(d_mom)}">${fmtPct(d_mom, 1)}</td>
          <td class="r ${deltaClass(d_qoq)}">${fmtPct(d_qoq, 1)}</td>
          <td class="r ${deltaClass(d_yoy)}">${fmtPct(d_yoy, 1)}</td>
          <td class="r">${s.z_score != null ? "z=" + (s.z_score >= 0 ? "+" : "") + s.z_score.toFixed(1) : "—"}</td>
          </tr>
          <tr class="${groupKey}">
            <td colspan="7" class="interp" style="border-bottom:1px solid rgba(120,145,180,0.10)">
              <span class="pill" style="background:${col.bg};color:${col.fg};margin-right:8px">${col.icon} ${sig}</span>
              ${s.interpretation || s.description || ""}
            </td>
          </tr>`);
      }
    }

    target.innerHTML = `
      <div class="jh-liq-panel">
        <div class="head">
          <h3>💧 Liquidity & Credit Pulse</h3>
          <div class="meta">
            ${d.n_series_ok || 0}/${d.n_series || 0} series · updated ${fmtAge(d.generated_at)}
          </div>
        </div>
        <div class="summary">${d.summary || "—"}</div>
        <div class="composites">
          <div class="composite-card" style="border-left:3px solid ${liqColor.fg}">
            <div class="label">Liquidity drain score</div>
            <div class="v" style="color:${liqColor.fg}">${c.liquidity_score != null ? c.liquidity_score : "—"}</div>
            <div class="regime" style="background:${liqColor.bg};color:${liqColor.fg}">
              ${liqColor.icon} ${c.liquidity_regime || "UNKNOWN"}
            </div>
            <div style="font-size:11px;color:#6b7a92;margin-top:8px;line-height:1.5">
              Polarity-adjusted balance-sheet drain rate. Higher = tighter (worse for risk assets).
            </div>
          </div>
          <div class="composite-card" style="border-left:3px solid ${credColor.fg}">
            <div class="label">Credit stress score</div>
            <div class="v" style="color:${credColor.fg}">${c.credit_stress_score != null ? c.credit_stress_score : "—"}</div>
            <div class="regime" style="background:${credColor.bg};color:${credColor.fg}">
              ${credColor.icon} ${c.credit_regime || "UNKNOWN"}
            </div>
            <div style="font-size:11px;color:#6b7a92;margin-top:8px;line-height:1.5">
              Mean of credit-spread z-scores (CCC, EuroHY, EMHY, HQM). z=2 → ELEVATED, z=3 → CRISIS.
            </div>
          </div>
        </div>
        <table class="jh-liq-table">
          <thead><tr>
            <th>Series</th>
            <th class="r">Latest</th>
            <th class="r" title="Week over week">WoW</th>
            <th class="r" title="Month over month">MoM</th>
            <th class="r" title="Quarter over quarter">QoQ</th>
            <th class="r" title="Year over year">YoY</th>
            <th class="r" title="1-year rolling z-score">Z (1y)</th>
          </tr></thead>
          <tbody>${rowsHtml.join("")}</tbody>
        </table>
      </div>
    `;
  }

  async function load() {
    try {
      const r = await fetch(ENDPOINT + "?t=" + Date.now());
      if (!r.ok) throw new Error("HTTP " + r.status);
      const d = await r.json();
      if (!window.JUSTHODL_LIQ_NO_PILL) buildPill(d);
      const panel = document.getElementById("liquidity-pulse-panel");
      if (panel) buildPanel(panel, d);
    } catch (e) {
      console.warn("[liquidity-pulse]", e.message);
    }
  }

  function init() {
    injectStyles();
    load();
    setInterval(load, REFRESH_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
