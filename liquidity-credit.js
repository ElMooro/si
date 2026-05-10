/* liquidity-credit.js — JustHodl liquidity & credit overlay
   Loads data/liquidity-credit-engine.json and injects:
     1. A floating status pill (always)  — opt-out: window.JUSTHODL_LCE_NO_PILL=true
     2. A full panel into <div id="liquidity-credit-panel"></div> if present

   Categories: balance_sheet · liquidity_facilities · credit_spreads · corporate_yields
   Series state colors: NORMAL/WATCH/ELEVATED/CRISIS
*/
(function() {
  const URL = "https://justhodl-dashboard-live.s3.amazonaws.com/data/liquidity-credit-engine.json";
  const REFRESH_MS = 5 * 60 * 1000;

  const STATE_STYLE = {
    "NORMAL":   { bg: "rgba(34,197,94,0.10)",  fg: "#22c55e", icon: "🟢" },
    "WATCH":    { bg: "rgba(245,158,11,0.12)", fg: "#f59e0b", icon: "🟡" },
    "ELEVATED": { bg: "rgba(249,115,22,0.14)", fg: "#f97316", icon: "🟠" },
    "CRISIS":   { bg: "rgba(239,68,68,0.16)",  fg: "#ef4444", icon: "🔴" },
  };

  const CAT_META = {
    balance_sheet: { icon: "🏦", short: "Balance Sheet",
                      blurb: "Fed assets, bank reserves, TGA, RRP, memo collateral pledges" },
    liquidity_facilities: { icon: "💧", short: "Liquidity Facilities",
                             blurb: "Primary credit, central-bank swap lines, emergency loans" },
    credit_spreads: { icon: "📉", short: "Credit Spreads",
                       blurb: "ICE BofA HY OAS — US, Euro, EM, IG" },
    corporate_yields: { icon: "🏢", short: "Corporate Yields",
                         blurb: "HQM 10y corporate bond rate · spread to Treasury" },
  };

  function injectStyles() {
    if (document.getElementById("jhLceStyles")) return;
    const s = document.createElement("style");
    s.id = "jhLceStyles";
    s.textContent = `
      .jh-lce-pill {
        position: fixed; right: 12px; bottom: 110px; z-index: 9997;
        display: flex; align-items: center; gap: 6px;
        padding: 6px 10px 6px 8px;
        background: rgba(8,9,15,0.92); border: 1px solid #2e2e2e;
        border-radius: 16px; font-size: 11px; line-height: 1;
        font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
        box-shadow: 0 4px 12px rgba(0,0,0,0.35);
        cursor: pointer; transition: all 0.15s; backdrop-filter: blur(6px);
      }
      .jh-lce-pill:hover { transform: translateY(-1px); border-color: #facc15; }
      .jh-lce-pill .dots { display: flex; gap: 3px; }
      .jh-lce-pill .dot { width: 7px; height: 7px; border-radius: 50%; }
      .jh-lce-pill .label { color: #a8b3c7; font-weight: 500; }
      .jh-lce-pill .score { color: #e6ecf5; font-weight: 700;
                             padding-left: 4px; border-left: 1px solid #2e2e2e; margin-left: 2px; }

      .jh-lce-panel { background: #0e1120; border: 1px solid rgba(120,145,180,0.12);
                       border-radius: 8px; padding: 0; overflow: hidden;
                       font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif; }
      .jh-lce-panel .head { padding: 14px 18px; border-bottom: 1px solid rgba(120,145,180,0.12);
                              background: #141828; display: flex; justify-content: space-between;
                              align-items: center; flex-wrap: wrap; gap: 10px; }
      .jh-lce-panel .head h3 { margin: 0; font-size: 14px; color: #e6ecf5; font-weight: 600; }
      .jh-lce-panel .head .meta { font-size: 11px; color: #6b7a92; }

      .jh-lce-cat-tabs { display: flex; gap: 0; border-bottom: 1px solid rgba(120,145,180,0.12);
                          background: #0a0c14; }
      .jh-lce-cat-tab { padding: 10px 16px; cursor: pointer; font-size: 11px;
                         color: #6b7a92; border-bottom: 2px solid transparent;
                         text-transform: uppercase; letter-spacing: 0.06em; font-weight: 600;
                         transition: 0.15s; user-select: none; }
      .jh-lce-cat-tab:hover { color: #e6ecf5; }
      .jh-lce-cat-tab.active { color: #facc15; border-bottom-color: #facc15; }
      .jh-lce-cat-tab .pill {
        display: inline-block; margin-left: 6px; padding: 1px 6px; border-radius: 4px;
        font-size: 10px; vertical-align: 1px;
      }

      .jh-lce-table { width: 100%; border-collapse: collapse; font-size: 12px; }
      .jh-lce-table th { padding: 8px 12px; text-align: left; font-size: 10px;
                          color: #6b7a92; text-transform: uppercase; letter-spacing: 0.05em;
                          font-weight: 600; border-bottom: 1px solid rgba(120,145,180,0.12);
                          background: #141828; }
      .jh-lce-table th.r, .jh-lce-table td.r { text-align: right;
                                                  font-variant-numeric: tabular-nums; }
      .jh-lce-table td { padding: 9px 12px; border-bottom: 1px solid rgba(255,255,255,0.04); }
      .jh-lce-table tr:hover { background: rgba(255,255,255,0.02); }
      .jh-lce-table .name { color: #e6ecf5; font-weight: 500; }
      .jh-lce-table .name .sid { color: #6b7a92; font-size: 10px; margin-left: 6px;
                                   font-family: ui-monospace, monospace; }
      .jh-lce-table .signal-pill { display: inline-block; padding: 2px 8px; border-radius: 3px;
                                     font-size: 10px; font-weight: 700; letter-spacing: 0.04em; }
      .jh-lce-table .delta { font-size: 11px; font-variant-numeric: tabular-nums; }
      .jh-lce-table .delta.up { color: #22c55e; }
      .jh-lce-table .delta.dn { color: #ef4444; }
      .jh-lce-table .delta.flat { color: #6b7a92; }
      .jh-lce-table .z { font-size: 10px; color: #6b7a92; }
      .jh-lce-table .z.high { color: #f97316; }
      .jh-lce-table .z.extreme { color: #ef4444; font-weight: 700; }
      .jh-lce-table .reason { font-size: 10px; color: #6b7a92; margin-top: 3px; line-height: 1.4; }
    `;
    document.head.appendChild(s);
  }

  function fmtPct(v, dp = 2) {
    if (v == null || isNaN(v)) return "—";
    return (v >= 0 ? "+" : "") + v.toFixed(dp) + "%";
  }
  function fmtNum(v, dp = 2) {
    if (v == null || isNaN(v)) return "—";
    return Number(v).toFixed(dp);
  }
  function deltaClass(v) {
    if (v == null || isNaN(v)) return "flat";
    if (v > 0.05) return "up";
    if (v < -0.05) return "dn";
    return "flat";
  }
  function zClass(z) {
    if (z == null) return "";
    const a = Math.abs(z);
    if (a >= 3) return "extreme";
    if (a >= 2) return "high";
    return "";
  }

  function buildPill(d) {
    const score = d.composite?.score ?? 0;
    const regime = d.regime || "CALM";
    // Dot per category, color based on worst signal in that category
    const cats = ["balance_sheet", "liquidity_facilities", "credit_spreads", "corporate_yields"];
    const series = d.series || {};
    const dotsHTML = cats.map(c => {
      const ids = (d.by_category || {})[c] || [];
      let worst = "NORMAL";
      const rank = { NORMAL: 0, WATCH: 1, ELEVATED: 2, CRISIS: 3 };
      for (const sid of ids) {
        const s = series[sid]?.signal;
        if (s && rank[s] > rank[worst]) worst = s;
      }
      const st = STATE_STYLE[worst];
      return `<span class="dot" title="${CAT_META[c].short}: ${worst}"
                    style="background:${st.fg}"></span>`;
    }).join("");

    let pill = document.querySelector(".jh-lce-pill");
    if (!pill) {
      pill = document.createElement("div");
      pill.className = "jh-lce-pill";
      pill.title = "Liquidity & Credit Engine — click for breakdown";
      pill.onclick = () => { window.location.href = "/liquidity.html#engine"; };
      document.body.appendChild(pill);
    }
    pill.innerHTML = `
      <span style="font-size:13px">💧</span>
      <span class="label">liq</span>
      <span class="dots">${dotsHTML}</span>
      <span class="score">${score}</span>`;
  }

  let activeTab = "balance_sheet";
  function buildPanel(target, d) {
    const series = d.series || {};
    const byCat = d.by_category || {};

    // Tab counts
    const tabHTML = ["balance_sheet", "liquidity_facilities", "credit_spreads", "corporate_yields"].map(c => {
      const ids = byCat[c] || [];
      const fired = ids.filter(sid => {
        const s = series[sid]?.signal;
        return s && s !== "NORMAL";
      }).length;
      const meta = CAT_META[c];
      const pill = fired > 0
        ? `<span class="pill" style="background:rgba(245,158,11,0.16);color:#f59e0b">${fired}</span>`
        : `<span class="pill" style="background:rgba(34,197,94,0.10);color:#22c55e">${ids.length}</span>`;
      return `<div class="jh-lce-cat-tab ${c === activeTab ? 'active' : ''}" data-cat="${c}">
        ${meta.icon} ${meta.short}${pill}</div>`;
    }).join("");

    // Active table rows
    const ids = byCat[activeTab] || [];
    const rows = ids.map(sid => {
      const s = series[sid];
      if (!s || !s.available) {
        return `<tr><td class="name">${sid} <span class="sid">unavailable</span></td>
          <td colspan="6" class="r" style="color:#6b7a92">—</td></tr>`;
      }
      const sig = s.signal || "NORMAL";
      const st = STATE_STYLE[sig];
      const pillStyle = `background:${st.bg};color:${st.fg}`;
      const wow = s.wow_pct, mom = s.mom_pct, qoq = s.qoq_pct, yoy = s.yoy_pct;
      const z = s.z_1y;
      const reasonHTML = s.signal_reason
        ? `<div class="reason">${s.signal_reason}</div>` : "";
      return `<tr>
        <td class="name">${s._label}<span class="sid">${sid}</span>${reasonHTML}</td>
        <td class="r"><strong>${fmtNum(s.latest_value)}</strong></td>
        <td class="r delta ${deltaClass(wow)}">${fmtPct(wow)}</td>
        <td class="r delta ${deltaClass(mom)}">${fmtPct(mom)}</td>
        <td class="r delta ${deltaClass(qoq)}">${fmtPct(qoq)}</td>
        <td class="r delta ${deltaClass(yoy)}">${fmtPct(yoy)}</td>
        <td class="r z ${zClass(z)}">${z != null ? `z=${z.toFixed(1)}` : "—"}</td>
        <td><span class="signal-pill" style="${pillStyle}">${st.icon} ${sig}</span></td>
      </tr>`;
    }).join("");

    const regime = d.regime || "—";
    const compScore = d.composite?.score ?? 0;
    const nFiring = d.composite?.n_firing ?? 0;
    const generatedAt = d.generated_at;
    const ageMin = generatedAt
      ? Math.round((Date.now() - new Date(generatedAt).getTime()) / 60000) + "m ago"
      : "—";

    target.innerHTML = `
      <div class="jh-lce-panel">
        <div class="head">
          <div>
            <h3>💧 Liquidity & Credit Engine</h3>
            <div class="meta">${CAT_META[activeTab].blurb}</div>
          </div>
          <div class="meta">
            regime <strong style="color:${STATE_STYLE[regime] ? STATE_STYLE[regime].fg : '#a8b3c7'}">${regime}</strong>
            · composite <strong>${compScore}/100</strong>
            · ${nFiring} firing · ${ageMin}
          </div>
        </div>
        <div class="jh-lce-cat-tabs">${tabHTML}</div>
        <div style="overflow-x:auto">
          <table class="jh-lce-table">
            <thead><tr>
              <th>Series</th>
              <th class="r">Latest</th>
              <th class="r">WoW</th>
              <th class="r">MoM</th>
              <th class="r">QoQ</th>
              <th class="r">YoY</th>
              <th class="r">z(1y)</th>
              <th>Signal</th>
            </tr></thead>
            <tbody>${rows || `<tr><td colspan="8" style="color:#6b7a92;padding:20px;text-align:center">No series in this category</td></tr>`}</tbody>
          </table>
        </div>
      </div>`;

    // Wire tab clicks
    target.querySelectorAll(".jh-lce-cat-tab").forEach(t => {
      t.addEventListener("click", () => {
        activeTab = t.dataset.cat;
        buildPanel(target, lastData);
      });
    });
  }

  let lastData = null;
  async function load() {
    try {
      const r = await fetch(URL + "?t=" + Date.now());
      if (!r.ok) throw new Error("HTTP " + r.status);
      const d = await r.json();
      lastData = d;
      if (!window.JUSTHODL_LCE_NO_PILL) buildPill(d);
      const panel = document.getElementById("liquidity-credit-panel");
      if (panel) buildPanel(panel, d);
    } catch (e) {
      console.warn("[liquidity-credit]", e.message);
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
