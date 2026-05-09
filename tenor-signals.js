/* tenor-signals.js — JustHodl tenor signal overlay
   Single module loaded by multiple pages. Reads
   data/auction-tenor-signals.json and injects a status pill into the page,
   plus optionally a full panel when an element with id="tenor-signals-panel"
   exists.

   Usage:
     <script src="/tenor-signals.js" defer></script>
     // optional panel target:
     <div id="tenor-signals-panel"></div>
     // optional opt-out:
     <script>window.JUSTHODL_TENOR_NO_PILL = true;</script>
*/
(function() {
  const ENDPOINT = "https://justhodl-dashboard-live.s3.amazonaws.com/data/auction-tenor-signals.json";
  const REFRESH_MS = 5 * 60 * 1000;

  const STATE_COLORS = {
    "OFF":     { bg: "rgba(34,197,94,0.10)",  fg: "#22c55e", icon: "🟢", label: "calm" },
    "WATCH":   { bg: "rgba(245,158,11,0.12)", fg: "#f59e0b", icon: "🟡", label: "watch" },
    "FIRING":  { bg: "rgba(249,115,22,0.14)", fg: "#f97316", icon: "🟠", label: "firing" },
    "EXTREME": { bg: "rgba(239,68,68,0.16)",  fg: "#ef4444", icon: "🔴", label: "extreme" },
  };

  const CHANNEL_META = {
    fed_path:     { icon: "📊", short: "FED", help: "2-year note auction signals Fed cuts/hikes" },
    eurodollar:   { icon: "💵", short: "EuroUSD", help: "1m/3m bills signal offshore dollar shortage" },
    qe_imminence: { icon: "🏦", short: "QE",  help: "30-year auction signals QE imminence" },
  };

  // ─── Pill (auto-injected on every page that loads this script) ───
  function injectStyles() {
    if (document.getElementById("jhTenorStyles")) return;
    const s = document.createElement("style");
    s.id = "jhTenorStyles";
    s.textContent = `
      .jh-tenor-pill {
        position: fixed; right: 12px; bottom: 60px; z-index: 9998;
        display: flex; align-items: center; gap: 6px;
        padding: 6px 10px 6px 8px;
        background: rgba(8,9,15,0.92); border: 1px solid #2e2e2e;
        border-radius: 16px; font-size: 11px; line-height: 1;
        font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
        box-shadow: 0 4px 12px rgba(0,0,0,0.35);
        cursor: pointer; transition: all 0.15s;
        backdrop-filter: blur(6px);
      }
      .jh-tenor-pill:hover { transform: translateY(-1px); border-color: #facc15; }
      .jh-tenor-pill .dots { display: flex; gap: 3px; }
      .jh-tenor-pill .dot { width: 7px; height: 7px; border-radius: 50%; }
      .jh-tenor-pill .label { color: #a8b3c7; font-weight: 500; }
      .jh-tenor-pill .score { color: #e6ecf5; font-weight: 700;
                               padding-left: 4px; border-left: 1px solid #2e2e2e; margin-left: 2px; }
      .jh-tenor-panel { background: #0e1120; border: 1px solid rgba(120,145,180,0.12);
                         border-radius: 8px; padding: 0; overflow: hidden;
                         font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif; }
      .jh-tenor-panel .head { padding: 12px 16px; border-bottom: 1px solid rgba(120,145,180,0.12);
                                background: #141828; display: flex; justify-content: space-between;
                                align-items: center; }
      .jh-tenor-panel .head h3 { margin: 0; font-size: 14px; color: #e6ecf5; font-weight: 600; }
      .jh-tenor-panel .head .meta { font-size: 11px; color: #6b7a92; }
      .jh-tenor-cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; padding: 16px; }
      @media(max-width: 800px) { .jh-tenor-cards { grid-template-columns: 1fr; } }
      .jh-tenor-card { background: #141828; border: 1px solid rgba(120,145,180,0.12);
                        border-radius: 6px; padding: 14px 16px; }
      .jh-tenor-card .label { font-size: 11px; color: #6b7a92; text-transform: uppercase;
                                letter-spacing: 0.06em; font-weight: 600; }
      .jh-tenor-card .name { font-size: 13px; color: #e6ecf5; font-weight: 600; margin: 4px 0; }
      .jh-tenor-card .state-badge { display: inline-block; padding: 3px 10px; border-radius: 4px;
                                      font-size: 11px; font-weight: 700; letter-spacing: 0.04em;
                                      text-transform: uppercase; margin: 2px 0 8px 0; }
      .jh-tenor-card .interp { font-size: 11px; color: #a8b3c7; line-height: 1.55; }
      .jh-tenor-card .evidence { margin-top: 8px; padding-top: 8px;
                                   border-top: 1px dashed rgba(120,145,180,0.12);
                                   font-size: 10px; color: #6b7a92; }
      .jh-tenor-card .evidence .row { display: flex; justify-content: space-between; padding: 2px 0; }
      .jh-tenor-card .evidence .v { color: #a8b3c7; font-variant-numeric: tabular-nums; }
    `;
    document.head.appendChild(s);
  }

  function buildPill(data) {
    const sigs = data.signals || {};
    const channels = ["fed_path", "eurodollar", "qe_imminence"];
    const dotsHTML = channels.map(ch => {
      const st = (sigs[ch] || {}).state || "OFF";
      const c = STATE_COLORS[st];
      return `<span class="dot" title="${CHANNEL_META[ch].short}: ${st}" style="background:${c.fg}"></span>`;
    }).join("");

    let pill = document.querySelector(".jh-tenor-pill");
    if (!pill) {
      pill = document.createElement("div");
      pill.className = "jh-tenor-pill";
      pill.title = "Treasury Tenor Signals — click for full breakdown";
      pill.onclick = () => { window.location.href = "/auctions.html#tenor-signals"; };
      document.body.appendChild(pill);
    }
    pill.innerHTML = `
      <span style="font-size:13px">📡</span>
      <span class="label">tenor</span>
      <span class="dots">${dotsHTML}</span>
      <span class="score">${data.composite_score ?? 0}</span>
    `;
  }

  function fmtAge(iso) {
    if (!iso) return "—";
    const t = new Date(iso);
    const mins = Math.round((Date.now() - t.getTime()) / 60000);
    if (mins < 60) return mins + "m ago";
    if (mins < 60 * 24) return Math.round(mins / 60) + "h ago";
    return Math.round(mins / 1440) + "d ago";
  }

  function buildPanel(target, data) {
    const sigs = data.signals || {};
    const cards = ["fed_path", "eurodollar", "qe_imminence"].map(ch => {
      const sig = sigs[ch] || {};
      const meta = CHANNEL_META[ch];
      const st = sig.state || "OFF";
      const c = STATE_COLORS[st];
      const dirText = sig.direction ? ` · ${sig.direction.replace(/_/g, " ").toLowerCase()}` : "";
      const evRows = (sig.evidence || []).map(e => `
        <div class="row"><span>${e.label}</span><span class="v">${e.value}</span></div>
      `).join("");
      return `
        <div class="jh-tenor-card" style="border-left: 3px solid ${c.fg};">
          <div class="label">${meta.icon} ${meta.short}${dirText}</div>
          <div class="name">${sig.label || meta.short}</div>
          <span class="state-badge" style="background:${c.bg};color:${c.fg}">${c.icon} ${st}</span>
          <div class="interp">${sig.interpretation || "—"}</div>
          <div class="evidence">${evRows}</div>
        </div>`;
    }).join("");

    target.innerHTML = `
      <div class="jh-tenor-panel">
        <div class="head">
          <h3>📡 Treasury Tenor Signals</h3>
          <div class="meta">
            composite ${data.composite_score ?? 0}/100 ·
            ${data.any_firing ? '<span style="color:#ef4444;font-weight:600">FIRING</span>' :
              data.any_watch ? '<span style="color:#f59e0b">WATCH</span>' :
              '<span style="color:#22c55e">CALM</span>'} ·
            updated ${fmtAge(data.generated_at)}
          </div>
        </div>
        <div class="jh-tenor-cards">${cards}</div>
      </div>
    `;
  }

  async function load() {
    try {
      const r = await fetch(ENDPOINT + "?t=" + Date.now());
      if (!r.ok) throw new Error("HTTP " + r.status);
      const data = await r.json();
      if (!window.JUSTHODL_TENOR_NO_PILL) buildPill(data);
      const panel = document.getElementById("tenor-signals-panel");
      if (panel) buildPanel(panel, data);
    } catch (e) {
      console.warn("[tenor-signals]", e.message);
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
