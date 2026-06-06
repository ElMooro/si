/* affiliates.js — JustHodl partner / affiliate link layer.
 *
 * Revenue on FREE traffic: when a user views a ticker or chart, offer tasteful,
 * DISCLOSED links to brokers/tools that pay per signup or recurring referral.
 *
 * HOW TO ACTIVATE each program (replace the placeholder ref/url after you join):
 *   • Interactive Brokers  → https://www.interactivebrokers.com/referral/  (per funded acct)
 *   • Webull               → Webull affiliate / referral program
 *   • Moomoo               → Moomoo partner program
 *   • tastytrade           → tastytrade affiliate
 *   • TradingView          → https://www.tradingview.com/affiliate (recurring %)
 * Until a program's `enabled` is true with a real `url`, that card is hidden,
 * so nothing fake ever renders.
 *
 * Usage:
 *   <script src="/affiliates.js"></script>
 *   Affiliates.renderInline('container-id', { ticker: 'NVDA' });   // inline card row
 *   Affiliates.renderFooterBar();                                  // slim disclosed bar
 */
(function () {
  if (window.Affiliates) return;

  // ── Partner registry. Set enabled:true + real url once you join each program.
  // {ticker} is replaced with the symbol when present. ──
  const PARTNERS = [
    {
      id: "ibkr", name: "Interactive Brokers", blurb: "Trade global markets, low cost",
      icon: "🟥", cta: "Open account",
      enabled: false,
      url: "https://www.interactivebrokers.com/",            // ← replace with your referral URL
    },
    {
      id: "tradingview", name: "TradingView", blurb: "Advanced charts & alerts",
      icon: "📈", cta: "Get TradingView",
      enabled: false,
      url: "https://www.tradingview.com/",                   // ← replace with affiliate URL
      tickerUrl: (t) => `https://www.tradingview.com/symbols/${encodeURIComponent(t)}/`,
    },
    {
      id: "webull", name: "Webull", blurb: "Commission-free, free stocks to start",
      icon: "🟦", cta: "Claim free stock",
      enabled: false,
      url: "https://www.webull.com/",                        // ← replace with referral URL
    },
    {
      id: "moomoo", name: "moomoo", blurb: "Pro-grade tools, signup bonus",
      icon: "🐮", cta: "Open account",
      enabled: false,
      url: "https://www.moomoo.com/",                        // ← replace with referral URL
    },
    {
      id: "tastytrade", name: "tastytrade", blurb: "Built for options traders",
      icon: "🟢", cta: "Open account",
      enabled: false,
      url: "https://www.tastytrade.com/",                    // ← replace with affiliate URL
    },
  ];

  const DISCLOSURE = "Some links are partner/affiliate links — JustHodl may earn a commission at no cost to you. Not a recommendation; we are not a broker or advisor.";

  function active() { return PARTNERS.filter(p => p.enabled && p.url && !p.url.endsWith(".com/")); }
  // NOTE: the `.endsWith(".com/")` guard hides any partner still on its bare
  // placeholder URL, so cards only show once you've put in a real referral link.

  function injectCSS() {
    if (document.getElementById("aff-css")) return;
    const s = document.createElement("style"); s.id = "aff-css";
    s.textContent = [
      ".aff-wrap{margin:14px 0}",
      ".aff-head{font-family:ui-monospace,Menlo,monospace;font-size:10px;color:#6f7b91;text-transform:uppercase;letter-spacing:.6px;margin-bottom:8px}",
      ".aff-row{display:flex;gap:10px;flex-wrap:wrap}",
      ".aff-card{display:flex;align-items:center;gap:10px;background:#0c1018;border:1px solid #1c2433;border-radius:10px;padding:10px 14px;text-decoration:none;transition:border-color .15s;min-width:200px}",
      ".aff-card:hover{border-color:#22d3ee}",
      ".aff-ic{font-size:18px}",
      ".aff-meta{display:flex;flex-direction:column}",
      ".aff-name{color:#e1e8f4;font-size:13px;font-weight:700}",
      ".aff-blurb{color:#6f7b91;font-size:11px}",
      ".aff-cta{margin-left:auto;color:#26ffaf;font-family:ui-monospace,Menlo,monospace;font-size:11px;font-weight:700;white-space:nowrap}",
      ".aff-disc{font-family:ui-monospace,Menlo,monospace;font-size:9.5px;color:#566072;margin-top:8px;line-height:1.4}",
      ".aff-bar{border-top:1px solid #1c2433;padding:12px 16px;text-align:center}",
      ".aff-bar .aff-row{justify-content:center}",
    ].join("");
    document.head.appendChild(s);
  }

  function esc(s) { return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }

  function cardHTML(p, ticker) {
    let href = p.url;
    if (ticker && p.tickerUrl) href = p.tickerUrl(ticker);
    return `<a class="aff-card" href="${esc(href)}" target="_blank" rel="sponsored noopener"
      data-aff="${esc(p.id)}">
      <span class="aff-ic">${p.icon}</span>
      <span class="aff-meta"><span class="aff-name">${esc(p.name)}</span><span class="aff-blurb">${esc(p.blurb)}</span></span>
      <span class="aff-cta">${esc(p.cta)} →</span>
    </a>`;
  }

  function trackClicks(root) {
    root.querySelectorAll("[data-aff]").forEach(a => a.addEventListener("click", () => {
      // light, privacy-safe click ping for your own conversion tracking (optional)
      try {
        fetch("https://justhodl-data-proxy.raafouis.workers.dev/aff-click?p=" + encodeURIComponent(a.dataset.aff), { method: "POST", keepalive: true }).catch(() => {});
      } catch (e) {}
    }));
  }

  function renderInline(targetId, opts) {
    opts = opts || {};
    const host = document.getElementById(targetId);
    if (!host) return;
    const list = active();
    if (!list.length) { host.innerHTML = ""; return; }   // nothing enabled → render nothing
    injectCSS();
    host.innerHTML =
      `<div class="aff-wrap"><div class="aff-head">${opts.heading || "Trade this idea"}</div>` +
      `<div class="aff-row">${list.map(p => cardHTML(p, opts.ticker)).join("")}</div>` +
      `<div class="aff-disc">${DISCLOSURE}</div></div>`;
    trackClicks(host);
  }

  function renderFooterBar() {
    const list = active();
    if (!list.length) return;
    injectCSS();
    const bar = document.createElement("div");
    bar.className = "aff-bar";
    bar.innerHTML = `<div class="aff-row">${list.map(p => cardHTML(p)).join("")}</div><div class="aff-disc">${DISCLOSURE}</div>`;
    document.body.appendChild(bar);
    trackClicks(bar);
  }

  window.Affiliates = { renderInline, renderFooterBar, PARTNERS, active };
})();
