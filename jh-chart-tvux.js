/* Supercharts chrome overlay for chart.html — compact TF, dark, watchlist, buy/sell. */
(function () {
  if (!/chart\.html/i.test(location.pathname || "")) return;
  if (window.__jhTvChrome) return;
  window.__jhTvChrome = true;

  var css = document.createElement("style");
  css.id = "jh-tv-chrome";
  css.textContent = [
    "html,body,#app{background:#131722;color:#d1d4dc}",
    "#tfbar [data-tf]{display:none!important}",
    '#tfbar [data-tf="1m"],#tfbar [data-tf="5m"],#tfbar [data-tf="15m"],#tfbar [data-tf="1h"],#tfbar [data-tf="1d"],#tfbar [data-tf="1w"],#tfbar [data-tf="1M"]{display:inline-flex!important}',
    "#tfbar{height:38px;gap:2px;padding:0 8px}",
    "#tfbar button{height:28px;padding:0 8px;border-radius:2px;font-size:13px}",
    "#btn-co,#btn-live,#btn-dwin,#btn-mini,#btn-left,#btn-zm,#btn-zp,#btn-watch,#btn-vol,#btn-lay1,#btn-lay2,#btn-lay4,#goto{display:none!important}",
    "#listfab{display:none!important}",
    ".watch,#watch{width:280px!important;min-width:260px!important;max-width:320px!important;overflow:auto!important}",
    ".watch.hide,#watch.hide{display:none!important}",
    "#quote{min-height:28px;padding:2px 12px;gap:10px;align-items:center}",
    ".jh-tv-bs{display:inline-flex;align-items:center;gap:4px;pointer-events:auto}",
    ".jh-tv-sell{background:rgba(242,54,69,.16);color:#f23645;font-weight:600;padding:3px 8px;border-radius:2px;font-size:11px}",
    ".jh-tv-spr{background:var(--chip,#2a2e39);color:var(--mut,#787b86);padding:3px 6px;border-radius:2px;font-size:11px}",
    ".jh-tv-buy{background:rgba(41,98,255,.16);color:#2962ff;font-weight:600;padding:3px 8px;border-radius:2px;font-size:11px}",
    "#dock .dtabs{height:32px;background:var(--raised,#1e222d);border-top:1px solid var(--line,#2a2e39)}",
    "#dock .dtabs button{padding:0 14px;font-size:12px;color:#787b86;background:transparent;border:0}",
    "#dock .dtabs button.on{color:#2962ff;border-top:0;border-bottom:2px solid #2962ff;background:transparent}",
    ".foot{height:28px}",
    ".rail{width:52px}",
    ".tabs{height:38px;background:#1e222d}",
    ".tab.on{background:#131722}"
  ].join("");
  document.documentElement.appendChild(css);

  function relabel() {
    var map = {
      fx: "Indicators",
      Cmp: "Compare",
      Rep: "Replay",
      Alrt: "Alert",
      Cam: "Snapshot",
      Screener: "Stock Screener",
      Paper: "Trading Panel",
      Strategy: "Strategy Tester",
      Notes: "Pine Editor"
    };
    document.querySelectorAll("#tfbar button, #dtabs button").forEach(function (b) {
      var t = (b.childNodes[0] && b.childNodes[0].textContent) || b.textContent;
      t = (t || "").trim();
      if (map[t] && b.dataset.jhRelabel !== map[t]) {
        b.textContent = map[t];
        b.dataset.jhRelabel = map[t];
      }
    });
  }

  function pills() {
    var q = document.getElementById("quote");
    if (!q) return;
    if (q.querySelector(".jh-tv-bs")) return;
    var last = q.querySelector(".last");
    if (!last) return;
    var px = last.textContent || "";
    var wrap = document.createElement("span");
    wrap.className = "jh-tv-bs";
    wrap.innerHTML =
      "<button type='button' class='jh-tv-sell'>" + px + " SELL</button>" +
      "<span class='jh-tv-spr'>0.01</span>" +
      "<button type='button' class='jh-tv-buy'>" + px + " BUY</button>";
    last.insertAdjacentElement("afterend", wrap);
  }

  function dark() {
    if (document.documentElement.getAttribute("data-theme") !== "dark") {
      var b = document.getElementById("btn-theme");
      if (b) b.click();
    }
  }

  function tick() {
    relabel();
    pills();
    dark();
  }

  var mo = new MutationObserver(function () { tick(); });
  window.addEventListener("load", function () {
    setTimeout(tick, 200);
    setTimeout(tick, 800);
    setTimeout(tick, 1800);
    var root = document.getElementById("app") || document.body;
    mo.observe(root, { childList: true, subtree: true });
  });
})();
