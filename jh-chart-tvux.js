/* jh-reskin-skip: Supercharts chrome overlay — chart.html only. */
(function () {
  var path = location.pathname || "";
  if (!/chart\.html?$|\/chart\/?$/i.test(path)) return;
  if (window.__jhTvChrome) return;
  window.__jhTvChrome = true;

  var css = document.createElement("style");
  css.id = "jh-tv-chrome";
  css.textContent = [
    "html,body,#app{background:#131722;color:#d1d4dc}",
    "#tfbar{height:38px;gap:2px;padding:0 8px;background:#131722}",
    "#tfbar button{height:28px;padding:0 8px;border-radius:4px;font-size:13px}",
    "#listfab,#tape,#chgbar{display:none!important}",
    ".watch,#watch{width:280px!important;min-width:260px!important;max-width:320px!important;overflow:auto!important}",
    ".jh-tv-trade{margin-left:auto;display:inline-flex;gap:6px;align-items:center}",
    "#quote .sell,#tfbar .sell{background:#f23645!important;color:#fff!important;font-weight:700;padding:5px 12px;border-radius:4px}",
    "#quote .buy,#tfbar .buy{background:#2962ff!important;color:#fff!important;font-weight:700;padding:5px 12px;border-radius:4px}",
    "#dock .dtabs{height:32px}",
    "#dock .dtabs button.on{color:#2962ff;border-bottom:2px solid #2962ff;background:transparent;border-top:0}",
    ".tabs{height:38px;background:#1e222d}",
    ".tab.on{background:#131722}",
    ".rail{width:52px;background:#1e222d}"
  ].join("");
  (document.head || document.documentElement).appendChild(css);

  try {
    document.documentElement.setAttribute("data-theme", "dark");
    var w = document.getElementById("watch");
    if (w && window.innerWidth >= 720) w.className = "watch";
  } catch (e) {}
})();
