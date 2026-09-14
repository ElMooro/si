/* jh-reskin-skip: Supercharts chrome overlay — chart.html only. */
(function () {
  var path = location.pathname || "";
  if (!/chart\.html?$|\/chart\/?$/i.test(path)) return;
  if (window.__jhTvChrome) return;
  window.__jhTvChrome = true;

  var css = document.createElement("style");
  css.id = "jh-tv-chrome";
  css.textContent = [
    "html[data-theme=dark],html[data-theme=dark] body,html[data-theme=dark] #app{background:#131722;color:#d1d4dc}",
    "html[data-theme=light],html[data-theme=light] body,html[data-theme=light] #app{background:#ffffff;color:#131722}",
    "#tabbar,.tabs,#tabs{height:48px!important;min-height:48px!important;flex:none}",
    "html[data-theme=dark] #tabbar,html[data-theme=dark] .tabs{background:#1e222d}",
    "html[data-theme=light] #tabbar,html[data-theme=light] .tabs{background:#f8f9fd}",
    ".tab{height:48px;font-size:13px;padding:0 14px}",
    "#tfbar,.bar#tfbar{height:48px!important;min-height:48px!important;gap:2px;padding:0 10px;flex:none}",
    "html[data-theme=dark] #tfbar{background:#131722}",
    "html[data-theme=light] #tfbar{background:#ffffff}",
    "#tfbar button{height:34px;padding:0 10px;border-radius:4px;font-size:14px}",
    "#tfbar .chg{height:32px!important;padding:0 9px!important;font-size:13px!important}",
    "#tfbar .wsico{width:34px!important;height:34px!important}",
    "#listfab,#tape,#chgbar,.jh-tv-trade,#btn-tv-sell,#btn-tv-buy,#jh-engine-data,.jdi-panel{display:none!important}",
    ".jh-tv-trade{margin-left:auto}",
    "#quote .sell,#tfbar .sell{background:#f23645!important;color:#fff!important;font-weight:700;padding:8px 16px;border-radius:4px;font-size:13px}",
    "#quote .buy,#tfbar .buy{background:#2962ff!important;color:#fff!important;font-weight:700;padding:8px 16px;border-radius:4px;font-size:13px}",
    "#dock .dtabs{height:32px}",
    "#dock .dtabs button.on{color:#2962ff;border-bottom:2px solid #2962ff;background:transparent;border-top:0}",
    "html[data-theme=dark] .tab.on{background:#131722}",
    "html[data-theme=light] .tab.on{background:#ffffff}",
    ".rail{width:52px}",
    "html[data-theme=dark] .rail{background:#1e222d}",
    "html[data-theme=light] .rail{background:#f8f9fd}"
  ].join("");
  (document.head || document.documentElement).appendChild(css);
})();
