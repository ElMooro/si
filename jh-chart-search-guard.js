/* Do not touch UniverseSearch. Raise header search above dock. */
(function () {
  if (!/chart-pro\.html/i.test(location.pathname || "")) return;
  var s = document.createElement("style");
  s.textContent =
    "#universe-search, #search-input, #search-modal { position: relative; z-index: 80 !important; }" +
    "#search-modal { z-index: 200 !important; }" +
    "#jh-chart-dock { top: 168px !important; left: 8px !important; z-index: 20 !important; }" +
    "#tv-rangebar { display: none !important; }";
  document.documentElement.appendChild(s);
})();
