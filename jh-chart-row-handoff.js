/* Route row/search clicks through the same qualified handoff as keyboard navigation. */
(function () {
  if (window.__jhRowHandoff) return;
  window.__jhRowHandoff = 1;
  document.addEventListener("click", function (ev) {
    var target = ev.target, row, symbol, fromHits, open;
    if (!target || !target.closest || target.closest(".w-cmp,.grip,.tvflag,.wx")) return;
    row = target.closest("#wlist [data-s], #hits [data-s]");
    if (!row) return;
    symbol = row.getAttribute("data-s");
    open = window.jhWatchlistOpen;
    // Existing handlers retain ownership before the qualified router is ready.
    // No delayed callback may replay an earlier click over a newer selection.
    if (!symbol || typeof open !== "function") return;
    fromHits = !!row.closest("#hits");
    ev.preventDefault();
    ev.stopPropagation();
    if (fromHits) {
      var q = document.getElementById("q");
      if (q) { q.value = ""; try { q.dispatchEvent(new Event("input")); } catch (e) {} }
    }
    // Pass the original identifier. The shared router owns definition checks,
    // explicit alternatives, unsupported IDs, and the visible evidence banner.
    open(symbol);
  }, true);
})();
