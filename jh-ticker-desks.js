/* Load inventory four-state + book-fuse on ticker.html and any ?symbol= desk.
   Skip inventory-drawdown (already has its own mount). */
(function () {
  if (window.__jhTickerDesks) return;
  window.__jhTickerDesks = true;
  var page = (location.pathname.split("/").pop() || "").toLowerCase();
  if (page === "inventory-drawdown.html") return;
  var q = "";
  try {
    var p = new URLSearchParams(location.search);
    q = p.get("symbol") || p.get("s") || p.get("ticker") || "";
  } catch (e) {}
  if (page !== "ticker.html" && !q) return;
  function load(src, cb) {
    if (document.querySelector('script[src*="' + src.replace(/^\//, "") + '"]')) { if (cb) cb(); return; }
    var s = document.createElement("script");
    s.src = src;
    s.onload = cb || function () {};
    document.head.appendChild(s);
  }
  load("/jh-book-fuse.js");
  load("/jh-inventory-v2.js", function () { load("/jh-inventory-v2-mount.js"); });
})();
