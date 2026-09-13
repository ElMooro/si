/* Move intel lines off #quote into #desk-intel (right rail). */
(function () {
  function dock() {
    var box = document.getElementById("desk-intel");
    var q = document.getElementById("quote");
    if (!box || !q) return;
    ["jh-rs-line", "jh-macro-intel", "jh-rail", "jh-src", "jh-ai-desk"].forEach(function (id) {
      var n = document.getElementById(id);
      if (n && n.parentNode !== box) box.appendChild(n);
    });
    Array.from(q.children).forEach(function (n) {
      if (n.id && n.id.indexOf("jh-") === 0 && n.parentNode !== box) box.appendChild(n);
    });
  }
  setInterval(dock, 1500);
  window.addEventListener("load", function () { setTimeout(dock, 400); });
})();
