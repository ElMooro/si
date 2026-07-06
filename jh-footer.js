/* jh-footer.js — shared sitewide footer (Design audit sec5 + "one CTA sitewide").
   Reuses the EXACT proven subscribe contract from index.html's hero capture:
   GET data/subscribe-endpoint.json -> {url}; POST {email,source} -> {ok}|{error}.
   Shares the same jh_sub localStorage flag so "subscribed" state is unified
   across the whole site, including the homepage's own hero form.
   Skips /screener (protected) and index.html (already has its own hero CTA —
   this avoids ever showing two competing subscribe boxes on one page). */
(function () {
  "use strict";
  var path = location.pathname;
  if (path.indexOf("/screener") === 0) return;
  if (path === "/" || /(^|\/)index\.html$/.test(path)) return;
  if (document.getElementById("jh-shared-footer")) return;

  var LINKS = [["About", "/about.html"], ["Glossary", "/glossary.html"],
               ["Pricing", "/pricing.html"], ["Status", "/status.html"],
               ["Terms", "/terms.html"], ["Privacy", "/privacy.html"]];

  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function build() {
    var f = document.createElement("footer");
    f.id = "jh-shared-footer"; f.className = "jhf";
    f.innerHTML =
      '<div class="jhf-row">' +
        '<div class="jhf-brand">JUSTHODL·AI</div>' +
        '<nav class="jhf-links">' + LINKS.map(function (l) {
          return '<a href="' + l[1] + '">' + esc(l[0]) + "</a>";
        }).join("") + "</nav>" +
      "</div>" +
      '<div class="jhf-row jhf-sub">' +
        '<div class="jhf-disclaimer">Research &amp; analytics, not investment advice.</div>' +
        '<div class="jhf-capture">' +
          '<input id="jhf-email" type="email" placeholder="you@company.com" autocomplete="email" aria-label="Email for the Morning Brief">' +
          '<button id="jhf-go" type="button">GET THE MORNING BRIEF</button>' +
          '<span id="jhf-msg" class="jhf-msg" role="status"></span>' +
        "</div>" +
      "</div>";
    document.body.appendChild(f);
    wire(f);
  }

  function wire(f) {
    var E = f.querySelector("#jhf-email"), G = f.querySelector("#jhf-go"), M = f.querySelector("#jhf-msg");
    var EP = null;
    function done() {
      G.textContent = "✓ SUBSCRIBED"; G.disabled = true; E.disabled = true;
      M.textContent = "you're on the list";
    }
    if (localStorage.getItem("jh_sub") === "1") { done(); return; }
    fetch("/data/subscribe-endpoint.json?t=" + Date.now(), { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (d) { EP = d.url; }).catch(function () {});
    function submit() {
      var v = (E.value || "").trim().toLowerCase();
      if (!/^[^@\s]+@[^@\s]+\.[^@\s]{2,}$/.test(v)) { M.textContent = "enter a valid email"; return; }
      if (!EP) { M.textContent = "endpoint warming — try again in a moment"; return; }
      M.textContent = "…";
      fetch(EP, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ email: v, source: "footer:" + path })
      }).then(function (r) { return r.json(); }).then(function (d) {
        if (d && d.ok) { localStorage.setItem("jh_sub", "1"); done(); }
        else M.textContent = (d && d.error) || "failed — try again";
      }).catch(function () { M.textContent = "network error — try again"; });
    }
    G.onclick = submit;
    E.addEventListener("keydown", function (e) { if (e.key === "Enter") submit(); });
  }

  if (document.body) build(); else document.addEventListener("DOMContentLoaded", build);
})();
