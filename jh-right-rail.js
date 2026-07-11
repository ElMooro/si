/* jh-right-rail.js — desk-detail insight rail (Design audit §8-C).
   Renders from window.__jhRail, which the build bakes per-page from REAL
   sources only: manifest title, feed references + live freshness, the
   audit's own IA taxonomy, and a small curated feeds-into map. No section
   is ever fabricated — empty data means the section is omitted, never a
   placeholder. Fixed-dock overlay (v1): additive, touches no page layout. */
(function () {
  "use strict";
  var D = window.__jhRail;
  if (!D || typeof D !== "object") return;
  var hasProv = D.feeds && D.feeds.length;
  var hasRel = D.related && D.related.length;
  var hasFeeds = D.feedsInto && D.feedsInto.length;
  var hasInterp = D.interpret && D.interpret.trim();
  if (!hasProv && !hasRel && !hasFeeds && !hasInterp) return;

  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function ageLabel(h) {
    if (h == null) return "";
    if (h < 1) return "fresh";
    if (h < 24) return Math.round(h) + "h ago";
    return Math.round(h / 24) + "d ago";
  }
  function ageClass(h) {
    if (h == null) return "";
    return h < 24 ? "jhr-ok" : h < 48 ? "jhr-mid" : "jhr-stale";
  }

  var sections = "";
  if (hasInterp) {
    sections += '<div class="jhr-sec"><div class="jhr-h">' + esc(D.title || "ABOUT") +
      '</div><p class="jhr-p">' + esc(D.interpret) + "</p></div>";
  }
  if (hasFeeds) {
    sections += '<div class="jhr-sec"><div class="jhr-h">FEEDS INTO</div><div class="jhr-chips">' +
      D.feedsInto.map(function (f) {
        return '<a class="jhr-chip" href="' + esc(f.href) + '">' + esc(f.label) + "</a>";
      }).join("") + "</div></div>";
  }
  if (hasRel) {
    sections += '<div class="jhr-sec"><div class="jhr-h">RELATED DESKS</div><ul class="jhr-list">' +
      D.related.map(function (p) {
        return '<li><a href="' + esc(p.href) + '">' + esc(p.title) + "</a></li>";
      }).join("") + "</ul></div>";
  }
  if (hasProv) {
    sections += '<div class="jhr-sec"><div class="jhr-h">DATA PROVENANCE</div><ul class="jhr-prov">' +
      D.feeds.map(function (f) {
        return '<li><span class="jhr-key">' + esc(f.label) + '</span>' +
          '<span class="jhr-age ' + ageClass(f.h) + '">' + esc(ageLabel(f.h)) + "</span></li>";
      }).join("") + "</ul></div>";
  }

  var wrap = document.createElement("div");
  wrap.className = "jhr-wrap";
  wrap.innerHTML =
    '<button class="jhr-tab" aria-label="Show desk info" aria-expanded="false">i</button>' +
    '<aside class="jhr-panel" aria-hidden="true">' +
    '<button class="jhr-close" aria-label="Close">×</button>' + sections + "</aside>";
  document.body.appendChild(wrap);

  var panel = wrap.querySelector(".jhr-panel"), tab = wrap.querySelector(".jhr-tab"),
      close = wrap.querySelector(".jhr-close");
  function open_() {
    wrap.classList.add("jhr-open");
    panel.setAttribute("aria-hidden", "false"); tab.setAttribute("aria-expanded", "true");
  }
  function shut() {
    wrap.classList.remove("jhr-open");
    panel.setAttribute("aria-hidden", "true"); tab.setAttribute("aria-expanded", "false");
  }
  function remember(v) { try { localStorage.setItem("jh_rail_open", v ? "1" : "0"); } catch (e) {} }
  tab.addEventListener("click", function () {
    var willOpen = !wrap.classList.contains("jhr-open");
    willOpen ? open_() : shut(); remember(willOpen);
  });
  close.addEventListener("click", function () { shut(); remember(false); });
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") { shut(); remember(false); }
  });
  // Hidden unless summoned (Khalid 2026-07-11) -- opens only on the
  // "i" tab, remembers the choice, same pattern as the nav drawer.
  var saved = null;
  try { saved = localStorage.getItem("jh_rail_open"); } catch (e) {}
  if (saved === "1") open_(); else shut();
})();
