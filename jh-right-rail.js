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
  var hasRes = D.research && typeof D.research === 'object';
  if (!hasProv && !hasRel && !hasFeeds && !hasInterp && !hasRes) return;

  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function sourceLink(f) {
    var safe = typeof f.href === 'string' && /^\/(?:data|cot)\/[A-Za-z0-9_./-]+\.json$/.test(f.href) && !f.href.includes('..');
    return safe ? '<a href="' + esc(f.href) + '">' + esc(f.label) + '</a>' : esc(f.label);
  }
  function stamp(s) {
    return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(s) && /(?:Z|[+-]\d{2}:\d{2})$/.test(s) && Number.isFinite(Date.parse(s)) ? s : null;
  }

  var sections = "";
  if (hasRes) {
    // Legacy payloads may contain unqualified, cached scores and divergences.
    sections += '<div class="jhr-sec"><div class="jhr-h">RELATED RESEARCH</div>' +
      '<p class="jhr-p"><a href="/panels.html">Open panels research</a>. Check its source dates and qualification on that desk.</p></div>';
  }
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
    sections += '<div class="jhr-sec"><div class="jhr-h">SOURCE REFERENCES</div>' +
      '<p class="jhr-p">Page snapshot' + (stamp(D.snapshot_at) ? ' at ' + esc(stamp(D.snapshot_at)) : ' · capture time unavailable') +
      '. File modification times do not establish observation freshness. Open each source for its current data and dates.</p><ul class="jhr-prov">' +
      D.feeds.map(function (f) {
        return '<li><span class="jhr-key">' + sourceLink(f) + '</span>' +
          '<span class="jhr-age">' + (stamp(f.modified_at) ? 'File modified ' + esc(stamp(f.modified_at)) : 'Live status unverified') + "</span></li>";
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
