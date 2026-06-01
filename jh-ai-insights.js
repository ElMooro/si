/* ───────────────────────────────────────────────────────────────────
 * jh-ai-insights.js  v1.0
 *
 * Global AI insights widget for justhodl.ai.
 * Drop this <script> on any page and a floating violet pill appears
 * in the bottom-right. Click it to expand a panel showing the
 * site-wide cross-engine AI synthesis, with the entry most relevant
 * to the current page surfaced first.
 *
 *   <script src="/jh-ai-insights.js" defer></script>
 *
 * Reads: https://justhodl-dashboard-live.s3.amazonaws.com/data/ai-website-synthesis.json
 * Cache: 15 minutes (matches the hourly refresh cadence)
 * Page detection: maps current pathname to per_page_focus keys
 * ──────────────────────────────────────────────────────────────── */
(function () {
  if (window.JHInsights) return;

  var DATA_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/data/ai-website-synthesis.json";
  var CACHE_KEY = "jh_ai_insights_cache";
  var CACHE_MAX_AGE_MS = 15 * 60 * 1000;  // 15 min

  // ─── Map current page → per_page_focus key ───
  var PAGE_MAP = {
    "auction-crisis":  "auction-crisis",
    "macro-frontrun":  "macro-frontrun",
    "frontrun":        "macro-frontrun",
    "crisis":          "crisis",
    "bonds":           "bonds",
    "repo":            "repo",
    "regime":          "regime",
    "correlation":     "correlation",
    "correlations":    "correlation",
    "sentiment":       "sentiment",
    "vol":             "volatility",
    "volatility":      "volatility",
  };

  function currentPageKey() {
    var p = (location.pathname || "").toLowerCase().replace(/\.html$/, "").replace(/^\//, "");
    return PAGE_MAP[p] || null;
  }

  // ─── Styles ───
  var CSS = "\
.jhi-fab{position:fixed;bottom:18px;right:18px;z-index:99999;\
  display:flex;align-items:center;gap:8px;\
  background:linear-gradient(135deg,rgba(167,139,250,.95),rgba(0,212,255,.92));\
  color:#07090f;font-family:'IBM Plex Mono',ui-monospace,monospace;\
  font-size:12px;font-weight:700;letter-spacing:1px;\
  padding:11px 16px;border-radius:30px;cursor:pointer;\
  box-shadow:0 4px 28px rgba(0,212,255,.32),0 2px 8px rgba(0,0,0,.6);\
  border:none;transition:all .18s ease;text-transform:uppercase;}\
.jhi-fab:hover{transform:translateY(-2px);box-shadow:0 6px 36px rgba(167,139,250,.42),0 2px 12px rgba(0,0,0,.7);}\
.jhi-fab .dot{width:7px;height:7px;border-radius:50%;background:#07090f;animation:jhi-pulse 2.2s ease-in-out infinite;}\
@keyframes jhi-pulse{0%,100%{opacity:1}50%{opacity:.35}}\
.jhi-fab.RISK_OFF,.jhi-fab.DEFENSIVE{background:linear-gradient(135deg,rgba(255,177,61,.95),rgba(255,92,122,.92));}\
.jhi-fab.EXTREME{background:linear-gradient(135deg,rgba(255,92,122,.98),rgba(255,80,80,.95));color:#fff;animation:jhi-flash 1.4s ease-in-out infinite;}\
@keyframes jhi-flash{0%,100%{box-shadow:0 4px 28px rgba(255,92,122,.4)}50%{box-shadow:0 4px 50px rgba(255,92,122,.85)}}\
.jhi-fab.RISK_ON{background:linear-gradient(135deg,rgba(56,255,168,.95),rgba(0,212,255,.92));}\
\
.jhi-panel{position:fixed;bottom:74px;right:18px;z-index:99998;\
  width:min(440px,calc(100vw - 40px));max-height:78vh;overflow-y:auto;\
  background:#0e1320;color:#e6ecf5;\
  font-family:'IBM Plex Sans',ui-sans-serif,system-ui,sans-serif;font-size:13px;line-height:1.55;\
  border:1px solid rgba(120,145,180,.4);border-radius:12px;\
  box-shadow:0 12px 60px rgba(0,0,0,.7),0 0 80px rgba(167,139,250,.12);\
  padding:18px 20px;\
  transform:translateY(20px);opacity:0;pointer-events:none;\
  transition:all .22s cubic-bezier(.2,.8,.2,1);}\
.jhi-panel.open{transform:translateY(0);opacity:1;pointer-events:auto;}\
\
.jhi-panel .hdr{display:flex;align-items:center;gap:10px;margin-bottom:14px;padding-bottom:10px;\
  border-bottom:1px solid rgba(120,145,180,.18);}\
.jhi-panel .hdr .ico{width:9px;height:9px;border-radius:50%;background:#a78bfa;box-shadow:0 0 12px #a78bfa;}\
.jhi-panel .hdr .ttl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:11px;letter-spacing:1.6px;\
  color:#a78bfa;font-weight:600;text-transform:uppercase;}\
.jhi-panel .hdr .age{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:10px;\
  color:#6b7a92;margin-left:auto;}\
.jhi-panel .hdr .close{background:transparent;border:none;color:#6b7a92;cursor:pointer;font-size:18px;\
  padding:0;margin-left:6px;line-height:1;}\
.jhi-panel .hdr .close:hover{color:#e6ecf5;}\
\
.jhi-posture{display:flex;align-items:center;gap:8px;margin-bottom:14px;}\
.jhi-posture .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#6b7a92;text-transform:uppercase;}\
.jhi-posture .val{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:13px;font-weight:700;\
  padding:3px 10px;border-radius:4px;letter-spacing:1px;}\
.jhi-posture .val.RISK_ON{background:rgba(56,255,168,.16);color:#38ffa8;border:1px solid rgba(56,255,168,.32);}\
.jhi-posture .val.NEUTRAL{background:rgba(0,212,255,.12);color:#00d4ff;border:1px solid rgba(0,212,255,.28);}\
.jhi-posture .val.RISK_OFF{background:rgba(255,177,61,.16);color:#ffb13d;border:1px solid rgba(255,177,61,.32);}\
.jhi-posture .val.DEFENSIVE{background:rgba(255,177,61,.2);color:#ffb13d;border:1px solid rgba(255,177,61,.45);}\
.jhi-posture .val.EXTREME{background:rgba(255,92,122,.2);color:#ff5c7a;border:1px solid rgba(255,92,122,.5);}\
\
.jhi-headline{font-size:14px;color:#e6ecf5;line-height:1.5;margin-bottom:12px;font-weight:500;}\
.jhi-headline strong{color:#a78bfa;}\
\
.jhi-section{margin-bottom:14px;}\
.jhi-section .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#00d4ff;font-weight:600;text-transform:uppercase;margin-bottom:6px;}\
.jhi-section .body{font-size:12.5px;color:#a8b3c7;line-height:1.6;}\
.jhi-section .body strong{color:#e6ecf5;font-weight:500;}\
.jhi-section ul{margin:0;padding-left:18px;}\
.jhi-section li{margin-bottom:3px;font-size:12px;color:#a8b3c7;}\
\
.jhi-call{background:linear-gradient(90deg,rgba(0,212,255,.08),rgba(167,139,250,.04),transparent);\
  border-left:3px solid #00d4ff;border-radius:0 6px 6px 0;\
  padding:11px 14px;margin-bottom:14px;font-size:13px;color:#e6ecf5;line-height:1.55;font-weight:500;}\
.jhi-call .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#00d4ff;font-weight:700;text-transform:uppercase;display:block;margin-bottom:5px;}\
\
.jhi-page-focus{background:rgba(167,139,250,.06);border-left:3px solid #a78bfa;border-radius:0 6px 6px 0;\
  padding:11px 14px;margin-bottom:14px;font-size:13px;color:#e6ecf5;line-height:1.55;}\
.jhi-page-focus .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#a78bfa;font-weight:700;text-transform:uppercase;display:block;margin-bottom:5px;}\
\
.jhi-loading,.jhi-err{padding:18px;text-align:center;color:#6b7a92;font-size:12px;\
  font-family:'IBM Plex Mono',ui-monospace,monospace;}\
.jhi-err{color:#ff5c7a;}\
\
.jhi-footer{margin-top:14px;padding-top:10px;border-top:1px solid rgba(120,145,180,.18);\
  display:flex;align-items:center;justify-content:space-between;\
  font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;\
  color:#6b7a92;letter-spacing:.5px;}\
.jhi-footer a{color:#a78bfa;text-decoration:none;}\
";

  function injectCSS() {
    if (document.getElementById("jhi-css")) return;
    var s = document.createElement("style");
    s.id = "jhi-css";
    s.textContent = CSS;
    document.head.appendChild(s);
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function highlight(s) {
    // Bold key terms: $XB, NNbp, NN%, dates, regime names
    return esc(s)
      .replace(/(\$[\d.]+(?:B|M|K|T)?)/g, '<strong>$1</strong>')
      .replace(/(\b\d+(?:\.\d+)?\s?bp)/g, '<strong>$1</strong>')
      .replace(/(\b\d+(?:\.\d+)?%)/g, '<strong>$1</strong>')
      .replace(/(\b\d{4}-\d{2}-\d{2}\b)/g, '<strong>$1</strong>')
      .replace(/\b(RISK_ON|RISK_OFF|NEUTRAL|DEFENSIVE|EXTREME|CALM|WATCH|ELEVATED|ACUTE_STRESS|GFC|Lehman|COVID|2008|2020|2024)\b/g, '<strong>$1</strong>');
  }

  function ageStr(iso) {
    if (!iso) return "";
    var m = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
    if (m < 1) return "now";
    if (m < 60) return m + "m ago";
    if (m < 1440) return Math.round(m / 60) + "h ago";
    return Math.round(m / 1440) + "d ago";
  }

  function getCached() {
    try {
      var raw = sessionStorage.getItem(CACHE_KEY);
      if (!raw) return null;
      var d = JSON.parse(raw);
      if (Date.now() - d._cached_at > CACHE_MAX_AGE_MS) return null;
      return d;
    } catch (e) { return null; }
  }

  function setCached(data) {
    try {
      data._cached_at = Date.now();
      sessionStorage.setItem(CACHE_KEY, JSON.stringify(data));
    } catch (e) { /* over quota — ignore */ }
  }

  function fetchSynthesis() {
    var cached = getCached();
    if (cached) return Promise.resolve(cached);
    return fetch(DATA_URL + "?t=" + Date.now()).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (d) {
      setCached(d);
      return d;
    });
  }

  function buildPanel(data) {
    var s = (data && data.synthesis) || {};
    var posture = s.global_posture || "NEUTRAL";
    var pageKey = currentPageKey();
    var pageFocus = (s.per_page_focus || {})[pageKey];

    var html = '<div class="hdr">' +
      '<div class="ico"></div>' +
      '<div class="ttl">AI Cross-Engine Synthesis</div>' +
      '<div class="age">' + ageStr(data.generated_at) + '</div>' +
      '<button class="close" data-jhi-close>&times;</button>' +
      '</div>';

    html += '<div class="jhi-posture">' +
      '<span class="lbl">Posture</span>' +
      '<span class="val ' + esc(posture) + '">' + esc(posture).replace(/_/g, ' ') + '</span>' +
      '</div>';

    if (s.headline) {
      html += '<div class="jhi-headline">' + highlight(s.headline) + '</div>';
    }

    if (pageFocus) {
      html += '<div class="jhi-page-focus"><span class="lbl">On this page</span>' + highlight(pageFocus) + '</div>';
    }

    if (s.decisive_call) {
      html += '<div class="jhi-call"><span class="lbl">Decisive Call</span>' + highlight(s.decisive_call) + '</div>';
    }

    if (s.thesis) {
      html += '<div class="jhi-section"><div class="lbl">Thesis</div><div class="body">' + highlight(s.thesis) + '</div></div>';
    }

    if (s.key_drivers && s.key_drivers.length) {
      html += '<div class="jhi-section"><div class="lbl">Key Drivers</div><ul>';
      s.key_drivers.slice(0, 5).forEach(function (d) {
        html += '<li>' + highlight(d) + '</li>';
      });
      html += '</ul></div>';
    }

    if (s.key_dissonances && s.key_dissonances.length) {
      html += '<div class="jhi-section"><div class="lbl">Dissonances</div><ul>';
      s.key_dissonances.slice(0, 4).forEach(function (d) {
        html += '<li>' + highlight(d) + '</li>';
      });
      html += '</ul></div>';
    }

    if (s.watch_list && s.watch_list.length) {
      html += '<div class="jhi-section"><div class="lbl">Watch Next 24h</div><ul>';
      s.watch_list.slice(0, 5).forEach(function (w) {
        html += '<li>' + highlight(w) + '</li>';
      });
      html += '</ul></div>';
    }

    html += '<div class="jhi-footer">' +
      '<span>model: claude-haiku-4-5</span>' +
      '<a href="/" rel="noopener">justhodl.ai</a>' +
      '</div>';

    return html;
  }

  function buildFab(posture, headline) {
    var posClass = posture || "NEUTRAL";
    var label;
    if (posture === "EXTREME") label = "🚨 EXTREME";
    else if (posture === "DEFENSIVE") label = "⚠ DEFENSIVE";
    else if (posture === "RISK_OFF") label = "RISK OFF";
    else if (posture === "RISK_ON") label = "RISK ON";
    else label = "AI READ";
    return '<span class="dot"></span><span>' + esc(label) + '</span>';
  }

  function mount() {
    injectCSS();

    // FAB
    var fab = document.createElement("button");
    fab.className = "jhi-fab";
    fab.setAttribute("aria-label", "AI Cross-Engine Insights");
    fab.innerHTML = '<span class="dot"></span><span>AI READ</span>';
    document.body.appendChild(fab);

    // Panel (hidden by default)
    var panel = document.createElement("div");
    panel.className = "jhi-panel";
    panel.innerHTML = '<div class="jhi-loading">loading cross-engine synthesis…</div>';
    document.body.appendChild(panel);

    function open() {
      panel.classList.add("open");
      // Trap escape key
      document.addEventListener("keydown", onEsc);
    }
    function close() {
      panel.classList.remove("open");
      document.removeEventListener("keydown", onEsc);
    }
    function onEsc(e) { if (e.key === "Escape") close(); }

    fab.addEventListener("click", function () {
      if (panel.classList.contains("open")) close();
      else open();
    });
    panel.addEventListener("click", function (e) {
      if (e.target.matches("[data-jhi-close]")) close();
    });

    // Fetch + render
    fetchSynthesis().then(function (data) {
      if (data && data.status === "error") {
        panel.innerHTML = '<div class="jhi-err">AI synthesis unavailable: ' + esc(data.error || "?") + '</div>';
        return;
      }
      var posture = (data.synthesis || {}).global_posture || "NEUTRAL";
      fab.className = "jhi-fab " + posture;
      fab.innerHTML = buildFab(posture);
      panel.innerHTML = buildPanel(data);
    }).catch(function (e) {
      panel.innerHTML = '<div class="jhi-err">Fetch error: ' + esc(e.message) + '</div>';
    });

    window.JHInsights = { open: open, close: close, refresh: function () {
      sessionStorage.removeItem(CACHE_KEY);
      panel.innerHTML = '<div class="jhi-loading">refreshing…</div>';
      fetchSynthesis().then(function (data) {
        panel.innerHTML = buildPanel(data);
      });
    }};
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount);
  } else {
    mount();
  }
})();
