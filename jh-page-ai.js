/* jh-page-ai.js — universal per-page AI panel (explain + analyze + grounded outlook).
   Derives the page from the URL, fetches data/page-ai/{page}.json, renders a floating
   panel. Degrades silently if no AI data exists yet for the page. */
(function () {
  "use strict";
  if (window.__jhPageAI) return; window.__jhPageAI = true;
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";

  function pageName() {
    var p = (location.pathname || "").split("/").pop() || "index.html";
    p = p.replace(/\.html?$/i, "");
    return p || "index";
  }
  function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}

  function gj(path) {
    return fetch("https://justhodl.ai/" + path + "?t=" + Date.now()).then(function (r) {
      if (r.ok) return r.json(); throw 0;
    }).catch(function () {
      return fetch(PROXY + "/" + path + "?t=" + Date.now()).then(function (r) { return r.ok ? r.json() : null; }).catch(function(){return null;});
    });
  }

  function css() {
    if (document.getElementById("jhpai-css")) return;
    var s = document.createElement("style"); s.id = "jhpai-css";
    s.textContent =
      "#jhpai-fab{position:fixed;right:16px;bottom:16px;z-index:99998;background:#6d5efc;color:#fff;border:none;"+
      "border-radius:22px;padding:9px 15px;font:600 13px -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"+
      "cursor:pointer;box-shadow:0 4px 18px rgba(0,0,0,.4);display:flex;align-items:center;gap:7px}"+
      "#jhpai-fab .d{width:7px;height:7px;border-radius:50%;background:#7CFFB2;box-shadow:0 0 7px #7CFFB2}"+
      "#jhpai-panel{position:fixed;right:16px;bottom:64px;z-index:99999;width:min(420px,92vw);max-height:74vh;overflow:auto;"+
      "background:#0f141b;color:#e6edf3;border:1px solid #222b36;border-radius:13px;"+
      "box-shadow:0 12px 44px rgba(0,0,0,.6);padding:15px 16px;display:none;font:13px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}"+
      "#jhpai-panel.open{display:block}"+
      "#jhpai-panel h4{margin:0 0 2px;font-size:15px;color:#fff}"+
      "#jhpai-panel .sec{margin-top:11px}"+
      "#jhpai-panel .lbl{font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:#8a95a3;margin-bottom:3px}"+
      "#jhpai-panel .txt{color:#c9d3de}"+
      "#jhpai-panel .out{border:1px solid #222b36;border-radius:9px;padding:9px 10px;margin-top:5px;background:#0b1017}"+
      "#jhpai-panel .badge{display:inline-block;font-size:10px;font-weight:700;border-radius:5px;padding:1px 7px;margin-bottom:5px}"+
      "#jhpai-panel .num{font-size:18px;font-weight:700}"+
      "#jhpai-panel .muted{color:#6b7480;font-size:11px}"+
      "#jhpai-panel .x{position:absolute;top:9px;right:11px;cursor:pointer;color:#6b7480;font-size:19px;background:none;border:none}"+
      "#jhpai-panel .row{display:flex;gap:14px;flex-wrap:wrap;margin-top:4px}"+
      "#jhpai-panel .gen{display:block;width:100%;margin:4px 0 8px;padding:8px 10px;border-radius:9px;border:1px solid #3b2f63;"+
      "background:linear-gradient(135deg,#2a1f4d,#1b1533);color:#cfc3ff;font-weight:700;font-size:12.5px;cursor:pointer}"+
      "#jhpai-panel .gen:hover{border-color:#6d5ae0}#jhpai-panel .gen:disabled{opacity:.6;cursor:wait}";
    document.head.appendChild(s);
  }

  var LIVECFG = null;
  function liveUrl() {
    if (LIVECFG !== null) return Promise.resolve(LIVECFG);
    return gj("data/page-ai-live.json").then(function (c) { LIVECFG = (c && c.url) || ""; return LIVECFG; });
  }
  function genFresh() {
    var b = document.getElementById("jhpai-gen");
    if (b) { b.disabled = true; b.textContent = "Generating\u2026"; }
    liveUrl().then(function (u) {
      if (!u) throw 0;
      return fetch(u + "?mode=live&page=" + encodeURIComponent(pageName()) + "&t=" + Date.now())
        .then(function (r) { if (!r.ok) throw 0; return r.json(); });
    }).then(function (d) {
      data = d; render(d);
    }).catch(function () {
      var b2 = document.getElementById("jhpai-gen");
      if (b2) { b2.disabled = false; b2.textContent = "\u2728 Generate AI analysis"; }
      var p = document.getElementById("jhpai-panel");
      if (p) { var n = document.createElement("div"); n.className = "muted"; n.style.marginTop = "8px";
        n.textContent = "Live generation is unavailable right now \u2014 showing the cached brief."; p.appendChild(n); }
    });
  }

  function outlookHtml(o) {
    if (!o) return "";
    var st = o.alpha_status || "UNGRADED";
    var color = st === "ALPHA_PROVEN" ? "#7CFFB2" : st === "ALPHA_NEGATIVE" ? "#ff8a8a" : "#fbbf24";
    var bg = st === "ALPHA_PROVEN" ? "rgba(52,211,153,.13)" : st === "ALPHA_NEGATIVE" ? "rgba(248,113,113,.13)" : "rgba(251,191,36,.12)";
    var h = '<div class="out"><span class="badge" style="color:' + color + ';background:' + bg + '">' + esc(st.replace("ALPHA_", "")) + '</span>';
    h += '<div class="muted">' + esc(o.confidence || "") + '</div>';
    if (o.mean_excess_vs_spy_pct != null) {
      var v = o.mean_excess_vs_spy_pct;
      h += '<div class="row">';
      h += '<div><div class="lbl">Hist. mean vs SPY</div><div class="num" style="color:' + (v >= 0 ? "#7CFFB2" : "#ff8a8a") + '">' + (v >= 0 ? "+" : "") + v + '%</div></div>';
      if (o.hit_rate_pct != null) h += '<div><div class="lbl">Hit rate</div><div class="num">' + o.hit_rate_pct + '%</div></div>';
      if (o.n_graded) h += '<div><div class="lbl">Graded picks</div><div class="num">' + o.n_graded + '</div></div>';
      h += '</div>';
      h += '<div class="muted" style="margin-top:6px">Forward excess return vs SPY over the grading window — the honest, track-record-based outlook (not a forecast). ' + (o.grade ? "Engine grade " + esc(o.grade) + "." : "") + '</div>';
    } else if (o.note) {
      h += '<div class="muted" style="margin-top:4px">' + esc(o.note) + '</div>';
    }
    return h + '</div>';
  }

  function render(d) {
    var p = document.getElementById("jhpai-panel");
    if (!d) { p.innerHTML = '<button class="x" data-x>&times;</button><div class="muted">No AI brief for this page yet.</div><button id="jhpai-gen" class="gen">\u2728 Generate AI analysis</button><div class="muted" style="margin-top:6px">Click to analyze this page\u2019s live data now.</div>'; return; }
    var h = '<button class="x" data-x>&times;</button>';
    h += '<button id="jhpai-gen" class="gen">\u2728 Generate AI analysis</button>';
    h += '<div class="muted" style="margin-bottom:4px">' + (d.generated_on_click ? "fresh \u00b7 " : "cached \u00b7 ")
       + esc(String(d.generated_at || "").slice(0, 16).replace("T", " ")) + " UTC</div>";
    h += '<h4>' + esc(d.title || d.page) + '</h4>';
    if (d.what_it_is) h += '<div class="sec"><div class="lbl">What this is</div><div class="txt">' + esc(d.what_it_is) + '</div></div>';
    if (d.what_it_does) h += '<div class="sec"><div class="lbl">How it works</div><div class="txt">' + esc(d.what_it_does) + '</div></div>';
    if (d.analysis) h += '<div class="sec"><div class="lbl">AI read of the live data</div><div class="txt">' + esc(d.analysis) + '</div></div>';
    if (d.pick_read) h += '<div class="sec"><div class="lbl">The picks</div><div class="txt">' + esc(d.pick_read) + '</div></div>';
    if (d.outlook) h += '<div class="sec"><div class="lbl">Grounded outlook — how these picks have actually performed</div>' + outlookHtml(d.outlook) + '</div>';
    if (d.generated_at) h += '<div class="muted" style="margin-top:11px">' + esc(String(d.generated_at).slice(0, 16).replace("T", " ")) + 'Z</div>';
    p.innerHTML = h;
  }

  function init() {
    css();
    var fab = document.createElement("button"); fab.id = "jhpai-fab";
    fab.innerHTML = '<span class="d"></span><span>AI: explain &amp; outlook</span>';
    var panel = document.createElement("div"); panel.id = "jhpai-panel";
    document.body.appendChild(fab); document.body.appendChild(panel);
    var loaded = false, data = null;
    function open() {
      panel.classList.add("open");
      if (!loaded) { panel.innerHTML = '<div class="muted">loading AI brief…</div>'; loaded = true;
        gj("data/page-ai/" + pageName() + ".json").then(function (d) { data = d; render(d); }); }
    }
    fab.addEventListener("click", function () { panel.classList.contains("open") ? panel.classList.remove("open") : open(); });
    panel.addEventListener("click", function (e) { if (e.target.matches("[data-x]")) panel.classList.remove("open"); if (e.target.id === "jhpai-gen") genFresh(); });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init); else init();
})();
