/* TradingView watchlist. Does not delete lists. Chart Pro custom lists are merged
   before the engine reads storage; this file paints the full list in saved order. */
(function () {
  if (window.__jhTvWatch) return;
  window.__jhTvWatch = 1;
  var UP = "#089981", DN = "#f23645", FLAG = { red: "#f23645", orange: "#ff6d00", yellow: "#fdd835", green: "#089981", blue: "#2962ff", purple: "#ab47bc" };
  var catalog = {}, quotes = {}, qSet = {}, inflight = 0, wait = [], lock = 0;
  var css = document.createElement("style");
  css.id = "jh-tvwatch-css";
  css.textContent = [
    "#watch{font-family:-apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,Ubuntu,sans-serif}",
    "#letters,#watch .filt,#paste,#nlists,.listbtn .n{display:none!important}",
    "#w-list{background:#131722}",
    "#cols{display:grid;grid-template-columns:3px 8px 22px minmax(0,1fr) 72px 56px 62px;gap:0 6px;padding:4px 10px 3px;color:#787b86;font-size:11px;letter-spacing:.02em}",
    "#cols span{cursor:pointer}",
    "#wlist .wrow{display:grid;grid-template-columns:3px 8px 22px minmax(0,1fr) 72px 56px 62px;gap:0 6px;align-items:center;height:32px;padding:0 10px 0 0;border:0;border-radius:0;background:transparent;color:#d1d4dc;font-size:13px;position:relative;width:100%;text-align:left}",
    "#wlist .wrow:hover{background:#2a2e39}",
    "#wlist .wrow.on{background:#2a2e39}",
    "#wlist .wrow .wacc{width:3px;height:32px;background:transparent!important;position:absolute;left:0;top:0}",
    "#wlist .wrow.on .wacc{background:#2962ff!important}",
    "#wlist .wsym{font-weight:400;color:#d1d4dc;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:0}",
    "#wlist .px,#wlist .chg{text-align:right;font-variant-numeric:tabular-nums;font-size:13px}",
    "#wlist .up{color:#089981}#wlist .dn{color:#f23645}",
    "#wlist .tvlogo{width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:10px;font-weight:600;color:#fff;background:#363a45}",
    "#wlist .tvflag{width:8px;height:8px;border-radius:50%;background:transparent;justify-self:center}",
    "#wlist .wsec{display:flex;align-items:center;height:28px;padding:0 12px;color:#787b86;font-size:11px;letter-spacing:.08em;text-transform:uppercase;background:#1e222d}",
    "#wlist .wsec span{margin-left:auto}",
    "#wlist .flash-up{animation:jhup .45s}#wlist .flash-dn{animation:jhdn .45s}",
    "@keyframes jhup{from{background:rgba(8,153,129,.35)}to{background:transparent}}",
    "@keyframes jhdn{from{background:rgba(242,54,69,.35)}to{background:transparent}}",
    ".listbtn .tvflag{width:10px;height:10px;border-radius:2px;flex:none;background:#787b86}",
    ".addsym{border:0!important;text-align:left!important;color:#2962ff!important;font-size:13px!important;padding:8px 12px!important}",
    "#watch .wtitle{height:0;padding:0;border:0;overflow:visible}",
    "#watch .wtitle b{display:none}",
    "#watch .wtitle .wops{position:absolute;right:6px;top:4px;z-index:5}",
    "#watch .whead{height:38px;padding:4px 78px 4px 8px}",
    "#listres .ld-row{display:flex;align-items:center;gap:8px}",
    "#listres .ld-flag{width:10px;height:10px;border-radius:2px;flex:none}"
  ].join("");
  document.documentElement.appendChild(css);

  function bare(s) { s = String(s || ""); return s.indexOf(":") >= 0 ? s.split(":").pop() : s; }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      if (c === "\"") return "&" + "quot;";
      return "&" + "#39;";
    });
  }
  function hue(s) { var h = 0, i; s = bare(s); for (i = 0; i < s.length; i++) h = (h * 33 + s.charCodeAt(i)) >>> 0; return h % 360; }
  function num(n) {
    if (typeof n !== "number" || !isFinite(n)) return "—";
    var a = Math.abs(n), d = a >= 100 ? 2 : a >= 1 ? 2 : a >= 0.01 ? 4 : 6;
    return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: d });
  }
  function activeSym() {
    var el = document.getElementById("symin");
    return el ? String(el.value || "").trim() : "";
  }
  function findList(id) {
    if (catalog[id]) return catalog[id];
    try {
      var custom = JSON.parse(localStorage.getItem("jh-chart-custom-lists") || "[]");
      var fav = JSON.parse(localStorage.getItem("jh-chart-favs") || "[]");
      if (id === "favorites" && fav.length) return { id: "favorites", name: "Favorites", symbols: fav, color: "#fdd835" };
      for (var i = 0; i < custom.length; i++) if (String(custom[i].id) === String(id)) return custom[i];
    } catch (e) {}
    return null;
  }
  function openSym(s) {
    var q = document.getElementById("q");
    if (!q) { if (window.jhOpenSymbol) window.jhOpenSymbol(s); return; }
    q.value = s;
    q.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
  }
  function rowHtml(s, on) {
    var b = bare(s), raw = quotes[s] || quotes[b], q = raw && raw.last != null ? raw : null, up = !q || q.chg >= 0, cls = up ? "up" : "dn";
    var flag = raw && raw.flag || "";
    var px = q ? num(q.last) : "—", chg = q ? ((q.chgv >= 0 ? "+" : "") + num(q.chgv)) : "—", pct = q ? ((q.chg >= 0 ? "+" : "") + (q.chg * 100).toFixed(2) + "%") : "—";
    return "<button type=button class='wrow tv" + (on ? " on" : "") + "' data-s='" + esc(s) + "'>" +
      "<i class=wacc></i><i class=tvflag" + (flag ? " style='background:" + esc(flag) + "'" : "") + "></i>" +
      "<i class=tvlogo style='background:hsl(" + hue(s) + ",42%,42%)'>" + esc((b || "?").slice(0, 1)) + "</i>" +
      "<span class=wsym>" + esc(b) + "</span>" +
      "<span class='px " + cls + "'>" + px + "</span><span class='chg " + cls + "'>" + chg + "</span><span class='chg " + cls + "'>" + pct + "</span></button>";
  }
  function remember(s, prev) {
    var box = document.getElementById("wlist"); if (!box) return;
    var btn = box.querySelector("[data-s='" + (window.CSS && CSS.escape ? CSS.escape(s) : s) + "']");
    if (!btn) return;
    var q = quotes[s]; if (!q) return;
    var spans = btn.querySelectorAll("span");
    if (spans.length < 4) return;
    var up = q.chg >= 0, cls = up ? "up" : "dn";
    spans[2].textContent = (q.chgv >= 0 ? "+" : "") + num(q.chgv);
    spans[3].textContent = (q.chg >= 0 ? "+" : "") + (q.chg * 100).toFixed(2) + "%";
    spans[2].className = "chg " + cls; spans[3].className = "chg " + cls;
    spans[1].textContent = num(q.last);
    spans[1].className = "px " + cls + (prev != null && prev !== q.last ? (q.last > prev ? " flash-up" : " flash-dn") : "");
  }
  function tickerOf(s) {
    var t = String(s || "").toUpperCase();
    if (/^[A-Z0-9.\-]{1,12}$/.test(t)) return t;
    t = bare(t).toUpperCase();
    return /^[A-Z0-9.\-]{1,12}$/.test(t) ? t : "";
  }
  function pump() {
    if (inflight >= 3 || !wait.length) return;
    var s = wait.shift(); inflight++;
    var t = tickerOf(s);
    if (!t) { inflight--; pump(); return; }
    fetch("https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=" + encodeURIComponent(t) + "&span=day&mult=1&days=6")
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        var bars = j && j.bars || [];
        if (bars.length >= 2) {
          var a = bars[bars.length - 2], b = bars[bars.length - 1];
          var prev = quotes[s] && quotes[s].last;
          var last = +b.close, p = +a.close;
          if (isFinite(last) && isFinite(p) && p) quotes[s] = { last: last, chg: (last - p) / p, chgv: last - p, flag: (quotes[s] && quotes[s].flag) || "" };
          remember(s, prev);
        }
      })
      .catch(function () {})
      .then(function () { inflight--; pump(); });
  }
  function want(s) {
    var hit = quotes[s];
    if ((hit && hit.last != null) || qSet[s] || !tickerOf(s)) return;
    qSet[s] = 1; wait.push(s); pump();
  }
  function paint() {
    if (lock) return;
    var sel = document.getElementById("list"), box = document.getElementById("wlist");
    if (!sel || !box) return;
    var qf = document.getElementById("q");
    if (qf && qf.value.trim()) return;
    if (document.querySelector("#letters button.on")) return;
    var L = findList(sel.value);
    if (!L || !L.symbols || !L.symbols.length) return;
    var cur = activeSym();
    var sig = sel.value + "|" + L.symbols.length + "|" + cur;
    if (box.dataset.sig === sig && box.querySelector(".wrow.tv")) { see(); return; }
    lock = 1;
    var html = [], i, s, sec = 0;
    for (i = 0; i < L.symbols.length; i++) {
      s = L.symbols[i];
      if (String(s).indexOf("###") === 0) {
        html.push("<div class=wsec>" + esc(String(s).replace(/^#+/, "")) + "</div>");
        sec++;
        continue;
      }
      html.push(rowHtml(s, bare(s) === bare(cur) || s === cur));
    }
    box.dataset.sig = sig;
    box.innerHTML = html.join("");
    box.querySelectorAll(".wrow").forEach(function (b) {
      b.onclick = function () { openSym(b.getAttribute("data-s")); };
      b.oncontextmenu = function (e) {
        e.preventDefault();
        var cols = ["#2962ff", "#089981", "#f23645", "#ff6d00", "#ab47bc", ""];
        var s = b.getAttribute("data-s");
        var curf = (quotes[s] && quotes[s].flag) || "";
        var next = cols[(cols.indexOf(curf) + 1) % cols.length];
        quotes[s] = quotes[s] || { last: null, chg: 0, chgv: 0, flag: "" };
        quotes[s].flag = next;
        try {
          var f = JSON.parse(localStorage.getItem("jh-chart-flags") || "{}");
          if (next) f[s] = next; else delete f[s];
          localStorage.setItem("jh-chart-flags", JSON.stringify(f));
        } catch (err) {}
        var dot = b.querySelector(".tvflag");
        if (dot) dot.style.background = next || "transparent";
      };
    });
    var btn = document.getElementById("listbtn");
    if (btn && !btn.querySelector(".tvflag")) {
      btn.insertAdjacentHTML("afterbegin", "<i class=tvflag></i>");
    }
    if (btn) {
      var dot = btn.querySelector(".tvflag");
      if (dot) dot.style.background = L.color && String(L.color).indexOf("#") === 0 ? L.color : "#787b86";
    }
    var cols = document.getElementById("cols");
    if (cols && !cols.dataset.tv) { cols.dataset.tv = "1"; cols.insertAdjacentHTML("afterbegin", "<span></span><span></span>"); }
    lock = 0;
    see();
  }
  function see() {
    var box = document.getElementById("wlist"); if (!box || !window.IntersectionObserver) return;
    if (see.io) see.io.disconnect();
    see.io = new IntersectionObserver(function (ents) {
      ents.forEach(function (en) { if (en.isIntersecting) want(en.target.getAttribute("data-s")); });
    }, { root: box, rootMargin: "80px" });
    box.querySelectorAll(".wrow").forEach(function (n) { see.io.observe(n); });
  }
  function flagsFromStore() {
    try {
      var f = JSON.parse(localStorage.getItem("jh-chart-flags") || "{}");
      Object.keys(f).forEach(function (k) { quotes[k] = quotes[k] || { last: null, chg: 0, chgv: 0, flag: f[k] }; if (f[k]) quotes[k].flag = f[k]; });
    } catch (e) {}
  }
  function loadCat() {
    flagsFromStore();
    fetch("/data/tv-watchlists.json?v=tvwatch").then(function (r) { return r.json(); }).then(function (j) {
      (j.lists || []).forEach(function (L) {
        if (!L || !L.name) return;
        catalog[String(L.id || L.name)] = L;
      });
      paint();
    }).catch(function () {});
  }
  function hook() {
    var box = document.getElementById("wlist");
    if (!box) return false;
    if (!box.dataset.tvw) {
      box.dataset.tvw = "1";
      try {
        if (!localStorage.getItem("jh-tv-list-sized")) {
          var h = Math.max(220, Math.min(560, Math.round((window.innerHeight || 800) * 0.46)));
          box.style.height = h + "px";
          document.documentElement.style.setProperty("--list-h", h + "px");
          localStorage.setItem("jh-tv-list-sized", "1");
        }
      } catch (e) {}
      new MutationObserver(function () { paint(); }).observe(box, { childList: true });
      var res = document.getElementById("listres");
      if (res) new MutationObserver(function () {
        if (lock) return;
        res.querySelectorAll(".ld-row").forEach(function (row) {
          if (row.querySelector(".ld-flag")) return;
          var id = row.getAttribute("data-id");
          var L = findList(id);
          var c = L && L.color && String(L.color).charAt(0) === "#" ? L.color : "#787b86";
          row.insertAdjacentHTML("afterbegin", "<i class=ld-flag style='background:" + c + "'></i>");
        });
      }).observe(res, { childList: true });
    }
    paint();
    return true;
  }
  document.addEventListener("keydown", function (e) {
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
    var tag = (e.target && e.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
    var w = document.getElementById("watch");
    if (!w || !w.classList.contains("is-open")) return;
    var rows = [].slice.call(document.querySelectorAll("#wlist .wrow"));
    if (!rows.length) return;
    var i = 0, k; for (k = 0; k < rows.length; k++) if (rows[k].classList.contains("on")) i = k;
    i = e.key === "ArrowDown" ? Math.min(rows.length - 1, i + 1) : Math.max(0, i - 1);
    e.preventDefault();
    rows[i].scrollIntoView({ block: "nearest" });
    openSym(rows[i].getAttribute("data-s"));
  });
  loadCat();
  var n = 0, t = setInterval(function () { if (hook() || ++n > 40) clearInterval(t); }, 250);
})();
