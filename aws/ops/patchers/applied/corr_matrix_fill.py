#!/usr/bin/env python3
"""Fill empty correlation cells from the warm daily tape.

The pairwise matrix on chart.html leaves an em dash when a symbol's
warehouse download fails. Append a filler to jh-chart-evr.js that
rewrites those cells from /data/warm/tv-bars once the grid is on screen.
Idempotent. Does not touch the chart engine.
"""
from pathlib import Path

MARKER = "/* corr-fill-1 */"
ROOT = Path(__file__).resolve().parents[3]
EVR = ROOT / "jh-chart-evr.js"
SNIPPET = r"""
/* corr-fill-1 */
(function () {
  if (window.__jhCorrFill) return;
  window.__jhCorrFill = 1;
  var cache = {};

  function bare(s) {
    s = String(s || "");
    var i = s.lastIndexOf(":");
    return (i >= 0 ? s.slice(i + 1) : s).replace(/[^A-Za-z0-9.\-]/g, "");
  }
  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      return "&" + "quot;";
    });
  }
  function dayKey(t) {
    var n = +t;
    if (n > 1e12) n = Math.floor(n / 1000);
    var d = new Date(n * 1000);
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  }
  function retsOf(bars) {
    var close = {}, order = [], i, t, c, k, prev;
    for (i = 0; i < bars.length; i++) {
      t = +bars[i][0];
      c = +bars[i][4];
      if (!(c > 0) || !isFinite(t)) continue;
      k = dayKey(t);
      if (close[k] == null) order.push(k);
      close[k] = c;
    }
    order.sort(function (a, b) { return a - b; });
    var out = {};
    for (i = 1; i < order.length; i++) {
      prev = close[order[i - 1]];
      if (prev) out[order[i]] = close[order[i]] / prev - 1;
    }
    return out;
  }
  function pearson(a, b) {
    var n = a.length;
    if (n < 8 || b.length !== n) return null;
    var sa = 0, sb = 0, i, ma, mb, num = 0, da = 0, db = 0, xa, xb, den;
    for (i = 0; i < n; i++) { sa += a[i]; sb += b[i]; }
    ma = sa / n; mb = sb / n;
    for (i = 0; i < n; i++) {
      xa = a[i] - ma; xb = b[i] - mb;
      num += xa * xb; da += xa * xa; db += xb * xb;
    }
    den = Math.sqrt(da * db);
    return den ? num / den : 0;
  }
  function pair(a, b) {
    if (!a || !b) return null;
    var keys = [], k;
    for (k in a) if (Object.prototype.hasOwnProperty.call(b, k)) keys.push(+k);
    keys.sort(function (x, y) { return x - y; });
    if (keys.length > 120) keys = keys.slice(-120);
    var xa = [], xb = [], i;
    for (i = 0; i < keys.length; i++) { xa.push(a[keys[i]]); xb.push(b[keys[i]]); }
    return pearson(xa, xb);
  }
  function loadSym(sym) {
    var key = bare(sym).toUpperCase();
    if (!key) return Promise.resolve(null);
    if (cache[key]) return cache[key];
    cache[key] = fetch("/data/warm/tv-bars/universe/US__" + encodeURIComponent(key) + ".json.gz", { credentials: "omit" })
      .then(function (res) {
        if (!res.ok) throw new Error(String(res.status));
        return res.arrayBuffer();
      })
      .then(function (buf) {
        var stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"));
        return new Response(stream).text();
      })
      .then(function (text) {
        var data = JSON.parse(text);
        if (!data || !data.bars || data.bars.length < 8) throw new Error("short");
        return retsOf(data.bars);
      })
      .catch(function () { delete cache[key]; return null; });
    return cache[key];
  }
  function symbols() {
    var nodes = document.querySelectorAll("#tabs button.tab[data-id]");
    var out = [], i, id;
    for (i = 0; i < nodes.length && out.length < 8; i++) {
      id = nodes[i].getAttribute("data-id");
      if (id && out.indexOf(id) < 0) out.push(id);
    }
    return out.length >= 2 ? out : ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "META"];
  }
  function write(el, syms, maps, tf) {
    var html = "<b data-corrfill=1>CORR · " + esc(tf) + "</b><div class=corm style='grid-template-columns:64px repeat(" + syms.length + ",1fr)'><span></span>";
    html += syms.map(function (s) { return "<span>" + esc(bare(s).slice(0, 5)) + "</span>"; }).join("");
    var i, j, c, bg;
    for (i = 0; i < syms.length; i++) {
      html += "<span>" + esc(bare(syms[i]).slice(0, 6)) + "</span>";
      for (j = 0; j < syms.length; j++) {
        c = i === j ? (maps[i] ? 1 : null) : pair(maps[i], maps[j]);
        bg = c == null ? "var(--chip)" : c >= 0 ? "rgba(8,153,129," + Math.min(0.7, Math.abs(c)) + ")" : "rgba(242,54,69," + Math.min(0.7, Math.abs(c)) + ")";
        html += "<span style='background:" + bg + ";text-align:center;padding:4px;color:#fff'>" + (c == null ? "—" : c.toFixed(2)) + "</span>";
      }
    }
    html += "</div>";
    el.innerHTML = html;
  }
  function needs(el) {
    var text = el.textContent || "";
    if (text.indexOf("CORR") < 0 || text.indexOf("Computing") >= 0) return false;
    if (text.indexOf("—") < 0) return false;
    if (el.querySelector("[data-corrfill]")) return false;
    return true;
  }
  var pending = false;
  function fill() {
    var el = document.getElementById("corr");
    if (!el || pending || !needs(el)) return Promise.resolve();
    var syms = symbols();
    var tf = "1d";
    var m = (el.textContent || "").match(/CORR · (\S+)/);
    if (m) tf = m[1];
    pending = true;
    return Promise.all(syms.map(loadSym)).then(function (maps) {
      pending = false;
      var now = document.getElementById("corr");
      if (!now || !needs(now)) return;
      write(now, syms, maps, tf);
    }).catch(function () { pending = false; });
  }
  function arm() {
    var fn = window.jhRenderCorr;
    if (!fn || fn.__corrfill) return;
    var wrapped = function () {
      return Promise.resolve(fn.apply(this, arguments)).then(function () { return fill(); });
    };
    wrapped.__corrfill = 1;
    window.jhRenderCorr = wrapped;
  }
  arm();
  setInterval(function () { try { arm(); fill(); } catch (e) {} }, 700);
  var host = document.getElementById("corr");
  if (host && window.MutationObserver) {
    new MutationObserver(function () { try { fill(); } catch (e) {} }).observe(host, { childList: true, subtree: true, characterData: true });
  }
  ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMZN", "META"].forEach(function (s) { loadSym(s); });
})();
"""

def main():
    text = EVR.read_text(encoding="utf-8")
    if MARKER in text:
        print("corr fill already present")
        return
    if not text.rstrip().endswith("})();"):
        raise SystemExit("jh-chart-evr.js does not end with the study closure")
    EVR.write_text(text.rstrip() + "\n" + SNIPPET.strip() + "\n", encoding="utf-8")
    print("appended corr fill", EVR.stat().st_size)

if __name__ == "__main__":
    main()
