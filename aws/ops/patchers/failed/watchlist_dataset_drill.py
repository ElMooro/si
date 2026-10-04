#!/usr/bin/env python3
"""Symbol search keeps the provider id, and a dataset opens its series instead of leaving the chart."""
import pathlib, sys

MARKER = "jh-dataset-drill"
ROOT = pathlib.Path(__file__).resolve()
while ROOT != ROOT.parent and not (ROOT / "jh-chart-find.js").exists():
    ROOT = ROOT.parent
TV = ROOT / "jh-chart-find.js"

DRILL = r'''
  var dirMeta = Object.create(null);
  var drillGen = 0;
  function rememberDir(id, meta) {
    if (!id) return;
    dirMeta[String(id).toUpperCase()] = meta || {};
  }
  function drillableId(s) {
    s = String(s || "");
    if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*:/.test(s)) return false;
    if (/^(DATA|DESK|provider|CQ|CQSNAP|CQARM|CQDOC|CISS):/i.test(s)) return false;
    return true;
  }
  function ensureDrill() {
    var panel = document.getElementById("jh-ds");
    if (panel) return panel;
    panel = document.createElement("div");
    panel.id = "jh-ds";
    panel.hidden = true;
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-label", "Series in this dataset");
    panel.innerHTML = "<div class=row><b data-title>Series</b><button type=button data-x aria-label=Close>Close</button></div>" +
      "<div class=muted data-ds></div>" +
      "<input data-q placeholder='Filter series — Germany, monthly, GDP' autocomplete=off spellcheck=false>" +
      "<div class=muted>The index suggests. One series loads only after you pick it. A dataset is not a line.</div>" +
      "<div data-body></div>";
    document.body.appendChild(panel);
    panel.querySelector("[data-x]").onclick = function () { panel.hidden = true; drillGen++; };
    var inp = panel.querySelector("[data-q]");
    inp.oninput = function () {
      clearTimeout(ensureDrill._t);
      ensureDrill._t = setTimeout(function () { openDrill(panel.getAttribute("data-id"), inp.value, true); }, 160);
    };
    inp.onkeydown = function (e) {
      var rows = panel.querySelectorAll("[data-series]");
      var cur = panel.querySelector("[data-series].on");
      var i = cur ? Array.prototype.indexOf.call(rows, cur) : -1;
      if (e.key === "Escape") { panel.hidden = true; drillGen++; return; }
      if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        if (!rows.length) return;
        if (cur) cur.classList.remove("on");
        i = e.key === "ArrowDown" ? Math.min(rows.length - 1, i + 1) : Math.max(0, i - 1);
        rows[i].classList.add("on");
        return;
      }
      if (e.key === "Enter" && cur) { e.preventDefault(); cur.click(); }
    };
    return panel;
  }
  function chartPicked(id) {
    var panel = document.getElementById("jh-ds");
    if (panel) panel.hidden = true;
    drillGen++;
    if (window.jhGoSymbol) window.jhGoSymbol(id, "chart");
    else if (window.jhOpenSymbol) window.jhOpenSymbol(id);
  }
  function paintDrill(panel, j) {
    var body = panel.querySelector("[data-body]");
    var rows = (j && j.rows) || [];
    var chartable = rows.filter(function (r) { return r && r.id && r.chartable !== false && r.kind !== "dataset"; });
    if (!chartable.length) {
      body.textContent = (j && j.error) ? ("No series: " + j.error) : "No chartable series for this filter. This dataset was not drawn as a line.";
      return 0;
    }
    var html = "";
    chartable.slice(0, 40).forEach(function (r) {
      var sub = [r.freq || "", r.n ? (r.n + " obs") : "", r.provider || ""].filter(Boolean).join(" · ");
      html += "<button type=button class=row data-series='" + esc(r.id) + "'><b>" + esc(r.name || r.id) + "</b><span class=muted>" + esc(sub || r.id) + "</span></button>";
    });
    if (j && j.truncated) html += "<div class=muted>Showing the first matches. Type another word to narrow.</div>";
    else if (j && j.hint) html += "<div class=muted>" + esc(j.hint) + "</div>";
    body.innerHTML = html;
    body.querySelectorAll("[data-series]").forEach(function (b) {
      b.onclick = function () { chartPicked(b.getAttribute("data-series")); };
    });
    return chartable.length;
  }
  function openDrill(ds, q, fromInput) {
    ds = String(ds || "").trim();
    if (!ds) return;
    var panel = ensureDrill();
    var my = ++drillGen;
    panel.hidden = false;
    panel.setAttribute("data-id", ds);
    panel.querySelector("[data-title]").textContent = "Series in dataset";
    panel.querySelector("[data-ds]").textContent = ds;
    var inp = panel.querySelector("[data-q]");
    if (!fromInput) inp.value = q || "";
    panel.querySelector("[data-body]").textContent = "Loading series from the warehouse…";
    var url = PROXY + "/browse?ds=" + encodeURIComponent(ds) + "&q=" + encodeURIComponent(inp.value || "") + "&limit=40";
    fetch(url).then(function (r) { return r.json(); }).then(function (j) {
      if (my !== drillGen) return;
      var n = paintDrill(panel, j || {});
      if (!n) {
        var meta = dirMeta[ds.toUpperCase()];
        if (meta && meta.browse === false && window.JHChartCatalog && window.JHChartCatalog.go && window.JHChartCatalog.go.__jhOrig) {
          panel.hidden = true;
          window.JHChartCatalog.go.__jhOrig(ds, "dataset");
        }
      }
    }).catch(function () {
      if (my !== drillGen) return;
      panel.querySelector("[data-body]").textContent = "Series browse unavailable. Nothing was charted.";
    });
  }
  function armDrill() {
    var cat = window.JHChartCatalog;
    if (!cat || typeof cat.go !== "function" || cat.go.__jhDrill) return;
    var orig = cat.go;
    function wrapped(s, dest) {
      s = String(s || "");
      if (dest === "dataset" && drillableId(s)) {
        var meta = dirMeta[s.toUpperCase()];
        if (meta && meta.browse === false) return orig(s, dest);
        openDrill(s, "");
        return true;
      }
      return orig(s, dest);
    }
    wrapped.__jhDrill = 1;
    wrapped.__jhOrig = orig;
    cat.go = wrapped;
  }

'''

OLD_ADD = '''  function addExtra(my, sym, name, ex) {
    if (my !== gen || !sym) return;
    sym = String(sym).trim();
    if (!sym || /sentinel/i.test(sym)) return;
    var q = lastQ;
    var rk = rankSym(sym, name, q);
    if (rk > 3) return;
    var key = sym.toUpperCase();
    if (extra.some(function (r) { return r.k === key; })) return;
    extra.push({ s: sym, n: name || sym, x: ex || "", k: key, rk: rk });
'''

NEW_ADD = '''  function addExtra(my, sym, name, ex, meta) {
    if (my !== gen || !sym) return;
    sym = String(sym).trim();
    if (!sym || /sentinel/i.test(sym)) return;
    meta = meta || {};
    var q = lastQ;
    var rk = meta.rk != null ? meta.rk : rankSym(sym, name, q);
    if (rk > 3 && !meta.keep) return;
    var key = sym.toUpperCase();
    if (extra.some(function (r) { return r.k === key; })) return;
    rememberDir(sym, meta);
    extra.push({ s: sym, n: name || sym, x: ex || "", k: key, rk: rk, kind: meta.kind || "", chartable: meta.chartable !== false, browse: !!meta.browse });
'''

OLD_TAKE = '''    take(PROXY + "/symsearch?q=" + encodeURIComponent(q) + "&limit=20", function (j) {
      var rows = (j && j.rows) || [];
      rows.forEach(function (row) {
        if (!row) return;
        addExtra(my, row.symbol || row.ticker || row.id, row.name || row.title || "", (row.exchange || row.provider || "") + " " + (row.type || row.kind || ""));
      });
    });
'''

NEW_TAKE = '''    take(PROXY + "/symsearch?q=" + encodeURIComponent(q) + "&limit=20", function (j) {
      var rows = (j && j.rows) || [];
      var sh = j && j.series_hits && j.series_hits.rows;
      if (sh && sh.length) rows = rows.concat(sh);
      rows.forEach(function (row, i) {
        if (!row) return;
        var id = row.id || row.symbol || row.ticker;
        if (!id) return;
        var chartable = row.chartable === true || row.kind === "series" || row.kind === "instrument";
        var browse = row.kind === "dataset" && row.browse !== false && row.chartable === false;
        rememberDir(id, { kind: row.kind || "", browse: row.browse, name: row.name || "" });
        addExtra(my, id, row.name || row.title || id, (row.provider_name || row.provider || row.exchange || "") + " " + (row.kind || row.type || "") + (chartable ? "" : " · open series"), { keep: true, rk: chartable ? i : 100 + i, kind: row.kind || "", chartable: chartable, browse: browse });
      });
    });
'''

OLD_CLICK = '''    host.querySelectorAll("[data-jh-sym]").forEach(function (b) {
      b.onclick = function () {
        var s = b.getAttribute("data-jh-sym");
        if (window.jhGoSymbol) window.jhGoSymbol(s, "chart");
        else if (window.jhOpenSymbol) window.jhOpenSymbol(s);
      };
    });
'''

NEW_CLICK = '''    host.querySelectorAll("[data-jh-sym]").forEach(function (b) {
      b.onclick = function (e) {
        e.preventDefault(); e.stopPropagation();
        var s = b.getAttribute("data-jh-sym");
        if (b.getAttribute("data-jh-browse") === "1") openDrill(s, "");
        else if (window.jhGoSymbol) window.jhGoSymbol(s, "chart");
        else if (window.jhOpenSymbol) window.jhOpenSymbol(s);
      };
    });
'''

OLD_BTN = '''      html += "<button type=button class=ss-hit data-jh-sym='" + esc(r.s) + "'><i class=ss-logo style=background:#F0B429>" + esc(tick.slice(0, 1)) + "</i><span><span class=nm>" + esc(tick) + "</span><span class=ds>" + esc(r.n) + "</span></span><span class=ss-ex>" + esc(r.x) + "</span><span class=ss-more> </span></button>";
'''

NEW_BTN = '''      html += "<button type=button class=ss-hit data-jh-sym='" + esc(r.s) + "' data-jh-kind='" + esc(r.kind || "") + "' data-jh-browse='" + (r.browse ? "1" : "0") + "'><i class=ss-logo style=background:#F0B429>" + esc(tick.slice(0, 1)) + "</i><span><span class=nm>" + esc(tick) + "</span><span class=ds>" + esc(r.n) + "</span></span><span class=ss-ex>" + esc(r.x) + "</span><span class=ss-more> </span></button>";
'''

OLD_CSS = '''  css.textContent = "#menu .jh-q{display:block;width:calc(100% - 16px);margin:4px 8px 6px;box-sizing:border-box;padding:7px 8px;border:1px solid var(--line,#17150E);border-radius:4px;background:transparent;color:inherit;font:13px IBM Plex Sans,sans-serif}#menu [data-jh-favblock]>.lab{color:#F0B429}#ssres [data-jh-hits] .ss-sec{padding:8px 14px 2px;font-size:10px;letter-spacing:1px;color:#787b86}";
'''

NEW_CSS = '''  css.textContent = "#menu .jh-q{display:block;width:calc(100% - 16px);margin:4px 8px 6px;box-sizing:border-box;padding:7px 8px;border:1px solid var(--line,#17150E);border-radius:4px;background:transparent;color:inherit;font:13px IBM Plex Sans,sans-serif}#menu [data-jh-favblock]>.lab{color:#F0B429}#ssres [data-jh-hits] .ss-sec{padding:8px 14px 2px;font-size:10px;letter-spacing:1px;color:#787b86}#jh-ds{position:fixed;z-index:140;top:58px;left:12px;width:min(560px,calc(100vw - 24px));max-height:72vh;overflow:auto;background:#12110c;color:#e6e1d3;border:1px solid #3a3424;border-radius:8px;padding:10px 12px;box-shadow:0 12px 40px rgba(0,0,0,.45);font:13px/1.35 IBM Plex Sans,sans-serif}#jh-ds .row{display:flex;justify-content:space-between;gap:8px;align-items:center}#jh-ds button{background:transparent;color:inherit;border:0;cursor:pointer;font:inherit}#jh-ds input{display:block;width:100%;box-sizing:border-box;margin:8px 0;padding:7px 8px;border:1px solid #3a3424;border-radius:4px;background:transparent;color:inherit}#jh-ds .muted{color:#9a917c;font-size:11px}#jh-ds [data-series]{display:block;width:100%;text-align:left;border-top:1px solid #2a261c;padding:8px 2px}#jh-ds [data-series].on,#jh-ds [data-series]:hover{background:#2a2416}#jh-ds [data-series] b{display:block}";
'''

OLD_BOOT = '''  hookOpen();
  watchMenu();
  watchSym();
'''

NEW_BOOT = '''  hookOpen();
  watchMenu();
  watchSym();
  armDrill();
  setInterval(armDrill, 500);
'''

def main():
    text = TV.read_text()
    if MARKER in text:
        print("already applied")
        return 0
    if "function providerRest" in text:
        sys.exit("wrong file")
    # marker lives in the inserted block
    block = DRILL.replace("function armDrill()", "function armDrill() /* " + MARKER + " */", 1)
    pairs = [
        (OLD_ADD, NEW_ADD),
        (OLD_TAKE, NEW_TAKE),
        (OLD_CLICK, NEW_CLICK),
        (OLD_BTN, NEW_BTN),
        (OLD_CSS, NEW_CSS),
        (OLD_BOOT, NEW_BOOT),
    ]
    for old, new in pairs:
        if text.count(old) != 1:
            sys.exit("anchor missing %s" % old[:60].replace("\n", " "))
        text = text.replace(old, new, 1)
    needle = "  function watchSym() {"
    if text.count(needle) != 1:
        sys.exit("no watchSym")
    text = text.replace(needle, block + needle, 1)
    if text.count(MARKER) != 1:
        sys.exit("marker %s" % text.count(MARKER))
    if "row.symbol || row.ticker || row.id" in text:
        sys.exit("bare id remains")
    TV.write_text(text)
    print("ok bytes", len(text.encode()))
    return 0

if __name__ == "__main__":
    sys.exit(main())
