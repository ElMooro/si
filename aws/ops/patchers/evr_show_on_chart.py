#!/usr/bin/env python3
"""Make Effort & Spread stay under Price Spread and actually paint on chart.html.

The study was added, then lost: the indicators menu is rebuilt on every open
while a one-shot data-evr flag blocked a second insert, and paint() is async
so it cleared #oscwrap after the pane was drawn.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EVR = ROOT / "jh-chart-evr.js"
ENG = ROOT / "jh-chart-engine.js"
MARKER = "/* evr-show-2 */"

OLD_ENSURE = (
    "    if (find()) return true;\n"
    '    var item = { id: ID, n: "Effort & Spread", on: 0, cat: "Volume", p: 20, externalPane: true, c: "#9aa3b5", c2: "#e0b15a" };'
)

NEW_ENSURE = (
    "    var existing = find();\n"
    "    if (existing) {\n"
    '      existing.cat = "Volatility";\n'
    "      existing.externalPane = true;\n"
    '      if (!existing.n) existing.n = "Effort & Spread";\n'
    "      return true;\n"
    "    }\n"
    '    var item = { id: ID, n: "Effort & Spread", on: 0, cat: "Volatility", p: 20, externalPane: true, c: "#9aa3b5", c2: "#e0b15a" };'
)

OLD_TAIL_START = "  function armMenu() {"
OLD_TAIL_NEEDLE = 'menu.getAttribute("data-evr") === "1"'

NEW_TAIL = r"""  function armMenu() {
    var menu = document.getElementById("menu");
    if (!menu || !menu.classList.contains("on")) return;
    if (menu.querySelector("[data-tog='effspread']")) return;
    var tog = menu.querySelector("[data-tog='pspread']");
    if (!tog) return;
    var row = tog.closest(".irow");
    if (!row || !row.parentNode) return;
    if (!ensure()) return;
    var item = find();
    var live = on(item);
    var div = document.createElement("div");
    div.className = "irow" + (live ? " on" : "");
    div.innerHTML = "<button type=button class=nm data-tog='effspread'>" + (live ? "\u2713  " : "") + "Effort & Spread</button><button type=button class=gr data-gear='effspread' title='Inputs and colors'>\u2699</button>";
    row.parentNode.insertBefore(div, row.nextSibling);
    div.querySelector("[data-tog]").onclick = function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      item.on = on(item) ? 0 : 1;
      item.hide = false;
      try { if (window.jhSaveLay) window.jhSaveLay(); } catch (e) {}
      var pr = (window.paint && window.lastBars && window.lastBars.length) ? window.paint(window.lastBars) : null;
      if (pr && typeof pr.then === "function") pr.then(function () { draw(); }, function () { draw(); });
      else setTimeout(draw, 0);
      var menuNow = document.getElementById("menu");
      if (menuNow) menuNow.className = "menu";
    };
    var gear = div.querySelector("[data-gear]");
    if (gear) gear.onclick = function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      var menuNow = document.getElementById("menu");
      if (menuNow) menuNow.className = "menu";
      if (window.jhInduxSet) window.jhInduxSet(item, true);
    };
  }

  function hookPaint() {
    if (!window.paint || window.paint.__evr) return;
    var orig = window.paint;
    function wrapped() {
      var r = orig.apply(this, arguments);
      function finish() { try { draw(); } catch (e) {} }
      if (r && typeof r.then === "function") r.then(finish, finish);
      else finish();
      return r;
    }
    wrapped.__evr = 1;
    window.paint = wrapped;
  }

  function watchWrap() {
    var wrap = document.getElementById("oscwrap");
    if (!wrap || wrap.getAttribute("data-evr-watch") === "1") return;
    wrap.setAttribute("data-evr-watch", "1");
    var busy = false;
    new MutationObserver(function () {
      if (busy) return;
      var item = find();
      if (!on(item)) return;
      if (document.getElementById("jh-effspread")) return;
      busy = true;
      setTimeout(function () {
        busy = false;
        try { draw(); } catch (e) {} }
      }, 0);
    }).observe(wrap, { childList: true });
  }

  function watchMenu() {
    var menu = document.getElementById("menu");
    if (!menu || menu.getAttribute("data-evr-watch") === "1") return;
    menu.setAttribute("data-evr-watch", "1");
    new MutationObserver(function () {
      try { armMenu(); } catch (e) {} }
    }).observe(menu, { childList: true, attributes: true, attributeFilter: ["class"] });
  }

  /* evr-show-2 */
  try { ensure(); hookPaint(); watchWrap(); watchMenu(); armMenu(); } catch (e0) {}
  setInterval(function () {
    try {
      ensure(); hookPaint(); watchWrap(); watchMenu(); armMenu();
      var item = find();
      if (on(item) && !document.getElementById("jh-effspread")) draw();
    } catch (e) {}
  }, 400);
})();
"""

OLD_ROW = 'row("pspread","Price Spread")+\n      row("relvol","Relative Volatility")+'
NEW_ROW = 'row("pspread","Price Spread")+\n      row("effspread","Effort & Spread")+\n      row("relvol","Relative Volatility")+'


def die(msg):
    raise SystemExit(msg)


def patch_evr():
    text = EVR.read_text()
    if MARKER in text:
        print("evr already showing")
        return False
    if OLD_ENSURE not in text:
        die("ensure needle missing in jh-chart-evr.js")
    text = text.replace(OLD_ENSURE, NEW_ENSURE, 1)
    start = text.find(OLD_TAIL_START)
    if start < 0 or OLD_TAIL_NEEDLE not in text[start:]:
        die("tail needle missing in jh-chart-evr.js")
    text = text[:start] + NEW_TAIL
    if 'cat: "Volume"' in text:
        die("volume category still present")
    if not text.rstrip().endswith("})();"):
        die("rewritten script does not end cleanly")
    if MARKER not in text or text.count(OLD_TAIL_START) != 1:
        die("rewrite failed")
    EVR.write_text(text)
    print("patched", EVR)
    return True


def patch_engine():
    text = ENG.read_text()
    if 'row("effspread","Effort & Spread")+' in text:
        print("engine row already present")
        return False
    if text.count(OLD_ROW) != 1:
        die("price-spread row needle missing or not unique in jh-chart-engine.js")
    ENG.write_text(text.replace(OLD_ROW, NEW_ROW, 1))
    print("patched", ENG)
    return True


if __name__ == "__main__":
    if not EVR.is_file() or not ENG.is_file():
        die("chart files missing next to repo root")
    evr_changed = patch_evr()
    eng_changed = patch_engine()
    if not evr_changed and not eng_changed:
        print("no changes")
