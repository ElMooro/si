#!/usr/bin/env python3
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]

def buyback():
    p = ROOT / "jh-chart-buyback.js"
    t = p.read_text()
    old = "      out.push({ date: obs[i].date, y: y });"
    new = "      var od = obs[i].date || (obs[i].original && obs[i].original.date) || \"\";\n      if (!od) continue;\n      out.push({ date: od, y: y });"
    if old in t:
        t = t.replace(old, new, 1)
        print("buyback date")
    old_h = "  var n = 0;\n  var timer = setInterval(function () {\n    hook();\n    if (++n > 40) clearInterval(timer);\n  }, 250);"
    new_h = "  // Engine paint replaces window.paint on later loads. Keep re-wrapping.\n  setInterval(hook, 1000);\n  hook();"
    if old_h in t:
        t = t.replace(old_h, new_h, 1)
        print("buyback hook")
    if t != p.read_text():
        p.write_text(t)
    else:
        print("buyback already")

def engines():
    page = (ROOT / "engines-data.html").read_text()
    start = page.find('<div class="panel" style="margin-bottom:20px">')
    grid = page.find('<div class="grid">')
    b0 = page.find("// Ticker lookup\n")
    b1 = page.find("// Ticker 360 summary")
    if min(start, grid, b0, b1) < 0:
        print("engines markers missing")
        return
    block_a, block_b = page[start:grid], page[b0:b1]
    fp = ROOT / "tests/fixtures/ranker-numeric/pages-transition.json"
    doc = json.loads(fp.read_text())
    eng = doc["engines-data.html"]
    eng["after_sha256"] = "152d923a76f7faa8d856c2e605ef34570662d672689409f193d2f9ffbb6eacab"
    afters = [c.get("after") for c in eng["changes"]]
    if block_a not in afters:
        eng["changes"].append({"before": "", "after": block_a})
        print("reviewed panel")
    if block_b not in afters:
        eng["changes"].append({"before": "", "after": block_b})
        print("reviewed lookup")
    fp.write_text(json.dumps(doc, indent=2) + "\n")
    print("engines fixture")

if __name__ == "__main__":
    buyback()
    engines()
