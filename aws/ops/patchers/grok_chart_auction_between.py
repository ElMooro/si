#!/usr/bin/env python3
"""Chart toolbar: Auction between Heat and Corr. Opens the Treasury auction desk."""
import re
from pathlib import Path

TOKEN = "auc=20261014"
HEAT_BTN = (
    "\"<button class='wsico wsdesk' id=btn-heat title='Universe Heatmap'><span class=g>▦</span><span class=l>Heat</span></button>\"+"
)
AUC_BTN = (
    "\"<button class='wsico wsdesk' id=btn-auc title='Treasury Auctions'><span class=g>Auc</span><span class=l>Auction</span></button>\"+"
)
HEAT_CLICK = 'var bht=document.getElementById("btn-heat"); if(bht) bht.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("heat"); };'
AUC_CLICK = '    var bauc=document.getElementById("btn-auc"); if(bauc) bauc.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("auction"); };'
META_OLD = 'heat: ["Universe Heatmap", "/universe-heatmap.html"],\n      corr:'
META_NEW = 'heat: ["Universe Heatmap", "/universe-heatmap.html"],\n      auction: ["Treasury Auctions", "/auctions.html"],\n      corr:'
TEST_OLD = "  const iHeat = src.indexOf('id=btn-heat');\n  const iCorr = src.indexOf('id=btn-corm');"
TEST_NEW = (
    "  const iHeat = src.indexOf('id=btn-heat');\n"
    "  const iAuc = src.indexOf('id=btn-auc');\n"
    "  const iCorr = src.indexOf('id=btn-corm');"
)
ASSERT_OLD = "  assert.ok(iMacro > 0 && iHeat > iMacro && iCorr > iHeat);"
ASSERT_NEW = "  assert.ok(iMacro > 0 && iHeat > iMacro && iAuc > iHeat && iCorr > iAuc);"
RAIL_OLD = "assert.match(rail, /bonds:"
RAIL_NEW = "assert.ok(rail.includes('/auctions.html'));\n  assert.match(rail, /bonds:"


def root() -> Path:
    here = Path(__file__).resolve()
    for candidate in (Path.cwd(), *here.parents):
        if (candidate / "jh-chart-engine.js").exists() and (candidate / "chart.html").exists():
            return candidate
    raise SystemExit("repo root not found")


def once(text: str, old: str, new: str, label: str) -> str:
    n = text.count(old)
    if n != 1:
        raise SystemExit(f"{label} count {n}")
    return text.replace(old, new, 1)


def bump(html: str, filename: str) -> str:
    pat = re.compile(r'(src="/' + re.escape(filename) + r')(\?[^"]*)?(")')
    found = {"n": 0}

    def repl(match: re.Match[str]) -> str:
        found["n"] += 1
        if found["n"] > 1:
            return match.group(0)
        query = match.group(2) or ""
        if TOKEN in query:
            return match.group(0)
        query = ("?" + TOKEN) if not query else query + "&" + TOKEN
        return match.group(1) + query + match.group(3)

    out = pat.sub(repl, html)
    if found["n"] < 1:
        raise SystemExit(f"missing script {filename}")
    return out


def main() -> int:
    base = root()
    engine = base / "jh-chart-engine.js"
    rail = base / "jh-chart-tvrail.js"
    page = base / "chart.html"
    test = base / "tests" / "desk-engines.test.js"
    eng = engine.read_text(encoding="utf-8")
    if "id=btn-auc" in eng and 'jhOpenWorkspace("auction")' in eng:
        print("engine already has auction")
    else:
        eng = once(eng, HEAT_BTN, HEAT_BTN + "\n      " + AUC_BTN, "heat button")
        eng = once(eng, HEAT_CLICK, HEAT_CLICK + "\n" + AUC_CLICK, "heat click")
        if not (eng.find("id=btn-heat") < eng.find("id=btn-auc") < eng.find("id=btn-corm")):
            raise SystemExit("auction button is not between heat and corr")
        engine.write_text(eng, encoding="utf-8")
        print("engine auction tab")
    body = rail.read_text(encoding="utf-8")
    if 'auction: ["Treasury Auctions", "/auctions.html"]' not in body:
        body = once(body, META_OLD, META_NEW, "workspace meta")
        rail.write_text(body, encoding="utf-8")
        print("rail auction workspace")
    else:
        print("rail already mapped")
    html = page.read_text(encoding="utf-8")
    bumped = bump(bump(html, "jh-chart-engine.js"), "jh-chart-tvrail.js")
    if bumped != html:
        page.write_text(bumped, encoding="utf-8")
        print("cache bust", TOKEN)
    else:
        print("cache already", TOKEN)
    spec = test.read_text(encoding="utf-8")
    if "btn-auc" not in spec:
        spec = once(spec, TEST_OLD, TEST_NEW, "test indexes")
        spec = once(spec, ASSERT_OLD, ASSERT_NEW, "test order")
        spec = once(spec, RAIL_OLD, RAIL_NEW, "test rail")
        test.write_text(spec, encoding="utf-8")
        print("test order")
    else:
        print("test already expects auction")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
