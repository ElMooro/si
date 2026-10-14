#!/usr/bin/env python3
"""Hide the empty cascade sleeve and cache-bust the heatmap iframe.

The live cascade book has no ranked names, so the desk opened on
"Cascade feed not loaded yet" and looked blank. Skip that sleeve when
it has no instruments. Bust chart.html + the rail so the iframe loads
the rebuilt universe page instead of a cached copy.
"""
import hashlib
from pathlib import Path

ROOT = Path(".")
CHART = ROOT / "chart-pro.html"
RAIL = ROOT / "jh-chart-tvrail.js"
HTML = ROOT / "chart.html"
HELPER = ROOT / "tests/helpers/chart-macro-line-preservation.cjs"

RENDER_OLD = """    const html = cats.map(cat => {
      const items = cat.items.map(it => this.cellHtml(cat, it)).join('')
        || (cat.auto
          ? '<div class="macro-empty-cat">Cascade feed not loaded yet. It fills in automatically when the engine data arrives.</div>'
          : '<div class="macro-empty-cat">No instruments yet. Use + to add any stock, ETF, index, FX pair, crypto or data series.</div>');"""
RENDER_NEW = """    const html = cats.map(cat => {
      if (cat.auto && !(cat.items && cat.items.length)) return '';
      const items = cat.items.map(it => this.cellHtml(cat, it)).join('')
        || (cat.auto
          ? ''
          : '<div class="macro-empty-cat">No instruments yet. Use + to add any stock, ETF, index, FX pair, crypto or data series.</div>');"""

RAIL_OLD = 'heat: ["Universe Heatmap", "/universe-heatmap.html"],'
RAIL_NEW = 'heat: ["Universe Heatmap", "/universe-heatmap.html?v=20261014b"],'
HTML_OLD = '/jh-chart-tvrail.js?v=20261001-snapshot&watch=watchlist-transaction-v1&auc=20261014'
HTML_NEW = HTML_OLD + '&heat=20261014b'

HEAT_HTML_AFTER = "edc3fb88fa09756baeccf417bb3afbad86dbd83f8aa1a38beac3c5c6cb7caa7a"
HEAT_HTML_BEFORE = "82504fa7fad057eb4def8da4487515974b2b7ff05fb3043053ad0fe82c3c96f3"
HEAT_RAIL_AFTER = "33c541f77bf942b178814ea115bb51aa65407e89dfe4d5daa3dd3cee5aabd1c3"
HEAT_RAIL_BEFORE = "7cb3b9eb3b5dca7c5184e24e4fa46910c865d997dc1c3bb60d0f8dc475de2bc7"

PEEL = r"""function peelHeatHtml(raw,file){
 const AFTER='edc3fb88fa09756baeccf417bb3afbad86dbd83f8aa1a38beac3c5c6cb7caa7a';
 const BEFORE='82504fa7fad057eb4def8da4487515974b2b7ff05fb3043053ad0fe82c3c96f3';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 const from='&heat=20261014b';
 assert.equal(raw.split(from).length,2,'chart.html heat cache');
 raw=raw.replace(from,'');
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
function peelHeatRail(raw,file){
 const AFTER='33c541f77bf942b178814ea115bb51aa65407e89dfe4d5daa3dd3cee5aabd1c3';
 const BEFORE='7cb3b9eb3b5dca7c5184e24e4fa46910c865d997dc1c3bb60d0f8dc475de2bc7';
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 const from='heat: ["Universe Heatmap", "/universe-heatmap.html?v=20261014b"],';
 const to='heat: ["Universe Heatmap", "/universe-heatmap.html"],';
 assert.equal(raw.split(from).length,2,'heat rail');
 raw=raw.replace(from,to);
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
"""
NOTE = "// 2026-10-14: peel the heatmap cache-bust so the auction census still sees the prior bytes.\n"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(path, old, new, label):
    text = path.read_text(encoding="utf-8")
    if new in text:
        print("already", label)
        return
    n = text.count(old)
    if n != 1:
        raise SystemExit(f"{label} needle count {n}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    print("patched", label)


def main():
    replace_once(CHART, RENDER_OLD, RENDER_NEW, "chart-pro cascade skip")
    replace_once(RAIL, RAIL_OLD, RAIL_NEW, "rail heatmap href")
    replace_once(HTML, HTML_OLD, HTML_NEW, "chart.html rail cache")
    if sha(RAIL) != HEAT_RAIL_AFTER:
        raise SystemExit("rail hash " + sha(RAIL))
    if sha(HTML) != HEAT_HTML_AFTER:
        raise SystemExit("chart.html hash " + sha(HTML))
    helper = HELPER.read_text(encoding="utf-8")
    if "function peelHeatHtml(" not in helper:
        needle = "function normalize(raw,file){raw=peelAucEngine(raw,file);"
        if helper.count(needle) != 1:
            raise SystemExit("normalize needle missing")
        helper = helper.replace(needle, PEEL + "function normalize(raw,file){raw=peelHeatHtml(raw,file);raw=peelHeatRail(raw,file);raw=peelAucEngine(raw,file);", 1)
        if NOTE not in helper:
            helper = helper.replace("const fs=require", NOTE + "const fs=require", 1)
        HELPER.write_text(helper, encoding="utf-8")
        print("patched preservation peel")
    else:
        print("already peelHeatHtml")


if __name__ == "__main__":
    main()
