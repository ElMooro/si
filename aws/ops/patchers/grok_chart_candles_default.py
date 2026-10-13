#!/usr/bin/env python3
"""Open chart.html on candles. Ignore a saved line from the 2026-10-05 default."""
from pathlib import Path

def locate(name):
    here = Path(name)
    if here.exists():
        return here
    root = Path(__file__).resolve().parents[3] / name
    if root.exists():
        return root
    raise SystemExit("missing " + name)

def swap(path, old, new, label):
    text = path.read_text(encoding="utf-8")
    if new in text and old not in text:
        print("already " + label)
        return
    count = text.count(old)
    if count != 1:
        raise SystemExit(label + " needle count " + str(count))
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    print("patched " + label)

def main():
    engine = locate("jh-chart-engine.js")
    html = locate("chart.html")
    helper = locate("tests/helpers/chart-macro-line-preservation.cjs")
    swap(
        engine,
        'kind="line", scaleMode=0; /* default chart type: line (2026-10-05) */',
        'kind="candles", scaleMode=0; /* default chart type: candles */',
        "default kind",
    )
    swap(
        engine,
        "if(lay.kind && lay.kindV===2) kind=lay.kind;",
        "if(lay.kind && lay.kindV===3) kind=lay.kind;",
        "kind restore",
    )
    swap(
        engine,
        "saveJSON(LAY_KEY,{kindV:2,",
        "saveJSON(LAY_KEY,{kindV:3,",
        "kind save",
    )
    swap(
        html,
        'src="/jh-chart-engine.js?v=20261002ac-shelves&line=20261005&watch=watchlist-transaction-v1&macro=20261006"',
        'src="/jh-chart-engine.js?v=20261002ac-shelves&line=20261005&watch=watchlist-transaction-v1&macro=20261006&candles=20261013"',
        "engine cache",
    )
    peel = r"""
const KIND_AFTER='fc98db4059553575fe80cf484f1e2a65463410466b22fb7a1a5d8ead7876afdc';
const KIND_BEFORE='e4b0b6584723968373644f9e9f07e2dc88d278933df1f6f865d65672b0de629a';
function peelKind(raw,file){
 if(file!=='jh-chart-engine.js'||hash(raw)!==KIND_AFTER)return raw;
 const pairs=[
  ['kind="candles", scaleMode=0; /* default chart type: candles */','kind="line", scaleMode=0; /* default chart type: line (2026-10-05) */'],
  ['if(lay.kind && lay.kindV===3) kind=lay.kind;','if(lay.kind && lay.kindV===2) kind=lay.kind;'],
  ['saveJSON(LAY_KEY,{kindV:3,','saveJSON(LAY_KEY,{kindV:2,']
 ];
 for(const pair of pairs){assert.equal(raw.split(pair[0]).length,2,'engine '+pair[0].slice(0,48));raw=raw.replace(pair[0],pair[1]);}
 assert.equal(hash(raw),KIND_BEFORE,'jh-chart-engine.js');
 return raw;
}
const HTML_KIND_FROM='src="/jh-chart-engine.js?v=20261002ac-shelves&line=20261005&watch=watchlist-transaction-v1&macro=20261006&candles=20261013"';
const HTML_KIND_TO='src="/jh-chart-engine.js?v=20261002ac-shelves&line=20261005&watch=watchlist-transaction-v1&macro=20261006"';
const HTML_KIND_AFTER='9291b0dcb516c792bd5dab6a37f2fe862d56d924cbdb5a97e73b0686829ee01b';
const HTML_KIND_BEFORE='cd53800c27da1e73f840a63906da4cdfe65a4cd55d7d6b99bdff929dc18db9d9';
function peelKindHtml(raw,file){
 if(file!=='chart.html'||hash(raw)!==HTML_KIND_AFTER)return raw;
 assert.equal(raw.split(HTML_KIND_FROM).length,2,'chart.html candles cache');
 raw=raw.replace(HTML_KIND_FROM,HTML_KIND_TO);
 assert.equal(hash(raw),HTML_KIND_BEFORE,'chart.html');
 return raw;
}
"""
    swap(
        helper,
        "function normalize(raw,file){raw=peelCorr(raw,file);raw=peelSos(raw,file);raw=peelEvr(raw,file);",
        peel + "function normalize(raw,file){raw=peelKind(raw,file);raw=peelKindHtml(raw,file);raw=peelCorr(raw,file);raw=peelSos(raw,file);raw=peelEvr(raw,file);",
        "preservation peel",
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
