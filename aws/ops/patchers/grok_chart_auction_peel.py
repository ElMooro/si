#!/usr/bin/env python3
"""Peel the Auction tab so chart preservation hashes still see the prior engine, rail, and chart.html."""
from pathlib import Path

COMMENT = "// 2026-10-13: peel the Correlation Matrix heatmap hook in jh-chart-tvrail.js before the census hash.\n"
COMMENT_NEW = COMMENT + "// 2026-10-14: peel the Auction tab (engine button, rail meta, chart.html cache) before that census.\n"
NEEDLE = "function normalize(raw,file){raw=peelKind(raw,file);raw=peelKindHtml(raw,file);raw=peelCorr(raw,file);raw=peelSos(raw,file);raw=peelEvr(raw,file);"
REPL = "function normalize(raw,file){raw=peelAucEngine(raw,file);raw=peelAucHtml(raw,file);raw=peelAucRail(raw,file);raw=peelKind(raw,file);raw=peelKindHtml(raw,file);raw=peelCorr(raw,file);raw=peelSos(raw,file);raw=peelEvr(raw,file);"
INSERT = r"""const AUC_ENGINE_AFTER='245e9b8d541a5c7de5d96bd3990d07ecfc6735c6f41f2604b455a0ff3585c993';
const AUC_ENGINE_BEFORE='fc98db4059553575fe80cf484f1e2a65463410466b22fb7a1a5d8ead7876afdc';
const AUC_BTN='      "<button class=\'wsico wsdesk\' id=btn-auc title=\'Treasury Auctions\'><span class=g>Auc</span><span class=l>Auction</span></button>"+\n';
const AUC_CLICK='    var bauc=document.getElementById("btn-auc"); if(bauc) bauc.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("auction"); };\n';
function peelAucEngine(raw,file){
 if(file!=='jh-chart-engine.js'||hash(raw)!==AUC_ENGINE_AFTER)return raw;
 assert.equal(raw.split(AUC_BTN).length,2,'jh-chart-engine.js auction button');
 assert.equal(raw.split(AUC_CLICK).length,2,'jh-chart-engine.js auction click');
 raw=raw.replace(AUC_BTN,'').replace(AUC_CLICK,'');
 assert.equal(hash(raw),AUC_ENGINE_BEFORE,'jh-chart-engine.js');
 return raw;
}
const AUC_HTML_AFTER='82504fa7fad057eb4def8da4487515974b2b7ff05fb3043053ad0fe82c3c96f3';
const AUC_HTML_BEFORE='9291b0dcb516c792bd5dab6a37f2fe862d56d924cbdb5a97e73b0686829ee01b';
const AUC_HTML_FROM='&auc=20261014';
function peelAucHtml(raw,file){
 if(file!=='chart.html'||hash(raw)!==AUC_HTML_AFTER)return raw;
 assert.equal(raw.split(AUC_HTML_FROM).length,3,'chart.html auction cache');
 raw=raw.split(AUC_HTML_FROM).join('');
 assert.equal(hash(raw),AUC_HTML_BEFORE,'chart.html');
 return raw;
}
const AUC_RAIL_AFTER='7cb3b9eb3b5dca7c5184e24e4fa46910c865d997dc1c3bb60d0f8dc475de2bc7';
const AUC_RAIL_BEFORE='64df1eb0c10f2b69708c6e840bdc64f31e904e7aa9c328f63cc423f0f495e546';
const AUC_RAIL_LINE='      auction: ["Treasury Auctions", "/auctions.html"],\n';
function peelAucRail(raw,file){
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AUC_RAIL_AFTER)return raw;
 assert.equal(raw.split(AUC_RAIL_LINE).length,2,'jh-chart-tvrail.js auction meta');
 raw=raw.replace(AUC_RAIL_LINE,'');
 assert.equal(hash(raw),AUC_RAIL_BEFORE,'jh-chart-tvrail.js');
 return raw;
}
"""


def main():
    path = Path("tests/helpers/chart-macro-line-preservation.cjs")
    text = path.read_text(encoding="utf-8")
    if "function peelAucEngine(" in text and REPL in text:
        print("already peelAuc")
        return
    if "function peelAucEngine(" in text:
        raise SystemExit("peelAuc present but normalize needle differs")
    if text.count(NEEDLE) != 1:
        raise SystemExit("normalize needle missing")
    if text.count(COMMENT) != 1:
        raise SystemExit("comment needle missing")
    text = text.replace(COMMENT, COMMENT_NEW, 1).replace(NEEDLE, INSERT + REPL, 1)
    path.write_text(text, encoding="utf-8")
    print("patched auction peel")


if __name__ == "__main__":
    main()
