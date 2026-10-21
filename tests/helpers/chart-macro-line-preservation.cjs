// Exact byte reversal of the 2026-10-05 macro line/candles/change-modes, auto-hide workspace and file-import change only.
// 2026-10-12: peel the Effort & Spread script tag before that reversal. The tag is the only new chart.html byte.
// 2026-10-13: peel the Correlation Matrix heatmap hook in jh-chart-tvrail.js before the census hash.
// 2026-10-14: peel the Auction tab (engine button, rail meta, chart.html cache) before that census.
// 2026-10-14: peel the heatmap cache-bust so the auction census still sees the prior bytes.
// 2026-10-21: peel the auto-hide cache-bust (session pin, position-based hiding) before the CDS census.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'../..'),transition=require('../fixtures/chart-macro-line/transition.json'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
const EVR_TAG='<script src="/jh-chart-evr.js?v=20261012evr"></script>\n';
const EVR_AFTER='e27c4f331af871ee8d71835631d6276a0328ba8e301e72a38302fe016e141385';
const EVR_BEFORE='566c14540edf2bc18ba28bb86310510794651f431b1ccd121521d69ed4b11af9';
function peelEvr(raw,file){
 if(file!=='chart.html'||hash(raw)!==EVR_AFTER)return raw;
 assert.equal(raw.split(EVR_TAG).length,2,'chart.html Effort & Spread tag');
 raw=raw.replace(EVR_TAG,'');
 assert.equal(hash(raw),EVR_BEFORE,'chart.html');
 return raw;
}
const SOS_FROM='src="/jh-chart-distribution.js?v=20261012sosoff"';
const SOS_TO='src="/jh-chart-distribution.js?v=20260925-dist"';
const SOS_AFTER='cd53800c27da1e73f840a63906da4cdfe65a4cd55d7d6b99bdff929dc18db9d9';
const SOS_BEFORE='e27c4f331af871ee8d71835631d6276a0328ba8e301e72a38302fe016e141385';
function peelSos(raw,file){
 if(file!=='chart.html'||hash(raw)!==SOS_AFTER)return raw;
 assert.equal(raw.split(SOS_FROM).length,2,'chart.html SOS/DIST cache');
 raw=raw.replace(SOS_FROM,SOS_TO);
 assert.equal(hash(raw),SOS_BEFORE,'chart.html');
 return raw;
}
const CORR_AFTER='64df1eb0c10f2b69708c6e840bdc64f31e904e7aa9c328f63cc423f0f495e546';
const CORR_BEFORE='0f28ea95d3899708d4f0f4967670a97070a9aa70a0bbd3554d795a1f4800bce0';
const CORR_FROM="    if (kind === \"corr\") {\n      var host = document.getElementById(\"ws-corr\");\n      host.style.padding = \"0\";\n      host.style.overflow = \"auto\";\n      host.style.background = \"#131722\";\n      host.innerHTML = \"<div style='padding:16px;color:#787b86'>Loading SPY decline heatmaps\u2026</div>\";\n      var run = function () {\n        if (window.jhCorrAlert) window.jhCorrAlert(host);\n        else host.innerHTML = \"<div style='padding:16px;color:#f23645'>Heatmap script did not start.</div>\";\n      };\n      if (window.jhCorrAlert) run();\n      else {\n        var s = document.createElement(\"script\");\n        s.src = \"/jh-chart-corr-alert.js?v=20261012spyheat\";\n        s.onload = run;\n        s.onerror = function () {\n          host.innerHTML = \"<div style='padding:16px;color:#f23645'>Could not load the heatmap.</div>\";\n        };\n        document.head.appendChild(s);\n      }\n    }";
const CORR_TO="    if (kind === \"corr\") {\n      var host = document.getElementById(\"ws-corr\");\n      host.style.padding = \"16px\";\n      host.style.overflow = \"auto\";\n      host.innerHTML = \"<div style='padding:16px;color:#787b86'>Computing watchlist correlations\u2026</div>\";\n      if (window.jhRenderCorr) {\n        Promise.resolve(window.jhRenderCorr()).then(function () {\n          var src = document.getElementById(\"corr\");\n          if (src) host.innerHTML = src.innerHTML;\n        });\n      } else {\n        host.innerHTML = \"<iframe src='/correlation.html' style='width:100%;height:100%;border:0'></iframe>\";\n      }\n    }";
function peelCorr(raw,file){
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==CORR_AFTER)return raw;
 assert.equal(raw.split(CORR_FROM).length,2,'jh-chart-tvrail.js corr heatmap');
 raw=raw.replace(CORR_FROM,CORR_TO);
 assert.equal(hash(raw),CORR_BEFORE,'jh-chart-tvrail.js');
 return raw;
}

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
const AUC_ENGINE_AFTER='245e9b8d541a5c7de5d96bd3990d07ecfc6735c6f41f2604b455a0ff3585c993';
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
function peelHeatHtml(raw,file){
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

function peelCdsHtml(raw,file){
 const AFTER='43e361e7de3e46fd7c557a7e1b6cf3063a85c3a6c86af812de15adb40c89e5e9';
 const BEFORE='edc3fb88fa09756baeccf417bb3afbad86dbd83f8aa1a38beac3c5c6cb7caa7a';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 const from='&cds=20261015';
 assert.equal(raw.split(from).length,3,'chart.html cds cache');
 raw=raw.split(from).join('');
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
function peelCdsEngine(raw,file){
 const AFTER='7754e62723a02db9a310408d0ab2e1becdd25da18322190a076f4512c3c5ae3c';
 const BEFORE='245e9b8d541a5c7de5d96bd3990d07ecfc6735c6f41f2604b455a0ff3585c993';
 const BTN='      "<button class=\'wsico wsdesk\' id=btn-cds title=\'Credit default swaps\'><span class=g>CDS</span><span class=l>CDS</span></button>"+\n';
 const CLICK='    var bcds=document.getElementById("btn-cds"); if(bcds) bcds.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("cds"); };\n';
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js cds button');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js cds click');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelCdsRail(raw,file){
 const AFTER='34a1101d55ae2e2eeb91b5f06c801aaf1ca85c34040ac4dce680a48b120d062b';
 const BEFORE='33c541f77bf942b178814ea115bb51aa65407e89dfe4d5daa3dd3cee5aabd1c3';
 const LINE='      cds: ["CDS · credit default swaps", "/cds.html"],\n';
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js cds meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
// 2026-10-21: peel the auto-hide cache-bust (jh-chart-autohide.js v=20261021-ah2: session-scoped pin, position-based hiding) before the CDS census.
function peelAutohideHtml(raw,file){
 const AFTER='8267de4a5c4158ca0b2803fc854f20f83c5f0615bedce8bb04f27787106e5543';
 const BEFORE='43e361e7de3e46fd7c557a7e1b6cf3063a85c3a6c86af812de15adb40c89e5e9';
 const FROM='<script src="/jh-chart-autohide.js?v=20261021-ah2"></script>';
 const TO='<script src="/jh-chart-autohide.js?v=20261005-ah1"></script>';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(FROM).length,2,'chart.html auto-hide cache');
 raw=raw.replace(FROM,TO);
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
// 2026-10-21: peel justhodl-tv-bars v1.2 (mode=polygon-append: Polygon grouped-daily appends the US sessions TradingView refuses) before the market-repair census.
function peelPolygonAppend(raw,file){
 const AFTER='ff6912c56bffc9c232277c9b29e9fff0e746868c07f059c7bd7a694389b5fbb6';
 const BEFORE='c5f612fb59f9fe7a60b22f1f211ebfdd685cfd4e597612104996f83f1103311c';
 const HANDLER='    if event.get("mode") == "polygon-append":\n        return polygon_append(event, context)\n';
 if(file!=='aws/lambdas/justhodl-tv-bars/source/lambda_function.py'||hash(raw)!==AFTER)return raw;
 const BLOCK=fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/tv-bars-polygon-append.block.txt'),'utf8');
 assert.equal(raw.split(BLOCK).length,2,'tv-bars polygon-append block');
 assert.equal(raw.split(HANDLER).length,2,'tv-bars polygon-append handler');
 raw=raw.replace(BLOCK,'').replace(HANDLER,'');
 assert.equal(hash(raw),BEFORE,'aws/lambdas/justhodl-tv-bars/source/lambda_function.py');
 return raw;
}
function normalize(raw,file){raw=peelAutohideHtml(raw,file);raw=peelPolygonAppend(raw,file);raw=peelCdsHtml(raw,file);raw=peelCdsEngine(raw,file);raw=peelCdsRail(raw,file);raw=peelHeatHtml(raw,file);raw=peelHeatRail(raw,file);raw=peelAucEngine(raw,file);raw=peelAucHtml(raw,file);raw=peelAucRail(raw,file);raw=peelKind(raw,file);raw=peelKindHtml(raw,file);raw=peelCorr(raw,file);raw=peelSos(raw,file);raw=peelEvr(raw,file);const row=transition.changes[file];if(!row)return normalizeLegacyTest(raw,file);const digest=hash(raw);
 if(digest===row.before_sha256||digest!==row.after_sha256)return raw;
 for(const e of [...row.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file+': '+e.after.slice(0,60));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),row.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
// The approved behaviour change (points → line/candles) rewrote three scalar tests; earlier gates see their exact prior bytes.
const adaptations=require('../fixtures/chart-macro-line/test-adaptations.json');
function normalizeLegacyTest(raw,file){const row=adaptations[file];if(!row||hash(raw)!==row.after_sha256)return raw;
 const old=fs.readFileSync(path.join(R,row.before_path),'utf8');assert.equal(hash(old),row.before_sha256,file);return old;}
module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr};
