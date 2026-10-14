// Exact byte reversal of the 2026-10-05 macro line/candles/change-modes, auto-hide workspace and file-import change only.
// 2026-10-12: peel the Effort & Spread script tag before that reversal. The tag is the only new chart.html byte.
// 2026-10-13: peel the Correlation Matrix heatmap hook in jh-chart-tvrail.js before the census hash.
// 2026-10-14: peel the Auction tab (engine button, rail meta, chart.html cache) before that census.
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
function normalize(raw,file){raw=peelAucEngine(raw,file);raw=peelAucHtml(raw,file);raw=peelAucRail(raw,file);raw=peelKind(raw,file);raw=peelKindHtml(raw,file);raw=peelCorr(raw,file);raw=peelSos(raw,file);raw=peelEvr(raw,file);const row=transition.changes[file];if(!row)return normalizeLegacyTest(raw,file);const digest=hash(raw);
 if(digest===row.before_sha256||digest!==row.after_sha256)return raw;
 for(const e of [...row.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file+': '+e.after.slice(0,60));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),row.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
// The approved behaviour change (points → line/candles) rewrote three scalar tests; earlier gates see their exact prior bytes.
const adaptations=require('../fixtures/chart-macro-line/test-adaptations.json');
function normalizeLegacyTest(raw,file){const row=adaptations[file];if(!row||hash(raw)!==row.after_sha256)return raw;
 const old=fs.readFileSync(path.join(R,row.before_path),'utf8');assert.equal(hash(old),row.before_sha256,file);return old;}
module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr};
