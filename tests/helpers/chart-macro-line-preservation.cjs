// Exact byte reversal of the 2026-10-05 macro line/candles/change-modes, auto-hide workspace and file-import change only.
// 2026-10-12: peel the Effort & Spread script tag before that reversal. The tag is the only new chart.html byte.
// 2026-10-13: peel the Correlation Matrix heatmap hook in jh-chart-tvrail.js before the census hash.
// 2026-10-14: peel the Auction tab (engine button, rail meta, chart.html cache) before that census.
// 2026-10-14: peel the heatmap cache-bust so the auction census still sees the prior bytes.
// 2026-10-21: peel the auto-hide cache-bust (session pin, position-based hiding) before the CDS census.
// 2026-11-05: peel the Leaders tab (engine button + click beside 13F, rail meta, chart.html cache) before every older peel.
// 2026-11-10: peel the Inflows & outflows desk button (engine button + click beside Macro, rail meta) before every older peel.
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
// 2026-11-10: peel the forming-bar merge (Khalid: "SPY delayed almost 24 hours") from justhodl-symdir's warehouse_routing.py before every older peel: the today-overlay reader, us_ticker / with_forming_bar / _banks_for helpers and the banked_ohlc wrapper over the renamed _banked_ohlc - nothing else.
function peelFormingBar(raw,file){
 const plan=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/forming-bar.edits.json'),'utf8'))[file];
 if(!plan||hash(raw)!==plan.after_sha256)return raw;
 for(const e of [...plan.edits].reverse()){assert.equal(raw.split(e.after).length,2,file+' forming bar: '+e.after.slice(0,50));raw=raw.replace(e.after,()=>e.before);}
 assert.equal(hash(raw),plan.before_sha256,file);
 return raw;
}
// 2026-11-02: peel the fleet-wide trigger cap (chain_guard v2) from justhodl-tv-bars and justhodl-symdir before every older peel: the chain_guard import, the guarded Lambda client in place of the raw one, chain_guard.begin at handler entry - nothing else.
function peelTriggerCap(raw,file){
 const plan=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/trigger-cap.edits.json'),'utf8'))[file];
 if(!plan||hash(raw)!==plan.after_sha256)return raw;
 for(const e of [...plan.edits].reverse()){assert.equal(raw.split(e.after).length,2,file+' trigger cap: '+e.after.slice(0,50));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),plan.before_sha256,file);
 return raw;
}
// 2026-10-25: peel justhodl-tv-bars v1.3 (mode=fmp-append: the non-tape chart names advance from the FMP warehouse, proof-gated - FMP_MAP + banked-list membership + continuity check) before the v1.2 peel.
function peelFmpAppend(raw,file){
 const AFTER='7a246ae6cfca9c0619f5873b5de4d4316effe2dc47716247f521b2d8ce4c8fc7';
 const BEFORE='ff6912c56bffc9c232277c9b29e9fff0e746868c07f059c7bd7a694389b5fbb6';
 const HANDLER='    if event.get("mode") == "fmp-append":\n        return fmp_append(event, context)\n';
 if(file!=='aws/lambdas/justhodl-tv-bars/source/lambda_function.py'||hash(raw)!==AFTER)return raw;
 const BLOCK=fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/tv-bars-fmp-append.block.txt'),'utf8');
 assert.equal(raw.split(BLOCK).length,2,'tv-bars fmp-append block');
 assert.equal(raw.split(HANDLER).length,2,'tv-bars fmp-append handler');
 raw=raw.replace(BLOCK,'').replace(HANDLER,'');
 assert.equal(hash(raw),BEFORE,'aws/lambdas/justhodl-tv-bars/source/lambda_function.py');
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
// 2026-10-21: peel the US-yield route change (Yahoo live index first, Treasury par before FRED, no ^IRX for the 2-year) in jh-chart-engine.js and its cache-bust in chart.html.
function peelUs10yEngine(raw,file){
 const AFTER='032822eebe43777ccd2539654b18271a31f382e0cbc215cc8813101998fcecc4';
 const BEFORE='7754e62723a02db9a310408d0ab2e1becdd25da18322190a076f4512c3c5ae3c';
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 const edits=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/engine-us10y-live.edits.json'),'utf8'));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,'jh-chart-engine.js us10y: '+e.after.slice(0,50));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelUs10yHtml(raw,file){
 const AFTER='2acc793b989d8cc26247abd58fc97341ca92df6e9ba0e171aec2cd089f0620ee';
 const BEFORE='8267de4a5c4158ca0b2803fc854f20f83c5f0615bedce8bb04f27787106e5543';
 const FROM='&cds=20261015&us10y=20261021"';
 const TO='&cds=20261015"';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(FROM).length,2,'chart.html us10y cache');
 raw=raw.replace(FROM,TO);
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
// 2026-10-21 (b): peel the yield-symbol routing (US10Y -> ^TNX, US02Y -> ustpar:2Y, US05Y -> ^FVX, US30Y -> ^TYX) in the engine and the watchlist, and the chart.html cache-bust.
function peelUs10yRouteEngine(raw,file){
 const AFTER='c689e7bb0cf37fe011e6812170c0a5c8ca85b8749f38d148978c3f30f5b81b23';
 const BEFORE='032822eebe43777ccd2539654b18271a31f382e0cbc215cc8813101998fcecc4';
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 const edits=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/engine-us10y-route.edits.json'),'utf8'));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,'jh-chart-engine.js us10y route: '+e.after.slice(0,50));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelUs10yRouteWatch(raw,file){
 const AFTER='d7dd68f586c33aca02605eec9ed8bd4ef8ecdc067fcb699005e854e67609a55a';
 const BEFORE='36b2ff12f5dd54835fe3593e8e6eba4a42453293e3ea4177e86c05053ecd7f1f';
 if(file!=='jh-chart-tvwatch.js'||hash(raw)!==AFTER)return raw;
 const edits=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/tvwatch-us10y-route.edits.json'),'utf8'));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,'jh-chart-tvwatch.js us10y route: '+e.after.slice(0,50));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),BEFORE,'jh-chart-tvwatch.js');
 return raw;
}
function peelUs10yRouteHtml(raw,file){
 const AFTER='543cc410c98d1b75f252c3f18b8d359d744c57b408fcdc86e16aec8a27016b05';
 const BEFORE='2acc793b989d8cc26247abd58fc97341ca92df6e9ba0e171aec2cd089f0620ee';
 const FROM='&cds=20261015&us10y=20261021c"';
 const TO='&cds=20261015&us10y=20261021"';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(FROM).length,2,'chart.html us10y route cache');
 raw=raw.replace(FROM,TO);
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
function peelUs10yRouteIdentityTest(raw,file){
 const AFTER='e3e2ee96066b0f6cb89e5f35b309a967cf6897d32ec5420c9d0c51331bf5b86a';
 const BEFORE='dce7c0e2efb8a808f034cc5c5bd928f67e7eb2a4c11f1ebee752847ce0fbc6c5';
 if(file!=='tests/watchlist-identity.test.js'||hash(raw)!==AFTER)return raw;
 const edits=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/identity-test-us10y-route.edits.json'),'utf8'));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,'watchlist-identity test us10y route');raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),BEFORE,'tests/watchlist-identity.test.js');
 return raw;
}
function peelUs10yRouteObsTest(raw,file){
 const AFTER='6907c99d3352c781623d8683dcfdcc0609fced425a22d83d352d1b1afe262367';
 const BEFORE='6c82a92f11ff9408cfbdc81e941635a29e66a3e5d79c6795fb135458e1fe56ce';
 if(file!=='tests/warehouse-observations.test.js'||hash(raw)!==AFTER)return raw;
 const edits=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/warehouse-observations-test-us10y-route.edits.json'),'utf8'));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,'warehouse-observations test us10y route');raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),BEFORE,'tests/warehouse-observations.test.js');
 return raw;
}
// 2026-11-05: peel the Leaders workspace tab (engine button beside 13F + its click, rail meta, chart.html cache-bust) before every older peel.
function peelLeadersHtml(raw,file){
 const AFTER='a7f076b4c5b328fd2d6832daf4b0e3be74f7f945e1aaa03d76532b5a6ea1a1d6';
 const BEFORE='543cc410c98d1b75f252c3f18b8d359d744c57b408fcdc86e16aec8a27016b05';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 const from='&ldr=20261105';
 assert.equal(raw.split(from).length,3,'chart.html leaders cache');
 raw=raw.split(from).join('');
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
function peelLeadersEngine(raw,file){
 const AFTER='7fe719832a34061e375cf0868455049d1c8c2565a8fa6b28bb6e262dd9d08028';
 const BEFORE='c689e7bb0cf37fe011e6812170c0a5c8ca85b8749f38d148978c3f30f5b81b23';
 const BTN='      "<button class=\'wsico wsdesk\' id=btn-ldr title=\'Leaders · industry leaders vs S&P 500\'><span class=g>👑</span><span class=l>Leaders</span></button>"+\n';
 const CLICK='    var bldr=document.getElementById("btn-ldr"); if(bldr) bldr.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace("leaders"); };\n';
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js leaders button');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js leaders click');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelLeadersRail(raw,file){
 const AFTER='cde2a7d704a24a5682119d4331e852306c01fdd28c9f2c3e647d7f9157ac3c40';
 const BEFORE='34a1101d55ae2e2eeb91b5f06c801aaf1ca85c34040ac4dce680a48b120d062b';
 const LINE='      leaders: ["Leaders · industry leaders vs S&P 500", "/leaders.html"],\n';
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js leaders meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
// 2026-11-05 (b): peel the Capitulation workspace re-route (rail meta capit -> /capitulation-desk.html, chart.html rail cache-bust) before the Leaders peels.
function peelCapitHtml(raw,file){
 const AFTER='11c209334122515ebe61e6ebb10f76aa3c5f05f2d2b518a82e16325613257827';
 const BEFORE='a7f076b4c5b328fd2d6832daf4b0e3be74f7f945e1aaa03d76532b5a6ea1a1d6';
 const FROM='&cds=20261015&capit=20261105&ldr=20261105"></script>';
 const TO='&cds=20261015&ldr=20261105"></script>';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(FROM).length,2,'chart.html capitulation rail cache');
 raw=raw.replace(FROM,TO);
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
function peelCapitRail(raw,file){
 const AFTER='2455f7e859dfa18f50d75f669fcd95538eb32944ba9f1e1b5e10dbee38393a79';
 const BEFORE='cde2a7d704a24a5682119d4331e852306c01fdd28c9f2c3e647d7f9157ac3c40';
 const FROM='      capit: ["Capitulation · panic volume at the lows", "/capitulation-desk.html"],\n';
 const TO='      capit: ["Capitulation · panic volume at the lows", "/wyckoff-desk.html?tab=capit"],\n';
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(FROM).length,2,'jh-chart-tvrail.js capitulation meta');
 raw=raw.replace(FROM,TO);
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
// 2026-11-10 (c): peel the Inflows & outflows desk button (engine button + click beside Macro, rail meta) before every older peel.
function peelFlowsEngine(raw,file){
 const AFTER='a31450e192466e529396caff40e57ef177cdb8ab42420e81dc89856c894b0a3e';
 const BEFORE='7fe719832a34061e375cf0868455049d1c8c2565a8fa6b28bb6e262dd9d08028';
 const BTN="      \"<button class='wsico wsdesk' id=btn-flows title='Inflows & outflows'><span class=g>⇄</span><span class=l>Flows</span></button>\"+\n";
 const CLICK="    var bflw=document.getElementById(\"btn-flows\"); if(bflw) bflw.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace(\"flows\"); };\n";
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js flows button');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js flows click');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelFlowsRail(raw,file){
 const AFTER='d0587c69cc2eaa07b5b321af2c8ee48ad2dad0054a496e02695d87d416b2cc92';
 const BEFORE='2455f7e859dfa18f50d75f669fcd95538eb32944ba9f1e1b5e10dbee38393a79';
 const LINE="      flows: [\"Inflows & outflows\", \"/inflows-outflows.html\"],\n";
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js flows meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
function peelCandleClarity(raw,file){
 const expect={
  'jh-chart-engine.js':{now:'5532845c393c4561aab72b10f095961a0c01346fdaff2ca475c47594e638ba20',prior:'c5df7d4d54010ebf1b6ad76a33cffc465086c99b4ae75ec8ab085d428ade27b7',pairs:[{"old": "grid:\"rgba(42,46,57,0.70)\",gridH:\"#2a2e39\"", "new": "grid:\"rgba(42,46,57,0.70)\",gridH:\"rgba(42,46,57,0.70)\"", "n": 1}, {"old": "grid:\"#f0f3fa\",gridH:\"#e8ecf5\"", "new": "grid:\"#f0f3fa\",gridH:\"#f0f3fa\"", "n": 1}, {"old": "var pl=mainSeries.createPriceLine({ price:+px, color:color||\"#787b86\", lineWidth:1, lineStyle:style==null?2:style, axisLabelVisible:true, title:title||\"\" });", "new": "var quiet=key===\"H\"||key===\"L\"||key===\"POC\"||key===\"VAH\"||key===\"VAL\";\n      var pl=mainSeries.createPriceLine({ price:+px, color:color||\"#787b86\", lineWidth:1, lineStyle:style==null?2:style, lineVisible:!quiet, axisLabelVisible:true, title:title||\"\" });", "n": 1}, {"old": "try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}", "new": "try{ requestAnimationFrame(function(){ if(current()){ refreshHiLoVP(); try{ barReadout(null); }catch(eBr){} } }); }catch(eR){}", "n": 1}, {"old": "try{ requestAnimationFrame(function(){ refreshHiLoVP(); }); }catch(e2){}", "new": "try{ requestAnimationFrame(function(){ refreshHiLoVP(); try{ barReadout(null); }catch(eBr){} }); }catch(e2){}", "n": 1}, {"old": "  chart.subscribeCrosshairMove(function(param){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)){if(hud){hud.style.display=\"\";hud.textContent=observationText(lastBars,param&&param.time);}if(ohlc)ohlc.textContent=observationText(lastBars,param&&param.time);renderDwin(param);return;}\n    if(!param||!param.time||!mainSeries){ hud.style.display=\"none\"; if(dwinOn){ var dw=document.getElementById(\"dwin\"); if(dw) dw.style.opacity=\".55\"; } return; }\n    var d=param.seriesData.get(mainSeries); if(!d){ hud.style.display=\"none\"; return; }\n    var o=d.open!=null?d.open:d.value, h=d.high!=null?d.high:d.value, l=d.low!=null?d.low:d.value, c=d.close!=null?d.close:d.value;\n    var extra=\"\", iH, bar=null, ix=-1;\n    if(lastBars && lastBars.length){\n      for(iH=0;iH<lastBars.length;iH++) if(lastBars[iH].time===param.time){ bar=lastBars[iH]; ix=iH; break; }\n    }\n    if(bar){\n      var rH=rvolAt(lastBars,ix,20);\n      var atrV=valAt(lastAtrPts, param.time);\n      var vwH=valAt(lastHudVwap, param.time);\n      extra=\"  ATR \"+(atrV!=null?fmt(atrV):\"—\")+\"  RVOL \"+(rH!==null?(rH>0&&rH<0.01?\"&lt;0.01\":rH.toFixed(2))+\"x\":\"Unavailable\");\n      if(vwH!=null && c) extra+=\"  VWAP \"+((c-vwH)>=0?\"+\":\"\")+((c-vwH)/vwH*100).toFixed(2)+\"%\";\n      if(lastVP.poc!=null) extra+=\"  POC \"+fmt(lastVP.poc)+(c>lastVP.vah?\" ↑VA\": c<lastVP.val?\" ↓VA\":\" inVA\");\n      if(lastAdr && lastAdr.adr) extra+=\"  ADR \"+lastAdr.used.toFixed(0)+\"%\";\n    }\n    hud.style.display=\"block\";\n    hud.innerHTML=new Date((param.time+tzOff*3600)*1000).toISOString().slice(0,16).replace(\"T\",\" \")+\" \"+tzName+\"  O \"+fmt(o)+\" H \"+fmt(h)+\" L \"+fmt(l)+\" C \"+fmt(c)+extra;\n    if(ohlc) ohlc.innerHTML=\"O \"+fmt(o)+\"  H <span class=up>\"+fmt(h)+\"</span>  L <span class=dn>\"+fmt(l)+\"</span>  C \"+fmt(c)+extra;\n    renderLegend(param.time);\n    renderDwin(param);\n  });\n", "new": "  /* candle-clarity-20261112 */\n  function barReadout(time, seriesPt){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)) return;\n    if(!lastBars||!lastBars.length) return;\n    var ix=-1, bar=null, iH;\n    if(time!=null){\n      for(iH=0;iH<lastBars.length;iH++) if(lastBars[iH].time===time){ bar=lastBars[iH]; ix=iH; break; }\n    }\n    var pin=!bar;\n    if(pin){ bar=lastBars[lastBars.length-1]; ix=lastBars.length-1; time=bar.time; seriesPt=null; }\n    var o,h,l,c;\n    if(seriesPt){\n      o=seriesPt.open!=null?seriesPt.open:seriesPt.value;\n      h=seriesPt.high!=null?seriesPt.high:seriesPt.value;\n      l=seriesPt.low!=null?seriesPt.low:seriesPt.value;\n      c=seriesPt.close!=null?seriesPt.close:seriesPt.value;\n    } else {\n      o=bar.open; h=bar.high; l=bar.low; c=bar.close;\n    }\n    var extra=\"\", prev=ix>0?lastBars[ix-1]:null;\n    var curR=roundTick(c), prevR=prev?roundTick(prev.close):null;\n    if(prevR!=null && isFinite(prevR) && prevR!==0 && isFinite(curR)){\n      var dlt=curR-prevR, up=dlt>=0;\n      extra+=\"  <span class=\"+(up?\"up\":\"dn\")+\">\"+(up?\"+\":\"\")+fmt(dlt)+\" \"+fmtXs(dlt/prevR)+\"</span>\";\n    }\n    extra+=\"  Vol \"+fmtVol(reportedVolume(bar.volume));\n    var rH=rvolAt(lastBars,ix,20);\n    var atrV=valAt(lastAtrPts, time);\n    var vwH=valAt(lastHudVwap, time);\n    extra+=\"  ATR \"+(atrV!=null?fmt(atrV):\"—\")+\"  RVOL \"+(rH!==null?(rH>0&&rH<0.01?\"<0.01\":rH.toFixed(2))+\"x\":\"Unavailable\");\n    if(vwH!=null && c) extra+=\"  VWAP \"+((c-vwH)>=0?\"+\":\"\")+((c-vwH)/vwH*100).toFixed(2)+\"%\";\n    if(lastVP.poc!=null) extra+=\"  POC \"+fmt(lastVP.poc)+(c>lastVP.vah?\" ↑VA\": c<lastVP.val?\" ↓VA\":\" inVA\");\n    if(lastAdr && lastAdr.adr) extra+=\"  ADR \"+lastAdr.used.toFixed(0)+\"%\";\n    var body=\"O \"+fmt(o)+\" H \"+fmt(h)+\" L \"+fmt(l)+\" C \"+fmt(c)+extra;\n    if(hud){\n      if(pin) hud.style.display=\"none\";\n      else {\n        hud.style.display=\"block\";\n        hud.innerHTML=new Date((time+tzOff*3600)*1000).toISOString().slice(0,16).replace(\"T\",\" \")+\" \"+tzName+\"  \"+body;\n      }\n    }\n    if(ohlc) ohlc.innerHTML=body;\n  }\n  chart.subscribeCrosshairMove(function(param){\n    var hud=document.getElementById(\"hud\"), ohlc=document.getElementById(\"ohlc\");\n    if(observationId(active)){if(hud){hud.style.display=\"\";hud.textContent=observationText(lastBars,param&&param.time);}if(ohlc)ohlc.textContent=observationText(lastBars,param&&param.time);renderDwin(param);return;}\n    if(!param||!param.time||!mainSeries){ try{ barReadout(null); }catch(e){} if(dwinOn){ var dw=document.getElementById(\"dwin\"); if(dw) dw.style.opacity=\".55\"; } return; }\n    var d=param.seriesData.get(mainSeries); if(!d){ try{ barReadout(null); }catch(e2){} return; }\n    try{ barReadout(param.time, d); }catch(e3){}\n    renderLegend(param.time);\n    renderDwin(param);\n  });\n", "n": 1}]},
  'chart.html':{now:'7422d4985604ff15f771b05b44bf20239328b03720cb88516028e720a005d8b6',prior:'3a5abb78e73edae965c2a19fbebc943102f7a6cff35b93af81fe46cda81ca59f',pairs:[{"old": "us10y=20261021c&fit=20261112&ldr=20261105", "new": "us10y=20261021c&fit=20261112&wick=20261112&ldr=20261105", "n": 1}]}
 };
 const row=expect[file];
 if(!row||hash(raw)!==row.now)return raw;
 let back=raw;
 for(let i=row.pairs.length-1;i>=0;i--){
  const p=row.pairs[i];
  const hits=back.split(p.new).length-1;
  if(hits!==p.n)throw new Error('candle-clarity peel count '+hits+' wanted '+p.n+' in '+file);
  back=back.split(p.new).join(p.old);
 }
 if(hash(back)!==row.prior)throw new Error('candle-clarity peel hash '+file+' '+hash(back));
 return back;
}

function peelScreenFit(raw,file){
 const expect={
  'jh-chart-engine.js':{now:'c5df7d4d54010ebf1b6ad76a33cffc465086c99b4ae75ec8ab085d428ade27b7',prior:'a31450e192466e529396caff40e57ef177cdb8ab42420e81dc89856c894b0a3e',pairs:[{"old": "timeScale:{ borderColor:p.border, borderVisible:true, timeVisible:true, secondsVisible:sec, rightOffset:12, ticksVisible:true, lockVisibleTimeRangeOnResize:true }", "new": "timeScale:{ borderColor:p.border, borderVisible:true, timeVisible:true, secondsVisible:sec, rightOffset:12, ticksVisible:true, lockVisibleTimeRangeOnResize:false }", "n": 1}, {"old": "if(preserveView&&saved)chart.timeScale().setVisibleLogicalRange(saved);else chart.timeScale().fitContent();", "new": "if(preserveView&&saved)chart.timeScale().setVisibleLogicalRange(saved);else { try{ applyView(); }catch(e){ chart.timeScale().fitContent(); } }", "n": 1}, {"old": "    } else {\n      try{ chart.timeScale().fitContent(); }catch(e){}\n    }\n    try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}", "new": "    } else {\n      try{ applyView(); }catch(e){ try{ chart.timeScale().fitContent(); }catch(e2){} }\n    }\n    try{ requestAnimationFrame(function(){ if(current())refreshHiLoVP(); }); }catch(eR){}", "n": 1}, {"old": "  function showRecentBars(n){", "new": "  /* screen-fit-20261112 */\n  var viewMode=\"screen\";\n  var scaleBeforeAll=null;\n  function plotWidthPx(){\n    var w=(host&&host.clientWidth)||640;\n    var axis=76;\n    try{ var pw=chart&&chart.priceScale(\"right\").width(); if(pw>40) axis=pw; }catch(e){}\n    return Math.max(240, w-axis);\n  }\n  function barTimeSec(b){\n    if(!b) return null;\n    var t=b.time;\n    if(typeof t===\"number\" && isFinite(t)) return t;\n    if(t && typeof t===\"object\" && t.year) return Date.UTC(t.year, (t.month||1)-1, t.day||1)/1000;\n    if(typeof t===\"string\" && /^\\d{4}-\\d{2}-\\d{2}/.test(t)){ var ms=Date.parse(t+\"T00:00:00Z\"); if(isFinite(ms)) return ms/1000; }\n    return null;\n  }\n  function syncScale(){\n    try{ chart.priceScale(\"right\").applyOptions({ mode: rightScaleMode() }); }catch(e){}\n  }\n  function allScaleOk(bars){\n    var i;\n    for(i=0;i<bars.length;i++) if(!(bars[i]&&bars[i].close>0)) return false;\n    return true;\n  }\n  function leaveAllScale(){\n    if(scaleBeforeAll==null) return;\n    scaleMode=scaleBeforeAll;\n    scaleBeforeAll=null;\n    syncScale();\n  }\n  function enterAllScale(){\n    if(mode!==\"price\" || comparingOn() || scaleMode===1) return;\n    if(!allScaleOk(lastBars||[])) return;\n    scaleBeforeAll=scaleMode;\n    scaleMode=1;\n    syncScale();\n  }\n  function applyScreenFit(target){\n    target=target||chart;\n    var bars=lastBars||[];\n    var n=bars.length;\n    var barPx=7, rightPad=12;\n    var slots=Math.max(rightPad+24, Math.floor(plotWidthPx()/barPx));\n    var count=Math.min(n, Math.max(24, slots-rightPad));\n    try{ target.timeScale().applyOptions({ barSpacing:barPx, rightOffset:rightPad, lockVisibleTimeRangeOnResize:false }); }catch(e){}\n    if(n<8){ try{ target.timeScale().fitContent(); }catch(e2){} return; }\n    try{ target.timeScale().setVisibleLogicalRange({ from:n-count, to:(n-1)+rightPad }); }\n    catch(e3){ try{ target.timeScale().fitContent(); }catch(e4){} }\n  }\n  function applyRangeYears(years){\n    var bars=lastBars||[];\n    var n=bars.length;\n    if(n<8 || !chart){ applyScreenFit(); return; }\n    var last=barTimeSec(bars[n-1]);\n    var from=0, i, ts;\n    if(last!=null){\n      var cut=last-years*365.25*86400;\n      for(i=0;i<n;i++){ ts=barTimeSec(bars[i]); if(ts!=null && ts>=cut){ from=i; break; } }\n    } else from=Math.max(0, n-Math.round(years*252));\n    if(n-from<8) from=Math.max(0, n-8);\n    try{ chart.timeScale().applyOptions({ rightOffset:8, lockVisibleTimeRangeOnResize:true }); }catch(e){}\n    try{ chart.timeScale().setVisibleLogicalRange({ from:from, to:(n-1)+8 }); }\n    catch(e2){ try{ chart.timeScale().fitContent(); }catch(e3){} }\n  }\n  function applyAllHistory(){\n    try{ chart.timeScale().applyOptions({ lockVisibleTimeRangeOnResize:true }); }catch(e){}\n    try{ chart.timeScale().fitContent(); }catch(e2){}\n  }\n  function applyView(){\n    if(viewMode===\"6m\") return applyRangeYears(0.5);\n    if(viewMode===\"1y\") return applyRangeYears(1);\n    if(viewMode===\"5y\") return applyRangeYears(5);\n    if(viewMode===\"all\"){\n      if(scaleMode===1 && !allScaleOk(lastBars||[])){\n        scaleMode=scaleBeforeAll!=null?scaleBeforeAll:0;\n        syncScale();\n      }\n      return applyAllHistory();\n    }\n    return applyScreenFit();\n  }\n  function setViewMode(next){\n    next=next||\"screen\";\n    if(next!==\"all\") leaveAllScale();\n    viewMode=next;\n    if(viewMode===\"all\") enterAllScale();\n    applyView();\n    preserveView=true;\n    try{ renderTf(); }catch(e){}\n    try{ requestAnimationFrame(function(){ refreshHiLoVP(); }); }catch(e2){}\n  }\n  function viewChipHtml(){\n    var views=[[\"screen\",\"Screen\",\"Readable window, about 7px per bar. Key 0\"],[\"6m\",\"6M\",\"Six months\"],[\"1y\",\"1Y\",\"One year\"],[\"5y\",\"5Y\",\"Five years\"],[\"all\",\"All\",\"All history, log when every close is positive. Shift+0\"]];\n    return views.map(function(v){\n      return \"<button class='\"+(viewMode===v[0]?\"on\":\"\")+\"' data-view='\"+v[0]+\"' title='\"+v[2]+\"'>\"+v[1]+\"</button>\";\n    }).join(\"\")+\"<span class=sep></span>\";\n  }\n  function copyMainRange(target){\n    try{\n      var vr=chart&&chart.timeScale().getVisibleLogicalRange();\n      if(vr&&vr.from!=null&&vr.to!=null){ target.timeScale().setVisibleLogicalRange(vr); return; }\n    }catch(e){}\n    try{ target.timeScale().fitContent(); }catch(e2){}\n  }\n  function showRecentBars(n){\n", "n": 1}, {"old": "refs[i].timeScale().fitContent()", "new": "copyMainRange(refs[i])", "n": 2}, {"old": "      tfHtml+\n      \"<button class=drop id=btn-tfmore title='All intervals'>\u25be</button>\"+\n      \"<span class=sep></span>\"+", "new": "      tfHtml+\n      \"<button class=drop id=btn-tfmore title='All intervals'>\u25be</button>\"+\n      \"<span class=sep></span>\"+\n      viewChipHtml()+", "n": 1}, {"old": "document.querySelectorAll(\"#tfbar [data-tf]\").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });", "new": "document.querySelectorAll(\"#tfbar [data-tf]\").forEach(function(b){ b.onclick=function(){ tf=b.dataset.tf; renderTf(); load(); }; });\n    document.querySelectorAll(\"#tfbar [data-view]\").forEach(function(b){ b.onclick=function(){ setViewMode(b.dataset.view); }; });", "n": 1}, {"old": "if(b.dataset.a===\"rst\"){ try{ chart.timeScale().fitContent(); }catch(e){} }", "new": "if(b.dataset.a===\"rst\"){ try{ setViewMode(\"screen\"); }catch(e){} }", "n": 1}, {"old": "if(e.key===\"0\"){ try{ chart.timeScale().fitContent(); }catch(err){} return; }", "new": "if(e.key===\"0\" || (e.shiftKey && e.code===\"Digit0\")){ e.preventDefault(); try{ setViewMode(e.shiftKey?\"all\":\"screen\"); }catch(err){ try{ chart.timeScale().fitContent(); }catch(e2){} } return; }", "n": 1}, {"old": "[\"+ / \u2212 / 0\",\"Zoom in / out / fit\"]", "new": "[\"+ / \u2212 / 0\",\"Zoom in / out / screen\"],[\"Shift+0\",\"All history\"]", "n": 1}]},
  'chart.html':{now:'3a5abb78e73edae965c2a19fbebc943102f7a6cff35b93af81fe46cda81ca59f',prior:'11c209334122515ebe61e6ebb10f76aa3c5f05f2d2b518a82e16325613257827',pairs:[{"old": "us10y=20261021c&ldr=20261105", "new": "us10y=20261021c&fit=20261112&ldr=20261105", "n": 1}]},
  'tests/helpers/chart-census-additional-preservation.cjs':{now:'7aa34b1a8a74cf1c0f092c1498cc8e557ec29b1e6b5398a85f44f3e1fb65fe0c',prior:'3230411de4b28694ce88a73a87fcb7acb21f59a243f09366a32e73f8f74d1b48',pairs:[{"old": "function normalize(raw,file){raw=require(\"./chart-imf-preservation.cjs\").normalize(raw,file);", "new": "function normalize(raw,file){raw=require(\"./chart-macro-line-preservation.cjs\").peelScreenFit(raw,file);raw=require(\"./chart-imf-preservation.cjs\").normalize(raw,file);", "n": 1}]}
 };
 const row=expect[file];
 if(!row||hash(raw)!==row.now)return raw;
 let back=raw;
 for(let i=row.pairs.length-1;i>=0;i--){
  const p=row.pairs[i];
  const hits=back.split(p.new).length-1;
  if(hits!==p.n)throw new Error('screen-fit peel count '+hits+' wanted '+p.n+' in '+file);
  back=back.split(p.new).join(p.old);
 }
 if(hash(back)!==row.prior)throw new Error('screen-fit peel hash '+file+' '+hash(back));
 return back;
}
// 2026-11-17: peel the three FRED desks (Dollar, Outright, BofA buttons + clicks beside 13F, rail meta) before every older peel.
function peelFredDesksEngine(raw,file){
 const AFTER='84081da5df78c806ae366a771e64533556897a6b1eb39c1fd5f1cdca99887860';
 const BEFORE='5532845c393c4561aab72b10f095961a0c01346fdaff2ca475c47594e638ba20';
 const BTN="      \"<button class='wsico wsdesk' id=btn-usd title='Dollar indexes · the DXY and every FRED dollar index'><span class=g>$</span><span class=l>Dollar</span></button>\"+\n      \"<button class='wsico wsdesk' id=btn-soma title='Fed securities held outright · every H.4.1 line on FRED'><span class=g>Fed</span><span class=l>Outright</span></button>\"+\n      \"<button class='wsico wsdesk' id=btn-bofa title='ICE BofA indices · spreads, yields and total return'><span class=g>ICE</span><span class=l>BofA</span></button>\"+\n";
 const CLICK="    var busd=document.getElementById(\"btn-usd\"); if(busd) busd.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace(\"dollar\"); };\n    var bsoma=document.getElementById(\"btn-soma\"); if(bsoma) bsoma.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace(\"soma\"); };\n    var bbofa=document.getElementById(\"btn-bofa\"); if(bbofa) bbofa.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace(\"bofa\"); };\n";
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js FRED desk buttons');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js FRED desk clicks');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelFredDesksRail(raw,file){
 const AFTER='5b3c7f2cfd7604e34acc56ab0917cb7af99adc4c8f55bf360dc26092f6efdefa';
 const BEFORE='d0587c69cc2eaa07b5b321af2c8ee48ad2dad0054a496e02695d87d416b2cc92';
 const LINE="      dollar: [\"Dollar indexes · DXY and FRED\", \"/dollar-indexes.html\"],\n      soma: [\"Fed securities held outright\", \"/securities-held-outright.html\"],\n      bofa: [\"ICE BofA indices\", \"/ice-bofa-indices.html\"],\n";
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js FRED desk meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
// 2026-11-19: peel the mark-desk wiring (rail meta dist -> /wyckoff-desk.html?tab=dist; the Acc / Bot / Dist marking functions wrapped so a click that
// turns the marks on also opens the all-stock desk, jhEnsureMarks and ?marks=) before the FRED-desk peels. Khalid: "accumulation, bottom,
// distribution engines should be built for the icons there it should show all the corresponding stocks".
// 2026-11-19 "Fix all of them": index ids (I:SPX, NASDAQ:NDX, DJ:DJI ...) take the live index route, a cached frame keeps its own label (engine); one reviewed index table in the
// watchlist router and the route exported for spread legs; the identity test re-applies the router edits. Every edit is a before/after pair under tests/fixtures/chart-index-routes.
function peelIndexRoutes(raw,file){
 const plans={
  'jh-chart-engine.js':{after:'bf37672d6bdd75d04ae60faa1b38e546c9df7831535644cfdbdc8e1b0d6d7989',before:'84081da5df78c806ae366a771e64533556897a6b1eb39c1fd5f1cdca99887860',edits:['engine.edits.json']},
  'jh-chart-tvwatch.js':{after:'73960757ad566281eb496e729cca8e2c1b4aff32f4f42e75b7f6ff05a3dd4a95',before:'15476552696562d0e9c9d0e7f62b49bdc8314fadc5141591b0cab799bb16a9af',edits:['tvwatch.edits.json','tvwatch-export.edits.json']},
  'tests/watchlist-identity.test.js':{after:'9bb2fdc7e091b7673446496d03e577a8495a8a2341300b39bbb6f0dc45a55fa2',before:'869b3cb8b924d7e9d18097a717198ab8de614419b5a5adedde4c0c230fffca89',edits:['identity-test.edits.json']},
  'aws/lambdas/justhodl-symdir/source/lambda_function.py':{after:'75b2e476db1634b4a410e47931d300cc386967f9c12771f69fa1d4018b0d4a53',before:'1c10a8a7e0f794f109bdc70f1c543bdca12e998f0421c3b12a0adce7709b4c22',edits:['symdir.edits.json']}
 },plan=plans[file];
 if(!plan||hash(raw)!==plan.after)return raw;
 const edits=[].concat(...plan.edits.map(f=>JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-index-routes',f),'utf8'))));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,file+' index routes: '+e.after.slice(0,50));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),plan.before,file);
 return raw;
}
// 2026-11-21 (Khalid: "INDEX:BTCUSD - No supported chart route; chart unchanged. i cant pull btc on chart. investigate why i cant pull symbols out of search bar"): one reviewed crypto
// table at the top of the watchlist router (a spot pair of a banked coin under a crypto venue or INDEX -> the coin's own tape; CME bitcoin / ether futures), which also outranks the
// native route; the engine opens the directory's X:<coin>USD ids on that tape and joins an outside daily series only when it is the same coin; the identity test re-applies the router
// edit and keeps the native-route cases on a coin with no banked bars. Every edit is a before/after pair under tests/fixtures/chart-crypto-routes.
// 2026-11-22: the Sovereign stress desk (button + click after Repo on the chart's bar, its rail entry, and the two cache-busts on chart.html
// so a browser holding the old engine or rail fetches the new one) is peeled before every older peel.
function peelSovDeskEngine(raw,file){
 const AFTER='dcefd93abba4c5523277b7342d8b29a22b59d789904c671f8ace098c07674913';
 const BEFORE='a61ccf2066f6fd2352bb4f71c22583094f65421101760f8ec2edb3f0e0b492cc';
 const BTN="      \"<button class='wsico wsdesk' id=btn-sov title='Sovereign stress · SovCISS, spreads to Germany, TARGET, crisis peaks'><span class=g>Sov</span><span class=l>Sovereign</span></button>\"+\n";
 const CLICK="    var bsov=document.getElementById(\"btn-sov\"); if(bsov) bsov.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace(\"sov\"); };\n";
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js Sovereign desk button');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js Sovereign desk click');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelSovDeskRail(raw,file){
 const AFTER='2f47a463a82cc92badecd64fbc4e2957a26867ef17d90d244b5fc1fc7a38679d';
 const BEFORE='eb7f074d2d5d80ea2a60801d9e0726849a6e430f73e8c911c7e613e14ba3633a';
 const LINE="      sov: [\"Sovereign stress · SovCISS, spreads, TARGET, crisis peaks\", \"/sovereign-desk.html\"],\n";
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js Sovereign desk meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
function peelSovDeskHtml(raw,file){
 const AFTER='46a52f2ac304b333056d4b0a7b0ece2c5b91dbbab093e2864d896dfff450bbf2';
 const BEFORE='ecd5c656a48d07fab61672114af310645c014e140e830f0d22a670f1c36199cf';
 const PAIRS=[["&cds=20261015&marks=20261119&repo=20261121&sov=20261122&capit=20261105&ldr=20261105\"></script>","&cds=20261015&marks=20261119&repo=20261121&capit=20261105&ldr=20261105\"></script>"],
              ["&fit=20261112&wick=20261112&repo=20261121&sov=20261122&ldr=20261105\"></script>","&fit=20261112&wick=20261112&repo=20261121&ldr=20261105\"></script>"]];
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 for(const [from,to] of PAIRS){assert.equal(raw.split(from).length,2,'chart.html Sovereign desk cache-bust');raw=raw.replace(from,to);}
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
// 2026-11-21: the Repo desk (button + click after CDS on the chart's bar, its rail entry, and the two cache-busts on chart.html
// so a browser holding the old engine or rail fetches the new one) is peeled before every older peel.
function peelRepoDeskEngine(raw,file){
 const AFTER='a61ccf2066f6fd2352bb4f71c22583094f65421101760f8ec2edb3f0e0b492cc';
 const BEFORE='8860815ca07acac8d1823ff8d99f1139e5bcedf1765d947b36c87efea3740fba';
 const BTN="      \"<button class='wsico wsdesk' id=btn-repo title='Repo · rates, venues, Fed facilities, crisis spikes'><span class=g>Rp</span><span class=l>Repo</span></button>\"+\n";
 const CLICK="    var brepo=document.getElementById(\"btn-repo\"); if(brepo) brepo.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace(\"repo\"); };\n";
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js Repo desk button');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js Repo desk click');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelRepoDeskRail(raw,file){
 const AFTER='eb7f074d2d5d80ea2a60801d9e0726849a6e430f73e8c911c7e613e14ba3633a';
 const BEFORE='60dc58a49a678431cbf3b7559aab662943b3ba62a4ba6c9311f13b4e90ebfe8d';
 const LINE="      repo: [\"Repo · rates, venues, Fed facilities, crisis spikes\", \"/repo.html\"],\n";
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js Repo desk meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
function peelRepoDeskHtml(raw,file){
 const AFTER='ecd5c656a48d07fab61672114af310645c014e140e830f0d22a670f1c36199cf';
 const BEFORE='bb070c3949ec601148624c257e8afc3e29a81a6776061bf0001783b7e1bbff38';
 const PAIRS=[['&cds=20261015&marks=20261119&repo=20261121&capit=20261105&ldr=20261105"></script>','&cds=20261015&marks=20261119&capit=20261105&ldr=20261105"></script>'],
              ['&fit=20261112&wick=20261112&repo=20261121&ldr=20261105"></script>','&fit=20261112&wick=20261112&ldr=20261105"></script>']];
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 for(const [from,to] of PAIRS){assert.equal(raw.split(from).length,2,'chart.html Repo desk cache-bust');raw=raw.replace(from,to);}
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
function peelCryptoRoutes(raw,file){
 const plans={
  'jh-chart-engine.js':{after:'8860815ca07acac8d1823ff8d99f1139e5bcedf1765d947b36c87efea3740fba',before:'3e155083ed083a0fe3e4c6cbf9973bc0ce71f3310b564a955c254ed568087272',edits:['engine.edits.json']},
  'jh-chart-tvwatch.js':{after:'9926aca0ef44415b771fe7d382b04e0784f501a600b4adc4a5c3bc70d9cf8d97',before:'73960757ad566281eb496e729cca8e2c1b4aff32f4f42e75b7f6ff05a3dd4a95',edits:['tvwatch.edits.json','tvwatch-route.edits.json']},
  'tests/watchlist-identity.test.js':{after:'7d3429ddc834e7ebf737f752bbfabdb1b5e0204c2dbf666dd34fc9224cb68d32',before:'9bb2fdc7e091b7673446496d03e577a8495a8a2341300b39bbb6f0dc45a55fa2',edits:['identity-test.edits.json']}
 },plan=plans[file];
 if(!plan||hash(raw)!==plan.after)return raw;
 const edits=[].concat(...plan.edits.map(f=>JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-crypto-routes',f),'utf8'))));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,file+' crypto routes: '+e.after.slice(0,50));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),plan.before,file);
 return raw;
}
function peelSpxCashWatch(raw,file){
 const AFTER='15476552696562d0e9c9d0e7f62b49bdc8314fadc5141591b0cab799bb16a9af';
 const BEFORE='d7dd68f586c33aca02605eec9ed8bd4ef8ecdc067fcb699005e854e67609a55a';
 if(file!=='jh-chart-tvwatch.js'||hash(raw)!==AFTER)return raw;
 const edits=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-spx-cash/tvwatch-spx-cash.edits.json'),'utf8'));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,'jh-chart-tvwatch.js S&P 500 cash-index route: '+e.after.slice(0,50));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),BEFORE,'jh-chart-tvwatch.js');
 return raw;
}
function peelSpxCashIdentityTest(raw,file){
 const AFTER='869b3cb8b924d7e9d18097a717198ab8de614419b5a5adedde4c0c230fffca89';
 const BEFORE='e3e2ee96066b0f6cb89e5f35b309a967cf6897d32ec5420c9d0c51331bf5b86a';
 if(file!=='tests/watchlist-identity.test.js'||hash(raw)!==AFTER)return raw;
 const edits=JSON.parse(fs.readFileSync(path.join(R,'tests/fixtures/chart-spx-cash/identity-test-spx-cash.edits.json'),'utf8'));
 for(const e of [...edits].reverse()){assert.equal(raw.split(e.after).length,2,'watchlist-identity test S&P 500 cash route');raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),BEFORE,'tests/watchlist-identity.test.js');
 return raw;
}
function peelMarkDesksRail(raw,file){
 const AFTER='09303dcb5f1e693d0efdade211b2e579c8d3b85a0b5157dd33048a7083277ed9';
 const BEFORE='5b3c7f2cfd7604e34acc56ab0917cb7af99adc4c8f55bf360dc26092f6efdefa';
 const LINE="      dist: [\"Distribution · a rally that fails to hold\", \"/wyckoff-desk.html?tab=dist\"],\n";
 const BLOCK="  /* The three icons that mark the chart (Acc, Bot, Dist) also open the desk that lists every stock carrying the mark\n     (justhodl-campaign-scan). Their marking functions live in files the annotation ledger keeps unchanged, so they are\n     wrapped here once they exist: a click that turns the marks on opens the desk too; the next click turns the marks\n     off, as it always did. A desk row, or a link with ?marks=accum|bottom|dist, asks for the marks without the desk. */\n  var jhMarkDesk = { accum: 1, bottom: 1, dist: 1 }, jhMarkQuiet = false;\n  function jhMarkOn(which) { return which === \"dist\" ? !!window.__jhDistOn : !!(window.__jhCampLayer && window.__jhCampLayer[which]); }\n  function jhWrapMarks() {\n    var camp = window.jhCampToggle, dist = window.jhDistToggle;\n    if (typeof camp === \"function\" && !camp.__jhDesk) {\n      window.jhCampToggle = function (which) {\n        var r = camp.apply(this, arguments);\n        if (!jhMarkQuiet && jhMarkDesk[which] && jhMarkOn(which) && window.jhOpenWorkspace) window.jhOpenWorkspace(which);\n        return r;\n      };\n      window.jhCampToggle.__jhDesk = true;\n    }\n    if (typeof dist === \"function\" && !dist.__jhDesk) {\n      window.jhDistToggle = function () {\n        var r = dist.apply(this, arguments);\n        if (!jhMarkQuiet && jhMarkOn(\"dist\") && window.jhOpenWorkspace) window.jhOpenWorkspace(\"dist\");\n        return r;\n      };\n      window.jhDistToggle.__jhDesk = true;\n    }\n  }\n  window.jhEnsureMarks = function (which) {\n    jhWrapMarks();\n    if (!jhMarkDesk[which] || jhMarkOn(which)) return;\n    jhMarkQuiet = true;\n    try {\n      if (which === \"dist\") { if (window.jhDistToggle) window.jhDistToggle(); }\n      else if (window.jhCampToggle) window.jhCampToggle(which);\n    } finally { jhMarkQuiet = false; }\n  };\n  function jhBootMarks() {\n    jhWrapMarks();\n    try {\n      var asked = new URLSearchParams(location.search).get(\"marks\");\n      if (asked) asked.split(\",\").forEach(function (w) { window.jhEnsureMarks(w); });\n    } catch (e) {}\n  }\n  if (document.readyState === \"loading\") document.addEventListener(\"DOMContentLoaded\", jhBootMarks); else jhBootMarks();\n\n";
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js mark-desk meta');
 raw=raw.replace(LINE,'');
 assert.equal(raw.split(BLOCK).length,2,'jh-chart-tvrail.js mark-desk wrapper');
 raw=raw.replace(BLOCK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
// 2026-11-19 (b): the rail's cache-bust on chart.html carries the mark-desk wiring, so a browser holding the old rail fetches the new one.
function peelMarkDesksHtml(raw,file){
 const AFTER='bb070c3949ec601148624c257e8afc3e29a81a6776061bf0001783b7e1bbff38';
 const BEFORE='7422d4985604ff15f771b05b44bf20239328b03720cb88516028e720a005d8b6';
 const FROM='&cds=20261015&marks=20261119&capit=20261105&ldr=20261105"></script>';
 const TO='&cds=20261015&capit=20261105&ldr=20261105"></script>';
 if(file!=='chart.html'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(FROM).length,2,'chart.html mark-desk rail cache');
 raw=raw.replace(FROM,TO);
 assert.equal(hash(raw),BEFORE,'chart.html');
 return raw;
}
// 2026-11-19: peel the Volatility desk (button + click after Leaders, rail meta) before every older peel.
function peelVolDeskEngine(raw,file){
 const AFTER='500cb50af75a6d5d7b5f95b98345b973f2e34a54c778ca362d713f9ffd4a21ec';
 const BEFORE='bf37672d6bdd75d04ae60faa1b38e546c9df7831535644cfdbdc8e1b0d6d7989';
 const BTN="      \"<button class='wsico wsdesk' id=btn-svol title='Volatility · most and least volatile stocks, price spread'><span class=g>σ</span><span class=l>Volatility</span></button>\"+\n";
 const CLICK="    var bsvol=document.getElementById(\"btn-svol\"); if(bsvol) bsvol.onclick=function(){ if(window.jhOpenWorkspace) window.jhOpenWorkspace(\"volatility\"); };\n";
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(BTN).length,2,'jh-chart-engine.js Volatility desk button');
 assert.equal(raw.split(CLICK).length,2,'jh-chart-engine.js Volatility desk click');
 raw=raw.replace(BTN,'').replace(CLICK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function peelVolDeskRail(raw,file){
 const AFTER='60dc58a49a678431cbf3b7559aab662943b3ba62a4ba6c9311f13b4e90ebfe8d';
 const BEFORE='09303dcb5f1e693d0efdade211b2e579c8d3b85a0b5157dd33048a7083277ed9';
 const LINE="      volatility: [\"Volatility · most and least volatile stocks, price spread\", \"/stock-volatility.html\"],\n";
 if(file!=='jh-chart-tvrail.js'||hash(raw)!==AFTER)return raw;
 assert.equal(raw.split(LINE).length,2,'jh-chart-tvrail.js Volatility desk meta');
 raw=raw.replace(LINE,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-tvrail.js');
 return raw;
}
// 2026-11-20: the chart's open tabs re-read their last price and day change on a timer (Khalid: "there are a lot of tabs that
// open automatically but they don't update automatically"). One contiguous block after the live-tick timer; nothing else moved.
function peelTabQuotes(raw,file){
 const AFTER='3e155083ed083a0fe3e4c6cbf9973bc0ce71f3310b564a955c254ed568087272';
 const BEFORE='500cb50af75a6d5d7b5f95b98345b973f2e34a54c778ca362d713f9ffd4a21ec';
 if(file!=='jh-chart-engine.js'||hash(raw)!==AFTER)return raw;
 const BLOCK=fs.readFileSync(path.join(R,'tests/fixtures/chart-macro-line/engine-tab-quotes.block.txt'),'utf8');
 assert.equal(raw.split(BLOCK).length,2,'jh-chart-engine.js tab quote refresh block');
 assert.ok(raw.indexOf("  liveT=setInterval(tickLive, 15000);\n"+BLOCK)>=0,'jh-chart-engine.js tab quote refresh sits after the live-tick timer');
 raw=raw.replace(BLOCK,'');
 assert.equal(hash(raw),BEFORE,'jh-chart-engine.js');
 return raw;
}
function normalize(raw,file){raw=peelSovDeskEngine(raw,file);raw=peelSovDeskRail(raw,file);raw=peelSovDeskHtml(raw,file);raw=peelRepoDeskEngine(raw,file);raw=peelRepoDeskRail(raw,file);raw=peelRepoDeskHtml(raw,file);raw=peelCryptoRoutes(raw,file);raw=peelTabQuotes(raw,file);raw=peelVolDeskEngine(raw,file);raw=peelVolDeskRail(raw,file);raw=peelIndexRoutes(raw,file);raw=peelSpxCashWatch(raw,file);raw=peelSpxCashIdentityTest(raw,file);raw=peelMarkDesksHtml(raw,file);raw=peelMarkDesksRail(raw,file);raw=peelFredDesksEngine(raw,file);raw=peelFredDesksRail(raw,file);raw=peelCandleClarity(raw,file);raw=peelScreenFit(raw,file);raw=peelFlowsEngine(raw,file);raw=peelFlowsRail(raw,file);raw=peelFormingBar(raw,file);raw=peelCapitHtml(raw,file);raw=peelCapitRail(raw,file);raw=peelLeadersHtml(raw,file);raw=peelLeadersEngine(raw,file);raw=peelLeadersRail(raw,file);raw=peelTriggerCap(raw,file);raw=peelUs10yRouteHtml(raw,file);raw=peelUs10yRouteEngine(raw,file);raw=peelUs10yRouteWatch(raw,file);raw=peelUs10yRouteIdentityTest(raw,file);raw=peelUs10yRouteObsTest(raw,file);raw=peelUs10yHtml(raw,file);raw=peelUs10yEngine(raw,file);raw=peelAutohideHtml(raw,file);raw=peelFmpAppend(raw,file);raw=peelPolygonAppend(raw,file);raw=peelCdsHtml(raw,file);raw=peelCdsEngine(raw,file);raw=peelCdsRail(raw,file);raw=peelHeatHtml(raw,file);raw=peelHeatRail(raw,file);raw=peelAucEngine(raw,file);raw=peelAucHtml(raw,file);raw=peelAucRail(raw,file);raw=peelKind(raw,file);raw=peelKindHtml(raw,file);raw=peelCorr(raw,file);raw=peelSos(raw,file);raw=peelEvr(raw,file);const row=transition.changes[file];if(!row)return normalizeLegacyTest(raw,file);const digest=hash(raw);
 if(digest===row.before_sha256||digest!==row.after_sha256)return raw;
 for(const e of [...row.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file+': '+e.after.slice(0,60));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),row.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
// The approved behaviour change (points → line/candles) rewrote three scalar tests; earlier gates see their exact prior bytes.
const adaptations=require('../fixtures/chart-macro-line/test-adaptations.json');
function normalizeLegacyTest(raw,file){const row=adaptations[file];if(!row||hash(raw)!==row.after_sha256)return raw;
 const old=fs.readFileSync(path.join(R,row.before_path),'utf8');assert.equal(hash(old),row.before_sha256,file);return old;}
module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr,peelScreenFit,peelCandleClarity,peelRepoDeskEngine,peelRepoDeskRail,peelRepoDeskHtml,peelSovDeskEngine,peelSovDeskRail,peelSovDeskHtml};
