// Exact byte reversal of the 2026-10-05 macro line/candles/change-modes, auto-hide workspace and file-import change only.
// 2026-10-12: peel the Effort & Spread script tag before that reversal. The tag is the only new chart.html byte.
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
function normalize(raw,file){raw=peelSos(raw,file);raw=peelEvr(raw,file);const row=transition.changes[file];if(!row)return normalizeLegacyTest(raw,file);const digest=hash(raw);
 if(digest===row.before_sha256||digest!==row.after_sha256)return raw;
 for(const e of [...row.replacements].reverse()){assert.equal(raw.split(e.after).length,2,file+': '+e.after.slice(0,60));raw=raw.replace(e.after,e.before);}
 assert.equal(hash(raw),row.before_sha256,file);assert.equal(raw,fs.readFileSync(path.join(R,row.before_path),'utf8'));return raw;
}
// The approved behaviour change (points → line/candles) rewrote three scalar tests; earlier gates see their exact prior bytes.
const adaptations=require('../fixtures/chart-macro-line/test-adaptations.json');
function normalizeLegacyTest(raw,file){const row=adaptations[file];if(!row||hash(raw)!==row.after_sha256)return raw;
 const old=fs.readFileSync(path.join(R,row.before_path),'utf8');assert.equal(hash(old),row.before_sha256,file);return old;}
module.exports={normalize,normalizeLegacyTest,transition,adaptations,peelEvr};
