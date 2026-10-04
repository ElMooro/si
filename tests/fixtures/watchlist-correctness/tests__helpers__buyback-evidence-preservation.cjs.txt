const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const R=path.join(__dirname,'../..'),t=require('../fixtures/buyback-evidence/transition.json'),hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function reverse(raw,e){assert.equal(hash(raw),e.after_sha256);for(const x of [...e.edits].reverse()){assert.equal(raw.split(x.after).length,2);raw=raw.replace(x.after,x.before);}assert.equal(hash(raw),e.before_sha256);assert.equal(raw,fs.readFileSync(path.join(R,e.before_path),'utf8'));return raw;}
function normalize(raw,file){return reverse(raw,t.changes[file]);}
function normalizeHelper(raw,file){return reverse(raw,t.hooks[file]);}
function normalizeHtml(raw){const legacy='/jh-khalid-sniper.js?v=20261001-snapshot',current='/jh-khalid-sniper.js?v=20261001-user-scope',old=raw.includes(legacy);if(old)raw=raw.replace(legacy,current);raw=normalize(raw,'chart.html');return old?raw.replace(current,legacy):raw;}
module.exports={normalize,normalizeHelper,normalizeHtml};
