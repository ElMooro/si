const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const api=require('../jh-ranker-numeric.js'),R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/ranker-numeric');
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
test('both whole pages preserve everything outside reviewed availability hooks',()=>{
 const transition=JSON.parse(fs.readFileSync(path.join(D,'pages-transition.json')));
 for(const [name,t] of Object.entries(transition)){
  let text=fs.readFileSync(path.join(R,name),'utf8');assert.equal(hash(text),t.after_sha256);
  for(const edit of [...t.changes].reverse()){assert.equal(text.split(edit.after).length,2);text=text.replace(edit.after,edit.before);}
  assert.equal(hash(text),t.before_sha256);assert.equal(text,fs.readFileSync(path.join(D,name+'.before'),'utf8'));
 }
});
const quality=(rankable=1,unranked=1)=>({contract:'ranker-numeric.v1',status:rankable===0?'unavailable':unranked?'partial':'usable',input_tickers:rankable+unranked,rankable_tickers:rankable,unranked_tickers:unranked});
test('complete unavailable and zero-count inventories are stated explicitly',()=>{
 assert.match(api.summary({numeric_quality:quality(0,3)}),/0 of 3.*3 withheld/);
 assert.match(api.summary({numeric_quality:quality(0,0)}),/0 of 0.*0 withheld/);
 assert.match(api.summary({numeric_quality:quality(1,0)}),/1 of 1.*0 withheld/);
});
test('missing legacy validation and malformed counters never become measured zero',()=>{
 assert.match(api.summary({}),/not reported/);
 for(const change of [{rankable_tickers:null},{rankable_tickers:true},{rankable_tickers:'1'},{input_tickers:5},{status:'usable'},{input_tickers:Infinity}])assert.match(api.summary({numeric_quality:{...quality(),...change}}),/inconsistent/);
});
test('whole reported calculation is text, includes last row, and remains keyboard inspectable',()=>{
 class Element{constructor(){this.children=[];this.style={};this.attrs={};this.textContent='';}appendChild(c){this.children.push(c);}replaceChildren(){this.children=[];}setAttribute(k,v){this.attrs[k]=v;}set innerHTML(v){throw Error('HTML insertion forbidden');}}
 const host=new Element(),old=global.document;global.document={getElementById:()=>host,createElement:()=>new Element()};
 try{
  const bad='<img src=x onerror="bad()">',packet={numeric_quality:quality(),unranked_tickers:Array.from({length:250},(_,i)=>({ticker:i===249?bad:'INVENTED',score:null,reasons:['invented '.repeat(50)]})),top_tickers:[{ticker:'QAONLY',score:0,score_calculation:{base:{score:0}}}]};
  api.render(packet,'fixture');const pre=host.children[2].children[1],record=JSON.parse(pre.textContent);
  assert.equal(record.unranked_tickers.length,250);assert.equal(record.unranked_tickers[249].ticker,bad);assert.equal(record.ranked_calculations[0].score,0);assert.equal(pre.tabIndex,0);assert.match(pre.attrs['aria-label'],/Complete/);
  api.render({},'fixture');assert.equal(host.children.length,3);assert.match(host.children[0].textContent,/not reported/);
 }finally{global.document=old;}
});
