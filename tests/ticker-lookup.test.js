const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.join(__dirname,'..'),D=path.join(__dirname,'fixtures/ticker-lookup');
const raw=fs.readFileSync(path.join(R,'engines-data.html'),'utf8'),before=fs.readFileSync(path.join(D,'before.html'),'utf8'),prePeer=fs.readFileSync(path.join(D,'before-peer.html'),'utf8');
const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
test('complete current page reverses to both retained predecessors outside exactly reviewed edits',()=>{
 const t=JSON.parse(fs.readFileSync(path.join(D,'transition.json')));assert.equal(hash(raw),t.after_sha256);assert.equal(hash(before),t.before_sha256);assert.equal(hash(prePeer),t.before_peer_sha256);
 let restored=raw;for(const edit of [...t.changes].reverse()){assert.equal(restored.split(edit.after).length,2);restored=restored.replace(edit.after,edit.before);}assert.equal(restored,before);
 let peer=before;const panel=peer.indexOf('<div class="panel" style="margin-bottom:20px">'),grid=peer.indexOf('<div class="grid">',panel);assert.ok(panel>0&&grid>panel);peer=peer.slice(0,panel)+peer.slice(grid);
 const a=peer.indexOf('// Ticker lookup'),b=peer.indexOf('// Ticker 360 summary',a);assert.ok(a>0&&b>a);peer=peer.slice(0,a)+peer.slice(b);assert.equal(peer,prePeer);
});
class Element {
 constructor(tag){this.tag=tag;this.children=[];this.attributes={};this.dataset={};this.events={};this.value='';this._text='';}
 set innerHTML(value){throw new Error('HTML insertion is forbidden');}
 get textContent(){return this._text+this.children.map(x=>x.textContent).join('');}
 set textContent(v){this._text=String(v);this.children=[];}
 appendChild(node){this.children.push(node);return node;}
 replaceChildren(...nodes){this._text='';this.children=nodes;}
 setAttribute(k,v){this.attributes[k]=v;}
 addEventListener(k,f){this.events[k]=f;}
}
function harness(legacy=false){
 const nodes=Object.fromEntries(['ticker-input','ticker-result','ticker-status','ticker-form'].map(k=>[k,new Element('div')])),pending=[];
 if(legacy)Object.defineProperty(nodes['ticker-result'],'innerHTML',{set(v){this.html=v;},get(){return this.html||'';}});
 const document={getElementById:k=>nodes[k],createElement:tag=>new Element(tag),createDocumentFragment:()=>new Element('fragment')};
 const context=vm.createContext({document,Date,get:()=>new Promise((resolve,reject)=>pending.push({resolve,reject}))});
 const source=legacy?before:raw,script=source.match(/<script>([\s\S]*?)<\/script>/)[1];
 const helpers=script.slice(script.indexOf('const countText'),script.indexOf('const t360cell'));
 const lookup=script.slice(script.indexOf('// Ticker lookup'),script.indexOf('// Ticker 360 summary'));
 vm.runInContext(helpers+'\n'+lookup+'\nglobalThis.api={lookupTicker'+(legacy?'':',tickerResult,invalidateTickerLookup')+'};',context);
 return {...context.api,nodes,pending,async run(symbol,packet){nodes['ticker-input'].value=symbol;const task=context.api.lookupTicker();pending.at(-1).resolve(packet);await task;return nodes['ticker-result'];}};
}
function expand(node){if(node.tag==='details'){node.open=true;node.events.toggle();}for(const child of node.children)expand(child);return node;}
function all(node,tag){return [...(node.tag===tag?[node]:[]),...node.children.flatMap(child=>all(child,tag))];}
const packet=(symbol='QAONLY',domains={zero:{as_of:'2026-10-02T00:00:00Z',data:0}})=>({generated_at:'2026-10-02T00:00:00Z',tickers:{[symbol]:{coverage_count:Object.keys(domains).length,domains}}});
test('retained original reproduces unsafe HTML, lost zero and record truncation',async()=>{
 const h=harness(true),p=packet('QAONLY',{zero:{data:0},'<img src=x onerror=bad()>':{data:'x'.repeat(1500)+'END_OF_RECORD'}});await h.run('QAONLY',p);
 assert.match(h.nodes['ticker-result'].html,/<img src=x/);assert.match(h.nodes['ticker-result'].html,/<pre>\{\}<\/pre>/);assert.ok(!h.nodes['ticker-result'].html.includes('END_OF_RECORD'));
});
test('retained original reproduces old-response overwrite',async()=>{
 const h=harness(true);h.nodes['ticker-input'].value='FIRST';const a=h.lookupTicker();h.nodes['ticker-input'].value='SECOND';const b=h.lookupTicker();h.pending[1].resolve(packet('SECOND'));await b;h.pending[0].resolve(packet('FIRST'));await a;assert.match(h.nodes['ticker-result'].html,/FIRST/);
});
test('user, packet, domain, timestamp and error markup remain text',async()=>{
 const h=harness(),bad='<img src=x onerror=bad()>',p=packet(bad.toUpperCase(),{[bad]:{as_of:bad,data:bad}});p.tickers[bad.toUpperCase()].coverage_count=bad;
 const res=expand(await h.run(bad,p));assert.ok(res.textContent.includes(bad));assert.match(res.textContent,/Unavailable reported domains/);assert.equal(all(res,'img').length,0);
 h.nodes['ticker-input'].value=bad;const task=h.lookupTicker();h.pending.at(-1).reject(new Error(bad));await task;assert.ok(h.nodes['ticker-status'].textContent.includes(bad));
});
test('full received parsed packet and selected record retain all values and last bytes',async()=>{
 const h=harness(),p=packet('QAONLY',{zero:{data:0},boolean:{data:false},null:{data:null},long:{data:'x'.repeat(15000)+'TAIL_SENTINEL'}});p.extra={provenance:'invented',zero:0};
 const res=expand(await h.run('QAONLY',p)),pres=all(res,'pre').map(x=>JSON.parse(x.textContent));assert.deepEqual(pres[0],p);assert.deepEqual(pres[1],p.tickers.QAONLY);assert.deepEqual(pres.slice(2),Object.values(p.tickers.QAONLY.domains));assert.ok(res.textContent.includes('TAIL_SENTINEL'));
 for(const pre of all(res,'pre')){assert.equal(pre.tabIndex,0);assert.ok(pre.attributes['aria-label']);}
});
test('large received JSON is rendered lazily once per audit without clipping',async()=>{
 const h=harness(),res=await h.run('QAONLY',packet());assert.equal(all(res,'pre').length,0);expand(res);const count=all(res,'pre').length;expand(res);assert.equal(all(res,'pre').length,count);
});
test('missing, false, null and array inventories stay unavailable and remain inspectable',async()=>{
 for(const value of [null,false,0,'',[],{}, {tickers:[]},{tickers:null}]){const h=harness(),res=expand(await h.run('QAONLY',value));assert.match(res.textContent,/invalid ticker inventory/);assert.deepEqual(JSON.parse(all(res,'pre')[0].textContent),value);}
});
test('explicit missing record differs from malformed received record',async()=>{
 const h=harness();assert.match((await h.run('MISSING',{tickers:{}})).textContent,/No record for MISSING.*0 ticker keys/);
 for(const record of [null,false,0,[],{}, {domains:null}]){const res=expand(await h.run('QAONLY',{tickers:{QAONLY:record}}));assert.match(res.textContent,/invalid ticker record or domain map/);assert.deepEqual(JSON.parse(all(res,'pre')[1].textContent),record);}
});
test('prototype names do not fabricate ticker records',async()=>{const h=harness();assert.match(h.tickerResult({tickers:{}},'constructor').textContent,/No record/);});
test('coverage checks preserve zero and distinguish invalid counts and disagreement',async()=>{
 const h=harness();assert.match((await h.run('QAONLY',packet('QAONLY',{}))).textContent,/0 reported domains; 0 received domain entries/);
 for(const count of [null,false,'1',-1,.5,2]){const p=packet();p.tickers.QAONLY.coverage_count=count;assert.match((await h.run('QAONLY',p)).textContent,/Coverage count is missing, invalid or inconsistent/);}
});
test('malformed domain values are retained individually, not replaced by empty objects',async()=>{
 const h=harness(),p=packet('QAONLY',{false:false,zero:0,null:null,array:[]});const res=expand(await h.run('QAONLY',p));assert.equal(all(res,'summary').filter(x=>x.textContent.includes('invalid domain record')).length,4);assert.deepEqual(all(res,'pre').slice(2).map(x=>JSON.parse(x.textContent)),[false,0,null,[]]);
});
test('an older success cannot overwrite a newer selected ticker',async()=>{
 const h=harness();h.nodes['ticker-input'].value='FIRST';const a=h.lookupTicker();h.nodes['ticker-input'].value='SECOND';const b=h.lookupTicker();h.pending[1].resolve(packet('SECOND'));await b;h.pending[0].resolve(packet('FIRST'));await a;assert.match(h.nodes['ticker-result'].textContent,/SECOND/);assert.ok(!h.nodes['ticker-result'].textContent.includes('FIRST'));assert.equal(h.nodes['ticker-result'].attributes['aria-busy'],'false');
});
test('an older error cannot replace a newer result or its loading status',async()=>{
 const h=harness();h.nodes['ticker-input'].value='FIRST';const a=h.lookupTicker();h.nodes['ticker-input'].value='SECOND';const b=h.lookupTicker();h.pending[0].reject(new Error('OLD_ERROR'));await a;assert.match(h.nodes['ticker-status'].textContent,/Loading SECOND/);assert.equal(h.nodes['ticker-result'].attributes['aria-busy'],'true');h.pending[1].resolve(packet('SECOND'));await b;assert.match(h.nodes['ticker-status'].textContent,/completed for SECOND/);
});
test('input editing invalidates a completed display and an in-flight response',async()=>{
 const h=harness();await h.run('FIRST',packet('FIRST'));h.nodes['ticker-input'].value='SECOND';h.nodes['ticker-input'].events.input();assert.equal(h.nodes['ticker-result'].textContent,'');const a=h.lookupTicker();h.nodes['ticker-input'].value='THIRD';h.nodes['ticker-input'].events.input();h.pending.at(-1).resolve(packet('SECOND'));await a;assert.equal(h.nodes['ticker-result'].textContent,'');assert.match(h.nodes['ticker-status'].textContent,/Selection changed/);
});
test('blank submits invalidate older requests without issuing a new request',async()=>{
 const h=harness();h.nodes['ticker-input'].value='FIRST';const a=h.lookupTicker();h.nodes['ticker-input'].value=' ';await h.lookupTicker();assert.equal(h.pending.length,1);h.pending[0].resolve(packet('FIRST'));await a;assert.equal(h.nodes['ticker-result'].textContent,'');assert.match(h.nodes['ticker-status'].textContent,/Enter a ticker/);
});
test('form submit uses the shared lookup path and prevents page navigation',async()=>{
 const h=harness();h.nodes['ticker-input'].value='QAONLY';let prevented=false;h.nodes['ticker-form'].events.submit({preventDefault(){prevented=true;}});assert.ok(prevented);assert.equal(h.pending.length,1);h.pending[0].resolve(packet());await new Promise(resolve=>setImmediate(resolve));assert.match(h.nodes['ticker-status'].textContent,/completed for QAONLY/);
});
