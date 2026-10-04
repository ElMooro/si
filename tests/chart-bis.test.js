const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
test('all exact BIS definitions are supported by picker and unknown identities stay inspect-only',()=>{
 const scope={window:{},Set,URLSearchParams,TextDecoder,Uint8Array,AbortController,URL,Blob};vm.runInNewContext(read('jh-chart-provider-browser.js'),scope);const api=scope.window.JHChartProviderBrowser;
 const cat=JSON.parse(read('aws/lambdas/justhodl-symdir/source/bis-policy-series.json'));
 for(const key of Object.keys(cat.series))assert.equal(api.action({id:'bis:WS_CBPOL:'+key,provider:'bis',kind:'series',chartable:true}),'chart');
 for(const id of ['bis:WS_CBPOL:M.XX','bis:WS_OTHER:M.US','bis:WS_CBPOL:Q.US'])assert.equal(api.action({id,provider:'bis',kind:'series',chartable:true}),'inspect');
 assert.equal(api.action({id:'bis:WS_EER',provider:'bis',kind:'dataset'}),'dataset');
});
test('thirteen new watchlist alternatives preserve request and disclose BIS definition limitations',()=>{
 const source=read('tests/watchlist-identity.test.js').split('const baseline=harness(null,true);')[0],scope={require,process,console,AbortController,setTimeout,clearTimeout};vm.createContext(scope);vm.runInContext(source+'\nglobalThis.make=harness;',scope);
 for(const cc of ['AR','CO','HR','HK','KW','PE','MY','MA','SA','RS','RO','PH','TH']){const c=scope.make(),requested='ECONOMICS:'+cc+'INTR',r=c.chartRoute(requested);assert.equal(r.handoff,'bis:WS_CBPOL:D.'+cc);assert.equal(r.requested,requested);assert.match(r.reason,/equivalence unverified/);assert.match(r.reason,/discontinued/);assert.doesNotMatch(c.chartRouteText(r,null,[]),/Yahoo/);c.openSym(requested);assert.equal(c.loads[0],r.handoff.toUpperCase());}
});
test('BIS delta and incoming peer alias delta preserve complete prior source bytes',()=>{
 const {normalize,transition}=require('./helpers/chart-bis-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row]of Object.entries(transition.changes)){const raw=read(file);assert.equal(hash(raw),row.after_sha256);assert.equal(normalize(raw,file),read(row.before_path));assert.throws(()=>normalize(raw+'\n// unexpected',file));}
 const ledger=transition.ledger;assert.equal(normalize(read(ledger.path),ledger.path),read(ledger.before_path));assert.throws(()=>normalize(read(ledger.path)+' ',ledger.path));
});
