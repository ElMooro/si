const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const R=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(R,p),'utf8');
function boot(){const window={};vm.runInNewContext(read('jh-chart-cftc.js'),{window});return window.JHChartCFTC;}
test('two reviewed current VIX alternatives retain exact original empty routes',()=>{
 const api=boot();
 for(const side of ['L','S']){
  const id='COT3:11700_F_AMP_'+side,original=api.resolve(id),alternative=api.alternative(id);
  assert.equal(original.contract_market_code,'11700');assert.equal(alternative.equivalence_verified,false);
  assert.equal(api.resolve(alternative.id).contract_market_code,'1170E1');assert.match(alternative.reason,/equivalence remain unverified/);
  assert.equal(alternative.requested,id);assert.match(alternative.id,new RegExp('asset_mgr_positions_'+(side==='L'?'long':'short')+'$'));
  alternative.id='bad';assert.notEqual(api.alternative(id).id,'bad');assert.equal(api.resolve(id).contract_market_code,'11700');
 }
 for(const id of ['COT3:011700_F_AMP_L','COT3:11700_F_LMP_L','COT:11700_F_AMP_L',null])assert.equal(api.alternative(id),null);
});
test('economic watchlist namespace has macro classification',()=>{
 const code=read('jh-chart-tvwatch.js'),start=code.indexOf('  function kindOf(s) {'),end=code.indexOf('\n  function descOf',start);
 const scope={exch:s=>s.split(':')[0],bare:s=>s.split(':').slice(1).join(':'),CRYPTO:{},ETF:{}};
 vm.runInNewContext(code.slice(start,end)+'\nglobalThis.classify=kindOf',scope);
 assert.equal(scope.classify('ECONOMICS:THINTR'),'Macro');assert.equal(scope.classify('NASDAQ:AAPL'),'Stock');assert.equal(scope.classify('BIS:WS_CBPOL:D.TH'),'Macro');
});
test('complete predecessors and exact market changes are retained without rewriting earlier fixtures',()=>{
 const {normalize,transition}=require('./helpers/chart-market-repair-preservation.cjs'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
 for(const [file,row]of Object.entries(transition.changes)){const current=read(file);assert.equal(hash(current),row.after_sha256,file);assert.equal(normalize(current,file),read(row.before_path));assert.throws(()=>normalize(current+'\n// unreviewed',file));}
 assert.equal(normalize(read(transition.ledger.path),transition.ledger.path),read(transition.ledger.before_path));
});
