const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const ctx={};vm.createContext(ctx);
const {page}=require('./crypto-market-cap-support.cjs');vm.runInContext(page('crypto/index.html').code,ctx);
const fixtures=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/crypto-stablecoin-stocks/invented-page-packets.json'),'utf8'));
test('every returned stock row survives rendering, including the last row',()=>{
 const h=ctx.renderCryptoStablecoins(fixtures.normal);assert.match(h,/30 returned rows/);assert.match(h,/Invented 29/);assert.equal((h.match(/<tr>/g)||[]).length,31);assert.match(h,/current selected price/);assert.match(h,/Snapshot observation dates and freshness are unavailable/);
});
test('zero remains zero while missing and zero denominator are unavailable',()=>{
 const z=ctx.renderCryptoStablecoins(fixtures.zero),m=ctx.renderCryptoStablecoins(fixtures.missing),p=ctx.renderCryptoStablecoins(fixtures.zero_prior);
 assert.match(z,/<td class='mono'>0<\/td>/);assert.match(z,/<td class='mono'>-100<\/td>/);assert.match(m,/Unavailable/);assert.match(p,/<td class='mono'>3<\/td><td class='mono'>Unavailable<\/td>/);
});
test('legacy flow, malformed population and permission promotion cannot render signals',()=>{
 for(const st of [fixtures.legacy,{}, {...fixtures.normal,reported_rows:29},{...fixtures.normal,calls_eligible:true},{...fixtures.normal,stablecoins:[null]}]){
  const h=ctx.renderCryptoStablecoins(st);assert.match(h,/Unavailable — no complete compatible/);assert.doesNotMatch(h,/\$99T|INFLOW|STABLE|EXPANDING/);
 }
});
test('provider-controlled names are text and cannot create network elements',()=>{
 const h=ctx.renderCryptoStablecoins(fixtures.injection);assert.doesNotMatch(h,/<img|<iframe|<script/);assert.match(h,/&lt;img/);
});
test('empty and unresolved populations remain different from missing packet',()=>{
 assert.match(ctx.renderCryptoStablecoins(fixtures.empty_population),/0 returned rows/);assert.match(ctx.renderCryptoStablecoins(fixtures.duplicate),/2 unresolved/);assert.match(ctx.renderCryptoStablecoins(fixtures.unavailable),/Unavailable — no complete/);
});
test('decimal display rejects nonnumeric and typed zero mistakes',()=>{
 for(const value of [null,undefined,true,0,'NaN','Infinity','<img>'])assert.equal(ctx.cryptoStablecoinDecimal(value),'Unavailable');
 for(const value of ['0','1E-324','-5','1.234E+300'])assert.equal(ctx.cryptoStablecoinDecimal(value),value);
});

test('complete Crypto render cannot promote a legacy stablecoin signal',()=>{
 for(const packet of Object.values(fixtures)){
  const nodes={main:{innerHTML:''},ts:{textContent:''}},errors=[];
  const scope={D:{stablecoins:packet},window:{},document:{getElementById:id=>nodes[id]},console:{error:e=>errors.push(String(e))}};
  vm.createContext(scope);vm.runInContext(page('crypto/index.html').code,scope);scope.render();
  assert.deepEqual(errors,[]);assert.match(nodes.main.innerHTML,/Reported stablecoin stocks/);assert.doesNotMatch(nodes.main.innerHTML,/Stbl Flow|Stablecoin Market State|\$99T/);
 }
});
test('provider flags preserve unknown rather than treating it as false',()=>{
 const stock=structuredClone(fixtures.normal);stock.stablecoins[0].provider_flags={delisted:true,deprecated:false,yieldBearing:null};
 assert.match(ctx.renderCryptoStablecoins(stock),/delisted: yes · deprecated: no · yieldBearing: unavailable/);
});
test('Classic stock summary rejects legacy, malformed and promoted packets',()=>{
 const {classic}=require('./crypto-stablecoin-support.cjs');
 for(const stock of [fixtures.normal,fixtures.empty_population,fixtures.legacy,{}, {...fixtures.normal,calls_eligible:true},{...fixtures.normal,reported_rows:29}]){
  const nodes={cryptoBody:{innerHTML:''},cryptoSub:{textContent:''}},scope={STATE:{data:{crypto:{stablecoins:stock}}},document:{getElementById:id=>nodes[id]}};
  vm.createContext(scope);vm.runInContext(classic().code,scope);scope.renderCrypto();
  assert.match(nodes.cryptoBody.innerHTML,/flow and observation dates unavailable; no sizing vote/);assert.doesNotMatch(nodes.cryptoBody.innerHTML,/INFLOW|MINTING|\$99T/);
  assert.match(nodes.cryptoSub.textContent,stock===fixtures.normal?/30 reported stock rows/:stock===fixtures.empty_population?/0 reported stock rows/:/Reported stocks unavailable/);
 }
});
