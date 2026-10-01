const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);const acorn=parser.exports;
function page(name){
 const html=fs.readFileSync(path.join(R,name),'utf8'),styles=[...html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)].map(m=>m[1]).join('\n');
 let functions=[];for(const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
  if(/\bsrc\s*=|type\s*=\s*["']application\//i.test(m[1]))continue;
  const tree=acorn.parse(m[2],{ecmaVersion:'latest'});functions.push(...tree.body.filter(n=>n.type==='FunctionDeclaration').map(n=>({name:n.id.name,code:m[2].slice(n.start,n.end)})));
 }
 const allowed=name==='crypto/index.html'?null:['cryptoMarketCapRatio','cryptoMarketCapText','renderCrypto','fmt','timeAgo','rowsHTML'];
 functions=functions.filter(f=>!allowed||allowed.includes(f.name));return {name,html,styles,functions,code:functions.map(f=>f.code).join('\n')};
}
function ratios(value=2){return {mvrv_approx:900,signal:'OVERVALUED',market_cap_extension:{contract:'btc-market-cap-to-returned-mean.v1',status:'descriptive',unit:'ratio',value,calls_eligible:false,sizing_eligible:false,execution_eligible:false,forecast_qualified:false,independent_investment_votes:0,numerator:{value_usd:typeof value==='number'?value*10:null},denominator:{value_usd:10,count:30},observation_window:{first:'2020-01-01T00:00:00+00:00',last:'2020-01-30T00:00:00+00:00'}}};}
function packet(value){return {onchain_ratios:value,fear_greed:{current:25,score:25,label:'Invented'},risk_score:{score:25,regime:'Invented',action:'Invented'}};}
function runtime(name){const p=page(name),nodes={main:{innerHTML:''},cardCrypto:{innerHTML:''},ts:{textContent:''}},errors=[],ctx={D:{},window:{},document:{getElementById:id=>nodes[id]||null},console:{error:e=>errors.push(String(e))}};vm.createContext(ctx);vm.runInContext(p.code,ctx);return {ctx,p,nodes,errors,render(value){if(name==='crypto/index.html'){ctx.D=packet(value);ctx.render();return nodes.main.innerHTML;}ctx.renderCrypto(packet(value));return nodes.cardCrypto.innerHTML;}};}
module.exports={page,ratios,packet,runtime};
