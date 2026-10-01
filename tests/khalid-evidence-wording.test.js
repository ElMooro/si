const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('jh-khalid-sniper.js','utf8');
const ctx={document:{createElement(tag){return {tag,textContent:'',children:[],append(...nodes){this.children.push(...nodes);}}}}};
vm.createContext(ctx);vm.runInContext(source.slice(source.indexOf('  function el('),source.indexOf('  function contract(')),ctx);
const definition='Existing risk_allows_entries flag is true; this evidence never grants permission.';
const text=n=>[n.textContent,...n.children.map(text)].join('\n');
test('risk criterion wording distinguishes required true from observed false without changing the input',()=>{
 for(const [status,value] of [['FAIL',false],['PASS',true]]){
  const c={id:'risk_permission',label:'Existing risk permission',definition,status,value,clocks:{}};
  const before=JSON.stringify(c),rendered=text(ctx.criterion(c));
  assert.match(rendered,new RegExp('Existing risk permission — '+status));
  assert.match(rendered,/Passing criterion: requires risk_allows_entries to be true/);
  assert.match(rendered,new RegExp('Value\\n'+value));assert.equal(JSON.stringify(c),before);
 }
});
test('unrecognized definitions remain verbatim instead of gaining a guessed passing condition',()=>{
 const c={id:'risk_permission',definition:'Future contract text',status:'UNAVAILABLE',value:null,clocks:{}};
 assert.match(text(ctx.criterion(c)),/Future contract text/);assert.doesNotMatch(text(ctx.criterion(c)),/Passing criterion:/);
});
test('Method distinguishes Katlin publication age from research and upstream freshness',()=>{
 const html=fs.readFileSync('khalid.html','utf8');assert.match(html,/<th>Source timestamp age<\/th><th>Producer TTL<\/th>/);
 assert.match(html,/For Katlin, it is publication age, not original research age/);
 assert.match(html,/a permission refresh does not renew research/);assert.match(html,/publication\/as-of, or object-modification fallback/);
 assert.match(html,/not independent freshness of research or upstream observations/);
});
