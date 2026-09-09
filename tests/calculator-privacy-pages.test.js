const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
function code(file,start){const source=fs.readFileSync(path.join(__dirname,'..',file),'utf8');const a=source.indexOf(start),b=source.indexOf('function render(data)',a);assert.ok(a>=0&&b>a);return source.slice(a,b);}
test('wealth planner sends private scenario in uncached POST body without URL parameters',async()=>{
  const fields={run:{},results:{},current_nav:{value:'234567'},annual_savings:{value:'34567'},annual_spending:{value:'45678'}};
  let call,rendered;const c=vm.createContext({document:{getElementById(id){return fields[id];}},ageEl:{value:'43'},retireEl:{value:'64'},currentProfile:'balanced',LAMBDA_URL:'https://calculator.invalid/',URLSearchParams,
    fetch:async(url,init)=>{call={url,init};return Response.json({fixture:true});},render(value){rendered=value;}});
  vm.runInContext(code('wealth-plan.html','async function run()'),c);await c.run();
  assert.equal(call.url,'https://calculator.invalid/');assert.equal(call.init.method,'POST');assert.equal(call.init.cache,'no-store');assert.equal(call.init.headers['Content-Type'],'application/json');
  assert.equal(JSON.parse(call.init.body).current_nav,'234567');assert.equal(JSON.parse(call.init.body).age,'43');assert.equal(rendered.fixture,true);assert.equal(fields.run.disabled,false);
});
test('tax planner keeps AGI and account distribution out of URLs',async()=>{
  const fields=Object.fromEntries(Object.entries({'i-fed':'0.24','i-state':'0.05','i-agi':'278901','i-filing':'single','i-tax':'60','i-ira':'30','i-roth':'10','i-cf':'4321'}).map(([k,v])=>[k,{value:v}]));fields.root={};
  let call,rendered;const c=vm.createContext({document:{getElementById(id){return fields[id];}},syncDisplay(){},ENDPOINT:'https://tax.invalid/',URLSearchParams,
    fetch:async(url,init)=>{call={url,init};return Response.json({fixture:true});},render(value){rendered=value;}});
  vm.runInContext(code('tax-plan.html','async function refresh()'),c);await c.refresh();
  assert.equal(call.url,'https://tax.invalid/');assert.equal(call.init.method,'POST');assert.equal(call.init.cache,'no-store');assert.equal(JSON.parse(call.init.body).agi,'278901');assert.equal(JSON.parse(call.init.body).taxable_pct,'0.6');assert.equal(rendered.fixture,true);
});
test('tax planner handles failed API status instead of displaying an error body as valid financial results',async()=>{
  const root={},c=vm.createContext({document:{getElementById(id){return id==='root'?root:{value:'1'};}},syncDisplay(){},ENDPOINT:'https://tax.invalid/',URLSearchParams,
    fetch:async()=>Response.json({error:'Calculation unavailable'},{status:503}),render(){throw new Error('invalid financial rendering');}});
  vm.runInContext(code('tax-plan.html','async function refresh()'),c);await c.refresh();assert.match(root.innerHTML,/Calculation unavailable/);
});
