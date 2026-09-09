/* Parse source only. Uses Acorn bundled with the build's Node runtime; input
 * JavaScript is never evaluated. Missing parser fails the build explicitly. */
'use strict';
const fs=require('node:fs');
const bundled=process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'];
if(!bundled)throw new Error('Node bundled Acorn parser unavailable; source ownership cannot be inferred');
const parserModule={exports:{}};Function('exports','module',bundled)(parserModule.exports,parserModule);
const acorn=parserModule.exports;
const hosts=new Set(['justhodl.ai','www.justhodl.ai','api.justhodl.ai','justhodl-data-proxy.raafouis.workers.dev','justhodl-dashboard-live.s3.amazonaws.com','justhodl-dashboard-live.s3.us-east-1.amazonaws.com']);
const UNKNOWN='__JH_DYNAMIC__';
function nodes(root){
 const out=[],stack=[root];
 while(stack.length){
  const n=stack.pop();if(!n||typeof n!=='object')continue;if(n.type)out.push(n);
  for(const value of Object.values(n)){
   if(Array.isArray(value)){for(const child of value){if(child&&typeof child==='object')stack.push(child);}}
   else if(value&&typeof value==='object')stack.push(value);
  }
 }
 return out;
}
function extract(code){
 let tree;try{tree=acorn.parse(code,{ecmaVersion:'latest',sourceType:'module',allowReturnOutsideFunction:true,allowAwaitOutsideFunction:true});}catch(error){return {keys:[],imports:[],error:'JavaScript parse failure at '+error.loc.line+':'+error.loc.column};}
 const all=nodes(tree),parents=new Map();for(const parent of all)for(const value of Object.values(parent)){if(Array.isArray(value)){for(const child of value)if(child&&child.type)parents.set(child,parent);}else if(value&&value.type)parents.set(value,parent);}
 const env=new Map(),conflicts=new Set(),keys=new Set(),imports=new Set();
 function resolve(n,scope=env){
  if(!n)return UNKNOWN;
  if(n.type==='Literal'&&typeof n.value==='string')return n.value;
  if(n.type==='Identifier')return scope.get(n.name)??UNKNOWN;
  if(n.type==='BinaryExpression'&&n.operator==='+')return resolve(n.left,scope)+resolve(n.right,scope);
  if(n.type==='TemplateLiteral')return n.quasis.map((q,i)=>(q.value.cooked??q.value.raw)+(i<n.expressions.length?resolve(n.expressions[i],scope):'')).join('');
  if(n.type==='ConditionalExpression'){const a=resolve(n.consequent,scope),b=resolve(n.alternate,scope);return a===b?a:/^[?&]/.test(a)&&/^[?&]/.test(b)?'?'+UNKNOWN:UNKNOWN;}
  return UNKNOWN;
 }
 for(let pass=0;pass<4;pass++)for(const n of all){
  if(n.type==='VariableDeclarator'&&n.id.type==='Identifier'&&n.init){
   const value=resolve(n.init);if(value.includes(UNKNOWN))continue;
   const old=env.get(n.id.name);if(old!==undefined&&old!==value){conflicts.add(n.id.name);env.delete(n.id.name);}else if(!conflicts.has(n.id.name))env.set(n.id.name,value);
  }
 }
 function add(value,allowBare=false){
  if(typeof value!=='string')return;
  value=value.trim();const absolute=value.startsWith('/')||/^https?:\/\//.test(value);
  if(/^https?:\/\//.test(value)){try{const url=new URL(value);if(url.hostname==='s3.amazonaws.com'&&url.pathname.startsWith('/justhodl-dashboard-live/'))value=url.pathname.slice('/justhodl-dashboard-live/'.length);else {if(!hosts.has(url.hostname))return;value=url.pathname;}}catch{return;}}
  value=value.split(/[?#]/,1)[0];
  if(value.includes(UNKNOWN)||value.includes('*')||value.includes('${')||value.includes('..')||/\s/.test(value))return;
  value=value.replace(/^\/+/, '');
  if(/^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*\.json(?:\.gz)?$/.test(value)&&(allowBare||absolute||value.includes('/')))keys.add(value);
 }
 const isFetch=n=>n?.type==='CallExpression'&&(n.callee.type==='Identifier'&&n.callee.name==='fetch'||n.callee.type==='MemberExpression'&&n.callee.property?.name==='fetch');
 const wrappers=new Map(),ambiguousWrappers=new Set(),functions=[];
 function localNodes(root){
  const out=[],stack=[root];while(stack.length){const n=stack.pop();if(!n||typeof n!=='object')continue;
   if(n!==root&&['FunctionDeclaration','FunctionExpression','ArrowFunctionExpression'].includes(n.type))continue;
   if(n.type)out.push(n);for(const value of Object.values(n)){if(Array.isArray(value)){stack.push(...value);}else if(value&&typeof value==='object')stack.push(value);}
  }return out;
 }
 for(const n of all){
  let name,fn;
  if(n.type==='FunctionDeclaration'){name=n.id?.name;fn=n;}
  if(n.type==='VariableDeclarator'&&['ArrowFunctionExpression','FunctionExpression'].includes(n.init?.type)){name=n.id.name;fn=n.init;}
  if(name)functions.push({name,fn});
 }
 for(let pass=0;pass<=functions.length;pass++){
  let changed=false;
  for(const {name,fn} of functions){
   const scoped=new Map(env),body=localNodes(fn.body);
   for(const item of body)if(item.type==='VariableDeclarator'&&item.id.type==='Identifier')scoped.delete(item.id.name);
   fn.params.forEach((p,i)=>{if(p.type==='Identifier')scoped.set(p.name,'__JH_PARAM_'+i+'__');});
   // Parameter-local URL variables are resolved without executing source.
   for(let i=0;i<body.length;i++){let progress=false;for(const item of body){
    if(item.type==='VariableDeclarator'&&item.id.type==='Identifier'&&item.init){const value=resolve(item.init,scoped);if(value!==UNKNOWN&&scoped.get(item.id.name)!==value){scoped.set(item.id.name,value);progress=true;}}
   }if(!progress)break;}
   const scopes=[scoped];
   for(const loop of body.filter(n=>n.type==='ForOfStatement'&&n.right.type==='ArrayExpression')){
    const param=loop.left.type==='VariableDeclaration'?loop.left.declarations[0]?.id:loop.left;
    if(param?.type==='Identifier')for(const option of loop.right.elements){const next=new Map(scoped);next.set(param.name,resolve(option,scoped));scopes.push(next);}
   }
   const patterns=[];
   for(const scope of scopes)for(const call of nodes(fn.body).filter(n=>n.type==='CallExpression')){
    if(isFetch(call))patterns.push(resolve(call.arguments[0],scope));
    else if(call.callee.type==='Identifier'&&wrappers.has(call.callee.name))for(const pattern of wrappers.get(call.callee.name))patterns.push(pattern.replace(/__JH_PARAM_(\d+)__/g,(_,i)=>resolve(call.arguments[+i],scope)));
   }
   const sorted=[...new Set(patterns.filter(x=>x!==UNKNOWN))].sort();
   if(sorted.length&&!ambiguousWrappers.has(name)){
    const duplicate=functions.filter(x=>x.name===name).length>1;
    if(duplicate&&wrappers.has(name)&&JSON.stringify(wrappers.get(name))!==JSON.stringify(sorted)){wrappers.delete(name);ambiguousWrappers.add(name);}
    else if(JSON.stringify(wrappers.get(name))!==JSON.stringify(sorted)){wrappers.set(name,sorted);changed=true;}
   }
  }if(!changed)break;
 }
 for(const n of all){
  if(n.type==='Literal'&&typeof n.value==='string'&&parents.get(n)?.type!=='BinaryExpression')add(n.value);
  if(['TemplateLiteral','BinaryExpression'].includes(n.type))add(resolve(n));
  if(isFetch(n))add(resolve(n.arguments[0]),true);
  if(n.type==='CallExpression'&&n.callee.type==='Identifier'&&wrappers.has(n.callee.name))for(const pattern of wrappers.get(n.callee.name)){
   add(pattern.replace(/__JH_PARAM_(\d+)__/g,(_,i)=>resolve(n.arguments[+i])),true);
  }
  if(['ImportDeclaration','ExportNamedDeclaration','ExportAllDeclaration'].includes(n.type)&&n.source?.value)imports.add(n.source.value);
  if(n.type==='ImportExpression'&&n.source?.type==='Literal')imports.add(n.source.value);
  if(n.type==='CallExpression'&&n.callee.name==='importScripts')for(const arg of n.arguments)if(arg.type==='Literal')imports.add(arg.value);
 }
 return {keys:[...keys].sort(),imports:[...imports].sort(),parser:'acorn-'+acorn.version};
}
const codes=JSON.parse(fs.readFileSync(0,'utf8'));process.stdout.write(JSON.stringify(codes.map(extract)));
