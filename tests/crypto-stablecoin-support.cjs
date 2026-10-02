const fs=require('node:fs'),path=require('node:path');
const R=path.join(__dirname,'..'),parser={exports:{}};Function('exports','module',process.binding('natives')['internal/deps/acorn/acorn/dist/acorn'])(parser.exports,parser);const acorn=parser.exports;
function classic(){
 const html=fs.readFileSync(path.join(R,'classic-dashboard.html'),'utf8'),styles=[...html.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/gi)].map(m=>m[1]).join('\n'),functions=[];
 for(const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)){
  if(/\bsrc\s*=|type\s*=\s*["']application\//i.test(m[1]))continue;
  const tree=acorn.parse(m[2],{ecmaVersion:'latest'});functions.push(...tree.body.filter(n=>n.type==='FunctionDeclaration').map(n=>m[2].slice(n.start,n.end)));
 }
 return {styles,code:functions.join('\n')};
}
module.exports={classic};
