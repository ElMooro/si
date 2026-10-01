const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
const source = name => fs.readFileSync(path.join(root, name), 'utf8');
// Small DOM contract stub: executes the complete chrome script and its handlers.
// Not a layout/paint/browser substitute. Timers are retained but never fired.
function chrome(code = source('jh-chart-indux.js'), w = {}) {
  const nodes = new Map(), timers = [], listeners = {};
  function node(tagName = "div") {
    return { tagName: tagName.toUpperCase(), isConnected: true, open: false, focus() { document.activeElement = this; }, closest() { return null; }, getClientRects() { return this.isConnected ? [{}] : []; }, setAttribute(k,v) { this[k]=v; }, showModal() { this.open=true; }, close() { this.open=false; if(this.onclose)this.onclose(); }, id: '', className: '', textContent: '', dataset: {}, children: new Map(),
      appendChild(n) { if (n.id) nodes.set(n.id, n); },
      set innerHTML(s) {
        this.html = s; for (const n of this.children.values()) n.isConnected = false; this.children.clear();
        for (const match of s.matchAll(/<button\b([^>]*)>/g)) {
          const attrs = {}, b = node();
          for (const a of match[1].matchAll(/([\w-]+)(?:=(?:'([^']*)'|"([^"]*)"|([^\s>]+)))?/g)) attrs[a[1]] = a[2] ?? a[3] ?? a[4] ?? '';
          b.getAttribute = k => attrs[k];
          if (attrs.id) nodes.set(attrs.id, b);
          for (const [k, v] of Object.entries(attrs)) if (k.startsWith('data-')) this.children.set(`[${k}${v ? `='${v}'` : ''}]`, b);
        }
      },
      get innerHTML() { return this.html || ''; },
      querySelector(sel) { return this.children.get(sel) || null; },
      querySelectorAll(sel) { return [...this.children].filter(([k]) => k.startsWith(sel.slice(0, -1) + '=')).map(([,v]) => v); }
    };
  }
  nodes.set('legend', node());
  const document = { readyState: 'loading', createElement: node, getElementById: id => nodes.get(id) || null,
    documentElement: node(), body: node(), activeElement: null, querySelector(sel) { for (const n of nodes.values()) { const found=n.querySelector(sel); if(found)return found; } return null; }, addEventListener() {}, querySelectorAll() { return []; } };
  w.addEventListener = (name, fn) => { listeners[name] = fn; };
  const localStorage = { getItem() { return null; }, setItem() {} };
  new Function('window', 'document', 'localStorage', 'setTimeout', code)(w, document, localStorage, fn => timers.push(fn));
  return { w, nodes, listeners, timers, document, node };
}
function engine() {
  const w = { __classifyCalls: 0, __distributionCalls: 0 };
  new Function('window', source('jh-chart-vol-events.js').replace('function classify(d) {', 'function classify(d) { window.__classifyCalls++;'))(w);
  new Function('window', source('jh-chart-distribution.js').replace('function distributionScan(d) {', 'function distributionScan(d) { window.__distributionCalls++;'))(w);
  new Function('window', source('jh-chart-tape.js'))(w);
  return w;
}
function rows(n) { return Array.from({length:n}, (_,i) => ({time:1700000000+i*86400,open:100,high:101,low:99,close:100,volume:100})); }
function context(bars = [], studies = [{id:'voltape', n:'Volume Tape', on:true}]) {
  return { INDS: studies, lastBars: bars, atTime: null, overlayMap:{}, valAt:()=>null,
    spec:()=>['1d','Daily'], active:'TEST', tf:'1d', compare:[], volOn:false, fmt:String, fmtVol:String };
}
function scFixture(mode) {
  const d = rows(125);
  if (mode === 'expiration') {
    Object.assign(d[90], {open:100,high:100,low:88,close:99,volume:300});
    for(let i=91;i<d.length;i++) Object.assign(d[i],{open:89,high:90,low:88.5,close:89});
  } else if (mode === 'rally') {
    Object.assign(d[90],{open:90,high:90,low:80,close:81,volume:300});
    for(let i=91;i<d.length;i++) Object.assign(d[i],{open:81,high:82,low:80.5,close:81});
    Object.assign(d[93],{open:81,high:85,low:81,close:84});
  } else {
    for(let i=90;i<d.length;i++) Object.assign(d[i],{open:85,high:86,low:84,close:85});
    for(const [i,low] of [[90,80],[98,79],[106,78]]) Object.assign(d[i],{open:90,high:90,low,close:85,volume:300});
  }
  return d;
}
// Whole deterministic retained distribution test scenario; invented data, not market history.
function distributionFixture() {
  const d=[]; let px=100;
  for(let i=0;i<340;i++) {
    if(i<210) px=100*Math.pow(1.0022,i);
    const o=px; let h=px*1.004,l=px*.996,c=px,v=1e6;
    if(i===220) {h=168;c=166;l=164;v=1.1e6;px=166;}
    else if(i>220&&i<236) {px=166-(i-220)*.45;h=px*1.004;l=px*.996;c=px;}
    else if(i===248) {h=171.5;c=170.2;l=166;v=2.4e6;px=170.2;}
    else if(i>248&&i<270) {px=170-(i-248)*1.15;h=px*1.01;l=px*.99;c=px;v=1.6e6;}
    else if(i>=270) {px=145;h=px*1.004;l=px*.996;c=px;}
    d.push({time:1700000000+i*86400,open:o,high:h,low:l,close:c,volume:v});
  }
  return d;
}
function clippedDistribution() {
  const d=rows(50).concat(distributionFixture()).map((b,i)=>({...b,time:1700000000+i*86400}));
  for(let i=299;i<=315;i++) Object.assign(d[i],{open:170,high:170.5,low:169,close:170,volume:1600000});
  for(let i=316;i<330;i++) Object.assign(d[i],{open:160,high:161,low:158,close:160,volume:1600000});
  return d;
}
module.exports={source,chrome,engine,rows,context,scFixture,distributionFixture,clippedDistribution};
