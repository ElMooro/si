const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const R=path.join(__dirname,'..'),negative=process.env.JH_CHART_LIFECYCLE_NEGATIVE_CONTROL==='1',raw=fs.readFileSync(path.join(R,negative?'tests/fixtures/chart-refresh-status/pre509/jh-chart-engine.js.txt':'jh-chart-engine.js'),'utf8');
const start=raw.indexOf('  function mkChart(el){'),end=raw.indexOf('\n  chart=mkChart(host);',start),source=raw.slice(start,end<0?undefined:end);
function setup(options={}){
 const observers=[],timers=new Map(),listeners=new Set(),charts=[],cancelled=[],queued=[];let id=0;
 const c={chartLook:()=>({retained:'look'}),LW:{createChart(el,look){const chart={el,look,resizes:[],removes:0,resize(w,h){this.resizes.push([w,h]);if(this.removes)throw Error('disposed chart resize');},remove(){this.removes++;if(options.throwRemove)throw Error('underlying remove failure');return 'removed';}};charts.push(chart);return chart;}},setTimeout(fn,ms){timers.set(++id,{fn,ms});queued.push(fn);return id;},clearTimeout(id){timers.delete(id);cancelled.push(id);},window:{addEventListener(type,fn){assert.equal(type,'resize');listeners.add(fn);},removeEventListener(type,fn){assert.equal(type,'resize');listeners.delete(fn);}}};
 if(!options.noObserver)c.ResizeObserver=class {constructor(fn){this.fn=fn;this.targets=[];this.disconnected=false;observers.push(this);}observe(el){this.targets.push(el);}disconnect(){this.disconnected=true;}};
 vm.createContext(c);vm.runInContext(source,c);const el={clientWidth:600,clientHeight:400,parentElement:{clientWidth:700,clientHeight:500}};return {c,el,observers,timers,listeners,charts,cancelled,queued};
}
test('current charts retain their sizing, original options and resize triggers',()=>{
 const h=setup(),chart=h.c.mkChart(h.el);assert.equal(chart.look.retained,'look');assert.equal(chart.look.width,600);assert.equal(chart.look.height,400);assert.equal(chart.look.autoSize,true);assert.deepEqual(h.observers[0].targets,[h.el,h.el.parentElement]);assert.deepEqual(Array.from(h.timers.values(),x=>x.ms),[0,250]);h.observers[0].fn();Array.from(h.listeners)[0]();h.queued.forEach(fn=>fn());assert.equal(chart.resizes.length,4);assert.deepEqual(chart.resizes[0],[600,400]);
});
test('removal disconnects only owned observer, timers and window listener before destruction',()=>{
 const h=setup(),chart=h.c.mkChart(h.el);assert.equal(chart.remove(),'removed');assert.equal(chart.removes,1);assert.equal(h.observers[0].disconnected,true);assert.equal(h.timers.size,0);assert.equal(h.listeners.size,0);assert.deepEqual(h.cancelled,[1,2]);
});
test('already queued callbacks cannot resize a removed chart when its host is reused',()=>{
 const h=setup(),chart=h.c.mkChart(h.el),resize=Array.from(h.listeners)[0];chart.remove();h.el.clientWidth=900;h.observers[0].fn();h.queued.forEach(fn=>fn());resize();assert.deepEqual(chart.resizes,[]);
});
test('repeated removal never destroys the same drawing object twice',()=>{
 const h=setup(),chart=h.c.mkChart(h.el);chart.remove();chart.remove();assert.equal(chart.removes,1);assert.deepEqual(h.cancelled,[1,2]);
});
test('cleanup still works without ResizeObserver support',()=>{
 const h=setup({noObserver:true}),chart=h.c.mkChart(h.el),resize=Array.from(h.listeners)[0];chart.remove();resize();h.queued.forEach(fn=>fn());assert.equal(h.listeners.size,0);assert.equal(h.timers.size,0);assert.deepEqual(chart.resizes,[]);
});
test('replacing one chart leaves another chart and its callbacks working',()=>{
 const h=setup(),a=h.c.mkChart(h.el),b=h.c.mkChart({...h.el});a.remove();assert.equal(h.listeners.size,1);assert.equal(h.timers.size,2);assert.equal(h.observers[0].disconnected,true);assert.equal(h.observers[1].disconnected,false);h.queued.forEach(fn=>fn());assert.deepEqual(a.resizes,[]);assert.equal(b.resizes.length,2);b.remove();assert.equal(h.listeners.size,0);
});
test('resource cleanup survives an underlying chart removal failure',()=>{
 const h=setup({throwRemove:true}),chart=h.c.mkChart(h.el);assert.throws(()=>chart.remove(),/underlying remove failure/);assert.equal(h.timers.size,0);assert.equal(h.listeners.size,0);h.queued.forEach(fn=>fn());assert.deepEqual(chart.resizes,[]);chart.remove();assert.equal(chart.removes,1);
});
