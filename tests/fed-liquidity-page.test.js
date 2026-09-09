const test=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const vm=require('node:vm');
const source=fs.readFileSync('census.html','utf8').match(/<script>\s*(const API_URL[\s\S]*?)<\/script>/)[1];
function page(response){
 const ids=new Map();const element=()=>({textContent:'',innerHTML:'',value:'',children:[],append(n){this.children.push(n)},replaceChildren(){this.children=[]}});
 const context={document:{getElementById(id){if(!ids.has(id))ids.set(id,element());return ids.get(id)},addEventListener(){},createElement:element},localStorage:{getItem(){return '{invalid'}},console,fetch:async()=>response};
 vm.createContext(context);vm.runInContext(source,context);return {context,ids};
}
test('actual Fed page maps summary schema and preserves full payload and zero',async()=>{
 const payload={summary:{WALCL:{name:'Assets',latest_value:0,latest_date:'2026-09-09',week_change:0,month_change:null,extra_field:false}},status:'COMPLETE',last_updated:'2026-09-09T10:00:00Z'};
 const {context,ids}=page({ok:true,json:async()=>payload});await context.fetchData();
 assert.equal(ids.get('fed-balance').textContent,'0.0000');assert.equal(ids.get('api-status').textContent,'Loaded');
 assert.equal(ids.get('last-update').textContent,payload.last_updated);assert.deepEqual(JSON.parse(ids.get('summary-payload').textContent),payload);
 assert.match(ids.get('all-series').innerHTML,/Assets/);assert.match(ids.get('risk-grid').textContent,/unavailable/);
});
test('HTTP failure and legacy schema cannot show live or retain successful cards',async()=>{
 for(const response of [{ok:false,json:async()=>({})},{ok:true,json:async()=>({data:[]})}]){
  const {context,ids}=page(response);await context.fetchData();assert.equal(ids.get('api-status').textContent,'Error');assert.match(ids.get('all-series').innerHTML,/Unable to load/);
 }
});
test('custom names are escaped before HTML attributes and calendar dates remain visible',()=>{
 const {context}=page({});
 const html=context.createSeriesCard({series_id:'WALCL',name:'\" onfocus=alert(1) <img>',value:1,date:'2026-09-09',week_comparison_date:'2026-09-02',changes:{wow:0}});
 assert.match(html,/&quot; onfocus/);assert.doesNotMatch(html,/<img>/);assert.match(html,/2026-09-02/);
});
test('full catalog is rendered as text without dropping series or categories',async()=>{
 const payload={series:{WALCL:'<img onerror=alert(1)>',M2SL:'Money'},categories:{money:['M2SL']},series_count:2};
 const {context,ids}=page({ok:true,json:async()=>payload});await context.loadCatalog();assert.equal(ids.get('series-picker').children.length,2);
 assert.deepEqual(JSON.parse(ids.get('catalog-payload').textContent),payload);
});

test('native money units normalize explicitly and absent values never become zeros',()=>{
 const {context}=page({});assert.equal(context.metricValue(6700000,'Millions of U.S. Dollars'),'6.70T USD');
 assert.equal(context.metricValue(23000,'Billions of Dollars'),'23.00T USD');
 assert.equal(context.metricValue(0,'Percent'),'0.0000%');assert.equal(context.metricValue(null,'Percent'),'Unavailable');
});
