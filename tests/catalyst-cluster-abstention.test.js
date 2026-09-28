const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../pre-pump-radar.html'),'utf8');
const start=html.indexOf('function renderClustersPanel(){'),end=html.indexOf('function _legacy_renderClustersPanel(){',start);
assert.ok(start>=0&&end>start);
const code=html.slice(start,end);
function render(packet){
 const nodes={'clusters-panel':{style:{}},'clusters-content':{innerHTML:''}};
 const scope={CLUSTERS:packet,document:{getElementById:id=>nodes[id]},esc:v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))};
 vm.runInNewContext(code+'\nrenderClustersPanel();',scope);
 return nodes['clusters-content'].innerHTML;
}
test('missing packet cannot imply zero clusters or silently hide its absence',()=>{
 for(const p of [null,{}, {status:'error',clusters:[]}])assert.match(render(p),/membership unavailable/);
 assert.match(render({clusters:[]}),/0 source-reported cluster occurrences/);
});
test('legacy grade and action cannot become a displayed recommendation',()=>{
 const p={clusters:[{quality_grade:'A',leader:'SYN_A',cluster_type:'TEMPORAL_EARNINGS',scope:'BASKET_LOCAL',members:['SYN_A','SYN_B','SYN_C'],recommendation:{action:'BOOST',leader_new_size:25,rationale:'buy now'}}]};
 const result=render(p);assert.match(result,/Research · abstain/);assert.match(result,/3 distinct reported tickers/);
 assert.doesNotMatch(result,/q-a|BOOST|buy now|25%|Leader:/);
});
test('withheld grade never falls back to D or trim',()=>{
 const result=render({clusters:[{quality_grade:null,members:['SYN_A'],recommendation:{action:'WAIT'}}]});
 assert.match(result,/No ranked leader/);assert.doesNotMatch(result,/q-d|TRIM|HEDGE/);
});
test('duplicates and untrusted source labels remain explicit and inert',()=>{
 const result=render({clusters:[null,1,{members:['<img src=x onerror=alert(1)>','SYN_A','SYN_A',null],cluster_type:'<script>bad</script>',start_date:'"<b>x</b>',end_date:'unknown'}]});
 assert.match(result,/3 member occurrences; 2 distinct/);assert.match(result,/&lt;img/);assert.match(result,/&lt;script/);
 assert.doesNotMatch(result,/<img|<script|<b>x/);
});
test('whole preceding page is retained and cluster fetch path is unchanged',()=>{
 const original=fs.readFileSync(path.join(__dirname,'fixtures/pre-cluster-abstention-pre-pump-radar.html.txt'),'utf8');
 assert.ok(original.includes('function renderClustersPanel(){'));assert.ok(original.length>200000);
 assert.match(html,/const CLUSTERS_URL\s*= "https:\/\/justhodl-data-proxy.raafouis.workers.dev\/data\/catalyst-clusters.json"/);
 assert.ok(html.includes('function _legacy_renderClustersPanel(){'));
});
