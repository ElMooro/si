const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync(require('node:path').join(__dirname,'../auctions.html'),'utf8');
const script=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(x=>x[1]).find(s=>s.includes('function renderMarketImpact'));
function page(){
 const nodes=new Map();const get=id=>{if(!nodes.has(id))nodes.set(id,{innerHTML:'',textContent:'',style:{},parentElement:{},classList:{add(){},remove(){}}});return nodes.get(id);};
 const env={document:{getElementById:get},Date,console};vm.createContext(env);
 vm.runInContext(script.slice(0,script.lastIndexOf('// Init')),env);return {env,get};
}
test('missing score and indicator observations cannot render zero or OFF',()=>{
 const {env,get}=page();env.renderHero({});env.renderIndicators({});
 assert.equal(get('scoreNum').textContent,'—');assert.equal(get('scoreRegime').textContent,'UNAVAILABLE');
 assert.doesNotMatch(get('indicators').innerHTML,/✓ OFF/);assert.match(get('indicators').innerHTML,/UNAVAILABLE/);
 env.renderIndicators({fed_funds_rate:0.1});assert.doesNotMatch(get('indicators').innerHTML,/🔴 FIRING/);
});
test('unsupported historical arrays and allocation prose do not become displayed estimates',()=>{
 const {env,get}=page();env.renderForward();env.renderBtc();env.renderAnchors({});env.renderMarketImpact({composite_score:90});
 assert.match(get('forwardMatrixBody').innerHTML,/Unavailable/);
 assert.doesNotMatch(get('forwardMatrixBody').innerHTML,/80\.0%|1\.5%|8\.0%/);
 assert.match(get('marketImpact').textContent,/No portfolio percentage/);
 assert.doesNotMatch(html,/Trim 10-20%|Equity exposure under 30%|const FORWARD_RETURNS|No backtest survivorship/);
});
test('calendar has no stale fixed-date fallback and escapes provider text',()=>{
 const {env,get}=page();env.renderTriggers();assert.match(get('triggers').innerHTML,/No current announced calendar/);
 env.renderTriggers({calendar:{auctions:[{auction_date:'2099-01-01',term:'<script>bad</script>',type:'Note'}]}});
 assert.match(get('triggers').innerHTML,/&lt;script&gt;/);assert.doesNotMatch(get('triggers').innerHTML,/<script>/);
 assert.doesNotMatch(html,/const TRIGGERS|Watch the May 14/);
});

test('refresh clears prior metrics and tape preserves zero versus missing',()=>{
 const {env,get}=page();env.renderHero({composite_score:90,n_recent_auctions_14d:7,issuance_anomaly:{pct_above_baseline:12}});
 env.renderHero({});assert.equal(get('m_auctions14d').textContent,'—');assert.equal(get('m_issuance').textContent,'—');
 assert.equal(get('scoreRing').style.background,'var(--bg-elev)');
 env.renderTape({recent_auctions:[{security_term:'<img>',high_rate:0}]});
 assert.match(get('tapeBody').innerHTML,/0\.000%/);assert.match(get('tapeBody').innerHTML,/&lt;img&gt;/);
 assert.doesNotMatch(get('tapeBody').innerHTML,/>0\.0<|<img>/);
 assert.doesNotMatch(html,/data-feeds="data\/auction-decisive-call\.json|<script src="\/jh-enhance\.js"/);
});
