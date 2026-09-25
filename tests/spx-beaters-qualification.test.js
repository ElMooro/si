const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../spx-beaters.html'),'utf8');
const script=[...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]).find(s=>s.includes('function aiLine('));
function scope(){const s={};vm.createContext(s);vm.runInContext(script.slice(0,script.lastIndexOf('load();')),s);return s;}

test('the actual SPX renderer withholds cached AI forecasts and handles hostile provider text',()=>{
 const s=scope(),r={t:'<img src=x onerror=boom()>',name:'<script>boom()</script>',score:100,n_legs:'<svg/onload=boom()>',
  fundamentals_intact:true,why:['<img onerror=boom()>'],odds_base_26w_pct:99,
  ai:{stance:'BUY',mode:'llm',odds_beat_spx_26w_pct:99,horizon_weeks:26,downside_risk_pct:2,one_liner:'UNSAFE_FORECAST'}};
 s.row=r;const rendered=vm.runInContext('rowHtml(row)',s);
 assert.match(rendered,/WAIT · forecast unqualified/);assert.match(rendered,/Forward return odds unqualified/);
 assert.doesNotMatch(rendered,/UNSAFE_FORECAST|99%|fundamentals intact|<img|<script|<svg/);
 assert.match(rendered,/&lt;img/);assert.match(rendered,/Legacy fundamental classification: unqualified/);
 assert.equal(vm.runInContext('fmt(null)',s),'—');assert.equal(vm.runInContext('fmt("12")',s),'—');
 assert.equal(vm.runInContext('fmt(Infinity)',s),'—');
 assert.doesNotThrow(()=>vm.runInContext('rowHtml({why:{malformed:true}})',s));
});

test('the actual SPX page load escapes packet text and labels sizing and cohort limitations',async()=>{
 const s=scope(),nodes={};s.document={getElementById:id=>nodes[id]||(nodes[id]={})};
 s.fetch=async()=>({json:async()=>({ok:true,engine_v:'<svg/onload=x>',min_score:'<img src=x>',weights:{},
  regime:{rotation_regime:'<script>bad()</script>'},ledger:{weeks:'<img src=x>'},buckets:{},
  base_rates:{window:'<img src=x>',momentum_quintiles:[{q:'<svg/onload=x>',n:'<img src=x>'}],comeback_cohort:{n:'<img src=x>'}},
  method:'<iframe src=x>',mom_status:{note:'<img src=x>'}})});
 await vm.runInContext('load()',s);
 for(const value of Object.values(nodes))assert.doesNotMatch(value.innerHTML||'',/<img|<svg|<script|<iframe/);
 assert.match(nodes.regime.innerHTML,/Position sizing/);assert.match(nodes.regime.innerHTML,/Unqualified/);
 assert.match(nodes.brates.innerHTML,/point-in-time membership/);assert.match(nodes.foot.innerHTML,/independent confirmation/);
});

test('every inline SPX script parses',()=>{for(const m of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))new vm.Script(m[1]);});
