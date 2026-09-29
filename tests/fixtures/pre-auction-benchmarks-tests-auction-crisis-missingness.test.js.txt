const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname,'../auction-crisis.js'),'utf8');
function page(initial = {}, suppliedSource = source) {
  const nodes = new Map();
  const get = id => {
    if (!nodes.has(id)) nodes.set(id,{innerHTML:'',textContent:'',className:'panel CALM',style:{}});
    return nodes.get(id);
  };
  const reads = [];
  const env = {document:{getElementById:get},Date,console,fixture:initial,
    fetch:async url=>{ reads.push(url);return {ok:true,json:async()=>initial}; }};
  vm.createContext(env);
  vm.runInContext(suppliedSource.slice(0,suppliedSource.lastIndexOf('load();')),env);
  vm.runInContext('DATA=fixture;',env);
  return {env,get,reads,set(value){env.fixture=value;vm.runInContext('DATA=fixture;',env);}};
}

test('exact predecessor reproduces unavailable score as zero',()=>{
  const old=fs.readFileSync(path.join(__dirname,'fixtures/pre-auction-page-missingness-auction-crisis.js.txt'),'utf8');
  const {env,get}=page({composite_score:null,regime:'UNAVAILABLE'},old);env.renderHero();
  assert.equal(get('composite-score').textContent,'0.0');
});

test('missing invalid and out-of-range scores never become calm or zero',()=>{
  for (const value of [undefined,null,false,true,'0',NaN,Infinity,-1,101]) {
    const {env,get}=page({composite_score:value,regime:'CALM'});env.renderHero();
    assert.equal(get('composite-score').textContent,'—');
    assert.equal(get('regime-text').textContent,'UNAVAILABLE');
    assert.doesNotMatch(get('score-circle').className,/\bCALM\b/);
  }
  const {env,get}=page({composite_score:0,regime:'CALM'});env.renderHero();
  assert.equal(get('composite-score').textContent,'0.0');
});

test('missing weight reason and refresh clear previous hero measurements',()=>{
  const {env,get,set}=page({composite_score:80,regime:'ACUTE_STRESS',fed_funds_rate:4.25,n_recent_auctions_14d:10});
  env.renderHero();set({composite_score:null,weighting_quality:{status:'missing_weight',n_missing_weights:2}});env.renderHero();
  assert.match(get('regime-desc').textContent,/2 auction observations lack/);
  assert.equal(get('fed-rate').textContent,'—');assert.equal(get('n-recent').textContent,'—');
  assert.doesNotMatch(get('score-circle').className,/\bACUTE_STRESS\b/);
});

test('auction table keeps zero and rejects numeric strings as measurements',()=>{
  const {env,get}=page({recent_auctions:[{security_type:'<img>',btc:0,high_rate:0,primary_dealer_pct:0,indirect_pct:null,composite_score:null},
    {security_type:'Note',btc:'<script>',high_rate:'4.2',primary_dealer_pct:false,composite_score:'0'}]});
  env.renderAuctionsTable();const html=get('auctions-tbl').innerHTML;
  assert.match(html,/&lt;img&gt;/);assert.match(html,/>0\.000</);assert.match(html,/stress-no-data/);
  assert.doesNotMatch(html,/<script>|<img>|4\.200|NaN|Infinity/);
});

test('composite line has real gaps and no invented area below missing dates',()=>{
  const series=[0,10,null,20,30].map((composite,i)=>({date:`2026-09-${20+i}`,composite}));
  const {env,get}=page({composite_history:{series,change_points:[]}});env.renderCompositeChart();
  const html=get('composite-chart').innerHTML;
  assert.equal((html.match(/<polyline/g)||[]).length,2);
  assert.equal((html.match(/<path/g)||[]).length,2);
  assert.doesNotMatch(html,/NaN|Infinity/);
});

test('one-point and malformed numeric histories cannot break the chart',()=>{
  for (const value of [0,null,true,'80',Infinity]) {
    const {env,get}=page({composite_history:{series:[{date:'<img>',composite:value}]}});env.renderCompositeChart();
    const html=get('composite-chart').innerHTML;assert.doesNotMatch(html,/NaN|Infinity|<img>/);
    if (value===0) assert.match(html,/<circle/);
  }
  const {env,get,set}=page({composite_history:{series:[{date:'2026-09-29',composite:30}]}});env.renderCompositeChart();
  set({});env.renderCompositeChart();assert.equal(get('composite-chart').innerHTML,'');
});

test('absent firing aggregate and issuance are unavailable rather than calm',()=>{
  const {env,get}=page({});env.renderIndicators();const html=get('indicator-grid').innerHTML;
  assert.match(html,/UNAVAILABLE/);assert.doesNotMatch(html,/>CALM<|0 \/ max 0/);
});

test('unverified action text cannot become an investment instruction',()=>{
  const {env,get}=page({triggers:[{name:'Threshold',current:25,threshold:50,action:'BUY 100% TLT immediately'}]});
  env.renderTriggers();env.renderResearchContext();
  assert.match(get('trigger-grid').innerHTML,/Descriptive threshold only/);
  assert.doesNotMatch(get('trigger-grid').innerHTML,/BUY 100%/);
  assert.match(get('decisive-text').textContent,/WAIT/);
  assert.match(get('ai-forward-grid').innerHTML,/Inspect the separate, unverified commentary packet/);
  assert.doesNotMatch(get('decisive-text').textContent,/BUY 100%/);
});

test('stale refresh clears previously rendered data',()=>{
  const {env,get}=page({composite_score:80,regime:'ACUTE_STRESS',recent_auctions:[{btc:2.5,composite_score:80}]});
  env.renderHero();env.renderAuctionsTable();env.showError('Source stale');
  assert.equal(get('composite-score').textContent,'—');
  assert.doesNotMatch(get('auctions-tbl').innerHTML,/2\.50|80\.0/);
  assert.equal(get('errorBanner').textContent,'Source stale');
});

test('ordinary load renders research without fetching commentary or an AI API',async()=>{
  const {env,get,reads}=page({generated_at:new Date().toISOString(),quality:{status:'fresh'},composite_score:null});
  await env.load();assert.equal(reads.length,1);assert.match(reads[0],/data\/auction-crisis\.json/);
  assert.match(get('decisive-text').textContent,/WAIT/);
});

test('economic quote bases are explicit and unavailable WI tails stay unavailable',()=>{
  const {env,get}=page({recent_auctions:[{security_type:'Note',instrument_kind:'TIPS',instrument_classification_status:'verified',
    quote_basis:'real_yield_pct',high_rate:2.1,high_minus_median_bp:0,wi_tail_bp:0,tail_bp:0,composite_score:0}]});
  env.renderAuctionsTable();const html=get('auctions-tbl').innerHTML;
  assert.match(html,/>TIPS</);assert.match(html,/>Real yield</);assert.match(html,/High–median bp/);
  assert.match(html,/No timestamped same-security when-issued quote is acquired">—/);
});

test('bill discount rates TIPS FRN and unverified rows do not get nominal benchmark performance labels',()=>{
  const rows=['BILL','TIPS','FRN','UNKNOWN'].map(instrument_kind=>({instrument_kind,instrument_contract:'treasury-instrument.v1',
    instrument_classification_status:'verified',postissue_classification:'STRONG',postissue_1d_bp:50}));
  const {env,get}=page({recent_auctions:rows});env.renderPostIssuePerformance();
  assert.match(get('postissue-section').innerHTML,/No verified nominal benchmark/);
  assert.doesNotMatch(get('postissue-section').innerHTML,/>STRONG<|50\.0/);
});

test('nominal comparisons preserve measured zero without implying investment performance',()=>{
  const {env,get}=page({recent_auctions:[{instrument_kind:'NOMINAL_COUPON',instrument_contract:'treasury-instrument.v1',
    instrument_classification_status:'verified',quote_basis:'nominal_yield_pct',high_rate:4.2,
    postissue_classification:'STRONG',postissue_1d_bp:0,postissue_5d_bp:null}]});
  env.renderPostIssuePerformance();const html=get('postissue-section').innerHTML;
  assert.match(html,/>4\.200</);assert.match(html,/>\+0\.0</);assert.match(html,/Descriptive only/);
  assert.doesNotMatch(html,/>STRONG</);
});

test('indicator and page labels cannot invent investor motive or net liquidity',()=>{
  const {env,get}=page({});env.renderIndicators();const html=get('indicator-grid').innerHTML;
  assert.match(html,/domestic and foreign investors/);assert.match(html,/not net liquidity injection/);
  assert.doesNotMatch(html,/exodus|stuck with paper|liquidity injection \/ panic/);
  const document=fs.readFileSync(path.join(__dirname,'../auction-crisis.html'),'utf8');
  assert.doesNotMatch(document,/AI Executive Summary|AI Scenario Commentary|STRONG=bid held|data refresh hourly/);
});

test('fresh reload clears stale banner and provider values cannot become active trigger HTML',async()=>{
  const {env,get}=page({generated_at:new Date().toISOString(),quality:{status:'fresh'},composite_score:0,regime:'CALM'});
  env.showError('Stale');await env.load();assert.equal(get('errorBanner').textContent,'');
  assert.equal(get('errorBanner').style.display,'none');assert.equal(get('composite-score').textContent,'0.0');
  env.fixture={triggers:[{name:'<img>',current:'<img>',threshold:'<img>',distance:'<img>',auctions_already_fired_14d:'<img>'}]};
  vm.runInContext('DATA=fixture;',env);env.renderTriggers();
  assert.match(get('trigger-grid').innerHTML,/&lt;img&gt;/);assert.doesNotMatch(get('trigger-grid').innerHTML,/<img>/);
});

test('tenor and analog values cannot inject markup and absent analogs clear old matches',()=>{
  const {env,get,set}=page({tenor_decomposition:{x:{rank:'<img>',max_composite:'<img>',n_auctions:'<img>',latest_date:'<img>'}},
    historical_analog:{top_matches:[{anchor_metrics:{btc:'<img>'}},{date:'<img>',similarity:null}]}});
  env.renderTenorDecomposition();env.renderAnalogDataOnly();
  assert.doesNotMatch(get('tenor-grid').innerHTML,/<img>/);assert.doesNotMatch(get('analog-top').innerHTML,/<img>/);
  assert.match(get('analog-others').innerHTML,/— similar/);assert.doesNotMatch(get('analog-others').innerHTML,/0% similar/);
  set({});env.renderAnalogDataOnly();assert.equal(get('analog-others').innerHTML,'');
});
