/* ───────────────────────────────────────────────────────────────────
 * jh-ai-insights.js  v2.0
 *
 * Research availability widget for justhodl.ai.
 * Drop this <script> on any page and a floating violet pill appears
 * in the bottom-right. Click it to expand a panel showing the
 * site-wide descriptive research status, with the entry most relevant
 * to the current page surfaced first.
 *
 *   <script src="/jh-ai-insights.js" defer></script>
 *
 * Reads: https://justhodl-dashboard-live.s3.amazonaws.com/data/ai-website-synthesis.json
 * Cache: 15 minutes (matches the hourly refresh cadence)
 * Page detection: maps current pathname to per_page_focus keys
 * ──────────────────────────────────────────────────────────────── */
(function () {
  if (window.JHInsights) return;

  var DATA_URL = "https://justhodl-dashboard-live.s3.amazonaws.com/data/ai-website-synthesis.json";
  var CACHE_KEY = "jh_research_status_cache_v2";
  var CACHE_MAX_AGE_MS = 15 * 60 * 1000;  // 15 min

  // ─── Map current page → per_page_focus key ───
  var PAGE_MAP = {
    "auction-crisis":  "auction-crisis",
    "macro-frontrun":  "macro-frontrun",
    "frontrun":        "macro-frontrun",
    "crisis":          "crisis",
    "bonds":           "bonds",
    "repo":            "repo",
    "regime":          "regime",
    "correlation":     "correlation",
    "correlations":    "correlation",
    "sentiment":       "sentiment",
    "vol":             "volatility",
    "volatility":      "volatility",
  };

  function currentPageKey() {
    var p = (location.pathname || "").toLowerCase().replace(/\.html$/, "").replace(/^\//, "");
    return PAGE_MAP[p] || null;
  }

  // ─── Styles ───
  var CSS = "\
.jhi-fab{position:fixed;bottom:18px;right:18px;z-index:99999;\
  display:flex;align-items:center;gap:8px;\
  background:linear-gradient(135deg,rgba(167,139,250,.95),rgba(0,212,255,.92));\
  color:#07090f;font-family:'IBM Plex Mono',ui-monospace,monospace;\
  font-size:12px;font-weight:700;letter-spacing:1px;\
  padding:11px 16px;border-radius:30px;cursor:pointer;\
  box-shadow:0 4px 28px rgba(0,212,255,.32),0 2px 8px rgba(0,0,0,.6);\
  border:none;transition:all .18s ease;text-transform:uppercase;}\
.jhi-fab:hover{transform:translateY(-2px);box-shadow:0 6px 36px rgba(167,139,250,.42),0 2px 12px rgba(0,0,0,.7);}\
.jhi-fab .dot{width:7px;height:7px;border-radius:50%;background:#07090f;animation:jhi-pulse 2.2s ease-in-out infinite;}\
@keyframes jhi-pulse{0%,100%{opacity:1}50%{opacity:.35}}\
.jhi-fab.RISK_OFF,.jhi-fab.DEFENSIVE{background:linear-gradient(135deg,rgba(255,177,61,.95),rgba(255,92,122,.92));}\
.jhi-fab.EXTREME{background:linear-gradient(135deg,rgba(255,92,122,.98),rgba(255,80,80,.95));color:#fff;animation:jhi-flash 1.4s ease-in-out infinite;}\
@keyframes jhi-flash{0%,100%{box-shadow:0 4px 28px rgba(255,92,122,.4)}50%{box-shadow:0 4px 50px rgba(255,92,122,.85)}}\
.jhi-fab.RISK_ON{background:linear-gradient(135deg,rgba(56,255,168,.95),rgba(0,212,255,.92));}\
\
.jhi-panel{position:fixed;bottom:74px;right:18px;z-index:99998;\
  width:min(440px,calc(100vw - 40px));max-height:78vh;overflow-y:auto;\
  background:#0e1320;color:#e6ecf5;\
  font-family:'IBM Plex Sans',ui-sans-serif,system-ui,sans-serif;font-size:13px;line-height:1.55;\
  border:1px solid rgba(120,145,180,.4);border-radius:12px;\
  box-shadow:0 12px 60px rgba(0,0,0,.7),0 0 80px rgba(167,139,250,.12);\
  padding:18px 20px;\
  transform:translateY(20px);opacity:0;pointer-events:none;\
  transition:all .22s cubic-bezier(.2,.8,.2,1);}\
.jhi-panel.open{transform:translateY(0);opacity:1;pointer-events:auto;}\
\
.jhi-panel .hdr{display:flex;align-items:center;gap:10px;margin-bottom:14px;padding-bottom:10px;\
  border-bottom:1px solid rgba(120,145,180,.18);}\
.jhi-panel .hdr .ico{width:9px;height:9px;border-radius:50%;background:#a78bfa;box-shadow:0 0 12px #a78bfa;}\
.jhi-panel .hdr .ttl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:11px;letter-spacing:1.6px;\
  color:#a78bfa;font-weight:600;text-transform:uppercase;}\
.jhi-panel .hdr .age{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:10px;\
  color:#6b7a92;margin-left:auto;}\
.jhi-panel .hdr .close{background:transparent;border:none;color:#6b7a92;cursor:pointer;font-size:18px;\
  padding:0;margin-left:6px;line-height:1;}\
.jhi-panel .hdr .close:hover{color:#e6ecf5;}\
\
.jhi-posture{display:flex;align-items:center;gap:8px;margin-bottom:14px;}\
.jhi-posture .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#6b7a92;text-transform:uppercase;}\
.jhi-posture .val{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:13px;font-weight:700;\
  padding:3px 10px;border-radius:4px;letter-spacing:1px;}\
.jhi-posture .val.RISK_ON{background:rgba(56,255,168,.16);color:#38ffa8;border:1px solid rgba(56,255,168,.32);}\
.jhi-posture .val.NEUTRAL{background:rgba(0,212,255,.12);color:#00d4ff;border:1px solid rgba(0,212,255,.28);}\
.jhi-posture .val.RISK_OFF{background:rgba(255,177,61,.16);color:#ffb13d;border:1px solid rgba(255,177,61,.32);}\
.jhi-posture .val.DEFENSIVE{background:rgba(255,177,61,.2);color:#ffb13d;border:1px solid rgba(255,177,61,.45);}\
.jhi-posture .val.EXTREME{background:rgba(255,92,122,.2);color:#ff5c7a;border:1px solid rgba(255,92,122,.5);}\
\
.jhi-headline{font-size:14px;color:#e6ecf5;line-height:1.5;margin-bottom:12px;font-weight:500;}\
.jhi-headline strong{color:#a78bfa;}\
\
.jhi-section{margin-bottom:14px;}\
.jhi-section .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#00d4ff;font-weight:600;text-transform:uppercase;margin-bottom:6px;}\
.jhi-section .body{font-size:12.5px;color:#a8b3c7;line-height:1.6;}\
.jhi-section .body strong{color:#e6ecf5;font-weight:500;}\
.jhi-section ul{margin:0;padding-left:18px;}\
.jhi-section li{margin-bottom:3px;font-size:12px;color:#a8b3c7;}\
\
.jhi-call{background:linear-gradient(90deg,rgba(0,212,255,.08),rgba(167,139,250,.04),transparent);\
  border-left:3px solid #00d4ff;border-radius:0 6px 6px 0;\
  padding:11px 14px;margin-bottom:14px;font-size:13px;color:#e6ecf5;line-height:1.55;font-weight:500;}\
.jhi-call .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#00d4ff;font-weight:700;text-transform:uppercase;display:block;margin-bottom:5px;}\
\
.jhi-page-focus{background:rgba(167,139,250,.06);border-left:3px solid #a78bfa;border-radius:0 6px 6px 0;\
  padding:11px 14px;margin-bottom:14px;font-size:13px;color:#e6ecf5;line-height:1.55;}\
.jhi-page-focus .lbl{font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;letter-spacing:1.5px;\
  color:#a78bfa;font-weight:700;text-transform:uppercase;display:block;margin-bottom:5px;}\
\
.jhi-loading,.jhi-err{padding:18px;text-align:center;color:#6b7a92;font-size:12px;\
  font-family:'IBM Plex Mono',ui-monospace,monospace;}\
.jhi-err{color:#ff5c7a;}\
\
.jhi-footer{margin-top:14px;padding-top:10px;border-top:1px solid rgba(120,145,180,.18);\
  display:flex;align-items:center;justify-content:space-between;\
  font-family:'IBM Plex Mono',ui-monospace,monospace;font-size:9.5px;\
  color:#6b7a92;letter-spacing:.5px;}\
.jhi-footer a{color:#a78bfa;text-decoration:none;}\
";


  CSS += '.jhi-panel{box-sizing:border-box;overflow-wrap:anywhere}.jhi-panel[hidden]{display:none}.jhi-panel .close:focus-visible,.jhi-fab:focus-visible{outline:2px solid #00d4ff;outline-offset:4px}.jhi-panel details{margin:12px 0}.jhi-panel summary{cursor:pointer}.jhi-input{padding:9px 0;border-top:1px solid #283247}.jhi-input code{font-size:10px;overflow-wrap:anywhere}.jhi-panel .jhi-footer{flex-wrap:wrap;gap:8px}.jhi-posture .val.WAIT{color:#c3cee0;background:#1b2638}.jhi-fab .dot{animation:none}.jhi-panel [data-jhi-refresh]{background:#1b2638;color:#e6ecf5;border:1px solid #65738c;border-radius:5px;min-height:36px;padding:6px 12px;font:inherit;cursor:pointer}.jhi-panel [data-jhi-refresh]:hover{border-color:#00d4ff}.jhi-panel [data-jhi-refresh]:focus-visible{outline:2px solid #00d4ff;outline-offset:3px}.jhi-panel .hdr .close{min-width:32px;min-height:32px}@media(prefers-reduced-motion:reduce){.jhi-fab,.jhi-panel{transition:none}}';

  var INPUTS = {
    signal_board:'data/signal-board.json', auction_crisis:'data/auction-crisis.json',
    auction_crisis_ai:'data/auction-crisis-ai.json', macro_frontrun:'data/macro-frontrun-sniffer.json',
    crisis_brief:'data/crisis-brief.json', bonds:'data/bond-trace.json', repo:'data/repo.json',
    regime:'data/regime.json', correlations:'data/correlations.json', global_stress:'data/global-stress.json',
    sentiment:'data/sentiment.json', volatility:'data/vol-radar.json'
  };
  var READ_STATES = ['parsed','missing','unavailable','malformed','oversize','reported_error'];
  var FLAGS = ['calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'];

  function injectCSS() {
    if (document.getElementById('jhi-css')) return;
    var style=document.createElement('style'); style.id='jhi-css'; style.textContent=CSS; document.head.appendChild(style);
  }
  function object(value) { return value!==null && typeof value==='object' && !Array.isArray(value); }
  function esc(value) { return String(value==null?'':value).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];}); }
  function clock(value) {
    if (typeof value!=='string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) return null;
    var year=+value.slice(0,4),month=+value.slice(5,7),day=+value.slice(8,10),hour=+value.slice(11,13),minute=+value.slice(14,16),second=+value.slice(17,19);
    var calendar=new Date(0);calendar.setUTCFullYear(year,month-1,day);calendar.setUTCHours(0,0,0,0);
    if(calendar.getUTCFullYear()!==year || calendar.getUTCMonth()!==month-1 || calendar.getUTCDate()!==day || hour>23 || minute>59 || second>59) return null;
    var stamp=Date.parse(value); return Number.isFinite(stamp)?stamp:null;
  }
  function ageStr(value) {
    var stamp=clock(value); if(stamp===null) return 'publication time unavailable';
    var age=Date.now()-stamp; if(age<0) return 'publication time is in the future';
    var minutes=Math.floor(age/60000); return minutes<1?'published less than 1m ago':minutes<60?'published '+minutes+'m ago':minutes<1440?'published '+Math.floor(minutes/60)+'h ago':'published '+Math.floor(minutes/1440)+'d ago';
  }
  function valid(data) {
    if(!object(data) || data.contract!=='website-research-status.v1' || data.schema_version!=='2.0' || data.status!=='unqualified' || data.model!=='deterministic-research-status-v1' || data.call!==null || !FLAGS.every(function(f){return data[f]===false;})) return false;
    if(!object(data.synthesis) || data.synthesis.global_posture!=='WAIT' || !object(data.decision) || data.decision.action!=='WAIT' || data.decision.abstain!==true || data.decision.eligible_votes!==0 || data.decision.portfolio_effect!==null || !object(data.input_status)) return false;
    if(data.engines_total!==12 || Object.keys(data.input_status).length!==12 || !Number.isInteger(data.engines_loaded)) return false;
    var names=Object.keys(INPUTS), parsed=0;
    var okay=names.every(function(name){
      var row=data.input_status[name];
      if(!object(row) || row.artifact!==INPUTS[name] || READ_STATES.indexOf(row.read_status)<0 || row.decision_eligible!==false || row.observation_freshness_verified!==false || row.original_source_evidence_verified!==false) return false;
      if(row.read_status==='parsed') {
        if(typeof row.body_sha256!=='string' || !/^[a-f0-9]{64}$/.test(row.body_sha256) || !Number.isSafeInteger(row.body_bytes) || row.body_bytes<=0) return false;
        parsed++;
      }
      return true;
    });
    return okay && parsed===data.engines_loaded;
  }
  function getCached() {
    try {
      var envelope=JSON.parse(sessionStorage.getItem(CACHE_KEY));
      if(!object(envelope) || envelope.version!==2 || typeof envelope.cached_at!=='number' || !Number.isFinite(envelope.cached_at)) return null;
      var age=Date.now()-envelope.cached_at;
      return age>=0 && age<CACHE_MAX_AGE_MS && valid(envelope.payload)?envelope.payload:null;
    } catch(e) { return null; }
  }
  function setCached(data) {
    if(!valid(data)) return;
    try { sessionStorage.setItem(CACHE_KEY,JSON.stringify({version:2,cached_at:Date.now(),payload:data})); } catch(e) {}
  }
  function fetchSynthesis(useCache, signal) {
    var cached=useCache && getCached(); if(cached) return Promise.resolve(cached);
    return fetch(DATA_URL+'?t='+Date.now(),{signal:signal,cache:'no-store'}).then(function(response){
      if(!response.ok) throw new Error('Research status unavailable');
      return response.json();
    }).then(function(data){if(!valid(data)) throw new Error('Research status contract unavailable');return data;});
  }
  function header(data) {
    return '<div class="hdr"><div class="ico" aria-hidden="true"></div><div class="ttl" id="jhi-title">Research status</div><div class="age">'+(data?esc(ageStr(data.generated_at)):'')+'</div><button type="button" class="close" data-jhi-close aria-label="Close research status">&times;</button></div>';
  }
  function textList(values) {
    if(!Array.isArray(values)) return '';
    return '<ul>'+values.filter(function(v){return typeof v==='string';}).map(function(v){return '<li>'+esc(v)+'</li>';}).join('')+'</ul>';
  }
  function buildPanel(data) {
    if(!valid(data)) return header(null)+'<div class="jhi-err" role="status">Research status unavailable. No investment recommendation is shown.</div>';
    var s=data.synthesis;
    var html=header(data)+'<div class="jhi-posture"><span class="lbl">Decision eligibility</span><span class="val WAIT">WAIT · UNQUALIFIED</span></div>';
    html+='<div class="jhi-headline">'+data.engines_loaded+' of '+data.engines_total+' input artifacts parsed. No investment vote is qualified.</div>';
    html+='<div class="jhi-call"><span class="lbl">Abstention</span>No new portfolio recommendation is supported by this availability report.</div>';
    html+='<div class="jhi-section"><div class="lbl">Scope</div><div class="body">Artifact availability and publication times do not establish fresh observations, independent evidence, a forecast edge or suitable position size.</div></div>';
    var page=currentPageKey(),focus=object(s.per_page_focus) && s.per_page_focus[page];
    if(typeof focus==='string') html+='<div class="jhi-page-focus"><span class="lbl">On this page</span>'+esc(focus)+'</div>';
    html+='<details><summary>Input availability and provenance</summary>';
    Object.keys(INPUTS).forEach(function(name){
      var row=data.input_status[name];
      html+='<div class="jhi-input"><strong>'+esc(name.replace(/_/g,' '))+'</strong> · '+esc(row.read_status.replace(/_/g,' '))+'<br><code>'+esc(row.artifact)+'</code>';
      if(typeof row.body_sha256==='string' && /^[a-f0-9]{64}$/.test(row.body_sha256)) html+='<br>Body SHA-256: <code>'+esc(row.body_sha256)+'</code>';
      if(typeof row.reported_as_of==='string') html+='<br>Reported observation date: '+esc(row.reported_as_of);
      if(typeof row.reported_generated_at==='string') html+='<br>Reported generation: '+esc(row.reported_generated_at);
      html+='<br>Observation freshness and source evidence: unverified.</div>';
    });
    html+='</details><div class="jhi-section"><div class="lbl">Qualification work</div>'+textList(s.watch_list)+'</div>';
    html+='<div class="jhi-footer"><span>Deterministic status · no model calls</span><button type="button" data-jhi-refresh>Refresh status</button><a href="/">justhodl.ai</a></div>';
    return html;
  }
  function buildFab(state) {return '<span class="dot" aria-hidden="true"></span><span>'+esc(state)+'</span>';}
  function mount() {
    if(window.JHInsights) return;
    injectCSS();
    var fab=document.createElement('button');fab.type='button';fab.className='jhi-fab';fab.setAttribute('aria-label','Open research status');fab.setAttribute('aria-expanded','false');fab.setAttribute('aria-controls','jhi-panel');fab.innerHTML=buildFab('Loading');document.body.appendChild(fab);
    var panel=document.createElement('div');panel.id='jhi-panel';panel.className='jhi-panel';panel.hidden=true;panel.inert=true;panel.setAttribute('role','dialog');panel.setAttribute('aria-modal','false');panel.setAttribute('aria-labelledby','jhi-title');panel.innerHTML=header(null)+'<div class="jhi-loading" role="status">Loading research status…</div>';document.body.appendChild(panel);
    var owner=0,active=null,priorFocus=null;
    function open() {
      if(!panel.hidden) return;
      priorFocus=document.activeElement;panel.hidden=false;panel.inert=false;panel.classList.add('open');fab.setAttribute('aria-expanded','true');panel.querySelector('[data-jhi-close]').focus();
    }
    function close() {
      if(panel.hidden) return;
      panel.classList.remove('open');panel.hidden=true;panel.inert=true;fab.setAttribute('aria-expanded','false');
      if(priorFocus && priorFocus.isConnected && typeof priorFocus.focus==='function') priorFocus.focus();else fab.focus();
    }
    function render(data,message) {
      var ownedFocus=panel.contains(document.activeElement);
      fab.className='jhi-fab';fab.innerHTML=buildFab(data?'WAIT':message==='loading'?'Loading':'Unavailable');
      panel.setAttribute('aria-busy',message==='loading'?'true':'false');
      panel.innerHTML=data?buildPanel(data):header(null)+(message==='loading'?'<div class="jhi-loading" role="status">Loading research status…</div>':'<div class="jhi-err" role="status">Research status unavailable. No investment recommendation is shown.</div><button type="button" data-jhi-refresh>Retry status</button>');
      if(ownedFocus && !panel.hidden) panel.querySelector('[data-jhi-close]').focus();
    }
    function refresh(useCache) {
      var request=++owner;if(active) active.abort();active=new AbortController();var controller=active;
      render(null,'loading');
      var timeout=setTimeout(function(){controller.abort();},15000);
      return fetchSynthesis(useCache===true,controller.signal).then(function(data){
        if(request!==owner) return null;setCached(data);render(data);return data;
      }).catch(function(){if(request===owner) render(null,'error');return null;}).finally(function(){clearTimeout(timeout);if(request===owner) active=null;});
    }
    fab.addEventListener('click',function(){if(panel.hidden) open();else close();});
    panel.addEventListener('click',function(event){if(event.target.closest('[data-jhi-close]')) close();else if(event.target.closest('[data-jhi-refresh]')) refresh(false);});
    document.addEventListener('keydown',function(event){if(event.key==='Escape' && !panel.hidden){event.preventDefault();close();}});
    window.JHInsights={open:open,close:close,refresh:function(){return refresh(false);}};
    refresh(true);
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',mount);else mount();
})();
