"""ops 2573 — run the LIVE page JS against REAL live data; check cache headers."""
import urllib.request, json, time, re, subprocess, os
UA = {"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"}
def get(url):
    r = urllib.request.urlopen(urllib.request.Request(url+f"?t={int(time.time())}", headers=UA), timeout=25)
    return r.read().decode("utf-8","ignore"), dict(r.headers)
PX = "https://justhodl-data-proxy.raafouis.workers.dev/data"
# page + cache header
page, hdr = get("https://justhodl.ai/upside-radar.html")
print("PAGE Cache-Control:", hdr.get("cache-control") or hdr.get("Cache-Control"))
print("PAGE ETag:", hdr.get("etag") or hdr.get("ETag"))
print("PAGE age/date:", hdr.get("age"), "|", hdr.get("date"))
# real feeds
feeds = {}
for f in ["upside-radar","upside-theses","flow-confluence","equity-confluence","bagger-engine","cyclical-bagger",
          "momentum-breakout","pead-signals","dark-pool","capital-flow","ark-holdings","short-interest",
          "estimate-revisions","sector-emergence","master-ranker","best-setups","theme-rotation"]:
    try: feeds[f] = json.loads(get(f"{PX}/{f}.json")[0])
    except Exception as e: feeds[f] = {}; print(f"  feed {f} FAILED: {str(e)[:40]}")
th = feeds["upside-theses"]
print(f"\ntheses: v={th.get('version')} n_cand={th.get('n_candidates')} n_ai={th.get('n_ai')}")
print("top_ranked[:6]:", th.get("top_ranked", [])[:6])
# spot-check real shape for top names
for t in th.get("top_ranked", [])[:2]:
    d = th["theses"][t]
    print(f"  {t}: dna={d.get('dna',{}).get('archetype')} canslim={d.get('canslim',{}).get('score')} ai={'yes' if d.get('ai') else 'no'} sm_score={d.get('smart_money',{}).get('score')}")

# extract inline JS, strip the auto-run calls, seed real data, exercise openThesis + mbscore
js = re.search(r'<script>\s*(const esc.*?)</script>', page, re.S).group(1).replace("load();","")
harness = "const store={};global.document={getElementById:id=>store[id]||(store[id]={innerHTML:'',textContent:'',style:{},dataset:{}}),querySelectorAll:()=>[],addEventListener:()=>{}};global.fetch=async()=>{throw new Error('x')};\n"
harness += js + "\n"
# seed D and X exactly as load() would
fmap = {"fc":"flow-confluence","ec":"equity-confluence","be":"bagger-engine","cb":"cyclical-bagger","mb":"momentum-breakout","pe":"pead-signals","dp":"dark-pool","cf":"capital-flow","ark":"ark-holdings","si":"short-interest","er":"estimate-revisions","se":"sector-emergence","mr":"master-ranker","bs":"best-setups","tr":"theme-rotation","th":"upside-theses"}
harness += "D=" + json.dumps(feeds["upside-radar"]) + ";\n"
harness += "X={" + ",".join(f"{k}:{json.dumps(feeds[v])}" for k,v in fmap.items()) + "};\n"
harness += "(D.scans&&D.scans.breakout||[]).forEach(b=>brkSet.add((b.t||'').toUpperCase()));\n"
harness += """
let errs=[];
for(const p of ['breakout','confluence','baggers','accum','squeeze','mbscore']){P=p;try{render();}catch(e){errs.push(p+': '+e.message);}}
console.log('TAB render errors:', errs.length?errs:'NONE');
const top=(X.th.top_ranked||[]).slice(0,5);
let modalErrs=[],built=0;
for(const t of top){try{openThesis(t);const h=store.modalBody.innerHTML;if(h.includes('dnaBanner')&&h.length>300)built++;if(h.includes('[object Object]')||h.includes('undefined/100'))modalErrs.push(t+' bad-render');}catch(e){modalErrs.push(t+': '+e.message);}}
console.log('MODAL built '+built+'/'+top.length+' · errors:', modalErrs.length?modalErrs:'NONE');
P='mbscore';render();console.log('mbscore tab rows:', (store.pane.innerHTML.match(/class="tkl"/g)||[]).length);
"""
open("/tmp/real_harness.js","w").write(harness)
try:
    out = subprocess.run(["node","/tmp/real_harness.js"], capture_output=True, text=True, timeout=60)
    print("\n--- NODE (real data) ---")
    print(out.stdout)
    if out.stderr: print("STDERR:", out.stderr[:500])
except Exception as e:
    print("node run err:", str(e)[:100])
print("DONE 2573")
