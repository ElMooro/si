import urllib.request, time
BASE="https://justhodl-data-proxy.raafouis.workers.dev/equity-research"
def get(u,t=20):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=t) as r: return r.status, r.read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return e.code,""
    except Exception as e: return None,str(e)[:40]
# PLTR was triggered ~2min ago in ops 2284; poll until it lands (proves cold flow end-to-end)
print("polling PLTR via nogen (cold-ticker triggered earlier)...")
landed=False
for i in range(20):
    s,b=get(f"{BASE}/PLTR.json?nogen=1&v={int(time.time())}")
    if s==200 and '"generated_at"' in b:
        import json; d=json.loads(b)
        print(f"  poll {i}: LANDED gen={d.get('generated_at')}")
        print(f"  PLTR: {d.get('company',{}).get('name')} ${d.get('quote',{}).get('price')} | options ±{(d.get('options_expectations') or {}).get('implied_move_pct')}% | analysts={(d.get('analyst_ratings',{}).get('distribution') or {}).get('total')}")
        landed=True; break
    print(f"  poll {i}: {s} not yet"); time.sleep(13)
print("RESULT:", "cold-ticker flow works end-to-end ✓" if landed else "not landed in window (Lambda still running)")
print("DONE 2285")
