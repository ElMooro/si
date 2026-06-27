import urllib.request, json, time
BASE="https://justhodl-data-proxy.raafouis.workers.dev/equity-research"
def get(u,t=20):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=t) as r: return r.status,r.read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return e.code,e.read(120).decode("utf-8","replace")
    except Exception as e: return None,str(e)[:50]
# 1) async trigger via NEW kickoff path
t=time.time(); s,b=get(f"{BASE}/PLTR.json?async=1&v={int(time.time())}")
print(f"async trigger: status={s} {time.time()-t:.1f}s body={b[:90]}")
# 2) poll nogen until it lands (proves kickoff->Event invoke->S3 write works)
print("polling PLTR (new kickoff path)...")
for i in range(24):
    time.sleep(12)
    s,b=get(f"{BASE}/PLTR.json?nogen=1&v={int(time.time())}")
    if s==200 and '"generated_at"' in b:
        d=json.loads(b)
        print(f"  +{(i+1)*12}s LANDED gen={d.get('generated_at')}")
        print(f"  PLTR {d.get('company',{}).get('name')} ${d.get('quote',{}).get('price')} | options ±{(d.get('options_expectations') or {}).get('implied_move_pct')}% | sources {(d.get('metadata') or {}).get('data_sources_ok')}")
        break
    print(f"  +{(i+1)*12}s {s} not yet")
else:
    print("  did NOT land — kickoff still not working")
print("DONE 2286")
