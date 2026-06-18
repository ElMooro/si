import urllib.request, json
def get(url,t=20):
    req=urllib.request.Request(url,headers={"User-Agent":"verify"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","replace")
try:
    st,body=get("https://justhodl.ai/eurodollar.html")
    print(f"PAGE {st} | board:{'funding board' in body.lower() or 'Eurodollar Funding' in body} | fetches json:{'eurodollar-plumbing.json' in body} | bytes={len(body)}")
except Exception as e: print("PAGE FAIL",e)
for base in ["https://justhodl-data-proxy.raafouis.workers.dev","https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com"]:
    try:
        st,body=get(base+"/data/eurodollar-plumbing.json")
        d=json.loads(body)
        print(f"JSON {st} {base.split('//')[1][:18]} verdict={d['verdict']} health={d['plumbing_health']} ai_state={d.get('ai',{}).get('state')} layers={len(d['layers'])}")
    except Exception as e: print("JSON FAIL",base,e)
