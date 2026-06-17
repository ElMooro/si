import urllib.request, json
PX="https://justhodl-data-proxy.raafouis.workers.dev/data/fomc-reaction.json"
S3="https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/fomc-reaction.json"
for url in [PX,S3]:
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"verify"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d=json.loads(r.read().decode()); sp=d.get("surprise",{})
            print(f"OK {r.status} {url.split('/data/')[0]}  surprise={sp.get('label')} basis={sp.get('basis')} assets={len(d.get('reaction_map',{}))}")
    except Exception as e:
        print(f"FAIL {url}: {e}")
