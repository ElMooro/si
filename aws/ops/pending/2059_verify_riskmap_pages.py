import urllib.request, time
def get(u):
    for _ in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:
                return r.getcode(), r.read().decode("utf-8","replace")
        except Exception as e:
            time.sleep(16)
    return None,""
c,b=get("https://justhodl.ai/regime-map.html?t="+str(int(time.time())))
print("regime-map.html:",c,"bytes",len(b),"| reads json:",'data/regime-map.json' in b,"| banner:",'banner' in b,"| booming:",'Booming' in b)
c2,b2=get("https://justhodl.ai/index.html?t="+str(int(time.time())))
print("index.html card:",'risk-map-card' in b2,"| fetches regime-map:",'data/regime-map.json' in b2)
c3,b3=get("https://justhodl.ai/directory.html?t="+str(int(time.time())))
print("directory link:",'/regime-map.html' in b3,"| 294:",'all 294 pages' in b3)
import json,boto3
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/regime-map.json")["Body"].read())
print("feed: regime",d["regime"]["label"],"| n",d["n_instruments"],"| booming0",d["booming"][0]["ticker"],"| destroyed0",d["destroyed"][0]["ticker"])
print("PAGES_OK" if (c==200 and 'data/regime-map.json' in b and 'risk-map-card' in b2) else "ISSUE")
print("DONE 2059")
