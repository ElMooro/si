"""ops 2052: verify strategist.html live + directory link."""
import urllib.request, time
def get(u):
    for _ in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh-verify"}),timeout=20) as r:
                return r.getcode(), r.read().decode("utf-8","replace")
        except Exception as e:
            print("  retry",str(e)[:50]); time.sleep(18)
    return None,""
c,b=get("https://justhodl.ai/strategist.html?t="+str(int(time.time())))
print("strategist.html:",c,"bytes",len(b),"| reads json:",'data/strategist.json' in b,"| hero:",'dominant_driver' in b,"| conviction:",'conviction' in b,"| contradictions:",'Contradictions' in b)
c2,b2=get("https://justhodl.ai/directory.html?t="+str(int(time.time())))
print("directory link present:",'/strategist.html' in b2,"| count 293:",'all 293 pages' in b2)
# confirm data feed still fresh
import json,boto3
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/strategist.json")["Body"].read())
print("data/strategist.json: ok",d.get("ok"),"| fresh",d["fleet"]["n_fresh"],"| consensus",d["fleet"]["consensus"],"| driver set:",bool((d.get('interpretation') or {}).get('dominant_driver')))
print("PAGE_OK" if (c==200 and 'data/strategist.json' in b) else "PAGE_ISSUE")
print("DONE 2052")
