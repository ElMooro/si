"""ops 2557 — verify upside-radar page deployed with all discovery tabs + feeds live."""
import urllib.request, time, boto3, json
time.sleep(55)
s3 = boto3.client("s3", "us-east-1")
url = "https://justhodl.ai/upside-radar.html"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
try:
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
    print("page bytes:", len(html))
    for n in ['data-p="confluence"','data-p="baggers"','data-p="accum"','data-p="squeeze"',
              'data-p="revisions"','data-p="emergence"','data-p="ranked"',
              'flow-confluence','bagger-engine','short-interest','estimate-revisions',
              'CROSS-ENGINE','SMART MONEY','also breaking out today']:
        print(f"  {'OK' if n in html else 'MISS'} {n}")
    print("  no double-escape:", "&amp;amp;" not in html)
except Exception as e:
    print("page err:", str(e)[:100])
feeds = ["flow-confluence","equity-confluence","bagger-engine","cyclical-bagger","momentum-breakout",
         "pead-signals","dark-pool","capital-flow","ark-holdings","short-interest","estimate-revisions",
         "sector-emergence","master-ranker","best-setups","theme-rotation"]
miss = []
for f in feeds:
    try: s3.head_object(Bucket="justhodl-dashboard-live", Key=f"data/{f}.json")
    except Exception: miss.append(f)
print(f"\ndiscovery feeds live: {len(feeds)-len(miss)}/{len(feeds)}")
if miss: print("  MISSING:", miss)
print("DONE 2557")
