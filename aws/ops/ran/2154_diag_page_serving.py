import boto3, urllib.request, time
s3=boto3.client("s3","us-east-1")
# 1. do the keys exist in the bucket, and where is hot-stocks.html?
for k in ["hot-stocks.html","options-confluence.html","flow-confluence.html"]:
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=k); print(f"S3 key '{k}': EXISTS {h['ContentLength']}b ct={h.get('ContentType')}")
    except Exception as e: print(f"S3 key '{k}': {str(e)[:45]}")
# 2. any other html keys / prefixes? list a few
print("\nsample .html keys in bucket root:")
r=s3.list_objects_v2(Bucket="justhodl-dashboard-live",MaxKeys=400)
htmls=[o["Key"] for o in r.get("Contents",[]) if o["Key"].endswith(".html")][:12]
print("  ",htmls)
# 3. fetch control vs new
for u in ["hot-stocks.html","options-confluence.html","flow-confluence.html"]:
    try: code=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/"+u+"?t="+str(int(time.time())),headers={"User-Agent":"jh"}),timeout=15).getcode()
    except Exception as e: code=str(e)[:50]
    # check served headers (cf-cache-status / server)
    try:
        resp=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/"+u+"?t="+str(int(time.time())),headers={"User-Agent":"jh"}),timeout=15)
        srv=resp.headers.get("server"); cf=resp.headers.get("cf-cache-status")
    except Exception as e: srv=cf=None
    print(f"  justhodl.ai/{u} -> {code}  server={srv} cf={cf}")
print("DONE 2154")
