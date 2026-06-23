import boto3, time, urllib.request
s3=boto3.client("s3","us-east-1")
for page in ["options-confluence.html","flow-confluence.html"]:
    html=open(page,encoding="utf-8").read()
    s3.put_object(Bucket="justhodl-dashboard-live",Key=page,Body=html.encode("utf-8"),
                  ContentType="text/html; charset=utf-8",CacheControl="public, max-age=300")
    print("uploaded",page,len(html),"bytes")
time.sleep(3)
for page in ["options-confluence.html","flow-confluence.html"]:
    try:
        code=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/"+page+"?t="+str(int(time.time())),headers={"User-Agent":"jh"}),timeout=15).getcode()
    except Exception as e: code=str(e)[:40]
    print("  https://justhodl.ai/"+page,"->",code)
print("DONE 2153")
