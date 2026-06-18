import json, boto3, urllib.request
REGION="us-east-1"; FN="justhodl-eurodollar-plumbing"
lam=boto3.client("lambda",region_name=REGION)
try:
    c=lam.get_function_configuration(FunctionName=FN)
    ev=c.get("Environment",{}).get("Variables",{})
    print("FUNCTION EXISTS:",c["FunctionName"],"| last_mod:",c["LastModified"][:19],"| has ZAI/ANTHROPIC:", "ZAI_API_KEY" in ev or "ANTHROPIC_API_KEY" in ev)
except Exception as e:
    print("FUNCTION MISSING:",e.__class__.__name__)
# is the json live?
S3="https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/eurodollar-plumbing.json"
try:
    d=json.loads(urllib.request.urlopen(S3,timeout=20).read())
    print("\nLIVE JSON: verdict=%s health=%s gen=%s" % (d.get("verdict"),d.get("plumbing_health"),d.get("generated_at")))
    ai=d.get("ai",{})
    print("AI:", json.dumps(ai)[:400])
    print("red_flags:",d.get("red_flags"),"| yellow_flags:",d.get("yellow_flags"))
    for lk,lv in (d.get("layers") or {}).items():
        ms=lv.get("metrics",[]); vals=[m for m in ms if m.get("value") is not None]
        print("  layer %-14s %-28s metrics=%d with_value=%d" % (lk,lv.get("title",""),len(ms),len(vals)))
except Exception as e:
    print("\nLIVE JSON MISSING/err:",e)
