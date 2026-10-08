import json, urllib.request, time, re
out={}
# pull the LIVE brain.html and extract the actual save code + identity logic
try:
    h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode()
    out["has_saveNote"]="saveNote" in h
    out["has_shard_queue"]="_runQueue" in h
    out["has_deviceId"]="deviceId" in h
    out["has_brainUid"]="brainUid" in h
    out["proxy"]=re.findall(r'const PROXY="([^"]+)"',h)[:1]
    # extract the _runQueue body
    m=re.search(r'async function _runQueue\(\)\{.*?\n\}',h,re.DOTALL)
    out["runQueue_snippet"]=(m.group(0)[:400] if m else "NOT FOUND")
    # how does it build the fetch URL?
    out["fetch_line"]=re.findall(r'fetch\(`\$\{PROXY\}/brain[^`]*`',h)[:3]
except Exception as e: out["err"]=str(e)[:100]
open("aws/ops/reports/1372_lc.json","w").write(json.dumps(out,indent=2,default=str));print("done")
