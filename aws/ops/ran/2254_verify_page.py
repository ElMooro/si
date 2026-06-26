import urllib.request, json, time
def fetch(u,t=25):
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh-verify"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","replace")
    except Exception as e: return None, str(e)[:80]
st,body=fetch("https://justhodl.ai/why.html")
print("PAGE why.html ->",st)
if st==200:
    for tok in ["renderCritique","d.devils_advocate","Kill points","what_bulls_underestimate","Devil's Advocate Review"]:
        print(f"   contains {tok!r}:", tok in body)
# confirm the data the page will read has devils_advocate
import boto3
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
da=d.get("devils_advocate") or {}
print("LDOS doc devils_advocate present:", bool(da), "| title:", da.get("title"))
print("DONE 2254")
