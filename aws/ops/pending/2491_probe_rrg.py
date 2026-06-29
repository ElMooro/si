import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
print("===== rotation-radar.json =====")
r=g("data/rotation-radar.json")
print("keys:",list(r.keys()))
eq=r.get("equity")
print("equity type:",type(eq).__name__, "| ", json.dumps(eq)[:500] if not isinstance(eq,list) else f"LIST[{len(eq)}] {json.dumps(eq[0])[:400]}")
sc=r.get("scores")
print("scores type:",type(sc).__name__,"|",json.dumps(sc)[:500] if not isinstance(sc,list) else f"LIST[{len(sc)}] {json.dumps(sc[0])[:400]}")
print("methodology:",json.dumps(r.get("methodology"))[:300])
print("\n===== finviz-groups.json sectors =====")
fg=g("data/finviz-groups.json"); secs=fg.get("sectors") or []
print("n_sectors:",len(secs))
if secs: print("sector[0] full:",json.dumps(secs[0]))
print("\n===== sector-rotation.json (page's current feed) =====")
sr=g("data/sector-rotation.json")
print("top keys:",list(sr.keys()))
ss=sr.get("sectors") or []
print("n:",len(ss),"| sector[0]:",json.dumps(ss[0]) if ss else "none")
print("DONE 2491")
