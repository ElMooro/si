import json, time, urllib.request, boto3
lam=boto3.client("lambda","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
t=time.time()
req=urllib.request.Request(f"{URL}?ticker=LDOS&refresh=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
with urllib.request.urlopen(req,timeout=270) as r: d=json.loads(r.read().decode())
print(f"generated {time.time()-t:.0f}s")
bm=d.get("business_mix") or {}
print("\nBUSINESS MIX:")
print("  segments:", bm.get("segments"))
print("  geography:", bm.get("geography"))
print("  segment_trend periods:", len(bm.get("segment_trend") or []))
print("  business_mix_assessment:", str(d.get("business_mix_assessment"))[:260])
ph=d.get("price_history") or []
print("\nPRICE HISTORY:", len(ph), "points; first:", ph[0] if ph else None, "last:", ph[-1] if ph else None)
m=d.get("margins") or {}
gt=[x for x in (m.get("operating_trend") or []) if x.get("value") is not None]
print("\nMARGINS (was null): operating_trend non-null:", len(gt), "| latest:", (m.get("operating_trend") or [{}])[0])
print("DONE 2262")
