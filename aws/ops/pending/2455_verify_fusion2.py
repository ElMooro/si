import boto3, json
s3=boto3.client("s3","us-east-1")
def rd(k):
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
cc=rd("data/cycle-clock.json")
pos=cc.get("positioning") or {}
print("=== CYCLE-CLOCK (correct key: positioning) ===")
print("capital_cycle_phases:",pos.get("capital_cycle_phases"))
print("scarcity_building:",pos.get("capital_cycle_scarcity_building"),"| flooding:",pos.get("capital_cycle_flooding"))
print("bottleneck_early_calls:",pos.get("bottleneck_early_calls"))
syn=cc.get("synthesis") or {}
alld=[d.get("label") for d in (syn.get("bullish_drivers") or [])+(syn.get("bearish_drivers") or [])]
print("capital-cycle driver shown in top-N:",[x for x in alld if "apital-cycle" in str(x) or "looding" in str(x) or "cure-for" in str(x)])
print("(note: drivers are truncated top-4/5; field presence above proves the block loaded + contributor fired)")
print("\n=== BEST-SETUPS ===")
bs=rd("data/best-setups.json")
print("top-level keys:",list(bs.keys())[:25])
# find the book: the list of dicts with 'signals'
book=None;bk=None
for k,v in bs.items():
    if isinstance(v,list) and v and isinstance(v[0],dict) and ("signals" in v[0] or "conviction" in v[0]):
        book=v;bk=k;break
print("book key:",bk,"| len:",len(book) if book else 0)
if book:
    hits=[s for s in book if any(g.get("key") in ("BOTTLENECK_BOOM","CAPITAL_CYCLE_EARLY") for g in (s.get("signals") or []))]
    print("setups carrying bottleneck/capital-cycle signal:",len(hits))
    for s in hits[:8]:
        print("  ",s.get("ticker"),s.get("verdict"),"conv=",s.get("conviction"),
              "sigs=",[g.get("key") for g in (s.get("signals") or [])][:7])
print("DONE 2455")
