import boto3, json, re
s3=boto3.client("s3","us-east-1")
def probe(f, listhint=None):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        ga=str(d.get("generated_at") or d.get("as_of") or "")[:10]
        print(f"\n{f} (gen {ga}): keys={list(d.keys())[:12]}")
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict):
                it=v[0]
                idk=[x for x in it.keys() if any(w in x.lower() for w in ("ticker","symbol","name","signal","input"))]
                sc=[x for x in it.keys() if any(w in x.lower() for w in ("score","inflect","tight","change","pctile","percentile","accel","z","backlog","quiet","direction"))]
                if idk or sc:
                    print(f"    '{k}' n={len(v)} id={idk[:2]} fields={sc[:6]}")
    except Exception as e: print(f"{f}: ERR {str(e)[:45]}")
probe("supply-inflection-scanner")
probe("bottleneck-boom")
probe("chokepoint")
probe("narrative-vs-tape")
probe("ai-infra-stack")
probe("revenue-acceleration")
# show the actual inflection signals in supply-inflection-scanner (the Micron-type inputs)
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection-scanner.json")["Body"].read())
    print("\n=== supply-inflection-scanner SIGNALS ===")
    for L in ("signals","inflecting","top_inflections","all_signals","tightening"):
        arr=d.get(L)
        if isinstance(arr,list) and arr:
            print(f"  list '{L}' ({len(arr)}):")
            for s in arr[:14]:
                nm=s.get("name") or s.get("signal") or s.get("input") or s.get("id")
                sc=s.get("score") or s.get("inflection_score") or s.get("tightness")
                ch=s.get("chg_90d") or s.get("change_90d") or s.get("pct_90d")
                print(f"     {str(nm)[:42]:<42} score={sc} chg90={ch}")
            break
except Exception as e: print("sig ERR",str(e)[:40])
print("DONE 2224")
