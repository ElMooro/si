"""ops 2054: dump schemas of the MISSING risk-on/trend/breadth engines so we can widen extraction."""
import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
TARGETS={"momentum":"data/momentum-scanner.json","momentum2":"data/momentum.json",
 "breadth-thrust":"data/breadth-thrust.json","market-internals":"data/market-internals.json",
 "trend-engine":"data/trend-engine.json","master-ranker":"data/master-ranker.json",
 "best-setups":"data/best-setups.json","boom-radar":"data/boom-radar.json",
 "upside-radar":"data/upside-radar.json","rotation-radar":"data/rotation-radar.json",
 "smart-beta":"data/smart-beta.json","ath":"data/ath.json","market-map":"data/market-map.json"}
for nm,key in TARGETS.items():
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
        if isinstance(d,dict):
            print(f"\n=== {nm} ({key}) topkeys: {list(d.keys())[:14]}")
            # show candidate verdict/score/pick fields with sample values
            for k,v in list(d.items())[:14]:
                if isinstance(v,(str,int,float)): print(f"    {k} = {str(v)[:50]}")
                elif isinstance(v,list): print(f"    {k} = [list n={len(v)}] sample0={json.dumps(v[0])[:90] if v else ''}")
                elif isinstance(v,dict): print(f"    {k} = {{dict keys={list(v.keys())[:8]}}}")
        else: print(f"\n=== {nm} ({key}) NON-DICT {type(d)}")
    except Exception as e:
        print(f"\n=== {nm} ({key}) ERR {str(e)[:50]}")
print("DONE 2054")
