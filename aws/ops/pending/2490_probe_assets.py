import boto3, json
s3=boto3.client("s3","us-east-1")
B="justhodl-dashboard-live"
KEYS=["data/rotation-radar.json","data/rotation-chains.json","data/dark-pool.json",
"data/13f-positions.json","data/breadth-divergence.json","data/breadth-history.json",
"data/options-flow.json","data/options-gamma.json","data/capital-flow-radar-state.json",
"data/capital-inflows.json","data/smart-beta.json","data/gold-equity-rotation.json",
"data/liquidity-flow.json","data/insider-aggregate-history.json","data/finviz-groups.json",
"data/etf-flows.json","data/flow-lookthrough.json"]
def short(v,n=180):
    s=json.dumps(v); return s[:n]
for k in KEYS:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        gen=(d.get("generated_at") or d.get("as_of") or d.get("timestamp") or d.get("updated") or d.get("date") or "?") if isinstance(d,dict) else "?"
        keys=list(d.keys())[:10] if isinstance(d,dict) else f"LIST[{len(d)}]"
        print(f"{k}\n   gen={gen} | keys={keys}")
        # try to surface sector dimensionality
        for probe in ("sectors","by_sector","rotation","sector_scores","groups","quadrants","names","tickers","entries","data"):
            if isinstance(d,dict) and probe in d:
                v=d[probe]; print(f"   .{probe}: {short(v)}"); break
    except Exception as e:
        print(f"{k}\n   ERR {str(e)[:60]}")
print("DONE 2490")
