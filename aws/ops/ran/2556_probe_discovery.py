"""ops 2556 — probe discovery/momentum/setup/accumulation/confluence feeds to
enrich upside-radar.html. Existence + shape + sample row."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
CAND = [
    "upside-radar","upside-radar-names",
    "bagger-engine","cyclical-bagger","52wk-quality-breakout","momentum-breakout","edge-discovery",
    "squeeze-risk","squeeze","short-squeeze","short-interest",
    "flow-confluence","capital-flow","capital-flow-radar","dark-pool","money-flow-state","sector-flow-state",
    "best-setups","master-ranker","signal-board",
    "resilience-radar","ignition","sector-emergence","emergence","strategist","theme-rotation","rotation-radar",
    "revision-momentum","estimate-revisions","analyst-revisions","pead-signals",
    "options-flow","polygon-options-flow","unusual-options",
    "ark-holdings","patent-velocity","political-stocks","insider-buying","sec-filings-intel",
    "retail-edges","equity-confluence","heatmap",
]
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=f"data/{k}.json")["Body"].read())
    except Exception as e: return None
def compact(d, n=14):
    if isinstance(d, list): return f"[list {len(d)}] " + (compact(d[0], 6) if d and isinstance(d[0], dict) else "")
    if not isinstance(d, dict): return str(d)[:60]
    out=[]
    for k,v in list(d.items())[:n]:
        if isinstance(v,(int,float,str,bool)) or v is None:
            sv=str(v); out.append(f"{k}={sv[:26]+'…' if len(sv)>26 else sv}")
        elif isinstance(v,list): out.append(f"{k}=[{len(v)}]")
        elif isinstance(v,dict): out.append(f"{k}={{{','.join(list(v)[:4])}}}")
    return " · ".join(out)
found=[]
for k in CAND:
    d=rd(k)
    if d is None: continue
    found.append(k)
    print(f"\n✓ {k}\n   {compact(d)[:300]}")
    # sample list-of-dicts (the ticker rows)
    src = d if isinstance(d, dict) else {}
    for kk,v in list(src.items()):
        if isinstance(v,list) and v and isinstance(v[0],dict) and any('tick' in str(key).lower() or 'symbol' in str(key).lower() for key in v[0]):
            print(f"   {kk}[0]: {compact(v[0],8)[:200]}"); break
print(f"\n=== {len(found)}/{len(CAND)} feeds present ===")
print("MISSING:", [k for k in CAND if k not in found])
print("DONE 2556")
