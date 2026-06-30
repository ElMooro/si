"""ops 2568 — probe shapes of high-value existing engines for creative upside-radar features."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=f"data/{k}.json")["Body"].read())
    except Exception as e: return None
def compact(d, n=16):
    if isinstance(d, list): return f"[list {len(d)}] " + (compact(d[0], 8) if d and isinstance(d[0], dict) else (str(d[:3]) if d else ""))
    if not isinstance(d, dict): return str(d)[:70]
    out=[]
    for k,v in list(d.items())[:n]:
        if isinstance(v,(int,float,str,bool)) or v is None:
            sv=str(v); out.append(f"{k}={sv[:30]+'…' if len(sv)>30 else sv}")
        elif isinstance(v,list): out.append(f"{k}=[{len(v)}]")
        elif isinstance(v,dict): out.append(f"{k}={{{','.join(list(v)[:5])}}}")
    return " · ".join(out)
def firstrow(d):
    if not isinstance(d, dict): return ""
    for kk,v in d.items():
        if isinstance(v,list) and v and isinstance(v[0],dict): return f"      {kk}[0]: {compact(v[0],9)[:240]}"
    return ""
for k in ["historical-analogs","smart-money-clusters","insider-aggregate","insider-buys-enriched",
          "insider-clusters-names","13f-positions","13f-price-divergence","political-stocks",
          "congress-party-map","catalyst-calendar","catalyst-skew-premove","chart-patterns",
          "peer-comparison","stock-valuations","earnings-quality","quality-on-sale",
          "eps-revision-velocity","analyst-actions","microcap-float-squeeze","_skill/opportunity-rankings",
          "backtest-summary"]:
    d = rd(k)
    if d is None: print(f"✗ {k} (missing)"); continue
    print(f"\n✓ {k}\n   {compact(d)[:320]}")
    fr = firstrow(d)
    if fr: print(fr)
print("\nDONE 2568")
