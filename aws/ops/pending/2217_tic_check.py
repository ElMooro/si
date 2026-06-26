import boto3, json, os, urllib.request
s3=boto3.client("s3","us-east-1")
# 1) current tic-flows output
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/tic-flows.json")["Body"].read())
    print("tic-flows.json keys:", list(d.keys()))
    print("  generated_at:", d.get("generated_at"))
    print("  net_flow_3mo:", d.get("net_flow_3mo"), "| net_flow_12mo:", d.get("net_flow_12mo"))
    print("  composite_score:", d.get("composite_score"), "| regime:", d.get("regime"))
    th=d.get("top_holders") or []
    print("  top_holders:", [(h.get("country"),h.get("current_b"),h.get("yoy_change_b")) for h in th[:5]])
except Exception as e: print("tic-flows.json ERR:", str(e)[:80])
# 2) does it capture TOTAL net TIC (all securities) or just Treasuries? test the FRED series it uses + the broad ones
FRED=os.environ.get("FRED_API_KEY","")
def fred_latest(sid):
    if not FRED: return "NO_KEY"
    try:
        u=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=3"
        j=json.loads(urllib.request.urlopen(u,timeout=15).read())
        obs=[(o["date"],o["value"]) for o in j.get("observations",[]) if o["value"]!="."]
        return obs[:2] if obs else "EMPTY"
    except Exception as e: return f"ERR {str(e)[:40]}"
print("\nFRED series test (what's live):")
for label,sid in [("engine NET_PURCHASES FANTPDQ027S","FANTPDQ027S"),
                  ("engine TOTAL_FOREIGN MFFICTQ027S","MFFICTQ027S"),
                  ("BOP financial acct net IEABCSN","IEABCSN"),
                  ("Net acq US LT secs by foreigners (TIC) — try","NETFITICA"),
                  ("Foreign holdings of US Treasuries FDHBFIN","FDHBFIN")]:
    print(f"  {label}: {fred_latest(sid)}")
print("DONE 2217")
