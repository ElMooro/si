import json, boto3, urllib.request
s3=boto3.client("s3","us-east-1")
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_e":str(e)[:60]}
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"; FRED="2f057499936072679d8843d7fce99989"
print("=== analogs forward_distribution shape ===")
an=gj("data/historical-analogs.json")
fd=an.get("forward_distribution")
print("forward_distribution:", json.dumps(fd)[:600])
print("analog[0] keys:", list((an.get('analogs') or [{}])[0].keys()))
print("\n=== global-liquidity shape ===")
gl=gj("data/global-liquidity.json")
print("keys:", list(gl.keys())[:25])
for k in ("regime","global_impulse_13w_pct","central_banks","components","cb_balances","g4"):
    if k in gl: print(f"  {k}:", json.dumps(gl[k])[:300])
print("\n=== Polygon ETF aggs test (1m/3m returns) ===")
import datetime as dt
def aggs(t):
    end=dt.date.today(); start=end-dt.timedelta(days=120)
    u=f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=200&apiKey={POLY}"
    try:
        j=json.loads(urllib.request.urlopen(u,timeout=12).read()); r=j.get("results") or []
        if len(r)<63: return f"{t}: thin({len(r)})"
        c=[x['c'] for x in r]
        m1=round((c[-1]/c[-22]-1)*100,1); m3=round((c[-1]/c[-63]-1)*100,1)
        return f"{t}: 1m {m1:+}% 3m {m3:+}%"
    except Exception as e: return f"{t}: ERR {str(e)[:40]}"
for t in ["SPY","QQQ","IWM","DBC","GLD","TLT","IEF","HYG","LQD","UUP","IWD","IWF","XLE","XLU"]:
    print(" ", aggs(t))
print("\n=== FRED global CB balance sheets ===")
def fred_last2(s):
    try:
        u=f"https://api.stlouisfed.org/fred/series/observations?series_id={s}&api_key={FRED}&file_type=json&observation_start=2024-01-01"
        j=json.loads(urllib.request.urlopen(u,timeout=12).read()); o=[x for x in j['observations'] if x['value'] not in('.','',None)]
        return f"{s}: last {o[-1]['date']}={o[-1]['value']} ({len(o)} obs from 2024)"
    except Exception as e: return f"{s}: ERR {str(e)[:50]}"
for s in ["ECBASSETSW","JPNASSETS","WALCL","RRPONTSYD","WTREGEN"]:
    print(" ", fred_last2(s))
print("DONE 2324")
