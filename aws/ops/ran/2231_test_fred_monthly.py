import urllib.request, json
from datetime import datetime, timezone, timedelta
KEY="2f057499936072679d8843d7fce99989"
end=datetime.now(timezone.utc).date(); start=end-timedelta(days=600)
for sid in ["PCOPPUSDM","DFII10","DTCDISA066MSFRBNY","PCU33443344"]:
    u=(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={KEY}"
       f"&observation_start={start.isoformat()}&observation_end={end.isoformat()}&file_type=json&sort_order=asc")
    try:
        j=json.loads(urllib.request.urlopen(u,timeout=20).read())
        obs=[o for o in j.get("observations",[]) if o["value"]!="."]
        print(f"  {sid}: {len(obs)} obs, latest={obs[-1] if obs else None}")
    except Exception as e:
        print(f"  {sid}: ERR {str(e)[:60]}")
print("DONE 2231")
