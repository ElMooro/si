"""ops 2764 — map Eastmoney MUTUAL_TYPE codes to identify Southbound (港股通).
Southbound rows have HK lead stocks (.HK). Fetch RPT_MUTUAL_DEAL_HISTORY (all
types) and print type/lead-stock/net/date so we pick the right codes.
Read-only. Report: 2764_hk_codes.json.
"""
import os, json, urllib.request, urllib.parse
from datetime import datetime, timezone
R = {"ops": 2764, "ts": datetime.now(timezone.utc).isoformat()}
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121 Safari/537.36"
def emfetch(params):
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": "https://data.eastmoney.com/"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())
# all types, latest 24 rows
doc = emfetch({"reportName": "RPT_MUTUAL_DEAL_HISTORY", "columns": "ALL", "source": "WEB",
              "sortColumns": "TRADE_DATE", "sortTypes": "-1", "pageSize": "24", "pageNumber": "1"})
res = (doc.get("result") or {}).get("data") or []
print("rows:", len(res))
seen = {}
for r in res:
    t = str(r.get("MUTUAL_TYPE")); lead = str(r.get("LEAD_STOCKS_CODE")); net = r.get("NET_DEAL_AMT")
    dt = str(r.get("TRADE_DATE"))[:10]; idx = r.get("INDEX_CLOSE_PRICE")
    is_hk = ".HK" in lead
    key = t
    if key not in seen:
        seen[key] = {"type": t, "lead": lead, "is_hk_leadstock": is_hk, "net": net, "date": dt, "index": idx}
    print("  type=%s lead=%-12s hk=%s net=%s date=%s idx=%s" % (t, lead, is_hk, net, dt, idx))
R["type_map"] = seen
southbound = [t for t, v in seen.items() if v["is_hk_leadstock"]]
R["southbound_codes"] = southbound
print("\nSOUTHBOUND (HK lead-stock) codes:", southbound)
for t in southbound:
    print("  ", t, "->", seen[t])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2764_hk_codes.json", "w"), indent=1, ensure_ascii=False, default=str)
print("OPS 2764 COMPLETE")
