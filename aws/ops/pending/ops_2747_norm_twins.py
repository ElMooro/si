"""ops 2747 — NORM-SCALE charts + twin expansion (Khalid: 2010 not visible,
scales not comparable).

Root causes: linear scaling flattened 2010-2016 into a zero-line on series
spanning 5-6 orders of magnitude (perceived as missing history), and metric
vs BTC rode different invisible scales. Page v3.1: log-transform where range
> 50x, then min-max normalize EVERY line to a shared 0-100%% vertical scale
(cards + modal; raw ranges labeled on modal axes). Twins: probe Coin Metrics
community for the two that silently dropped (NVTAdj, DiffMean) + VelCur1yr;
wire whichever return >900 daily points into spec.twins; rerun engine so the
series feed regenerates with them. Report: 2747_norm_twins.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=890, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2747, "ts": datetime.now(timezone.utc).isoformat()}

print("settling 25s…"); time.sleep(25)
print("== 1/3 twin probe (Coin Metrics community) ==")
CM = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
def cm_count(metric):
    n, url = 0, CM + "?assets=btc&metrics=%s&frequency=1d&page_size=10000&start_time=2010-07-01" % metric
    for _ in range(3):
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/2.1"})
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                doc = json.loads(r.read())
        except Exception as e:
            return 0, str(e)[:80]
        n += sum(1 for row in doc.get("data") or [] if row.get(metric) is not None)
        url = doc.get("next_page_url")
        if not url: break
    return n, None
spec = json.loads(s3.get_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json")["Body"].read())
twins = dict(spec.get("twins") or {})
for cq, cm in (("btc_nvt", "NVTAdj"), ("btc_difficulty", "DiffMean"), ("btc_velocity", "VelCur1yr"),
               ("btc_nvt_golden", "NVTAdj90"), ("btc_realized_price", "CapRealUSD")):
    n, err = cm_count(cm)
    ok = n >= 900
    print("  %-18s -> %-12s pts=%-6s %s" % (cq, cm, n, "WIRED" if ok else (err or "insufficient")))
    R.setdefault("twin_probe", {})[cm] = n
    if ok: twins[cq] = cm
    time.sleep(0.4)
spec["twins"] = twins
s3.put_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json",
              Body=json.dumps(spec, indent=1).encode(), ContentType="application/json")
print("  twins now:", sorted(twins))
R["twins"] = sorted(twins)

print("== 2/3 engine rerun (series regen w/ new twins) ==")
r = lam.invoke(FunctionName="justhodl-cryptoquant", InvocationType="RequestResponse",
               Payload=json.dumps({}).encode())
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:220])
assert not r.get("FunctionError") and pay.get("ok"), pay
sr = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-series.json")["Body"].read())
twn = {k: len(v["d"]) for k, v in (sr.get("twins") or {}).items()}
print("  series twins:", twn)
assert len(twn) >= len(R["twins"]) - 1 and min(twn.values()) >= 900
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
tw_metrics = [k for k in d["metrics"] if "2010" in (d["metrics"][k].get("stats_window") or "")]
print("  metrics on 2010-window stats:", len(tw_metrics), tw_metrics)
R["series_twins"], R["stats_2010"] = twn, tw_metrics

print("== 3/3 page v3.1 at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"NORM-SCALE v3.1" in pub("onchain.html")
    except Exception: pass
    print("  attempt %d: %s" % (a + 1, "v3.1 LIVE" if okp else "pending"))
    if okp: break
assert okp, "page v3.1 not at edge"
for f in ("data/cryptoquant-onchain.json", "data/cryptoquant-series.json"):
    json.loads(pub(f).decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
R["page"] = "LIVE_v3.1"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2747_norm_twins.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2747 COMPLETE — the decade is visible and the scales agree")
