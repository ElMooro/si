"""ops 1939 — Massive RORO data coverage probe (probe-before-build).

The user wants ALL Massive data that signals risk-on/risk-off, applied across
the system. Before building, determine exactly which RORO-relevant data classes
Massive actually returns on the current subscription. Existing engines already
pull 9 FX pairs (fx-regime, works) and a futures-curve engine exists but its own
docstring says "currently silent" (built for the old $29 futures tier).

Probe every RORO data class against api.massive.com AND api.polygon.io with the
Massive key. Report CONFIRMED (latest value + date) vs DEAD per ticker/class.
No system mutation — read-only.
"""
import json, urllib.request, urllib.error, concurrent.futures as cf
from datetime import datetime, timezone, timedelta
import os, boto3

KEY = os.environ.get("MASSIVE_API_KEY") or ""
if not KEY:
    try:
        KEY = boto3.client("ssm", "us-east-1").get_parameter(
            Name="/justhodl/massive-api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        KEY = ""
print("massive key len:", len(KEY))

BASES = ["https://api.massive.com", "https://api.polygon.io"]

def _get(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-RORO-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read()), None
    except urllib.error.HTTPError as e:
        return None, f"HTTP{e.code}"
    except Exception as e:
        return None, str(e)[:40]

def probe_aggs(ticker):
    """Daily aggs — works for C: (fx) and I: (index). Returns (ok, base, last, date)."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=12)
    for base in BASES:
        url = (f"{base}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
               f"?adjusted=true&sort=desc&limit=5&apiKey={KEY}")
        d, err = _get(url)
        if d and (d.get("results")):
            r0 = d["results"][0]
            return (True, base.split("//")[1], r0.get("c"),
                    datetime.utcfromtimestamp(r0.get("t", 0)/1000).strftime("%Y-%m-%d") if r0.get("t") else "?")
    return (False, None, None, err)

def probe_prev(ticker):
    for base in BASES:
        url = f"{base}/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={KEY}"
        d, err = _get(url)
        if d and d.get("results"):
            return (True, base.split("//")[1], d["results"][0].get("c"), "prev")
    return (False, None, None, err)

def probe_fut_snapshot(ticker):
    """Futures snapshot — v3/snapshot/futures."""
    for base in BASES:
        url = f"{base}/v3/snapshot/futures/{ticker}?apiKey={KEY}"
        d, err = _get(url)
        if d and (d.get("results") or d.get("result")):
            res = d.get("results") or d.get("result")
            return (True, base.split("//")[1], str(res)[:60], "snap")
    return (False, None, None, err)

# ── RORO data classes ──
FX = ["C:USDCHF","C:CHFUSD","C:USDJPY","C:AUDJPY","C:EURJPY","C:NZDJPY","C:AUDUSD",
      "C:NZDUSD","C:USDMXN","C:USDZAR","C:USDTRY","C:USDBRL","C:USDCNH","C:USDKRW",
      "C:USDSEK","C:USDNOK","C:XAUUSD","C:XAGUSD"]
IDX = ["I:VIX","I:VIX1D","I:VIX9D","I:VIX3M","I:VIX6M","I:VVIX","I:VXN","I:RVX",
       "I:MOVE","I:SKEW","I:SPX","I:NDX","I:RUT","I:TNX","I:DXY","I:OVX","I:GVZ"]
FUT_AGG = ["GC1!","CL1!","HG1!","SI1!","ES1!","NQ1!","RTY1!","ZN1!","ZB1!","6J1!","VX1!",
           "F:GCZ4","F:CLZ4"]
FUT_SNAP = ["GC","CL","HG","SI","ES","NQ","RTY","ZN","ZB","VX","6J"]
OPT = ["SPY","QQQ","HYG","TLT"]  # options snapshot (put/call build)

print("\n========== FX (havens / carry / EM) ==========")
fx_ok = []
with cf.ThreadPoolExecutor(max_workers=10) as ex:
    for t, r in zip(FX, ex.map(probe_aggs, FX)):
        ok, base, last, dt = r
        print(f"  {'OK ' if ok else 'DEAD'} {t:11s} {('%.4f'%last) if isinstance(last,(int,float)) else last}  {dt}  {base or ''}")
        if ok: fx_ok.append(t)

print("\n========== INDEX (vol family / rates / breadth) ==========")
idx_ok = []
with cf.ThreadPoolExecutor(max_workers=10) as ex:
    for t, r in zip(IDX, ex.map(lambda x: probe_aggs(x) if probe_aggs(x)[0] else probe_prev(x), IDX)):
        ok, base, last, dt = r
        print(f"  {'OK ' if ok else 'DEAD'} {t:9s} {last}  {dt}  {base or ''}")
        if ok: idx_ok.append(t)

print("\n========== FUTURES (aggs format) ==========")
fa_ok = []
with cf.ThreadPoolExecutor(max_workers=8) as ex:
    for t, r in zip(FUT_AGG, ex.map(probe_aggs, FUT_AGG)):
        ok, base, last, dt = r
        print(f"  {'OK ' if ok else 'DEAD'} {t:9s} {last}  {dt}  {base or ''}")
        if ok: fa_ok.append(t)

print("\n========== FUTURES (snapshot format) ==========")
fs_ok = []
with cf.ThreadPoolExecutor(max_workers=8) as ex:
    for t, r in zip(FUT_SNAP, ex.map(probe_fut_snapshot, FUT_SNAP)):
        ok, base, last, dt = r
        print(f"  {'OK ' if ok else 'DEAD'} {t:5s} {last}  {base or ''}")
        if ok: fs_ok.append(t)

print("\n========== OPTIONS (snapshot — for put/call) ==========")
for t in OPT:
    for base in BASES:
        url = f"{base}/v3/snapshot/options/{t}?limit=10&apiKey={KEY}"
        d, err = _get(url)
        if d and d.get("results"):
            print(f"  OK  {t}  n_contracts_sample={len(d['results'])}  {base.split('//')[1]}")
            break
    else:
        print(f"  DEAD {t}  {err}")

print("\n=== SUMMARY ===")
print("FX OK:", fx_ok)
print("IDX OK:", idx_ok)
print("FUT_AGG OK:", fa_ok)
print("FUT_SNAP OK:", fs_ok)
