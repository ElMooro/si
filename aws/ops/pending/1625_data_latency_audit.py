"""Data-latency audit (Z culprit #3): screener freshness, fundamentals recency
vs latest SEC filing, and stored-vs-live price drift."""
import json, time, urllib.request, statistics, boto3
from datetime import datetime, timezone, date
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
now = datetime.now(timezone.utc)

def load(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:120]}

def get(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"_err": str(e)[:80]}

# ── 1) screener snapshot freshness ──
scr = load("screener/data.json")
gen = scr.get("generated_at")
stocks = scr.get("stocks", [])
age_h = None
if gen:
    try:
        g = datetime.fromisoformat(gen.replace("Z","+00:00"))
        age_h = round((now - g).total_seconds()/3600, 1)
    except Exception: pass
print(f"screener/data.json: generated_at={gen} | age={age_h}h | stocks={len(stocks)}")
if stocks:
    print("  record fields:", list(stocks[0].keys())[:18])

# ── 2) sample 20 liquid names: fundamentals recency + price drift ──
sample = [s for s in stocks if s.get("price")][:20]
syms = [s.get("symbol") for s in sample if s.get("symbol")]
stored_px = {s["symbol"]: s.get("price") for s in sample if s.get("symbol")}

# live quotes
live = {}
q = get(f"https://financialmodelingprep.com/stable/batch-quote-short?symbols={','.join(syms)}&apikey={FMP}")
for row in (q if isinstance(q, list) else []):
    if row.get("price"): live[row.get("symbol")] = float(row["price"])

filing_ages, price_drifts = [], []
for sym in syms[:15]:
    inc = get(f"https://financialmodelingprep.com/stable/income-statement?symbol={sym}&limit=1&apikey={FMP}")
    rec = inc[0] if isinstance(inc, list) and inc else {}
    acc = rec.get("acceptedDate") or rec.get("fillingDate") or rec.get("date")
    if acc:
        try:
            ad = datetime.fromisoformat(str(acc)[:19].replace("Z","")).replace(tzinfo=timezone.utc) \
                 if len(str(acc)) > 10 else datetime.combine(date.fromisoformat(str(acc)[:10]), datetime.min.time(), timezone.utc)
            filing_ages.append((now - ad).days)
        except Exception: pass
    if sym in stored_px and sym in live and live[sym]:
        price_drifts.append(abs(stored_px[sym] - live[sym]) / live[sym] * 100)
    time.sleep(0.15)

def med(xs): return round(statistics.median(xs),1) if xs else None
print(f"\nfundamentals recency (latest SEC filing): median {med(filing_ages)}d old, "
      f"range {min(filing_ages) if filing_ages else '-'}..{max(filing_ages) if filing_ages else '-'}d  (n={len(filing_ages)})")
print(f"stored-vs-live price drift: median {med(price_drifts)}% (n={len(price_drifts)})  "
      f"[>2% suggests stale stored prices]")
