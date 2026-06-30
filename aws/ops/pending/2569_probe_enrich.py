"""ops 2569 — probe exact fields for engine-v2 enrichment wiring."""
import boto3, json
s3 = boto3.client("s3", "us-east-1"); B = "justhodl-dashboard-live"
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=f"data/{k}.json")["Body"].read())
    except: return None
# 13f: how to know which famous funds hold a ticker
d = rd("13f-positions")
print("13f by_fund keys:", list((d.get("by_fund") or {}).keys()))
abt = d.get("aggregate_by_ticker") or {}
k0 = next(iter(abt), None)
print("13f aggregate_by_ticker sample", k0, "->", json.dumps(abt.get(k0), default=str)[:240] if k0 else None)
bf = d.get("by_fund") or {}
f0 = next(iter(bf), None)
print("13f by_fund sample", f0, "->", json.dumps(bf.get(f0), default=str)[:200] if f0 else None)
# political-stocks: per-ticker congress/trump
d = rd("political-stocks")
print("\npolitical-stocks keys:", list(d.keys())[:20])
for kk in ["trump_holdings","recent_trades","top_traded","congress_trades","by_ticker","most_bought"]:
    v = d.get(kk)
    if v is not None: print(f"  {kk}:", (json.dumps(v, default=str)[:200]))
# stock-valuations lookup
d = rd("stock-valuations")
sp = d.get("sp_table") or []
print("\nstock-valuations sp_table[0]:", json.dumps(sp[0], default=str)[:200] if sp else None)
print("  coverage:", len(sp), "tickers")
# chart-patterns categories
d = rd("chart-patterns")
print("\nchart-patterns counts:", d.get("counts"))
for cat in ["volume_breakouts","double_bottoms","cross_up_200dma"]:
    v = d.get(cat) or []
    if v: print(f"  {cat}[0]:", json.dumps(v[0], default=str)[:160]); break
# backtest-summary track record
d = rd("backtest-summary")
bs = d.get("by_signal") or {}
print("\nbacktest-summary by_signal keys:", list(bs.keys()))
s0 = next(iter(bs), None)
print("  sample", s0, "->", json.dumps(bs.get(s0), default=str)[:260] if s0 else None)
# eps-revision-velocity estimates
d = rd("eps-revision-velocity")
aq = d.get("all_qualifying") or []
if aq: print("\neps-velocity[0]:", json.dumps(aq[0], default=str)[:260])
print("DONE 2569")
