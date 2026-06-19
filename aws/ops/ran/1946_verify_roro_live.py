"""ops 1946 — VERIFY the live RORO system end-to-end before extending it.
Audit-only (no deploys). Confirms:
  - data/risk-regime.json + data/polygon-fx-regime.json are FRESH (running on schedule)
  - the synthesizer's 4 blocks are all live
  - signal-board / master-ranker / best-setups / morning-intel carry RORO in LIVE output
"""
import json
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
now = datetime.now(timezone.utc)

def get(k):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e:
        return {"__err__": str(e)}

def age_h(iso):
    try:
        t = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if t.tzinfo is None: t = t.replace(tzinfo=timezone.utc)
        return round((now - t).total_seconds() / 3600, 1)
    except Exception:
        return None

print("=== SYNTHESIZER: data/risk-regime.json ===")
rr = get("data/risk-regime.json")
print(f"  generated_at={rr.get('generated_at')}  age={age_h(rr.get('generated_at'))}h  v={rr.get('version')}")
print(f"  score={rr.get('risk_regime_score')}  regime={rr.get('risk_regime')}")
print(f"  posture={json.dumps(rr.get('posture'))}")
for b in rr.get("blocks_used", []):
    print(f"    block {b.get('block'):8s} w={b.get('weight')} score={b.get('score')}")
comp = rr.get("components", {})
for ck in ("fx", "options", "vix", "credit"):
    c = comp.get(ck, {})
    keys = {k: c.get(k) for k in list(c)[:5] if k != "tells"}
    print(f"    {ck:8s}: {json.dumps(keys)[:150]}")
print(f"  tells={rr.get('tells')}")

print("\n=== FX SOURCE: data/polygon-fx-regime.json ===")
fx = get("data/polygon-fx-regime.json")
print(f"  generated_at={fx.get('generated_at')}  age={age_h(fx.get('generated_at'))}h  v={fx.get('version')}  n_pairs={fx.get('n_pairs')}")
fr = fx.get("fx_roro", {})
print(f"  fx_roro_score={fr.get('fx_roro_score')}  regime={fr.get('fx_roro_regime')}  havens_bid={fr.get('havens_bid_count')}")
print(f"  drivers={json.dumps(fr.get('drivers'))[:240]}")

print("\n=== CONSUMER 1: signal-board ===")
sb = get("data/signal-board.json")
print("  RORO feed present:", "Risk Regime (RORO)" in json.dumps(sb))

print("\n=== CONSUMER 2: master-ranker ===")
mr = get("data/master-ranker.json")
print("  top-level risk_regime:", json.dumps(mr.get("risk_regime"))[:200])
tt = mr.get("top_tickers", [])
tl = [t for t in tt if isinstance(t.get("risk_regime_mult"), (int, float)) and t["risk_regime_mult"] != 1.0]
print(f"  top_tickers={len(tt)}  with RORO tilt!=1.0={len(tl)}")

print("\n=== CONSUMER 3: best-setups ===")
bs = get("data/best-setups.json")
st = bs.get("setups") or bs.get("top_setups") or []
bl = [s for s in st if isinstance(s.get("risk_regime_mult"), (int, float)) and s["risk_regime_mult"] != 1.0]
print(f"  setups={len(st)}  with RORO mult!=1.0={len(bl)}  generated={bs.get('generated_at')}")

print("\n=== CONSUMER 4: morning-intelligence (RORO line source) ===")
mi = get("data/morning-intelligence.json")
blob = json.dumps(mi)
print("  RISK_REGIME line/marker present:", ("RISK_REGIME" in blob or "RORO" in blob), " age=", age_h(mi.get("generated_at")))

print("\nDONE 1946")
