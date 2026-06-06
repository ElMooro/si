"""justhodl-engine-conflicts — 'the system arguing with itself.' Surfaces names
where the platform's own engines DISAGREE, instead of papering over it with a
false-confident single verdict. The honest, high-value cases:

  • VALUE vs MOMENTUM: dislocation says cheap, but price/trend is rolling over
    (falling knife risk).
  • FLOW vs PRICE: institutions accumulating, but the tape is weak.
  • BULL SIGNALS vs DEVIL: high conviction but the devil's-advocate flags a rule
    violation / high bear risk.
  • NARRATIVE vs TAPE: already its own engine; we cross-reference it.

For each conflict we present BOTH sides so the user decides with eyes open.
OUTPUT: data/engine-conflicts.json · SCHEDULE: every 6h.
"""
import json, time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/engine-conflicts.json"
s3 = boto3.client("s3", region_name=REGION)


def rj(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def lambda_handler(event=None, context=None):
    t0 = time.time()
    disl = rj("data/dislocations.json") or {}
    opp = rj("data/opportunities.json") or {}
    cf = rj("data/capital-flow.json") or {}
    devils = rj("data/devils-advocate.json") or {}
    bs = rj("data/best-setups.json") or {}

    # momentum / trend read from opportunities (use any momentum/RS field present)
    mom = {}
    for r in (opp.get("all") or []):
        tk = (r.get("ticker") or "").upper()
        gi = r.get("growth_intel") or {}
        m = r.get("momentum") or r.get("rs") or gi.get("momentum") or r.get("price_chg_1m") or r.get("perf_1m")
        if tk and m is not None:
            try: mom[tk] = float(m)
            except (ValueError, TypeError): pass

    cheap = {}
    for r in [*(disl.get("buy_the_laggard") or []), *(disl.get("top_dislocations") or [])]:
        tk = (r.get("ticker") or "").upper()
        if tk: cheap[tk] = r
    accum = {(r.get("ticker") or r.get("symbol") or "").upper() for r in (cf.get("accumulating") or [])}
    distrib = {(r.get("ticker") or r.get("symbol") or "").upper() for r in (cf.get("distributing") or [])}
    devil_by = devils.get("by_ticker") or {}

    conflicts = []

    # VALUE vs MOMENTUM (cheap but falling)
    for tk, rec in cheap.items():
        m = mom.get(tk)
        if m is not None and m < -8:   # cheap but down >8% recently
            conflicts.append({"ticker": tk, "type": "VALUE vs MOMENTUM",
                "bull": f"Dislocation engine: cheap vs peers" + (" & inflecting" if rec.get("cheap_and_inflecting") else ""),
                "bear": f"But momentum is {round(m,1)}% — possible falling knife; the discount may be deserved.",
                "resolution": "Wait for a momentum turn / base before the value thesis is confirmed."})

    # FLOW vs PRICE (accumulating but weak tape)
    for tk in accum:
        m = mom.get(tk)
        if m is not None and m < -6:
            conflicts.append({"ticker": tk, "type": "FLOW vs PRICE",
                "bull": "Capital-flow: institutions accumulating.",
                "bear": f"But price is {round(m,1)}% — accumulation hasn't shown in the tape yet.",
                "resolution": "Smart money may be early; size for patience or wait for price confirmation."})

    # CONVICTION vs DEVIL (high conviction but devil flags a rule violation)
    for s in (bs.get("top_setups") or [])[:20]:
        tk = s.get("ticker"); dv = devil_by.get(tk)
        if dv and (dv.get("violates_your_rule") or dv.get("risk_level") == "high") and (s.get("conviction") or 0) >= 70:
            conflicts.append({"ticker": tk, "type": "CONVICTION vs YOUR RULES",
                "bull": f"Conviction {s.get('conviction')} — {s.get('verdict')}.",
                "bear": "Devil's advocate: " + (dv.get("bear_case") or "") + (f" Breaks your rule: {dv.get('violates_your_rule')}." if dv.get("violates_your_rule") else ""),
                "resolution": "The board likes it but it conflicts with your own discipline — you decide which wins."})

    # dedup by ticker+type
    seen = set(); uniq = []
    for c in conflicts:
        k = (c["ticker"], c["type"])
        if k not in seen: seen.add(k); uniq.append(c)

    out = {"engine": "engine-conflicts", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "conflicts": uniq[:25], "n_conflicts": len(uniq),
           "note": "Where the system's own engines disagree — both sides shown so you decide with eyes open."}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[engine-conflicts] {len(uniq)} conflicts")
    return {"statusCode": 200, "body": json.dumps({"n": len(uniq)})}
