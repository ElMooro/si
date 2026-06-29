"""
justhodl-sector-flow-state — canonical fused SECTOR CONVICTION feed.

Every other engine that wants a sector view (deal-scanner, master-ranker, best-setups,
bottleneck-boom) currently re-derives it ad hoc from different sources. This engine fuses
the sector signals already computed by sector-rotation into ONE per-sector conviction +
posture + confluence, blends in the market-wide liquidity backdrop (Druckenmiller's master
variable), and emits a single source-of-truth:  s3://justhodl-dashboard-live/data/sector-flow-state.json

conviction = rotation_score
           + RRG-quadrant adj  (Leading +8 / Improving +5 / Weakening -5 / Lagging -8)
           + ETF-flow-confirm  (STRONG_INFLOW +6 ... STRONG_OUTFLOW -6)
           + money-flow        (MFI>60 +3 / <40 -3)
           + liquidity tilt    (draining -2 to risk-on sectors)
confluence = count of agreeing bullish signals (RRG up, inflow, MFI strong, RS accelerating, cycle-favored)
posture    = OVERWEIGHT (conv>=62 & confluence>=3) / UNDERWEIGHT (conv<=42) / NEUTRAL
"""
import json
import boto3
from datetime import datetime, timezone

S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
RISK_ON = {"XLK", "XLY", "XLF", "XLC", "XLB", "XLE", "XLI", "XLRE"}


def rj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[rj] {key}: {str(e)[:60]}")
        return {}


def rrg_quad(rank, slope):
    strong, rising = rank >= 50, slope >= 0
    if strong and rising:
        return "Leading"
    if strong and not rising:
        return "Weakening"
    if not strong and not rising:
        return "Lagging"
    return "Improving"


def lambda_handler(event, context):
    sec = rj("data/sector-rotation.json")
    liq = rj("data/liquidity-flow.json")
    sectors = sec.get("sectors") or []
    liq_drain = (liq.get("regime") == "draining")
    out = []
    for s in sectors:
        sym = s.get("symbol")
        if not sym:
            continue
        base = float(s.get("rotation_score") or 0)
        rank = float(s.get("rs_pct_rank_1y") or 50)
        slope = float(s.get("rs_slope_21d_pct_per_day") or 0)
        quad = rrg_quad(rank, slope)
        q_adj = {"Leading": 8, "Improving": 5, "Weakening": -5, "Lagging": -8}[quad]
        fc = str(s.get("etf_flow_confirm") or "").upper()
        flow_adj = {"STRONG_INFLOW": 6, "INFLOW": 3, "NEUTRAL": 0, "OUTFLOW": -3, "STRONG_OUTFLOW": -6}.get(fc, 0)
        mfi = float(s.get("money_flow_index_14") or 50)
        mfi_adj = 3 if mfi > 60 else -3 if mfi < 40 else 0
        liq_adj = -2 if (liq_drain and sym in RISK_ON) else 0
        conv = max(0.0, min(100.0, base + q_adj + flow_adj + mfi_adj + liq_adj))
        drivers = []
        if quad in ("Leading", "Improving"):
            drivers.append(f"RRG {quad}")
        if flow_adj > 0:
            drivers.append(f"flow {fc.replace('_', ' ').lower()}")
        if mfi > 60:
            drivers.append("money-flow strong")
        if "RS_ACCELERATING" in (s.get("rotation_in_flags") or []):
            drivers.append("RS accelerating")
        if s.get("in_current_cycle"):
            drivers.append("cycle-favored")
        conf = len(drivers)
        posture = "OVERWEIGHT" if (conv >= 62 and conf >= 3) else "UNDERWEIGHT" if (conv <= 42) else "NEUTRAL"
        out.append({
            "symbol": sym, "name": s.get("name"), "conviction": round(conv, 1), "posture": posture,
            "quadrant": quad, "confluence": conf, "drivers": drivers,
            "rotation_score": round(base, 1), "rs_rank_1y": round(rank, 1), "rs_slope": round(slope, 4),
            "flow_confirm": fc or None, "in_cycle": bool(s.get("in_current_cycle")),
        })
    out.sort(key=lambda x: -x["conviction"])
    doc = {
        "engine": "justhodl-sector-flow-state", "version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "liquidity_regime": liq.get("regime"),
        "cycle_phase": (sec.get("macro_context") or {}).get("cycle_phase"),
        "n_sectors": len(out),
        "overweight": [x["symbol"] for x in out if x["posture"] == "OVERWEIGHT"],
        "underweight": [x["symbol"] for x in out if x["posture"] == "UNDERWEIGHT"],
        "sectors": out,
        "methodology": ("Fused per-sector conviction = rotation_score + RRG-quadrant + ETF-flow-confirm "
                        "+ money-flow + liquidity tilt; confluence = count of agreeing signals; posture from "
                        "conviction+confluence. Source: sector-rotation + liquidity-flow."),
        "consumers": "deal-scanner, master-ranker, best-setups, bottleneck-boom (map ticker -> SPDR sector)",
    }
    S3.put_object(Bucket=BUCKET, Key="data/sector-flow-state.json",
                  Body=json.dumps(doc).encode(), ContentType="application/json")
    print(f"emitted sector-flow-state: {len(out)} sectors, OW={doc['overweight']}, UW={doc['underweight']}")
    return {"ok": True, "n": len(out), "ow": doc["overweight"], "uw": doc["underweight"]}
