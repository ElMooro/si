"""justhodl-position-sizer — turns picks into SIZED picks. Combines each setup's
conviction + the user's Brain risk_posture + the current regime (bond-vol +
funding-plumbing) into a suggested position-size band, using a fractional-Kelly-
style scaling (capped, conservative). Not advice — a disciplined sizing frame.

OUTPUT: data/position-sizing.json · SCHEDULE: every 6h.
"""
import json, time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/position-sizing.json"
s3 = boto3.client("s3", region_name=REGION)


def rj(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def lambda_handler(event=None, context=None):
    t0 = time.time()
    bs = rj("data/best-setups.json") or {}
    brain = rj("data/brain.json") or {}
    bv = rj("data/bond-vol.json") or {}
    fp = rj("data/funding-plumbing.json") or {}
    dg = rj("data/dealer-gex.json") or {}
    vs = rj("data/vol-surface.json") or {}

    directive = brain.get("directive") or {}
    posture = (directive.get("risk_posture") or "balanced").lower()
    posture_mult = 1.3 if "aggressive" in posture else 0.6 if "defensive" in posture else 1.0

    # regime risk multiplier — shrink size as the macro backdrop deteriorates
    bvr = (bv.get("regime") or "NORMAL").upper()
    fpr = (fp.get("regime") or "AMPLE").upper()
    regime_mult = {"CRISIS": 0.4, "ELEVATED": 0.7, "NORMAL": 1.0, "BOND_VOL_LOW": 1.05}.get(bvr, 1.0)
    regime_mult *= {"STRESS": 0.5, "FRAGILE": 0.7, "TIGHTENING": 0.85, "AMPLE": 1.0}.get(fpr, 1.0)

    # dealer-gamma + vol-surface multiplier (Massive options) — negative gamma = dealers amplify
    # moves = vol-expansion risk = size down; vol-surface stress / inverted term structure = size down.
    gamma_regime = ((dg.get("market_composite") or {}).get("composite_regime") or "UNKNOWN").upper()
    vs_regime = (vs.get("regime") or "NORMAL").upper()
    ts_inverted = bool((vs.get("term_structure") or {}).get("inverted"))
    gamma_mult = (0.8 if "ALL_NEGATIVE" in gamma_regime else 0.9 if "NEGATIVE" in gamma_regime
                  else 1.05 if "POSITIVE" in gamma_regime else 1.0)
    vol_mult = (0.7 if vs_regime in ("STRESS", "ELEVATED", "HIGH", "CRISIS")
                else 0.85 if ts_inverted else 1.0)
    gamma_vol_mult = round(gamma_mult * vol_mult, 2)
    regime_mult *= gamma_vol_mult

    # base size from conviction via fractional-Kelly-ish curve (cap 6% full-Kelly
    # equivalent, then scaled). conviction 50→~0%, 100→~max.
    MAX_BASE = 6.0  # % of book at top conviction, before posture/regime scaling
    sized = []
    for s in (bs.get("top_setups") or [])[:30]:
        conv = s.get("conviction") or 0
        if conv < 55:
            continue
        edge = max(0.0, (conv - 50) / 50.0)        # 0..1
        base = MAX_BASE * (edge ** 1.4)            # convex — reward high conviction
        size = base * posture_mult * regime_mult
        size = max(0.0, min(8.0, size))            # hard cap 8% per name
        band_lo = round(size * 0.6, 1); band_hi = round(size, 1)
        sized.append({
            "ticker": s.get("ticker"), "verdict": s.get("verdict"), "conviction": conv,
            "suggested_size_pct": round(size, 1), "size_band": f"{band_lo}–{band_hi}%",
            "rationale": f"conv {conv} × {posture.split('—')[0].strip() or posture} posture ({posture_mult}×) × regime {bvr}/{fpr}/gamma ({round(regime_mult,2)}×)",
        })
    sized.sort(key=lambda x: -x["suggested_size_pct"])
    total = round(sum(x["suggested_size_pct"] for x in sized[:15]), 1)

    out = {"engine": "position-sizer", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "risk_posture": posture, "posture_mult": posture_mult,
           "regime": {"bond_vol": bvr, "plumbing": fpr, "gamma_regime": gamma_regime,
                      "vol_surface_regime": vs_regime, "term_inverted": ts_inverted,
                      "gamma_vol_mult": gamma_vol_mult, "combined_mult": round(regime_mult, 2)},
           "sized_positions": sized[:20],
           "suggested_gross_top15_pct": total,
           "note": ("Disciplined sizing frame: fractional-Kelly scaled by your risk posture and the "
                    "current regime. Conservative caps (8%/name). Research, not advice."),
           "caveat": "Sizes shrink automatically when the macro regime deteriorates."}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[position-sizer] {len(sized)} sized, posture={posture_mult}x regime={round(regime_mult,2)}x gamma={gamma_regime} gv_mult={gamma_vol_mult}")
    return {"statusCode": 200, "body": json.dumps({"n": len(sized)})}
