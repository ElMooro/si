"""ops/827 - data-coverage probe: audit-before-build gate for the Risk
Radar (the platform's defensive desk - deterioration / avoid / short-
candidate screen).

The platform has 8+ long-idea screens and zero risk screen. Before
building one, this probe establishes EXACTLY what deterioration signal
is computable on real data - no hollow engine. It dumps:

  1. screener/data.json    - universe size + per-field POPULATION rate
     (what fraction of stocks actually carry altmanZ, piotroski,
     priceTargetUpsidePct, debt ratios, returns, any grade/quality field)
  2. eps-revision-velocity - whether NEGATIVE-revision names are carried
     (the positive list is known; the question is the downgrade side)
  3. asymmetric-scorer     - value_traps shape
  4. fundamentals          - confirm altman_z / piotroski / dcf_gap
  5. deep-value            - structure
  6. momentum-breakout     - qualifying-list shape (for breakdown x-check)
"""
import json
from collections import Counter
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

report = {"ops": 827, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Risk Radar data-coverage probe (audit-before-build)"}


def get(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def field_population(rows):
    """For a list of dicts, what fraction of rows has each field non-null."""
    n = len(rows)
    cnt = Counter()
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k, v in r.items():
            if v is not None and v != "":
                cnt[k] += 1
    return {k: round(c / n, 3) for k, c in
            sorted(cnt.items(), key=lambda x: -x[1])} if n else {}


try:
    # -- 1. screener/data.json -------------------------------------------
    sc = get("screener/data.json")
    rows = sc.get("stocks")
    if not isinstance(rows, list):
        bs = sc.get("by_symbol") or {}
        rows = list(bs.values()) if isinstance(bs, dict) else []
    pop = field_population(rows)
    # negative-target-upside count (a soft overvaluation signal)
    neg_upside = 0
    distress_z = 0
    have_z = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        up = r.get("priceTargetUpsidePct")
        if isinstance(up, (int, float)) and up < 0:
            neg_upside += 1
        z = r.get("altmanZ")
        if isinstance(z, (int, float)):
            have_z += 1
            if z < 1.8:
                distress_z += 1
    report["screener"] = {
        "universe": len(rows),
        "field_population": pop,
        "sample_keys": sorted(rows[0].keys()) if rows else [],
        "n_negative_target_upside": neg_upside,
        "n_with_altman_z": have_z,
        "n_altman_distress_zone": distress_z,
    }

    # -- 2. eps-revision-velocity ----------------------------------------
    try:
        ev = get("data/eps-revision-velocity.json")
        q = ev.get("all_qualifying", [])
        scores = [x.get("score") for x in q
                  if isinstance(x.get("score"), (int, float))]
        report["eps_revision"] = {
            "top_keys": list(ev.keys()),
            "n_qualifying": len(q),
            "score_min": min(scores) if scores else None,
            "score_max": max(scores) if scores else None,
            "n_negative_score": sum(1 for s in scores if s < 0),
            "sample_item_keys": sorted(q[0].keys()) if q else [],
            "summary_keys": list(ev.get("summary", {}).keys())
            if isinstance(ev.get("summary"), dict) else None,
        }
    except Exception as e:
        report["eps_revision"] = {"error": str(e)}

    # -- 3. asymmetric-scorer value_traps --------------------------------
    try:
        asy = get("data/asymmetric-scorer.json")
        vt = asy.get("value_traps", [])
        report["asymmetric"] = {
            "top_keys": list(asy.keys()),
            "n_value_traps": len(vt),
            "value_trap_item_keys": sorted(vt[0].keys()) if vt else [],
            "value_trap_sample": vt[:3],
        }
    except Exception as e:
        report["asymmetric"] = {"error": str(e)}

    # -- 4. fundamentals -------------------------------------------------
    try:
        fu = get("data/fundamentals.json")
        co = fu.get("companies", [])
        report["fundamentals"] = {
            "top_keys": list(fu.keys()),
            "n_companies": len(co),
            "item_keys": sorted(co[0].keys()) if co else [],
            "sample": co[0] if co else None,
        }
    except Exception as e:
        report["fundamentals"] = {"error": str(e)}

    # -- 5. deep-value ---------------------------------------------------
    try:
        dv = get("data/deep-value.json")
        aq = dv.get("all_qualifying", [])
        report["deep_value"] = {
            "top_keys": list(dv.keys()),
            "n_qualifying": len(aq),
            "item_keys": sorted(aq[0].keys()) if aq else [],
            "summary_keys": list(dv.get("summary", {}).keys())
            if isinstance(dv.get("summary"), dict) else None,
        }
    except Exception as e:
        report["deep_value"] = {"error": str(e)}

    # -- 6. momentum-breakout --------------------------------------------
    try:
        mb = get("data/momentum-breakout.json")
        aq = mb.get("all_qualifying", [])
        report["momentum_breakout"] = {
            "top_keys": list(mb.keys()),
            "n_qualifying": len(aq),
            "item_keys": sorted(aq[0].keys()) if aq else [],
        }
    except Exception as e:
        report["momentum_breakout"] = {"error": str(e)}

    report["all_pass"] = True
except Exception as e:
    import traceback
    report["error"] = f"{type(e).__name__}: {e}"
    report["trace"] = traceback.format_exc()[-1400:]
    report["all_pass"] = False

with open("aws/ops/reports/827_risk_data_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
