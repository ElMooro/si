"""ops 3347 — GSI data-depth probe. Establish ground truth before redesigning the
calibration page: how deep is dim-history really, why is N only 269, what's in the
4-horizon term structure, and which stress feeds exist. Read-only, no mutations.
"""
import json
import boto3
from datetime import datetime, timezone
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _get(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())


with report("3347_gsi_data_depth_probe") as r:
    # ---- 1. dim-history depth + per-dimension coverage ----
    r.section("1. gsi-dim-history depth & coverage")
    try:
        dh = _get("data/gsi-dim-history.json")
        snaps = dh.get("snapshots") or []
        r.ok(f"dim-history: {len(snaps)} snapshots")
        if snaps:
            dates = sorted(s.get("date", "") for s in snaps if s.get("date"))
            r.log(f"  span: {dates[0]} -> {dates[-1]}")
            # per-dim non-null coverage
            dims = ("market", "credit", "vix", "rate_vol", "contagion", "sovereign")
            cov = {d: 0 for d in dims}
            has_spy = 0
            for s in snaps:
                if isinstance(s.get("spy_close"), (int, float)):
                    has_spy += 1
                for d in dims:
                    v = (s.get("dims") or {}).get(d)
                    if isinstance(v, (int, float)):
                        cov[d] += 1
            r.log(f"  spy_close present: {has_spy}/{len(snaps)}")
            for d in dims:
                r.log(f"  dim '{d}': {cov[d]}/{len(snaps)} non-null ({cov[d]*100//max(1,len(snaps))}%)")
            # earliest date each dim has data (when did each come online?)
            first_seen = {}
            for s in sorted(snaps, key=lambda x: x.get("date", "")):
                for d in dims:
                    if d not in first_seen and isinstance((s.get("dims") or {}).get(d), (int, float)):
                        first_seen[d] = s.get("date")
            r.log(f"  first non-null date per dim: {first_seen}")
    except Exception as e:
        r.fail(f"dim-history: {type(e).__name__}: {str(e)[:120]}")

    # ---- 2. calibration report: why N=269 ----
    r.section("2. gsi-calibration.json current state")
    try:
        cal = _get("data/gsi-calibration.json")
        r.log(f"  mode={cal.get('mode')} sample_size={cal.get('sample_size')} "
              f"snapshots_total={cal.get('snapshots_total')}")
        r.log(f"  ic={cal.get('ic')}")
        r.log(f"  n_by_dim={cal.get('n_by_dim')}")
        r.log(f"  weights={cal.get('weights')}")
    except Exception as e:
        r.fail(f"calibration: {type(e).__name__}: {str(e)[:120]}")

    # ---- 3. horizon term structure ----
    r.section("3. gsi-horizons.json term structure")
    try:
        hz = _get("data/gsi-horizons.json")
        top = list(hz.keys())
        r.log(f"  top keys: {top[:14]}")
        # try common shapes
        for hk in ("horizons", "by_horizon", "term_structure"):
            if hk in hz:
                blk = hz[hk]
                if isinstance(blk, dict):
                    for h, v in blk.items():
                        ic = (v or {}).get("ic") if isinstance(v, dict) else None
                        n = (v or {}).get("sample_size") if isinstance(v, dict) else None
                        r.log(f"  horizon {h}: N={n} ic={json.dumps(ic)[:160] if ic else '?'}")
        r.ok("horizons file present")
    except Exception as e:
        r.fail(f"horizons: {type(e).__name__}: {str(e)[:120]}")

    # ---- 4. inventory of stress feeds that could feed the calibrator ----
    r.section("4. stress feed inventory (candidate signals beyond 6 dims)")
    candidates = [
        "data/ciss-stress.json", "data/eurodollar-stress.json", "data/credit-stress.json",
        "data/sovereign-stress.json", "data/systemic-stress.json", "data/crisis-composite.json",
        "data/bank-stress.json", "data/vix-curve.json", "data/vvix-vov-regime.json",
        "data/tail-risk.json", "data/risk-regime.json", "data/global-stress.json",
        "data/contagion.json", "data/bond-vol.json",
    ]
    live = []
    for key in candidates:
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            age_h = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600
            live.append((key, round(age_h, 1)))
        except Exception:
            pass
    r.ok(f"{len(live)}/{len(candidates)} candidate stress feeds live")
    for k, a in live:
        r.log(f"  {k} — {a}h old")
