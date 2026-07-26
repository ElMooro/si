"""
ops_3910 — PROBE: the decisive closed-loop audit. Repo grep shows the
plumbing exists (master-ranker reads SSM /justhodl/calibration/weights,
best-setups has learned_weights from cascade-calibration, signal-board reads
confluence-meta) — but every 'applied fleet-wide' claim this session has
needed live verification. This checks: (1) does the SSM parameter actually
EXIST with real non-flat values, and when was it last modified; (2) does
best-setups' live output show learned weights actually diverging from
defaults; (3) does anything anywhere exclude the two Fade-Index-confirmed-
broken engines (macro/equity_frontrun_sniffer). Writes no code.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def main():
    with report("3910_closed_loop_live_audit") as rep:
        rep.heading("ops 3910 — is the closed loop actually closed, live")

        rep.section("1. SSM /justhodl/calibration/weights — exists? real values? fresh?")
        try:
            p = ssm.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]
            val = json.loads(p["Value"])
            rep.ok(f"  EXISTS, last modified {p.get('LastModifiedDate')}, version {p.get('Version')}")
            rep.log(f"  full value: {json.dumps(val, default=str)[:800]}")
            flat = all(abs(v - 1.0) < 0.001 for v in val.values()) if isinstance(val, dict) and val else None
            rep.kv(n_weights=len(val) if isinstance(val, dict) else None, all_flat_1_0=flat)
        except ssm.exceptions.ParameterNotFound:
            rep.fail("  PARAMETER DOES NOT EXIST — master-ranker has been running on flat "
                     "weights every single run; the plumbing was never fed")
        except Exception as e:
            rep.fail(f"  read failed: {str(e)[:200]}")

        rep.section("2. best-setups live output — do learned weights actually diverge from defaults")
        try:
            bs = json.loads(s3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
            lw = bs.get("learned_weights") or bs.get("weights_used") or bs.get("meta", {}).get("learned_weights")
            rep.log(f"  top-level keys: {sorted(bs.keys())[:20]}")
            rep.log(f"  learned_weights field: {json.dumps(lw, default=str)[:600]}")
        except Exception as e:
            rep.fail(f"  best-setups.json unreadable: {str(e)[:200]}")

        rep.section("3. cascade-calibration feed (what best-setups learns from) — real? fresh?")
        try:
            o = s3.get_object(Bucket=BUCKET, Key="data/cascade-calibration.json")
            cc = json.loads(o["Body"].read())
            age_h = round((datetime.now(timezone.utc) - o["LastModified"]).total_seconds()/3600, 1)
            rep.ok(f"  exists, {age_h}h old")
            attr = cc.get("feature_attribution_by_tier") or {}
            rep.kv(n_tiers=len(attr))
            for tier, d in list(attr.items())[:5]:
                ranked = d.get("ranked_by_hit_rate_lift") or []
                rep.log(f"    {tier}: top={ranked[0] if ranked else None}")
        except Exception as e:
            rep.fail(f"  cascade-calibration.json unreadable: {str(e)[:150]}")

        rep.section("4. do the two confirmed-broken frontrun sniffers still feed anything downstream")
        consumers = []
        # check the live feeds most likely to consume them
        for key in ("data/signal-board.json", "data/apex-fusion.json", "data/confluence-meta.json"):
            try:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                blob = json.dumps(doc)
                hit_macro = "macro_frontrun" in blob
                hit_equity = "equity_frontrun" in blob
                excluded = "fade" in blob.lower() or "excluded" in blob.lower()
                rep.log(f"  {key}: mentions macro_frontrun={hit_macro} equity_frontrun={hit_equity} "
                        f"has fade/exclusion language={excluded}")
                if hit_macro or hit_equity:
                    consumers.append(key)
            except Exception as e:
                rep.log(f"  {key}: unreadable ({str(e)[:100]})")
        rep.kv(feeds_still_carrying_broken_sniffers=str(consumers))

        rep.ok("PROBE COMPLETE — see each section for the live truth")


if __name__ == "__main__":
    main()
