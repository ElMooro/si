"""
ops_3822 — cycle bands into justhodl-onchain-ratios (Rainbow + Pi Cycle)

AUDIT FIRST (grep across 748 engines) — these already exist and are NOT rebuilt:
  MVRV 15 engines · NUPL 7 · Puell 7 · Mayer 4 · realized price 6
The only TRUE zeros in Khalid's list were:
  rainbow  -> 0 hits      pi_cycle -> 0 hits
Both added to justhodl-onchain-ratios (data/onchain-ratios.json), which already
owns the on-chain cycle-valuation vocabulary and has its own page (onchain.html).
Extending beats a fourth crypto engine.

WHAT THE GATES ACTUALLY PROVE (not just "a number appeared"):
  • the Rainbow is a REAL OLS fit — R^2 in a sane range, sigma > 0, n >= 800,
    and the fair value must sit within an order of magnitude of spot. A stub
    or a degenerate fit fails these.
  • Pi Cycle used a genuine 350-day window (ma_350d > 0 and != ma_111d).
  • the honesty caveats are PRESENT in the feed — they are load-bearing, not
    decoration, and stripping them silently is the failure mode to prevent.
"""
import json
import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402
from ops_report import report  # noqa: E402

FN = "justhodl-onchain-ratios"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/onchain-ratios.json"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3822_cycle_bands") as rep:
        rep.heading("ops 3822 — Rainbow + Pi Cycle into onchain-ratios")

        rep.section("G0. No-rebuild check")
        lam_dir = ROOT / "lambdas"
        for metric, pat in (("rainbow", "rainbow"), ("pi_cycle", "pi_cycle")):
            hits = [p.parent.parent.name for p in lam_dir.glob("*/source/*.py")
                    if pat in p.read_text(encoding="utf-8", errors="ignore").lower()
                    and p.parent.parent.name != FN]
            if hits:
                rep.warn(f"  {metric} also referenced by {hits[:4]} — verify no duplication")
            else:
                rep.ok(f"  {metric}: still zero other engines — genuine gap")

        rep.section("1. Inherit env + deploy")
        env = (lam.get_function_configuration(FunctionName=FN)
               .get("Environment", {}).get("Variables", {})) or {}
        env.setdefault("S3_BUCKET", BUCKET)
        rep.log(f"  preserving env keys: {sorted(env)}")
        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC, env_vars=env,
            timeout=300, memory=1024,
            description="On-chain BTC/ETH ratios + cycle bands (Rainbow OLS, Pi Cycle, Mayer, 200wMA)",
            create_function_url=False, smoke=False,
        )

        rep.section("2. Zip-settle")
        for i in range(40):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                rep.ok(f"  settled after {i*5}s")
                break
            time.sleep(5)
        else:
            rep.fail("never settled")
            sys.exit(1)

        rep.section("3. Invoke")
        cfg = boto3.session.Config(read_timeout=290, retries={"max_attempts": 0})
        r = boto3.client("lambda", region_name="us-east-1", config=cfg).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {str(pl)[:600]}")
            sys.exit(1)
        rep.log(f"  {str(pl)[:220]}")

        rep.section("4. Verify the fit is REAL")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        cb = d.get("cycle_bands") or {}
        checks, fails = [], []

        def chk(n, cond, det=""):
            (checks if cond else fails).append(n)
            (rep.ok if cond else rep.fail)(f"  {n} {det}")

        chk("cycle_bands present", bool(cb.get("available")),
            f"reason={cb.get('reason','')}")
        if not cb.get("available"):
            rep.fail("cycle_bands unavailable — cannot verify")
            sys.exit(1)

        rb = cb.get("rainbow") or {}
        pi = cb.get("pi_cycle") or {}
        price = cb.get("price")

        chk("history >= 800 closes", (cb.get("n_daily_closes") or 0) >= 800,
            f"= {cb.get('n_daily_closes')} from {rb.get('history_starts')}")
        chk("rainbow fit available", bool(rb.get("available")),
            f"reason={rb.get('reason','')}")
        r2 = rb.get("r_squared")
        chk("R^2 in a sane range (0.5–0.999)",
            r2 is not None and 0.5 <= r2 <= 0.999, f"= {r2}")
        chk("residual sigma > 0", (rb.get("residual_sigma") or 0) > 0,
            f"= {rb.get('residual_sigma')}")
        fv = rb.get("fair_value")
        chk("fair value within 10x of spot",
            bool(fv and price and 0.1 <= fv / price <= 10),
            f"fair {fv} vs spot {price}")
        chk("band + z published",
            rb.get("band") is not None and rb.get("z_sigma") is not None,
            f"z={rb.get('z_sigma')} band='{rb.get('band')}'")
        chk("7 band prices", len(rb.get("band_prices") or {}) == 7)

        chk("pi cycle available", bool(pi.get("available")),
            f"reason={pi.get('reason','')}")
        chk("350DMA genuinely computed",
            bool(pi.get("ma_350d")) and pi.get("ma_350d") != pi.get("ma_111d"),
            f"111d={pi.get('ma_111d')} 350d={pi.get('ma_350d')}")
        chk("pi signal + distance",
            pi.get("signal") is not None
            and pi.get("distance_to_trigger_pct") is not None,
            f"{pi.get('signal')} · {pi.get('distance_to_trigger_pct')}% to trigger")
        chk("pi ships n=3", pi.get("historical_n") == 3)

        rc = (rb.get("caveat") or "").lower()
        pc = pi.get("caveat") or ""
        chk("rainbow caveat: no predictive claim",
            "cannot predict" in rc and "never a target" in rc)
        chk("rainbow caveat: fit-to-own-history stated",
            "own history" in rc and "drift" in rc)
        chk("pi caveat ships n=3 explicitly", "n=3" in pc)

        mm = cb.get("mayer_multiple") or {}
        if mm:
            rep.log(f"    Mayer {mm.get('value')} ({mm.get('read')}) "
                    f"pctile {mm.get('percentile_all_history')}")
        w200 = cb.get("ma_200_week") or {}
        if w200:
            rep.log(f"    200wMA {w200.get('value')} · {w200.get('pct_above')}% above")

        rep.kv(price=price, n_closes=cb.get("n_daily_closes"),
               rainbow_band=rb.get("band"), z_sigma=rb.get("z_sigma"),
               fair_value=fv, r_squared=r2,
               pi_signal=pi.get("signal"),
               pi_distance_pct=pi.get("distance_to_trigger_pct"),
               mayer=mm.get("value"))

        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}")
            sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)}")


if __name__ == "__main__":
    main()
