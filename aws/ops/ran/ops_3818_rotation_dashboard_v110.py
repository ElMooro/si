"""
ops_3818 — rotation-dashboard v1.1.0: close the two `degraded` entries

Probe 3817 established the real shapes; this wires against them and GATES that
the degradation actually clears rather than assuming it did:

  (a) dollar-radar 3m lives at bbdxy.dxy_synth.chg_3m_pct (= +2.87 at probe),
      NOT top-level. Reading it top-level silently neutralised the L1 dollar
      tilt — a rising dollar should be penalising EM/commodities/gold and it
      was doing nothing.
  (b) cftc-all-cache rows live under data{CODE: {..., weekly_reports:[...]}},
      net positioning field = net_speculator. Keyed by CFTC contract CODE.

Also logs the etf-true-flows shape: every assets[].flows came back None on the
v1.0.0 run while the feed itself loaded, so symbol matching is failing. Probing
it here so the next pass fixes it against a known shape, not a guess.
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

FN = "justhodl-rotation-dashboard"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/rotation-dashboard.json"
DONORS = ["justhodl-equity-research", "justhodl-industry-rotation",
          "justhodl-asset-compass"]

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3818_rotation_dashboard_v110") as rep:
        rep.heading("ops 3818 — rotation-dashboard v1.2.0 (COT low-n + flows field names)")

        # ── G0: assert the probe-confirmed paths still exist in the LIVE feeds ─
        rep.section("G0. KEY CONTRACT — against LIVE artifacts, not source")
        g0 = []
        dr = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/dollar-radar.json")["Body"].read())
        v = ((dr.get("bbdxy") or {}).get("dxy_synth") or {}).get("chg_3m_pct")
        if v is None:
            g0.append("bbdxy.dxy_synth.chg_3m_pct missing")
            rep.fail("  dollar: bbdxy.dxy_synth.chg_3m_pct NOT present")
        else:
            rep.ok(f"  dollar: bbdxy.dxy_synth.chg_3m_pct = {v}")

        cf = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/cftc-all-cache.json")["Body"].read())
        dd = cf.get("data")
        ok_cot = isinstance(dd, dict) and any(
            isinstance(r, dict) and r.get("weekly_reports") for r in dd.values())
        if not ok_cot:
            g0.append("cftc data{}.weekly_reports missing")
            rep.fail("  cftc: data{}.weekly_reports NOT present")
        else:
            rep.ok(f"  cftc: data{{}} has {len(dd)} contracts with weekly_reports")
        if g0:
            rep.fail(f"G0 FAILED: {g0}")
            sys.exit(1)

        # ── probe etf-true-flows shape (informational, for the next pass) ──
        rep.section("G0b. etf-true-flows shape (why flows joined 0 rows)")
        try:
            ef = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/etf-true-flows.json")["Body"].read())
            rep.log(f"  top-level: {list(ef)[:16]}")
            for k, val in ef.items():
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    rep.log(f"  '{k}': list[{len(val)}], row keys={list(val[0])[:18]}")
                    rep.log(f"    sample: {json.dumps(val[0], default=str)[:300]}")
                    break
                if isinstance(val, dict) and val:
                    ik = list(val)[:6]
                    rep.log(f"  '{k}': dict[{len(val)}] first keys={ik}")
        except Exception as e:
            rep.warn(f"  etf-true-flows unreadable: {e}")

        # ── deploy ──
        rep.section("1. Deploy v1.1.0")
        env = {"S3_BUCKET": BUCKET}
        for d in DONORS:
            if "POLYGON_API_KEY" in env:
                break
            de = (lam.get_function_configuration(FunctionName=d)
                  .get("Environment", {}).get("Variables", {}))
            if "POLYGON_API_KEY" in de:
                env["POLYGON_API_KEY"] = de["POLYGON_API_KEY"]
                rep.ok(f"  POLYGON_API_KEY from {d}")
        if "POLYGON_API_KEY" not in env:
            rep.fail("no donor carries POLYGON_API_KEY")
            sys.exit(1)

        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC, env_vars=env,
            timeout=900, memory=1024,
            description="Cross-asset rotation dashboard: regime x ratios x trend gate x RS rank x flows x crowding",
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
        cfg = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})
        r = boto3.client("lambda", region_name="us-east-1", config=cfg).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {str(payload)[:600]}")
            sys.exit(1)
        rep.log(f"  {str(payload)[:260]}")

        # ── verify the degradation actually cleared ──
        rep.section("4. Verify — did degraded shrink?")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        deg = d.get("degraded") or []
        checks, fails = [], []

        def chk(name, cond, detail=""):
            (checks if cond else fails).append(name)
            (rep.ok if cond else rep.fail)(f"  {name} {detail}")

        dollar = d["layer1_regime"].get("dollar") or {}
        chk("dollar 3m populated", dollar.get("chg_3m_pct") is not None,
            f"= {dollar.get('chg_3m_pct')} ({dollar.get('direction')})")
        chk("dollar tilt applied into prior",
            d["layer1_regime"].get("dollar_tilt_applied") is not None,
            f"= {d['layer1_regime'].get('dollar_tilt_applied')}")
        chk("no 'dollar-radar 3m' in degraded",
            not any("dollar-radar 3m" in x for x in deg))

        crowded = [a for a in d["assets"] if a.get("crowding")]
        chk("crowding populated on >=4 assets", len(crowded) >= 4,
            f"= {len(crowded)} assets")
        chk("no 'cftc-all-cache unmapped' in degraded",
            not any("cftc-all-cache unmapped" in x for x in deg))
        flowed = [a for a in d["assets"] if a.get("flows")]
        chk("ETF flows joined on >=10 assets", len(flowed) >= 10,
            f"= {len(flowed)} assets")
        for a in flowed[:6]:
            fl = a["flows"]
            rep.log(f"    {a['ticker']:<5} {fl['state']:<8} "
                    f"{fl['horizon']} ${(fl['net_flow_usd'] or 0)/1e9:,.2f}B "
                    f"({fl.get('pct_of_aum')}% AUM)")
        chk("still scoring full universe", d["layer3_layer4"]["n_scored"] >= 35,
            f"= {d['layer3_layer4']['n_scored']}")
        chk("trend gate still discriminates",
            0 < d["layer3_layer4"]["n_eligible"] < d["layer3_layer4"]["n_scored"],
            f"= {d['layer3_layer4']['n_eligible']}/{d['layer3_layer4']['n_scored']}")

        for a in crowded[:8]:
            c = a["crowding"]
            rep.log(f"    {a['ticker']:<5} {c.get('contract'):<5} "
                    f"COT idx {c.get('cot_index')} -> {c.get('state')} "
                    f"(n={c.get('n_obs')})")
        rep.log("  ── top 8 after dollar tilt ──")
        for a in d["assets"][:8]:
            rep.log(f"    #{a['rank']:>2} {a['ticker']:<5} {a['confluence_score']:>7} "
                    f"{a['rrg']['quadrant'] or '-':<10} "
                    f"gate={'PASS' if a['trend_gate']['eligible'] else 'FAIL'}")

        rep.kv(regime=(d["layer1_regime"].get("quadrant") or {}).get("regime"),
               dxy_3m=dollar.get("chg_3m_pct"),
               dollar_direction=dollar.get("direction"),
               crowding_rows=len(crowded),
               overweight=", ".join(o["ticker"] for o in d["overweight"]),
               degraded="; ".join(deg) or "NONE")

        if deg:
            rep.warn(f"  remaining degraded: {deg}")
        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}")
            sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)}")


if __name__ == "__main__":
    main()
