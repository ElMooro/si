"""
ops_3846 — industry exposure attached to every port

Khalid: attach the industries most likely affected by each port, how, and by
what percentage.

The percentage is only defensible because import-canary publishes MEASURED
per-country shares of US imports per industry line (probe 3845 confirmed
shares_pct is a real dict, 16 industries, 10+ countries). exposure_pct =
port_yoy_pct x share_pct/100.

GATES THAT MATTER:
  • exposure_pct must RECONCILE to the arithmetic on a live row — if it does not
    reproduce port_yoy_pct x share_pct/100 exactly, the number is invented.
  • share_pct must come from the feed, so at least one port's top industry share
    must match import-canary's own shares_pct for that country.
  • the three limits must ship on every available row — an exposure number
    without them reads as a forecast, which is precisely what it is not.
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

FN = "justhodl-portwatch"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
KEY = "data/portwatch.json"
MARKER = "attach_industry_exposure"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
CFG = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})


def main():
    with report("3846_port_industry") as rep:
        rep.heading("ops 3846 — industry exposure per port")

        rep.section("G0. import-canary must publish real shares_pct")
        ic = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/import-canary.json")["Body"].read())
        lines = [l for l in (ic.get("lines") or [])
                 if isinstance((l.get("concentration") or {}).get("shares_pct"), dict)]
        if len(lines) < 5:
            rep.fail(f"  only {len(lines)} lines carry shares_pct"); sys.exit(1)
        rep.ok(f"  {len(lines)}/{len(ic.get('lines') or [])} lines carry shares_pct")
        before = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        base_fields = set((before.get("ports") or [{}])[0].keys())
        rep.ok(f"  baseline: {len(before.get('ports') or [])} ports, "
               f"{len(base_fields)} fields/row")

        rep.section("1. Deploy")
        env = (lam.get_function_configuration(FunctionName=FN)
               .get("Environment", {}).get("Variables", {})) or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=600, memory=1024,
                      description="PortWatch shipping monitor - throughput, chokepoints, per-port industry exposure",
                      create_function_url=False, smoke=False)

        rep.section("2. ZIP-SETTLE by marker")
        import io as _io, urllib.request as _u, zipfile as _z
        for i in range(60):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                try:
                    blob = _u.urlopen(lam.get_function(FunctionName=FN)["Code"]["Location"],
                                      timeout=60).read()
                    with _z.ZipFile(_io.BytesIO(blob)) as zf:
                        if MARKER in zf.read("lambda_function.py").decode("utf-8", "ignore"):
                            rep.ok(f"  settled after {i*10}s"); break
                except Exception as e:
                    rep.log(f"    zip retry ({str(e)[:50]})")
            time.sleep(10)
        else:
            rep.fail("marker never landed"); sys.exit(1)

        rep.section("3. Invoke")
        r = boto3.client("lambda", region_name="us-east-1", config=CFG).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {r['Payload'].read()[:500]}"); sys.exit(1)
        rep.ok("  invoked clean")

        rep.section("4. Verify the numbers are REAL, not invented")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        ports = d.get("ports") or []
        summ = d.get("industry_exposure_summary") or {}
        checks, fails = [], []

        def chk(n, cond, det=""):
            (checks if cond else fails).append(n)
            (rep.ok if cond else rep.fail)(f"  {n} {det}")

        chk("port count preserved", len(ports) >= len(before.get("ports") or []),
            f"{len(before.get('ports') or [])} -> {len(ports)}")
        now_fields = set(ports[0].keys()) if ports else set()
        chk("no pre-existing port field removed", not (base_fields - now_fields),
            f"lost={sorted(base_fields - now_fields)}")
        chk("summary published", bool(summ) and "error" not in summ,
            f"err={summ.get('error','')}")
        withx = [p for p in ports if (p.get("industry_exposure") or {}).get("available")]
        chk("exposure on >= 15 ports", len(withx) >= 15,
            f"= {len(withx)}/{len(ports)}")
        chk("industries covered >= 8",
            len(summ.get("industries_covered") or []) >= 8,
            f"= {len(summ.get('industries_covered') or [])}")

        # ── the arithmetic must reconcile ──
        recon_ok, recon_note = False, "no row to test"
        for p in withx:
            ie = p["industry_exposure"]
            y = ie.get("port_yoy_pct")
            for row in ie.get("industries") or []:
                if isinstance(y, (int, float)) and isinstance(row.get("share_pct"), (int, float)) \
                        and isinstance(row.get("exposure_pct"), (int, float)):
                    expect = round(y * row["share_pct"] / 100.0, 2)
                    recon_ok = abs(expect - row["exposure_pct"]) < 0.02
                    recon_note = (f"{p['name']} {row['industry']}: "
                                  f"{y} x {row['share_pct']}% = {expect} "
                                  f"vs published {row['exposure_pct']}")
                    break
            if recon_note != "no row to test":
                break
        chk("exposure_pct reconciles to the stated arithmetic", recon_ok, recon_note)

        # ── share must come from the feed, not be invented ──
        feed_shares = set()
        for l in lines:
            for cty, sh in (l["concentration"]["shares_pct"] or {}).items():
                if isinstance(sh, (int, float)):
                    feed_shares.add(round(float(sh), 2))
        pub_shares = {row["share_pct"] for p in withx
                      for row in (p["industry_exposure"].get("industries") or [])
                      if isinstance(row.get("share_pct"), (int, float))}
        chk("published shares all trace back to import-canary",
            pub_shares and pub_shares.issubset(feed_shares),
            f"{len(pub_shares)} distinct shares, all in feed="
            f"{bool(pub_shares and pub_shares.issubset(feed_shares))}")

        chk("limits ship on every available row",
            all("NOT A FORECAST" in (p["industry_exposure"].get("limits") or "")
                for p in withx))

        rep.log("  ── sample: worst-hit ports and what they touch ──")
        for p in sorted(withx, key=lambda x: x.get("yoy_pct") or 0)[:5]:
            ie = p["industry_exposure"]
            rep.log(f"    {p['name']} ({p['country']}) yoy={p.get('yoy_pct')}%")
            for row in (ie.get("industries") or [])[:3]:
                rep.log(f"       {row['industry']:<24} share {row['share_pct']:>5}% "
                        f"-> exposure {row['exposure_pct']}%  [{row['line']}]")

        rep.kv(ports=len(ports), ports_with_exposure=len(withx),
               countries=summ.get("countries_with_composition"),
               industries=len(summ.get("industries_covered") or []))
        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}"); sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)}")


if __name__ == "__main__":
    main()
