"""
ops_3832 — master-ranker sector map: finish the rotation wire

ops 3831 left master-ranker at 1/25 tilted. Diagnosis: _ticker_sector only
walked contributing-system rows for a "sector" key and most lack one, while the
census join that could supply it runs ~104 lines AFTER the overlays. Since
census_idx() is globally cached it can be consulted at any point, so the map is
built up front and _ticker_sector falls back to it.

⚠ This does NOT only fix rotation — _roro_overlay, _liq_overlay,
_sectorflow_overlay and _nowcast_overlay ALL take the same _sector argument and
have therefore been no-ops on ~24/25 rows too. Expect several multipliers to
come alive at once; the ops reports each so the blast radius is visible rather
than silent.

The census matrix is ~215 columns and the sector column name is not guaranteed,
so the engine DISCOVERS it from candidates and this ops gates which one was used
plus the resulting coverage. Zip-settle by marker (ops 3830's lesson).
"""
import io as _io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402
from ops_report import report  # noqa: E402

FN = "justhodl-master-ranker"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
MARKER = "_harvest_sectors"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
CFG = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})


def main():
    with report("3832_ranker_sector_map") as rep:
        rep.heading("ops 3832 — census sector map -> finish rotation wire")

        rep.section("G0. Do the harvest source feeds carry sector?")
        found = {}
        for key in ("screener/data.json", "data/capital-flow-radar.json",
                    "data/deep-value.json", "data/accumulation-radar.json",
                    "data/asymmetric-scorer.json"):
            try:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            except Exception as e:
                rep.log(f"  {key}: unreadable ({str(e)[:50]})")
                continue
            m = {}

            def walk(o):
                if isinstance(o, dict):
                    t = o.get("ticker") or o.get("symbol") or o.get("t")
                    sec = o.get("sector") or o.get("sectorName")
                    if isinstance(t, str) and isinstance(sec, str) and t and sec:
                        m.setdefault(t.upper(), sec)
                    for v in o.values():
                        walk(v)
                elif isinstance(o, list):
                    for v in o:
                        walk(v)
            walk(doc)
            found.update(m)
            rep.log(f"  {key}: +{len(m)} pairs")
        if len(found) < 100:
            rep.fail(f"  harvest only yields {len(found)} pairs — insufficient")
            sys.exit(1)
        col = f"harvest({len(found)} pairs)"
        rep.ok(f"  harvested {len(found)} ticker->sector pairs")
        rep.log(f"  sectors seen: {sorted(set(found.values()))[:12]}")

        rep.section("1. Deploy")
        env = (lam.get_function_configuration(FunctionName=FN)
               .get("Environment", {}).get("Variables", {})) or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Master ranker — multi-system conviction with regime overlays",
                      create_function_url=False, smoke=False)

        rep.section("2. ZIP-SETTLE by marker (ops 3830's lesson)")
        for i in range(60):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                try:
                    url = lam.get_function(FunctionName=FN)["Code"]["Location"]
                    blob = urllib.request.urlopen(url, timeout=60).read()
                    with zipfile.ZipFile(_io.BytesIO(blob)) as zf:
                        src = zf.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARKER in src:
                        rep.ok(f"  settled with '{MARKER}' after {i*10}s")
                        break
                    rep.log("    artifact lacks marker — waiting")
                except Exception as e:
                    rep.log(f"    zip read retry ({str(e)[:50]})")
            time.sleep(10)
        else:
            rep.fail("marker never reached the artifact"); sys.exit(1)

        rep.section("3. Invoke")
        r = boto3.client("lambda", region_name="us-east-1", config=CFG).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {r['Payload'].read()[:500]}"); sys.exit(1)
        rep.ok("  invoked clean")

        rep.section("4. Verify — every sector-dependent overlay, not just rotation")
        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/master-ranker.json")["Body"].read())
        rows = d.get("top_tickers") or []
        checks, fails = [], []

        def chk(n, cond, det=""):
            (checks if cond else fails).append(n)
            (rep.ok if cond else rep.fail)(f"  {n} {det}")

        chk("rows present", len(rows) >= 20, f"= {len(rows)}")
        for field, label, floor in (("rotation_mult", "rotation", 8),
                                    ("risk_regime_mult", "roro", 0),
                                    ("nowcast_regime_mult", "nowcast", 0),
                                    ("liquidity_regime_mult", "liquidity", 0)):
            live = [x for x in rows if x.get(field) not in (None, 1.0)]
            rep.log(f"    {label:<10} active on {len(live)}/{len(rows)} rows")
            if floor:
                chk(f"{label} tilt active on >={floor} rows", len(live) >= floor,
                    f"= {len(live)}")
        rot = [x for x in rows if x.get("rotation_mult") not in (None, 1.0)]
        for x in rot[:8]:
            rep.log(f"    {x.get('ticker'):<6} x{x.get('rotation_mult')} "
                    f"{x.get('rotation_note')}")
        chk("rotation coverage beats the 1/25 baseline", len(rot) > 1,
            f"= {len(rot)}/{len(rows)} (was 1/25)")

        rep.kv(sector_col=col,
               rotation_tilted=f"{len(rot)}/{len(rows)}",
               harvest_pairs=len(found))

        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}"); sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)}")


if __name__ == "__main__":
    main()
