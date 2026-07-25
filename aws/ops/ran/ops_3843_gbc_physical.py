"""
ops_3843 — physical confirmation INTO global-business-cycle (source-level fix)

global-business-cycle classifies 34 economies from an EQUITY-MOMENTUM composite.
That leads, but its documented failure mode is firing in drawdowns that never
become recessions — and downstream justhodl-global-recession found 84.3% of its
GDP-weighted headline resting on exactly those unconfirmed labels (ops 3828).

ops 3839 fixed that inside global-recession by joining portwatch itself. This
moves the check UPSTREAM to the source, so every consumer of by_country inherits
it instead of each re-deriving the same join.

STRICTLY ADDITIVE — `phase` is never modified. A new per-country `physical`
block plus a top-level `physical_confirmation` summary. Existing consumers
(global-recession, canary feeds, pages) are unaffected by construction, and the
ops gates that the pre-existing contract still holds.
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

FN = "justhodl-global-business-cycle"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
KEY = "data/global-business-cycle.json"
MARKER = "load_port_physical"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
CFG = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})


def main():
    with report("3843_gbc_physical") as rep:
        rep.heading("ops 3843 — port confirmation into global-business-cycle")

        rep.section("G0. Baseline contract BEFORE the change")
        before = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        bc0 = before.get("by_country") or {}
        base_keys = set(next(iter(bc0.values())).keys()) if bc0 else set()
        rep.ok(f"  {len(bc0)} countries · {len(base_keys)} fields/row")
        pw = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/portwatch.json")["Body"].read())
        n_yoy = sum(1 for p in (pw.get("ports") or [])
                    if isinstance(p, dict) and isinstance(p.get("yoy_pct"), (int, float)))
        if n_yoy < 30:
            rep.fail(f"  portwatch only has {n_yoy} ports with yoy_pct"); sys.exit(1)
        rep.ok(f"  portwatch: {n_yoy} ports carry yoy_pct")

        rep.section("1. Deploy")
        env = (lam.get_function_configuration(FunctionName=FN)
               .get("Environment", {}).get("Variables", {})) or {}
        deploy_lambda(report=rep, function_name=FN, source_dir=SRC, env_vars=env,
                      timeout=900, memory=1536,
                      description="Global business cycle - 34 economies, synthetic CLI + physical port confirmation",
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
                            rep.ok(f"  settled with '{MARKER}' after {i*10}s")
                            break
                except Exception as e:
                    rep.log(f"    zip retry ({str(e)[:50]})")
            time.sleep(10)
        else:
            rep.fail("marker never reached the artifact"); sys.exit(1)

        rep.section("3. Invoke")
        r = boto3.client("lambda", region_name="us-east-1", config=CFG).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {r['Payload'].read()[:500]}"); sys.exit(1)
        rep.ok("  invoked clean")

        rep.section("4. Verify — additive, and the confirmation is real")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        bc = d.get("by_country") or {}
        checks, fails = [], []

        def chk(n, cond, det=""):
            (checks if cond else fails).append(n)
            (rep.ok if cond else rep.fail)(f"  {n} {det}")

        chk("country count preserved", len(bc) >= len(bc0),
            f"{len(bc0)} -> {len(bc)}")
        now_keys = set(next(iter(bc.values())).keys()) if bc else set()
        lost = base_keys - now_keys
        chk("NO pre-existing field removed (additive contract)", not lost,
            f"lost={sorted(lost)}")
        chk("physical block on every country",
            all("physical" in r_ for r_ in bc.values() if isinstance(r_, dict)))

        summ = d.get("physical_confirmation") or {}
        chk("summary published", bool(summ) and "error" not in summ,
            f"err={summ.get('error','')}")
        cnts = summ.get("counts") or {}
        chk("countries with port coverage >= 8",
            (summ.get("countries_with_ports") or 0) >= 8,
            f"= {summ.get('countries_with_ports')}")
        chk("confirmation states populated", sum(cnts.values()) == len(bc),
            f"= {cnts}")
        chk("at least one CONFIRMED or DIVERGENT",
            (cnts.get("CONFIRMED", 0) + cnts.get("DIVERGENT", 0)) >= 5,
            f"conf={cnts.get('CONFIRMED')} div={cnts.get('DIVERGENT')}")
        chk("limits disclosed (trade proxy caveat)",
            "under-read" in (summ.get("limits") or ""))

        for iso in ("CHN", "USA", "DEU", "JPN", "IND"):
            r_ = bc.get(iso) or {}
            ph = r_.get("physical") or {}
            rep.log(f"    {iso}: phase={r_.get('phase'):<10} "
                    f"{ph.get('state'):<12} median_yoy="
                    f"{ph.get('median_yoy_pct')} n_ports={ph.get('n_ports')}")
        if summ.get("unmapped_port_countries"):
            rep.warn(f"  unmapped (visible, not dropped): "
                     f"{summ['unmapped_port_countries']}")

        rep.kv(countries=len(bc), ports_countries=summ.get("countries_with_ports"),
               **{k.lower(): v for k, v in cnts.items()})
        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}"); sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)}")


if __name__ == "__main__":
    main()
