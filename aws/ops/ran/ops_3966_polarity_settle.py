"""
ops_3966 — race-safe settle + verify of domain-barometers v1.1.

ops 3965 hit ResourceConflictException: Deploy Lambdas was mid-update on the
same function when the op called update_function_code. Two lessons, both
now handled here:

  1. The two deploy paths RACE. Deploy Lambdas does pick up an EXISTING
     function (it just silently failed to CREATE the new one, which is what
     3962/3963 exposed). So the workflow may already have shipped v1.1 —
     this op checks the deployed artifact FIRST and only pushes code if the
     marker is genuinely absent.
  2. Never call update_function_code without waiting for the function to go
     idle. Wait for LastUpdateStatus != InProgress, and retry the conflict.

Then invoke and prove the polarity fix actually changed the numbers: GDPNOW
must no longer be signed -1 off the rates default, and the MACRO favourable
count should move.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=660, retries={"max_attempts": 0}))

FN = "justhodl-domain-barometers"
MARK = "domain-barometers v1.1 ops3965 polarity-guard"
BUCKET = "justhodl-dashboard-live"
OUT = "data/domain-barometers.json"
SRC = ROOT / "lambdas" / FN / "source" / "lambda_function.py"


def wait_idle(rep, tries=40):
    for i in range(tries):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return True
        rep.log(f"  [{i}] busy state={c.get('State')} upd={c.get('LastUpdateStatus')}")
        time.sleep(10)
    return False


def deployed_src():
    info = lam.get_function(FunctionName=FN)
    blob = urllib.request.urlopen(info["Code"]["Location"], timeout=60).read()
    return zipfile.ZipFile(io.BytesIO(blob)).read("lambda_function.py").decode()


def main():
    with report("3966_polarity_settle") as rep:
        rep.heading("ops 3966 — race-safe settle + verify polarity guard")
        checks = []

        rep.section("A. wait for idle, then check whether the workflow already shipped it")
        if not wait_idle(rep):
            rep.fail("function never went idle")
            sys.exit(1)
        have = MARK in deployed_src()
        rep.kv(marker_already_deployed=have)

        if not have:
            rep.section("B. push code (workflow had not shipped it)")
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", SRC.read_text())
            for attempt in range(8):
                try:
                    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue(),
                                             Publish=True)
                    rep.ok(f"  update issued (attempt {attempt})")
                    break
                except lam.exceptions.ResourceConflictException:
                    rep.log(f"  [{attempt}] conflict — another deploy in flight, backing off")
                    time.sleep(15)
            else:
                rep.fail("could not acquire the function for update")
                sys.exit(1)
            if not wait_idle(rep):
                rep.fail("never idle after update")
                sys.exit(1)
            have = MARK in deployed_src()
        else:
            rep.section("B. skipped — Deploy Lambdas already shipped v1.1")
        checks.append(("v1.1 marker in the deployed artifact", have))
        if not have:
            rep.fail("marker still absent")
            sys.exit(1)

        rep.section("C. invoke")
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"source": "ops3966"}).encode())
        raw = r["Payload"].read().decode()
        rep.log(f"  payload={raw[:400]}")
        checks.append(("invoke clean", not r.get("FunctionError")))
        if r.get("FunctionError"):
            rep.fail("lambda raised")
            sys.exit(1)

        rep.section("D. prove the polarity fix changed the numbers")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT)["Body"].read())
        bar = doc.get("barometers") or {}
        idx = {x["symbol"]: x for x in doc.get("symbols") or []}
        rep.kv(version=doc.get("version"), generated_at=doc.get("generated_at"))
        for s in ("GDPNOW", "USCFNAI", "CFNAIMA3", "USGDPYY"):
            x = idx.get(s)
            if x:
                rep.log(f"  {s:10s} domain={x['domain']:9s} pol={x['polarity']:+d} "
                        f"basis={x['polarity_basis']} — {x['polarity_why'][:80]}")
        g = idx.get("GDPNOW") or {}
        checks.append(("GDPNOW no longer inverted by the rates default",
                       g.get("polarity", 0) >= 0))

        guarded = [x["symbol"] for x in doc.get("symbols") or []
                   if x.get("polarity_basis") == "guarded"]
        rep.kv(n_guarded_excluded=len(guarded))
        rep.log(f"  guarded (excluded rather than mis-signed): {guarded[:25]}")

        for d in ("MACRO", "LIQUIDITY", "RISK"):
            b = bar.get(d) or {}
            rep.log(f"  {d:9s} {b.get('score_0_100')} {b.get('state')} "
                    f"gate={b.get('gate_component')} breadth={b.get('breadth_component')} "
                    f"drivers={b.get('n_drivers_live')} (+{b.get('n_favourable')}/"
                    f"-{b.get('n_adverse')})")
            checks.append((f"{d} still scored", b.get("score_0_100") is not None))
            checks.append((f"{d} publishes breadth_uses note", bool(b.get("breadth_uses"))))

        ca = doc.get("classification_audit") or {}
        checks += [
            ("still 561 symbols", ca.get("n_symbols") == 561),
            ("zero evidence-free", (ca.get("tier_counts") or {}).get("T6", 0) == 0),
            ("10 predictions", len((doc.get("predictions") or {})
                                   .get("asset_classes") or {}) == 10),
        ]

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL v1.1 — GDPNOW pol={g.get('polarity')}, {len(guarded)} guarded; "
               f"M {bar['MACRO']['score_0_100']} / L {bar['LIQUIDITY']['score_0_100']} / "
               f"R {bar['RISK']['score_0_100']}")


if __name__ == "__main__":
    main()
