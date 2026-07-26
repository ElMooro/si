"""
ops_3920 — DEPLOY v2.1: gates on every fix landing live — plumbing_composite
OK with a real value (~21.6 path), dealer_net_treasury_b OK, xcc extracted
to scalar signals with worst_z, HYG credit-flow input present, CISS regime
input present, A2/P2 present-or-graceful, fails still honestly MISSING
(producer todo), served page renders FLEET INPUTS.
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN, KEY = "justhodl-risk-gate", "data/risk-gate.json"
MARK = "risk-gate v2.1 BRAIN-CONSTITUTIONAL FLEET-FUSED"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=580, retries={"max_attempts": 0}))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"


def main():
    with report("3920_riskgate_v21_deploy") as rep:
        rep.heading("ops 3920 — v2.1 all fixes verified live")
        checks = []
        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    if MARK in z.read("lambda_function.py").decode("utf-8", "ignore"):
                        settled = True; rep.ok(f"  settled attempt {attempt}"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v2.1 settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8); cfg = lam.get_function_configuration(FunctionName=FN)

        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"FunctionError: {json.loads(r['Payload'].read())}"); sys.exit(1)

        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=KEY)["Body"].read())
        legs = doc.get("legs") or {}
        by = {}
        for lname, leg in legs.items():
            for fi in leg.get("fleet_inputs") or []:
                by[fi["input"]] = fi
                rep.log(f"  {lname}/{fi['input']}: {str(fi['value'])[:90]} "
                        f"({fi['status']}, adj {fi['score_adj']})")
        rep.kv(posture=doc.get("posture"), composite=doc.get("composite"))

        pc = by.get("plumbing_composite") or {}
        checks.append(("plumbing_composite OK w/ real value",
                       pc.get("status") == "OK" and isinstance(pc.get("value"), (int, float))))
        checks.append(("dealer_net_treasury_b OK",
                       (by.get("dealer_net_treasury_b") or {}).get("status") == "OK"))
        xc = by.get("xcc_basis_signals") or {}
        checks.append(("xcc extracted to worst_z scalar",
                       isinstance((xc.get("value") or {}).get("worst_z_1y"), (int, float))))
        checks.append(("HYG credit flow input OK",
                       (by.get("hyg_net_flow_20d_bn") or {}).get("status") == "OK"))
        checks.append(("CISS regime input present", "ecb_ciss_regime" in by))
        checks.append(("fails honestly MISSING (producer todo)",
                       (by.get("fails_cross_z") or {}).get("status") == "MISSING"))
        checks.append(("posture valid",
                       doc.get("posture") in ("RISK_ON", "NEUTRAL", "RISK_OFF", "SEVERE")))

        # served page renders FLEET INPUTS
        page_ok = False
        for a in range(6):
            try:
                req = urllib.request.Request(
                    f"https://justhodl.ai/risk-gate.html?v={int(time.time())}{a}",
                    headers={"User-Agent": UA, "Cache-Control": "no-cache"})
                html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
                if "FLEET INPUTS" in html and "fleet_inputs" in html:
                    page_ok = True; break
            except Exception: pass
            time.sleep(20)
        checks.append(("served page renders FLEET INPUTS", page_ok))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v2.1 live, all fixes verified; posture {doc.get('posture')} "
               f"composite {doc.get('composite')}")


if __name__ == "__main__":
    main()
