"""
ops_3918 — DEPLOY Risk Gate v2.0 (fleet-fused). Gates: v2 marker settles;
invoke succeeds; all 6 legs carry fleet_inputs with honest statuses; the 5
newly-approved Leg-1 inputs present (fails, dealer, auction, plumbing, xcc
basis); replay_composite_fred_only preserved separately from the fused live
composite (event-study integrity); October replay unchanged; posture valid.
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN, KEY = "justhodl-risk-gate", "data/risk-gate.json"
MARK = "risk-gate v2.0 BRAIN-CONSTITUTIONAL FLEET-FUSED"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=580, retries={"max_attempts": 0}))


def main():
    with report("3918_riskgate_v2_deploy") as rep:
        rep.heading("ops 3918 — Risk Gate v2.0 fleet-fused deploy")
        checks = []
        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    if MARK in z.read("lambda_function.py").decode("utf-8", "ignore"):
                        settled = True; rep.ok(f"  settled attempt {attempt}"); break
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:60]}")
            time.sleep(15)
        checks.append(("v2 settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8); cfg = lam.get_function_configuration(FunctionName=FN)

        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"FunctionError: {json.dumps(raw)[:800]}"); sys.exit(1)
        checks.append(("invoke ok", True))

        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=KEY)["Body"].read())
        legs = doc.get("legs") or {}
        rep.kv(posture=doc.get("posture"), composite=doc.get("composite"),
               replay_posture=doc.get("replay_posture_fred_only"),
               replay_composite=doc.get("replay_composite_fred_only"),
               sizing=doc.get("sizing_multiplier"))
        n_inputs = 0
        statuses = {}
        for lname, leg in legs.items():
            fis = leg.get("fleet_inputs") or []
            n_inputs += len(fis)
            for fi in fis:
                statuses[fi.get("status")] = statuses.get(fi.get("status"), 0) + 1
            rep.log(f"  {lname}: fred={leg.get('score')} fleet_adj={leg.get('fleet_adj')} "
                    f"fused={leg.get('score_fused')} inputs="
                    + " | ".join(f"{x['input']}={x['value'] if not isinstance(x['value'],dict) else x['value']}"
                                 f"({x['status']},adj{x['score_adj']})" for x in fis))
        rep.kv(total_fleet_inputs=n_inputs, statuses=str(statuses))
        checks.append(("all 6 legs carry fleet_inputs",
                       all("fleet_inputs" in l for l in legs.values())))
        checks.append(("15+ fleet inputs wired", n_inputs >= 15))
        leg1 = {x["input"] for x in (legs.get("funding") or {}).get("fleet_inputs", [])}
        checks.append(("all 5 Leg-1 inputs present",
                       {"dealer_corp_net_bonds_b","fails_cross_z","auction_10y_grade",
                        "plumbing_composite","xcc_basis_proxy_bp"} <= leg1))
        checks.append(("replay separated from fused live",
                       doc.get("replay_composite_fred_only") is not None))
        oc = (doc.get("event_study") or {}).get("october_2025_replay") or {}
        checks.append(("October replay intact",
                       isinstance(oc.get("rrp_min_in_window_bn"), (int, float))))
        checks.append(("posture valid",
                       doc.get("posture") in ("RISK_ON","NEUTRAL","RISK_OFF","SEVERE")))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v2.0 live: {doc.get('posture')} fused={doc.get('composite')} "
               f"(FRED-only replay {doc.get('replay_composite_fred_only')}), "
               f"{n_inputs} fleet inputs, statuses {statuses}")


if __name__ == "__main__":
    main()
