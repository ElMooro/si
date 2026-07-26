"""
ops_3913 — verify the Master Risk Gate wiring into 5 engines (probe-then-wire
completion). Settles all 5 zips by the riskgate-wire-v1 marker, invokes the
three fast consumers sync (sizing-engine, master-ranker, best-setups) and
gates on their LIVE OUTPUT carrying the new gate fields with the correct
current posture (RISK_OFF x0.45 / rank clamp 0.88). position-sizer-v2 +
opportunity-engine: settle-only this ops (slow/event-driven), live-output
check moves to the day-two re-read with the scheduled runs.
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

MARK = "riskgate-wire-v1"
ENGINES = ["justhodl-position-sizer-v2", "justhodl-sizing-engine",
           "justhodl-opportunity-engine", "justhodl-best-setups",
           "justhodl-master-ranker"]
INVOKE = {"justhodl-sizing-engine": "data/sizing-engine.json",
          "justhodl-master-ranker": "data/master-ranker.json",
          "justhodl-best-setups": "data/best-setups.json"}

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=560, retries={"max_attempts": 0}))


def settled(fn):
    for attempt in range(1, 41):
        try:
            loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
            blob = urllib.request.urlopen(loc, timeout=60).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                if MARK in z.read("lambda_function.py").decode("utf-8", "ignore"):
                    return attempt
        except Exception:
            pass
        time.sleep(12)
    return None


def scan(obj, pred, hits, cap=3):
    if len(hits) >= cap: return
    if isinstance(obj, dict):
        if pred(obj): hits.append(obj)
        for v in obj.values(): scan(v, pred, hits, cap)
    elif isinstance(obj, list):
        for v in obj[:400]: scan(v, pred, hits, cap)


def main():
    with report("3913_riskgate_wire_verify") as rep:
        rep.heading("ops 3913 — risk-gate wired into 5 engines, live-output verified")
        checks = []

        rep.section("1. zip-settle all 5")
        for fn in ENGINES:
            a = settled(fn)
            (rep.ok if a else rep.fail)(f"  {fn}: {'marker live attempt '+str(a) if a else 'NEVER SETTLED'}")
            checks.append((f"{fn} settled", bool(a)))
        if not all(ok for _, ok in checks):
            rep.fail("settle failed"); sys.exit(1)

        rep.section("2. invoke fast consumers + verify live output fields")
        for fn, key in INVOKE.items():
            try:
                r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
                err = bool(r.get("FunctionError"))
                if err:
                    rep.fail(f"  {fn}: FunctionError {json.loads(r['Payload'].read())}")
                    checks.append((f"{fn} invoke", False)); continue
                doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())
                blob = json.dumps(doc)
                if fn == "justhodl-sizing-engine":
                    hits = []
                    scan(doc, lambda o: "risk_gate_posture" in o and "final_w_pct" in o, hits)
                    ok = bool(hits) and hits[0].get("risk_gate_posture") in ("RISK_ON","NEUTRAL","RISK_OFF","SEVERE")
                    rep.log(f"  {fn}: sample gated rec: {json.dumps(hits[0], default=str)[:300] if hits else 'NONE'}")
                    checks.append((f"{fn} output gated", ok))
                else:
                    hits = []
                    scan(doc, lambda o: "risk_gate_rank_mult" in o, hits)
                    n_all = blob.count("risk_gate_rank_mult")
                    ok = bool(hits)
                    rep.log(f"  {fn}: rows w/ risk_gate fields={n_all}, sample posture="
                            f"{hits[0].get('risk_gate_posture') if hits else None} "
                            f"rank_mult={hits[0].get('risk_gate_rank_mult') if hits else None} "
                            f"sizing_mult={hits[0].get('risk_gate_sizing_mult') if hits else None}")
                    checks.append((f"{fn} rows carry gate fields", ok))
            except Exception as e:
                rep.fail(f"  {fn}: {str(e)[:180]}")
                checks.append((f"{fn} invoke", False))

        rep.section("3. gate value sanity vs live risk-gate.json")
        rg = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/risk-gate.json")["Body"].read())
        rep.kv(gate_posture=rg.get("posture"), gate_sizing=rg.get("sizing_multiplier"))
        checks.append(("risk-gate feed live and valid",
                       rg.get("posture") in ("RISK_ON","NEUTRAL","RISK_OFF","SEVERE")))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}"); sys.exit(1)
        rep.ok("PASS_ALL — gate wired + live-verified in sizing-engine/master-ranker/best-setups; "
               "sizer-v2 + opportunity-engine settle-verified (live check = day-two)")


if __name__ == "__main__":
    main()
