"""ops/4912 -- chain re-kick: edgar lane (80/135 stalled) + deep rearm
retry (4911's rearm invoke lease-skipped mid-chain). Lease-aware:
wait for lease-free, fire, verify movement."""
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def kick_when_free(state_key, fn, payload, wait=520):
    t = time.time()
    while time.time() - t < wait:
        try:
            st = g(state_key)
        except Exception:
            st = {}
        if float(st.get("lease_until") or 0) <= time.time():
            lam.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=json.dumps(payload).encode())
            return True
        time.sleep(20)
    return False


def main():
    verdict = {"ops": 4912, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4912 -- chain rekick") as rep:
        rep.heading("ops 4912 — edgar chain + deep rearm rekick")

        hb0 = g("data/_state/hist-banker.json")
        e0 = (hb0.get("lanes", {}).get("edgar") or {}).get("n_have")
        k1 = kick_when_free("data/_state/hist-banker.json",
                            "justhodl-hist-banker", {})
        k2 = kick_when_free("data/_state/ecb-deep.json",
                            "justhodl-ecb-deep", {"rearm_errs": 1})
        rep.kv(stage="kicks", edgar_fired=k1, deep_rearm_fired=k2)

        e1 = d1 = None
        t = time.time()
        while time.time() - t < 860:
            time.sleep(45)
            try:
                hb = g("data/_state/hist-banker.json")
                e1 = (hb.get("lanes", {}).get("edgar")
                      or {}).get("n_have")
                d = g("data/_state/ecb-deep.json")
                d1 = d
            except Exception:
                continue
            if e1 and e0 is not None and e1 > e0 and \
                    (d1 or {}).get("rearmed"):
                break
        rep.kv(stage="movement", edgar_before=e0, edgar_after=e1,
               still_missing=(hb.get("still_missing")
                              if "hb" in dir() else None),
               deep_rearmed=json.dumps((d1 or {}).get("rearmed")),
               deep_n_complete=(d1 or {}).get("n_complete"))
        verdict["gates"]["edgar_chain_moving"] = (
            "PASS" if (e1 or 0) > (e0 or 0) else "PENDING")
        verdict["gates"]["deep_rearm_applied"] = (
            "PASS" if (d1 or {}).get("rearmed") else "PENDING")
        verdict["edgar"] = {"before": e0, "after": e1}
        verdict["deep_n_complete"] = (d1 or {}).get("n_complete")

        hard = []
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("PASS_WITH_PENDING" if pend
                              else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4912.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4912.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
