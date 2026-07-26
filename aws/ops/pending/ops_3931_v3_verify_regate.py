"""
ops_3931 — v3.0 verification via the ESTABLISHED long-engine pattern (3928's
sync invoke hit ConnectionClosedError on the throttled cold run — the Lambda
kept running). (1) Read the artifact: if generated_at is fresh and api_calls
present, RUN 1 completed. (2) Event-invoke RUN 2, poll S3 LastModified until
newer, gate the cadence properties + conversion spot-checks from artifacts.
"""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
KEY = "data/tradingview.json"


def head():
    return s3.head_object(Bucket="justhodl-dashboard-live", Key=KEY)["LastModified"]


def read():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=KEY)["Body"].read())


def main():
    with report("3931_v3_verify_regate") as rep:
        rep.heading("ops 3929 — v3.0 async verify (Event-invoke + freshness)")
        checks = []
        lm1 = head()
        d1 = read()
        age_min = (datetime.now(timezone.utc) - lm1).total_seconds() / 60
        rep.kv(run1_artifact_age_min=round(age_min, 1),
               marker=d1.get("marker"), n_live_1=d1.get("n_live"),
               coverage_1=d1.get("coverage_pct"),
               fred_calls_1=(d1.get("api_calls") or {}).get("fred"),
               cached_1=d1.get("n_cached"))
        run1_ok = d1.get("marker") == "tradingview-vault v3.0 CADENCE-AWARE" and age_min < 45
        checks.append(("RUN 1 completed despite closed connection", run1_ok))
        if not run1_ok:
            rep.fail("run1 artifact not fresh v3.0 — engine may have failed; aborting")
            sys.exit(1)
        idx = {r_["symbol"]: r_ for r_ in d1.get("symbols") or []}
        for s_ in ("EUINTR", "US03Y", "GB10Y", "USCLI", "10USNOTE", "NOVO_B"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
            checks.append((f"{s_} LIVE", rw.get("status") == "LIVE"))
        by = {}
        for r_ in d1.get("symbols") or []:
            by[r_.get("status")] = by.get(r_.get("status"), 0) + 1
        rep.kv(statuses=str(by))
        checks.append(("n_live >= 450", (d1.get("n_live") or 0) >= 450))
        checks.append(("zero UNRESOLVED", by.get("UNRESOLVED", 0) == 0))

        rep.section("RUN 2 — Event-invoke, poll freshness, prove the cache")
        lam.invoke(FunctionName="justhodl-tradingview", InvocationType="Event", Payload=b"{}")
        newer = False
        for _ in range(40):
            time.sleep(15)
            if head() > lm1:
                newer = True; break
        checks.append(("run2 artifact refreshed", newer))
        if newer:
            d2 = read()
            rep.kv(n_live_2=d2.get("n_live"),
                   fred_calls_2=(d2.get("api_calls") or {}).get("fred"),
                   cached_2=d2.get("n_cached"))
            checks.append(("run2 fred calls < 60 (cadence working)",
                           ((d2.get("api_calls") or {}).get("fred") or 999) < 60))
            checks.append(("run2 cached > 300", (d2.get("n_cached") or 0) > 300))
            checks.append(("run2 n_live within tolerance of run1",
                           abs((d2.get("n_live") or 0) - (d1.get("n_live") or 0))
                           <= max(6, int((d1.get("n_live") or 0) * 0.015))))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v3.0 proven: {d1.get('n_live')} LIVE ({d1.get('coverage_pct')}%), "
               f"cadence cache verified on run 2")


if __name__ == "__main__":
    main()
