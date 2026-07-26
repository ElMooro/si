"""
ops_3928 — deploy vault v3.0 CADENCE-AWARE and prove BOTH properties:
RUN 1 (cold, cache from v2.3): converts the 429-victims + new aliases —
gates: n_live >= 450; EUINTR/US03Y/GB10Y LIVE (throttle fix proof);
USCLI LIVE from the fleet CLI family; 10USNOTE (ZN=F) LIVE; NOVO_B LIVE;
zero UNRESOLVED. RUN 2 (immediate re-invoke): proves the cadence machinery —
n_cached large, fred api_calls SMALL (<60), n_live within 1% of run 1.
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=880, retries={"max_attempts": 0}))
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.0 CADENCE-AWARE"


def invoke_and_read():
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    if r.get("FunctionError"):
        raise RuntimeError(json.dumps(json.loads(r["Payload"].read()))[:800])
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3928_vault_v3_cadence") as rep:
        rep.heading("ops 3928 — v3.0 cadence-aware: conversion run + cache-proof run")
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
        checks.append(("v3.0 settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        cfg = None
        for _ in range(25):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8)
        checks.append(("POLYGON_KEY inherited",
                       "POLYGON_KEY" in ((cfg.get("Environment") or {}).get("Variables") or {})))

        rep.section("RUN 1 — cold conversion")
        try:
            d1 = invoke_and_read()
        except RuntimeError as e:
            rep.fail(f"run1 error: {e}"); sys.exit(1)
        rep.kv(n_live_1=d1.get("n_live"), coverage_1=d1.get("coverage_pct"),
               fred_calls_1=(d1.get("api_calls") or {}).get("fred"),
               cached_1=d1.get("n_cached"))
        idx = {r_["symbol"]: r_ for r_ in d1.get("symbols") or []}
        for s_ in ("EUINTR", "US03Y", "GB10Y", "USCLI", "10USNOTE", "NOVO_B"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
            checks.append((f"{s_} LIVE", rw.get("status") == "LIVE"))
        by = {}
        for r_ in d1.get("symbols") or []:
            by[r_.get("status")] = by.get(r_.get("status"), 0) + 1
        rep.kv(statuses_1=str(by))
        checks.append(("n_live >= 450", (d1.get("n_live") or 0) >= 450))
        checks.append(("zero UNRESOLVED", by.get("UNRESOLVED", 0) == 0))

        rep.section("RUN 2 — immediate re-invoke: cadence cache must carry")
        try:
            d2 = invoke_and_read()
        except RuntimeError as e:
            rep.fail(f"run2 error: {e}"); sys.exit(1)
        rep.kv(n_live_2=d2.get("n_live"), fred_calls_2=(d2.get("api_calls") or {}).get("fred"),
               cached_2=d2.get("n_cached"))
        checks.append(("run2 fred calls < 60 (cadence working)",
                       ((d2.get("api_calls") or {}).get("fred") or 999) < 60))
        checks.append(("run2 cached > 300", (d2.get("n_cached") or 0) > 300))
        checks.append(("run2 n_live within 1% of run1",
                       abs((d2.get("n_live") or 0) - (d1.get("n_live") or 0))
                       <= max(6, int((d1.get("n_live") or 0) * 0.01))))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v3.0: {d1.get('n_live')} LIVE ({d1.get('coverage_pct')}%), "
               f"run2 cached {d2.get('n_cached')} w/ {(d2.get('api_calls') or {}).get('fred')} FRED calls")


if __name__ == "__main__":
    main()
