"""
ops_3933 — v3.0 superset verify, ASYNC pattern (3932's sync force-invoke hit
ConnectionClosedError — same wall the parallel session solved in 3929/3931):
invoke Event {"force":true}, poll data/tradingview.json generated_at until it
moves, then gate; second SYNC invoke is cache-fast and proves the cadence
machinery. Also ensures POLYGON_KEY (donor merge + settle).
"""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
FN = "justhodl-tradingview"


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def wait_env():
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return c
        time.sleep(8)
    return lam.get_function_configuration(FunctionName=FN)


def main():
    with report("3933_v3_async_gate") as rep:
        rep.heading("ops 3933 — v3.0 async full-run gate")
        checks = []
        cfg = wait_env()
        env = dict((cfg.get("Environment") or {}).get("Variables") or {})
        if "POLYGON_KEY" not in env:
            donor = lam.get_function_configuration(FunctionName="justhodl-equity-research")
            dk = ((donor.get("Environment") or {}).get("Variables") or {}).get("POLYGON_KEY")
            if dk:
                env["POLYGON_KEY"] = dk
                lam.update_function_configuration(FunctionName=FN,
                                                 Environment={"Variables": env})
                wait_env()
                rep.ok("  POLYGON_KEY merged + settled")
        checks.append(("POLYGON_KEY present", "POLYGON_KEY" in env))

        before = (get_doc() or {}).get("generated_at", "")
        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"force": True}).encode())
        rep.log(f"  async force fired at {t_mark}; polling artifact…")
        doc = None
        for i in range(60):
            time.sleep(15)
            d = get_doc()
            if d.get("generated_at", "") > t_mark:
                doc = d; rep.ok(f"  artifact refreshed after ~{(i+1)*15}s"); break
        checks.append(("force run wrote fresh artifact", doc is not None))
        if not doc:
            rep.fail("artifact never refreshed"); sys.exit(1)
        st = doc.get("status_counts") or {}
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st), fred_calls=doc.get("fred_calls_this_run"),
               elapsed_s=doc.get("elapsed_s"))
        checks.append(("LIVE >= 450", (doc.get("n_live") or 0) >= 450))
        checks.append(("zero bare UNRESOLVED", st.get("UNRESOLVED", 0) == 0))
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}
        for s_, want in (("USCLI", "fleet"), ("EUINTR", "fred_alias"),
                         ("10USNOTE", "yahoo:ZN=F"), ("NOVO_B", "yahoo:NOVO-B.CO"),
                         ("EU02Y-TVC", "ecb:")):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
            checks.append((f"{s_} LIVE via {want}",
                           rw.get("status") == "LIVE" and want in str(rw.get("source"))))

        rep.section("cached run (sync, fast)")
        r2 = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw2 = json.loads(r2["Payload"].read())
        rep.log(f"  invoke2: {json.dumps(raw2, default=str)[:280]}")
        checks.append(("cache engaged (n_cached >= 100)", (raw2.get("n_cached") or 0) >= 100))
        checks.append(("fred calls small on cached run", (raw2.get("fred_calls") or 999) < 80))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v3.0 superset verified: {doc.get('n_live')} LIVE "
               f"({doc.get('coverage_pct')}%), fred_calls {doc.get('fred_calls_this_run')} "
               f"-> {raw2.get('fred_calls')} cached")


if __name__ == "__main__":
    main()
