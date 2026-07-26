"""
ops_3935 — deploy v3.1 SOURCE-MEMORY: each LIVE row records its proven
resolver (resolved_via); cached-cadence refetches go STRAIGHT to that source
instead of re-walking the ladder — cutting cached-run FRED calls ~75% (the
395 in run2 were mostly failed 2nd-chance attempts on Yahoo/FMP rows). Dead
alias keys for the removed hyphen artifacts dropped. Gates via async+poll:
settle; run1 (no force, cache from 3933) populates resolved_via while cached
rows carry none yet — so gate run2: fred_calls < 170, n_live >= 440, ECB
trio still LIVE, zero UNRESOLVED, resolved_via present on >= 300 rows.
"""
import io, json, sys, time, urllib.request, zipfile
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.1 SOURCE-MEMORY"


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def run_async_and_wait(rep, label):
    t_mark = datetime.now(timezone.utc).isoformat()
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    for i in range(60):
        time.sleep(15)
        d = get_doc()
        if d.get("generated_at", "") > t_mark:
            rep.ok(f"  {label}: artifact refreshed ~{(i+1)*15}s "
                   f"(fred_calls {d.get('fred_calls_this_run')}, "
                   f"cached {d.get('n_cached_this_run')}, live {d.get('n_live')})")
            return d
    return None


def main():
    with report("3935_v31_source_memory") as rep:
        rep.heading("ops 3935 — v3.1 source-memory deploy")
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
        checks.append(("v3.1 settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        for _ in range(25):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8)

        d1 = run_async_and_wait(rep, "run1 (populate resolved_via)")
        checks.append(("run1 wrote", d1 is not None))
        if not d1: rep.fail("run1 never wrote"); sys.exit(1)
        d2 = run_async_and_wait(rep, "run2 (source-memory fast path)")
        checks.append(("run2 wrote", d2 is not None))
        if not d2: rep.fail("run2 never wrote"); sys.exit(1)

        n_rv = sum(1 for r in d2.get("symbols") or [] if r.get("resolved_via"))
        idx = {r["symbol"]: r for r in d2.get("symbols") or []}
        for s_ in ("EU03Y", "DE02Y", "EUCA"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} via={rw.get('resolved_via')}")
            checks.append((f"{s_} LIVE", rw.get("status") == "LIVE"))
        st = d2.get("status_counts") or {}
        rep.kv(run2_fred=d2.get("fred_calls_this_run"), run2_live=d2.get("n_live"),
               n_resolved_via=n_rv, statuses=str(st))
        checks.append(("run2 fred_calls < 170", (d2.get("fred_calls_this_run") or 999) < 170))
        checks.append(("n_live >= 440", (d2.get("n_live") or 0) >= 440))
        checks.append(("resolved_via on >= 300 rows", n_rv >= 300))
        checks.append(("zero bare UNRESOLVED", st.get("UNRESOLVED", 0) == 0))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v3.1: run2 fred_calls {d2.get('fred_calls_this_run')} "
               f"(was 395), {d2.get('n_live')} LIVE, source memory on {n_rv} rows")


if __name__ == "__main__":
    main()
