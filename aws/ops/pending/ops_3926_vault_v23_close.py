"""ops_3926 — v2.3: alias the REAL registry keys (ES10Y-TVC/FR10Y-TVC — the
-TVC suffix is baked into the brain tags) to the fleetsum euro yields; BDI/
EUGDPYY documented as producer-export todos. Gates: both LIVE via fleetsum,
LIVE >= 404, zero UNRESOLVED."""
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v2.3 SUFFIX-KEYS"


def main():
    with report("3926_vault_v23_close") as rep:
        rep.heading("ops 3926 — v2.3 suffix-key close")
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
        checks.append(("settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        for _ in range(25):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"FunctionError: {json.loads(r['Payload'].read())}"); sys.exit(1)
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/tradingview.json")["Body"].read())
        by = {}
        for row in doc.get("symbols") or []:
            by[row.get("status")] = by.get(row.get("status"), 0) + 1
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"), statuses=str(by))
        idx = {row["symbol"]: row for row in doc.get("symbols") or []}
        for s_ in ("ES10Y-TVC", "FR10Y-TVC"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
            checks.append((f"{s_} LIVE via fleetsum",
                           rw.get("status") == "LIVE" and "fleetsum" in str(rw.get("source"))))
        checks.append(("LIVE >= 404", (doc.get("n_live") or 0) >= 404))
        checks.append(("zero bare UNRESOLVED", by.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v2.3: {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%)")


if __name__ == "__main__":
    main()
