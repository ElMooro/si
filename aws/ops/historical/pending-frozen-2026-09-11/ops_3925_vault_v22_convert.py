"""
ops_3925 — deploy vault v2.2 (isalnum second-chance FRED — the guard that
blocked every alphanumeric FRED id; plain-Yahoo rung; fleetsum euro yields;
UNTAGGED->META) + probe BDI/EUGDPYY paths in eurodollar-plumbing and
macro-nowcast feeds for the next wire. Gates: LIVE > 405; DCPN3M +
BAMLC4A0C710YEY + RIFSPPNA2P2D90NB live via fred_2nd_chance; UVXY live;
ES10Y/FR10Y live via fleetsum; UNTAGGED META; zero UNRESOLVED.
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v2.2 ALNUM-YAHOO-FLEETSUM"


def find_paths(obj, name, path="", hits=None, cap=5):
    if hits is None: hits = []
    if len(hits) >= cap: return hits
    if isinstance(obj, dict):
        for k, v in obj.items():
            if name.lower() in str(k).lower() and not isinstance(v, (dict, list)):
                hits.append(f"{path}.{k}={str(v)[:50]}")
            find_paths(v, name, f"{path}.{k}", hits, cap)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:10]):
            find_paths(v, name, f"{path}[{i}]", hits, cap)
    return hits


def main():
    with report("3925_vault_v22_convert") as rep:
        rep.heading("ops 3925 — v2.2 conversion pass + BDI/EUGDPYY path probe")
        checks = []
        rep.section("probe: BDI + EUGDPYY paths for the next wire")
        for key, term in (("data/eurodollar-plumbing.json", "bdi"),
                          ("data/eurodollar-plumbing.json", "baltic"),
                          ("data/macro-nowcast.json", "eu"),
                          ("data/macro-nowcast.json", "gdp")):
            try:
                d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())
                rep.log(f"  {key} :: {term}: " + (" | ".join(find_paths(d, term)) or "none"))
            except Exception as e:
                rep.log(f"  {key}: {str(e)[:80]}")

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
        checks.append(("v2.2 settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        for _ in range(25):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"FunctionError: {json.dumps(raw)[:900]}"); sys.exit(1)
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/tradingview.json")["Body"].read())
        by_status = {}
        for row in doc.get("symbols") or []:
            by_status[row.get("status")] = by_status.get(row.get("status"), 0) + 1
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(by_status))
        idx = {row["symbol"]: row for row in doc.get("symbols") or []}
        for s_, want_src in (("DCPN3M", "fred_2nd_chance"), ("BAMLC4A0C710YEY", "fred_2nd_chance"),
                             ("RIFSPPNA2P2D90NB", "fred_2nd_chance"), ("UVXY", "yahoo"),
                             ("ES10Y", "fleetsum"), ("FR10Y", "fleetsum")):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
            checks.append((f"{s_} LIVE", rw.get("status") == "LIVE"))
        checks.append(("UNTAGGED = META", (idx.get("UNTAGGED") or {}).get("status") == "META"))
        checks.append(("LIVE > 405", (doc.get("n_live") or 0) > 405))
        checks.append(("zero bare UNRESOLVED", by_status.get("UNRESOLVED", 0) == 0))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v2.2: {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%), "
               f"statuses {by_status}")


if __name__ == "__main__":
    main()
