"""
ops_3924 — TWO JOBS: (A) AUDIT: dump the exact NO_FREE_SOURCE list from the
live vault and grep the ENTIRE fleet's engine sources (runner checkout) for
each symbol — Khalid's question: do we already compute these somewhere? The
map (symbol -> producing engines) drives the next alias pass. (B) DEPLOY
vault v2.1 (fleet-resolver rung + ~20 certified aliases), invoke, gate on
LIVE strictly increasing over 379, BTPBUND LIVE FROM THE FLEET's own feed,
zero bare UNRESOLVED preserved.
"""
import io, json, subprocess, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=880, retries={"max_attempts": 0}))
FN, MARK = "justhodl-tradingview", "tradingview-vault v2.1 FLEET-RESOLVER"
LDIR = ROOT / "lambdas"


def main():
    with report("3924_fleet_audit_and_v21") as rep:
        rep.heading("ops 3924 — fleet audit of the 186 + vault v2.1 fleet-resolver")
        checks = []

        rep.section("A. which NO_FREE_SOURCE symbols does the fleet ALREADY compute?")
        doc0 = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                        Key="data/tradingview.json")["Body"].read())
        nfs = sorted(r["symbol"] for r in doc0.get("symbols") or []
                     if r.get("status") == "NO_FREE_SOURCE")
        rep.kv(n_no_free_source_before=len(nfs))
        found = {}
        for sym in nfs:
            if len(sym) < 3:
                continue
            try:
                out = subprocess.run(
                    ["grep", "-rl", "--include=lambda_function.py", sym, str(LDIR)],
                    capture_output=True, text=True, timeout=30).stdout
                engines = sorted({Path(l).parent.parent.name
                                  for l in out.strip().splitlines() if l})
                engines = [e for e in engines if e != "justhodl-tradingview"]
                if engines:
                    found[sym] = engines[:4]
            except Exception:
                pass
        rep.kv(n_found_in_fleet=len(found))
        for sym, eng in sorted(found.items()):
            rep.log(f"  {sym} -> {', '.join(eng)}")
        checks.append(("fleet audit ran", True))

        rep.section("B. vault v2.1 settle + invoke")
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
        bb = idx.get("BTPBUND") or {}
        rep.log(f"  BTPBUND: {bb.get('status')} value={bb.get('value')} src={bb.get('source')}")
        for s_ in ("IT10Y", "USM2", "EUINTR", "JPM3"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
        checks.append(("LIVE increased over 379", (doc.get("n_live") or 0) > 379))
        checks.append(("BTPBUND LIVE from the fleet's own feed",
                       bb.get("status") == "LIVE" and "fleet:" in str(bb.get("source"))))
        checks.append(("zero bare UNRESOLVED preserved", by_status.get("UNRESOLVED", 0) == 0))

        # persist the audit map for the next alias pass
        s3.put_object(Bucket="justhodl-dashboard-live", Key="data/tv-fleet-map.json",
                      Body=json.dumps({"generated_at": doc.get("generated_at"),
                                       "no_free_source_remaining":
                                           [r["symbol"] for r in doc.get("symbols") or []
                                            if r.get("status") == "NO_FREE_SOURCE"],
                                       "found_in_fleet_sources": found}, default=str),
                      ContentType="application/json")
        rep.ok("  audit map persisted to data/tv-fleet-map.json")

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v2.1: {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%), "
               f"{len(found)} of the remaining symbols located in fleet sources for the next pass")


if __name__ == "__main__":
    main()
