"""
ops_3932 — verify the v3.0 SUPERSET engine (parallel-session reconcile: their 5ec00 v3.0 proved 447 LIVE; my 7d548 rewrite adds ECB adapter/EUCA/EU tenors/Polygon env-ensure/BOJ probe and is now the deployed source). Also: (a) ensure POLYGON_KEY is
on the function env (merged from justhodl-equity-research donor, settle-wait
after update); (b) BOJ endpoint PROBE for JPLG — try candidate stat-search
CSV URLs from the runner and report which respond (wire only verified, next
ops). Gates: marker settles; force-invoke full run: LIVE >= 450, zero bare
UNRESOLVED, fred_calls reported; spots USCLI (fleet CLI), EUINTR, 10USNOTE
(ZN=F), NOVO_B, EU02Y-TVC (ECB curve); second invoke (no force) proves the
cadence cache: n_cached large, fred_calls small.
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


def wait_settle_env(fn):
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return c
        time.sleep(8)
    return lam.get_function_configuration(FunctionName=fn)


def main():
    with report("3932_vault_v3_superset_verify") as rep:
        rep.heading("ops 3932 — v3.0 superset verify (post-collision reconcile)")
        checks = []

        rep.section("BOJ probe for JPLG (report-only; wire on verified URL next)")
        boj_candidates = [
            "https://www.stat-search.boj.or.jp/ssi/mtshtml/md02_m_1.csv",
            "https://www.stat-search.boj.or.jp/info/dload_e.html",
            "https://www.boj.or.jp/en/statistics/dl/loan/ldo/ldo.csv",
        ]
        for u in boj_candidates:
            try:
                req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as r:
                    body = r.read(400)
                rep.log(f"  {u} -> HTTP {r.status}, head: {body[:120]!r}")
            except Exception as e:
                rep.log(f"  {u} -> {str(e)[:100]}")

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
        cfg = wait_settle_env(FN)

        env = dict((cfg.get("Environment") or {}).get("Variables") or {})
        if "POLYGON_KEY" not in env:
            donor = lam.get_function_configuration(FunctionName="justhodl-equity-research")
            dk = ((donor.get("Environment") or {}).get("Variables") or {}).get("POLYGON_KEY")
            if dk:
                env["POLYGON_KEY"] = dk
                lam.update_function_configuration(FunctionName=FN,
                                                 Environment={"Variables": env})
                rep.ok("  POLYGON_KEY merged from justhodl-equity-research")
                wait_settle_env(FN)
            else:
                rep.log("  donor has no POLYGON_KEY — polygon rung stays inert")
        checks.append(("POLYGON_KEY present", "POLYGON_KEY" in env))

        rep.section("force-invoke: full re-resolution")
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"force": True}).encode())
        raw = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"FunctionError: {json.dumps(raw)[:900]}"); sys.exit(1)
        rep.log(f"  invoke: {json.dumps(raw, default=str)[:300]}")
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/tradingview.json")["Body"].read())
        st = doc.get("status_counts") or {}
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st), fred_calls=doc.get("fred_calls_this_run"))
        checks.append(("LIVE >= 450", (doc.get("n_live") or 0) >= 450))
        checks.append(("zero bare UNRESOLVED", st.get("UNRESOLVED", 0) == 0))
        idx = {row["symbol"]: row for row in doc.get("symbols") or []}
        for s_, want in (("USCLI", "fleet"), ("EUINTR", "fred_alias"),
                         ("10USNOTE", "yahoo:ZN=F"), ("NOVO_B", "yahoo:NOVO-B.CO"),
                         ("EU02Y-TVC", "ecb:")):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
            checks.append((f"{s_} LIVE via {want}",
                           rw.get("status") == "LIVE" and want in str(rw.get("source"))))

        rep.section("second invoke (no force): cadence cache proof")
        r2 = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw2 = json.loads(r2["Payload"].read())
        rep.log(f"  invoke2: {json.dumps(raw2, default=str)[:300]}")
        checks.append(("cache engaged (n_cached >= 100)", (raw2.get("n_cached") or 0) >= 100))
        checks.append(("fred calls dropped on cached run",
                       (raw2.get("fred_calls") or 999) < (doc.get("fred_calls_this_run") or 0)))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — v3.0 cadence-aware: {doc.get('n_live')} LIVE "
               f"({doc.get('coverage_pct')}%), statuses {st}")


if __name__ == "__main__":
    main()
