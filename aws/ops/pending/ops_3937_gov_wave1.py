"""
ops_3937 — deploy vault v3.2 GOV-ADAPTERS wave 1 (Treasury/Eurostat/Norges,
all endpoint-verified in 3936) + NFS-retry fix (previously-LIVE rows retry
on their cadence instead of freezing 27d). Also: BCRP Peru catalog grep for
the exact ToT series code (wired next wave). Gates via async force + poll:
US02MY LIVE via treasury.gov; ITGDG/ESGDG/EUGDG LIVE via eurostat; NO03Y
LIVE via norges-bank; n_live >= 447; zero bare UNRESOLVED.
"""
import io, json, re, sys, time, urllib.request, zipfile
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.2 GOV-ADAPTERS"


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3937_gov_wave1") as rep:
        rep.heading("ops 3937 — gov-adapters wave 1")
        checks = []

        rep.section("BCRP Peru — ToT series-code discovery (wire next wave)")
        try:
            url = "https://estadisticas.bcrp.gob.pe/estadisticas/series/metadata"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                meta = r.read().decode("utf-8", "ignore")
            hits = [ln for ln in meta.splitlines()
                    if "rminos de intercambio" in ln.lower()][:8]
            for h in hits:
                rep.log(f"  {h[:180]}")
            if not hits:
                rep.log(f"  no ToT lines; catalog {len(meta)}b head: {meta[:200]!r}")
        except Exception as e:
            rep.log(f"  catalog fetch: {str(e)[:120]}")

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
        checks.append(("v3.2 settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        for _ in range(25):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8)

        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"force": True}).encode())
        rep.log(f"  async force fired {t_mark}; polling…")
        doc = None
        for i in range(60):
            time.sleep(15)
            d = get_doc()
            if d.get("generated_at", "") > t_mark:
                doc = d; rep.ok(f"  artifact refreshed ~{(i+1)*15}s"); break
        checks.append(("force run wrote", doc is not None))
        if not doc: rep.fail("never wrote"); sys.exit(1)

        st = doc.get("status_counts") or {}
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st), fred_calls=doc.get("fred_calls_this_run"))
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}
        for s_, want in (("US02MY", "treasury.gov"), ("ITGDG", "eurostat"),
                         ("ESGDG", "eurostat"), ("EUGDG", "eurostat"),
                         ("NO03Y", "norges-bank")):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} "
                    f"src={rw.get('source')} asof={rw.get('asof')}")
            checks.append((f"{s_} LIVE via {want}",
                           rw.get("status") == "LIVE" and want in str(rw.get("source"))))
        checks.append(("n_live >= 447", (doc.get("n_live") or 0) >= 447))
        checks.append(("zero bare UNRESOLVED", st.get("UNRESOLVED", 0) == 0))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — wave 1 live: {doc.get('n_live')} LIVE "
               f"({doc.get('coverage_pct')}%)")


if __name__ == "__main__":
    main()
