"""ops_3938 — v3.2.1: Norges SDMX uses 'structures' (plural list) where ECB
uses 'structure' — walker KeyError'd to None. Also tighter BCRP grep for the
monthly Peru ToT INDEX code. Gates: NO03Y LIVE via norges-bank, n_live>=451,
zero UNRESOLVED."""
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.2.1 NORGES-FIX"


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3938_norges_close") as rep:
        rep.heading("ops 3938 — Norges close + BCRP index-code grep")
        checks = []
        rep.section("BCRP monthly ToT index code")
        try:
            url = "https://estadisticas.bcrp.gob.pe/estadisticas/series/metadata"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                meta = r.read().decode("utf-8", "ignore")
            hits = [ln for ln in meta.splitlines()
                    if "intercambio" in ln.lower() and
                    ("ndice" in ln.lower() or "index" in ln.lower()) and
                    ";Mensual;" in ln][:6]
            for h in hits:
                rep.log(f"  {h[:170]}")
            if not hits:
                any_m = [ln for ln in meta.splitlines()
                         if "intercambio" in ln.lower() and ";Mensual;" in ln][:6]
                for h in any_m:
                    rep.log(f"  (any monthly) {h[:170]}")
        except Exception as e:
            rep.log(f"  {str(e)[:120]}")

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
        checks.append(("v3.2.1 settled", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        for _ in range(25):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8)

        # NO03Y is daily-cadence + previously-NFS-with-no-resolved_via… but the
        # v3.2 retry fix keys on resolved_via; force ONLY this path via a full
        # force run? Cheaper: normal invoke — NO03Y cached NFS from a run <27d
        # ago would freeze. Use force to be safe (async+poll, proven).
        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"force": True}).encode())
        doc = None
        for i in range(60):
            time.sleep(15)
            d = get_doc()
            if d.get("generated_at", "") > t_mark:
                doc = d; rep.ok(f"  refreshed ~{(i+1)*15}s"); break
        checks.append(("force run wrote", doc is not None))
        if not doc: rep.fail("never wrote"); sys.exit(1)
        st = doc.get("status_counts") or {}
        rw = ({r["symbol"]: r for r in doc.get("symbols") or []}).get("NO03Y") or {}
        rep.log(f"  NO03Y: {rw.get('status')} value={rw.get('value')} src={rw.get('source')}")
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st))
        checks.append(("NO03Y LIVE via norges-bank",
                       rw.get("status") == "LIVE" and "norges" in str(rw.get("source"))))
        checks.append(("n_live >= 451", (doc.get("n_live") or 0) >= 451))
        checks.append(("zero bare UNRESOLVED", st.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%)")


if __name__ == "__main__":
    main()
