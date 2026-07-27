"""ops_3940 — v3.2.2 closes NO03Y: the v3.2 alias replace was a SILENT NO-OP
(NO03Y sits on a shared line with CN10Y; the standalone-line pattern never
matched, alias stayed none:) — parser was fine all along, norges_latest was
never called. Patched on the real string with assert-verify. Also: BCRP
catalog hunt where the SERIES NAME itself is the ToT index (monthly).
Gates: NO03Y LIVE via norges-bank ~4.4-4.6, n_live>=451, zero UNRESOLVED."""
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.2.2 NO03Y-ALIAS"


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3940_no03y_close") as rep:
        rep.heading("ops 3940 — NO03Y close (silent-no-op fixed)")
        checks = []

        rep.section("BCRP — series whose NAME is the ToT index, monthly")
        try:
            url = "https://estadisticas.bcrp.gob.pe/estadisticas/series/metadata"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                meta = r.read().decode("utf-8", "ignore")
            hits = []
            for ln in meta.splitlines():
                cols = ln.split(";")
                if len(cols) > 10 and "Mensual" in ln:
                    name = cols[3].lower()
                    if "rminos de intercambio" in name and "precios" not in name:
                        hits.append(ln[:170])
            for h in hits[:8]:
                rep.log(f"  {h}")
            if not hits:
                rep.log("  none matched — ToT headline may be the group median; "
                        "candidates by adjacency below")
                for code in ("PN38921BM", "PN38922BM", "PN38923BM", "PN38924BM"):
                    try:
                        u2 = (f"https://estadisticas.bcrp.gob.pe/estadisticas/series/"
                              f"api/{code}/json/2026-1/2026-5")
                        with urllib.request.urlopen(
                                urllib.request.Request(u2, headers={"User-Agent": "Mozilla/5.0"}),
                                timeout=20) as r2:
                            d2 = json.loads(r2.read())
                        rep.log(f"  {code}: {(d2.get('config') or {}).get('title','')[:90]}")
                    except Exception as e2:
                        rep.log(f"  {code}: {str(e2)[:70]}")
        except Exception as e:
            rep.log(f"  {str(e)[:120]}")

        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARK in src and '"NO03Y": "norges:' in src:
                        settled = True; rep.ok(f"  settled attempt {attempt} (alias verified in zip)"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v3.2.2 settled + alias in artifact", settled))
        if not settled: rep.fail("never settled"); sys.exit(1)
        for _ in range(25):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress": break
            time.sleep(8)

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
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%), "
               f"NO03Y {rw.get('value')} from Norges Bank")


if __name__ == "__main__":
    main()
