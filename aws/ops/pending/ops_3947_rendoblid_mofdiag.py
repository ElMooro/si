"""
ops_3947 — v3.5.2: (a) SNB switched to rendoblid DAILY windowed (rendoblim
proven stale 2025-07); (b) MOF fetch instrumented — per-URL error/status
recorded and exported as debug_mof in the artifact so the Lambda-side proxy
failure NAMES ITSELF; (c) IMF unkeyed data pull — grep the first <Obs line
for real attribute names/codes (DSD parse came back empty). Gates: CH02Y
LIVE with 2026 asof via rendoblid; debug_mof non-empty and printed; n_live
>= 455; zero UNRESOLVED. JP02Y soft-logged (hard-gated next once the error
is known).
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.5.2 RENDOBLID-MOFDIAG"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3947_rendoblid_mofdiag") as rep:
        rep.heading("ops 3947 — rendoblid switch + MOF self-diagnosis + IMF unkeyed")
        checks = []

        rep.section("IMF unkeyed pull — learn real series attributes inline")
        for u in ("https://api.imf.org/external/sdmx/2.1/data/IRFCL?lastNObservations=1&startPeriod=2026",
                  "https://api.imf.org/external/sdmx/2.1/data/IRFCL/all?lastNObservations=1"):
            try:
                req = urllib.request.Request(u, headers=UA)
                with urllib.request.urlopen(req, timeout=40) as r:
                    body = r.read(300000).decode("utf-8", "ignore")
                m = re.search(r"<Obs [^>]+>", body)
                rep.log(f"  {u[-40:]}: {len(body)}b Obs={'YES' if m else 'no'}")
                if m:
                    rep.ok(f"  FIRST OBS ATTRS: {m.group(0)[:400]}")
                    break
            except Exception as e:
                rep.log(f"  {u[-40:]}: {str(e)[:100]}")

        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARK in src and "rendoblid" in src and "debug_mof" in src:
                        settled = True; rep.ok(f"  settled attempt {attempt}"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v3.5.2 settled (strings in zip)", settled))
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

        rep.section("MOF self-diagnosis from inside the Lambda")
        for line in doc.get("debug_mof") or []:
            rep.log(f"  {line}")
        checks.append(("debug_mof captured", bool(doc.get("debug_mof"))))

        st_ = doc.get("status_counts") or {}
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}
        for s_ in ("JP02Y", "CH02Y", "CH03Y"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} "
                    f"src={rw.get('source')} asof={rw.get('asof')}")
        ch = idx.get("CH02Y") or {}
        checks.append(("CH02Y LIVE + 2026 asof via rendoblid",
                       ch.get("status") == "LIVE" and "2026" in str(ch.get("asof"))))
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_))
        checks.append(("n_live >= 455", (doc.get("n_live") or 0) >= 455))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%), "
               f"CH02Y {ch.get('value')} @ {ch.get('asof')}")


if __name__ == "__main__":
    main()
