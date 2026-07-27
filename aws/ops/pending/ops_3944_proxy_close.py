"""
ops_3944 — v3.5.1: (a) RUNNER-VERIFY the /gov proxy for MOF + IMF before
gating (worker allowlist now +mof +boj +api.imf.org; 3943's JP02Y fail
likely raced the worker deploy); (b) imf_latest direct-then-proxy; (c) SNB
picks MAX DATE not values[-1] (asof 2025-07 smelled positional). Gates:
proxy returns MOF CSV + IMF OBS_VALUE from the runner; JP02Y + USFER LIVE;
FER >= 3; CH02Y asof >= 2026; n_live >= 458; zero UNRESOLVED.
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.5.1 PROXY-IMF-SNBDATE"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3944_proxy_close") as rep:
        rep.heading("ops 3944 — proxy verify + v3.5.1 close")
        checks = []

        rep.section("runner-verify the /gov proxy (worker must be live)")
        mof = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
        imf = ("https://api.imf.org/external/sdmx/2.1/data/IRFCL/M.US.RAF_USD"
               "?lastNObservations=1")
        proxy_ok = {"mof": False, "imf": False}
        for name, tgt, marker in (("mof", mof, "Date,1Y"), ("imf", imf, "OBS_VALUE")):
            for a in range(10):
                try:
                    u = PROXY + urllib.request.quote(tgt, safe="")
                    req = urllib.request.Request(u, headers=UA)
                    with urllib.request.urlopen(req, timeout=25) as r:
                        body = r.read().decode("utf-8", "ignore")
                    if marker in body:
                        proxy_ok[name] = True
                        rep.ok(f"  {name} via proxy: attempt {a+1}, {len(body)}b, marker found")
                        break
                    rep.log(f"  {name} attempt {a+1}: {len(body)}b, no marker; "
                            f"head {body[:90]!r}")
                except Exception as e:
                    rep.log(f"  {name} attempt {a+1}: {str(e)[:90]}")
                time.sleep(20)
            checks.append((f"proxy serves {name}", proxy_ok[name]))
        if not all(proxy_ok.values()):
            rep.fail("proxy not serving both — worker deploy issue"); sys.exit(1)

        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARK in src and "max((_dv(x)" in src:
                        settled = True; rep.ok(f"  settled attempt {attempt}"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v3.5.1 settled (strings in zip)", settled))
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

        st_ = doc.get("status_counts") or {}
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}
        for s_, want in (("JP02Y", "mof"), ("USFER", "imf"),
                         ("CH02Y", "snb"), ("CH03Y", "snb")):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} "
                    f"src={rw.get('source')} asof={rw.get('asof')}")
            checks.append((f"{s_} LIVE via {want}",
                           rw.get("status") == "LIVE" and want in str(rw.get("source"))))
        ch_asof = str((idx.get("CH02Y") or {}).get("asof", ""))
        checks.append(("CH02Y asof is 2026 (max-date fix)", "2026" in ch_asof))
        n_fer = sum(1 for s_ in ("USFER", "EUFER", "JPFER", "CHFER")
                    if (idx.get(s_) or {}).get("status") == "LIVE")
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_), fer_live=n_fer)
        checks.append(("FER family >= 3 LIVE", n_fer >= 3))
        checks.append(("n_live >= 458", (doc.get("n_live") or 0) >= 458))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%)")


if __name__ == "__main__":
    main()
