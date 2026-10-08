"""
ops_3943 — v3.5: SNB rendoblim adapter (CH02Y/CH03Y), IMF new sdmx API
(USFER/EUFER/JPFER/CHFER via IRFCL), MOF via /gov edge proxy (mof.go.jp
blocks AWS IPs — runner fetched, Lambda could not; worker allowlist +mof
+boj in same push), GB30Y note precise (BoE max par tenor = 20Y). BOJ PDF
zlib stream-decompress probe for the API base. Gates: JP02Y + CH02Y +
CH03Y + USFER LIVE, FER family >= 3 live, n_live >= 458, zero UNRESOLVED.
"""
import io, json, re, sys, time, urllib.request, zipfile, zlib
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.5 SNB-IMF-GOVPROXY"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3943_snb_imf_govproxy") as rep:
        rep.heading("ops 3943 — v3.5 SNB+IMF+gov-proxy")
        checks = []

        rep.section("BOJ api manual — zlib stream decompress, hunt the base URL")
        try:
            req = urllib.request.Request(
                "https://www.stat-search.boj.or.jp/info/api_manual_en.pdf", headers=UA)
            with urllib.request.urlopen(req, timeout=30) as r:
                pdf = r.read()
            found = set()
            for m in re.finditer(rb"stream\r?\n(.*?)endstream", pdf, re.S):
                try:
                    txt = zlib.decompress(m.group(1))
                except Exception:
                    continue
                for u in re.findall(rb"https?://[A-Za-z0-9\.\-_/\?\=&%~]+", txt):
                    us = u.decode("ascii", "ignore")
                    if "boj" in us or "api" in us:
                        found.add(us)
            for u in sorted(found)[:12]:
                rep.log(f"  {u[:130]}")
            if not found:
                rep.log("  no URLs in decompressed streams — manual may embed the "
                        "base as styled text; try api_notice_en.pdf next")
        except Exception as e:
            rep.log(f"  {str(e)[:120]}")

        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARK in src and '"CH02Y": "snb:' in src and 'gov?u=' in src:
                        settled = True; rep.ok(f"  settled attempt {attempt} (strings in zip)"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v3.5 settled + strings in artifact", settled))
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
        for s_, want in (("JP02Y", "mof"), ("CH02Y", "snb"), ("CH03Y", "snb"),
                         ("USFER", "imf")):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} "
                    f"src={rw.get('source')} asof={rw.get('asof')}")
            checks.append((f"{s_} LIVE via {want}",
                           rw.get("status") == "LIVE" and want in str(rw.get("source"))))
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
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%): "
               f"JP02Y {idx.get('JP02Y',{}).get('value')} · CH02Y "
               f"{idx.get('CH02Y',{}).get('value')} · USFER {idx.get('USFER',{}).get('value')}")


if __name__ == "__main__":
    main()
