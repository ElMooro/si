"""
ops_3942 — v3.4: JP02Y wired via MOF Japan official JGB CSV (jgbcme.csv,
verified 3941); boe adapter kind added (the /iadb/ path returns pure CSV,
SONIA proven). Probes in the same run: (a) BOJ api_manual_en.pdf — regex the
raw bytes for https URLs (/URI entries) to find the new API base, probe it;
(b) BoE gilt-yield candidate codes for GB30Y by title; (c) SNB known cube
ids rendoblid/rendoblim; (d) IMF sdmx dataflow grep for IRFCL + one data
probe. Gates: JP02Y LIVE via mof-japan, n_live>=453, zero UNRESOLVED.
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.4 MOF-BOE"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3942_mof_boe_wire") as rep:
        rep.heading("ops 3942 — MOF wire + BOJ-API/BoE-gilts/SNB/IMF discovery")
        checks = []

        rep.section("a. BOJ api_manual_en.pdf — URL extraction")
        boj_urls = []
        try:
            st, pdf = fetch("https://www.stat-search.boj.or.jp/info/api_manual_en.pdf", 30)
            rep.log(f"  manual: HTTP {st}, {len(pdf)}b")
            found = re.findall(rb"https?://[A-Za-z0-9\.\-_/\?\=&%~]+", pdf)
            uniq = sorted({u.decode("ascii", "ignore") for u in found
                           if b"boj" in u or b"api" in u})[:12]
            for u in uniq:
                rep.log(f"  url-in-pdf: {u[:120]}")
                boj_urls.append(u)
        except Exception as e:
            rep.log(f"  manual: {str(e)[:120]}")
        for u in [x for x in boj_urls if "api" in x.lower()][:3]:
            try:
                st, b = fetch(u, 20)
                rep.log(f"  probe {u[:80]} -> HTTP {st}, {len(b)}b, head {b[:110]!r}")
            except Exception as e:
                rep.log(f"  probe {u[:80]} -> {str(e)[:90]}")

        rep.section("b. BoE gilt candidates for GB30Y (by CSV title/value)")
        for code in ("IUDLNPY", "IUDMNPY", "IUDSNPY", "IUDMIZC", "IUDLIZC"):
            try:
                st, b = fetch("https://www.bankofengland.co.uk/boeapps/iadb/"
                              f"fromshowcolumns.asp?csv.x=yes&SeriesCodes={code}"
                              "&CSVF=TN&UsingCodes=Y&Datefrom=01/Jul/2026", 25)
                head = b[:160].decode("utf-8", "ignore").replace("\r", " ").replace("\n", " | ")
                rep.log(f"  {code}: {len(b)}b :: {head[:130]}")
            except Exception as e:
                rep.log(f"  {code}: {str(e)[:80]}")

        rep.section("c. SNB cube ids")
        for cube in ("rendoblid", "rendoblim"):
            try:
                st, b = fetch(f"https://data.snb.ch/api/cube/{cube}/data/json/en", 25)
                rep.log(f"  {cube}: HTTP {st}, {len(b)}b, head {b[:130]!r}")
            except Exception as e:
                rep.log(f"  {cube}: {str(e)[:90]}")

        rep.section("d. IMF — IRFCL in dataflow + data probe")
        try:
            st, b = fetch("https://api.imf.org/external/sdmx/2.1/dataflow", 30)
            xml = b.decode("utf-8", "ignore")
            hits = re.findall(r'id="([^"]*IRFCL[^"]*)"', xml)[:6] or \
                   re.findall(r'id="([^"]*RESERVE[^"]*)"', xml, re.I)[:6]
            rep.log(f"  dataflow ids: {hits}")
            if hits:
                flow = hits[0]
                st2, b2 = fetch(f"https://api.imf.org/external/sdmx/2.1/data/{flow}"
                                "/M.US.RAF_USD?lastNObservations=1", 25)
                rep.log(f"  data {flow}: HTTP {st2}, {len(b2)}b, head {b2[:130]!r}")
        except Exception as e:
            rep.log(f"  imf: {str(e)[:120]}")

        rep.section("deploy gate — v3.4 + JP02Y")
        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARK in src and '"JP02Y": "mofjp:' in src:
                        settled = True; rep.ok(f"  settled attempt {attempt} (alias in zip)"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v3.4 settled + JP02Y alias in artifact", settled))
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
        rw = ({r["symbol"]: r for r in doc.get("symbols") or []}).get("JP02Y") or {}
        rep.log(f"  JP02Y: {rw.get('status')} value={rw.get('value')} "
                f"src={rw.get('source')} asof={rw.get('asof')}")
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_))
        checks.append(("JP02Y LIVE via mof-japan",
                       rw.get("status") == "LIVE" and "mof" in str(rw.get("source"))))
        checks.append(("n_live >= 453", (doc.get("n_live") or 0) >= 453))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%), "
               f"JP02Y {rw.get('value')} from MOF Japan")


if __name__ == "__main__":
    main()
