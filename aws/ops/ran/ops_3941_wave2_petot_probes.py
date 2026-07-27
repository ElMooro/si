"""
ops_3941 — WAVE 2 opener: deploy v3.3 (PETOT wired via confirmed BCRP code
PN38923BM) + discovery probes for the rest of the wave, all from the runner:
(1) BOJ's NEW official API (Khalid's link, notice 2026-02-18) — fetch the
notice, extract the API base/docs URL, probe it; (2) MOF JGB — fetch the
interest-rate index, extract the current jgbcm CSV link, probe it; (3) BoE
IADB — iterate 3 param variants for IUDSOIA until CSV comes back; (4) SNB
cube list; (5) IMF new-API candidates. Gates: PETOT LIVE via bcrp-peru,
n_live >= 452, zero UNRESOLVED, string-in-zip settle.
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.3 BCRP-PERU"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def fetch(url, timeout=25, dec="utf-8"):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3941_wave2_petot_probes") as rep:
        rep.heading("ops 3941 — wave 2: PETOT wire + BOJ-API/MOF/BoE/SNB/IMF discovery")
        checks = []

        rep.section("1. BOJ new official API (notice 2026-02-18)")
        api_hints = []
        try:
            st, body = fetch("https://www.boj.or.jp/en/statistics/outline/notice_2026/not260218a.htm")
            html = body.decode("utf-8", "ignore")
            if len(html) < 500:
                html = body.decode("shift_jis", "ignore")
            rep.log(f"  notice: HTTP {st}, {len(body)}b")
            urls = re.findall(r'href="(https?://[^"]+)"', html)
            api_hints = [u for u in urls if "api" in u.lower()][:8]
            for u in api_hints:
                rep.log(f"  api-ish link: {u[:120]}")
            for frag in re.findall(r'(https?://api[^\s"<>]+)', html)[:5]:
                rep.log(f"  inline api url: {frag[:120]}")
                api_hints.append(frag)
            if not api_hints:
                text = re.sub(r"<[^>]+>", " ", html)
                idx = text.lower().find("api")
                rep.log(f"  no links; text around 'api': {text[max(0,idx-100):idx+300]!r}")
        except Exception as e:
            rep.log(f"  notice fetch: {str(e)[:120]}")
        for u in api_hints[:3]:
            try:
                st, b = fetch(u, timeout=20)
                rep.log(f"  probe {u[:80]} -> HTTP {st}, {len(b)}b, head {b[:100]!r}")
            except Exception as e:
                rep.log(f"  probe {u[:80]} -> {str(e)[:90]}")

        rep.section("2. MOF JGB — current CSV link from the index")
        try:
            st, body = fetch("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/index.htm")
            html = body.decode("utf-8", "ignore")
            rep.log(f"  index: HTTP {st}, {len(body)}b")
            csvs = re.findall(r'href="([^"]+\.csv)"', html)[:8]
            for c in csvs:
                rep.log(f"  csv link: {c[:120]}")
            if csvs:
                target = csvs[0]
                if target.startswith("/"):
                    target = "https://www.mof.go.jp" + target
                elif not target.startswith("http"):
                    target = ("https://www.mof.go.jp/english/policy/jgbs/reference/"
                              "interest_rate/" + target)
                st2, b2 = fetch(target, timeout=25)
                head = b2[:200].decode("utf-8", "ignore")
                rep.log(f"  probe {target[:100]} -> HTTP {st2}, {len(b2)}b :: {head[:120]}")
        except Exception as e:
            rep.log(f"  mof: {str(e)[:120]}")

        rep.section("3. BoE IADB param iteration (IUDSOIA)")
        variants = [
            ("TN+dates", "https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?csv.x=yes&SeriesCodes=IUDSOIA&CSVF=TN&UsingCodes=Y&VPD=Y&Datefrom=01/Jul/2026&Dateto=26/Jul/2026"),
            ("TT+dates", "https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?csv.x=yes&SeriesCodes=IUDSOIA&CSVF=TT&UsingCodes=Y&VPD=Y&Datefrom=01/Jul/2026&Dateto=26/Jul/2026"),
            ("iadb-fromshowcolumns", "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes&SeriesCodes=IUDSOIA&CSVF=TN&UsingCodes=Y&Datefrom=01/Jul/2026&Dateto=26/Jul/2026"),
        ]
        for label, u in variants:
            try:
                st, b = fetch(u, timeout=25)
                head = b[:140].decode("utf-8", "ignore").replace("\r", " ").replace("\n", " ")
                is_csv = ("DATE" in head.upper() and "<" not in head[:20])
                rep.log(f"  [{label}] HTTP {st}, {len(b)}b, csv={is_csv} :: {head[:110]}")
            except Exception as e:
                rep.log(f"  [{label}] {str(e)[:90]}")

        rep.section("4. SNB cube list")
        for u in ("https://data.snb.ch/api/cube", "https://data.snb.ch/en/api"):
            try:
                st, b = fetch(u, timeout=20)
                rep.log(f"  {u} -> HTTP {st}, {len(b)}b, head {b[:150]!r}")
            except Exception as e:
                rep.log(f"  {u} -> {str(e)[:90]}")

        rep.section("5. IMF new-API candidates")
        for u in ("https://api.imf.org/external/sdmx/2.1/dataflow",
                  "https://data.imf.org/api/dataflow"):
            try:
                st, b = fetch(u, timeout=20)
                rep.log(f"  {u} -> HTTP {st}, {len(b)}b, head {b[:120]!r}")
            except Exception as e:
                rep.log(f"  {u} -> {str(e)[:90]}")

        rep.section("deploy gate — v3.3 + PETOT")
        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARK in src and '"PETOT": "bcrp:' in src:
                        settled = True; rep.ok(f"  settled attempt {attempt} (alias in zip)"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v3.3 settled + PETOT alias in artifact", settled))
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
        rw = ({r["symbol"]: r for r in doc.get("symbols") or []}).get("PETOT") or {}
        rep.log(f"  PETOT: {rw.get('status')} value={rw.get('value')} "
                f"src={rw.get('source')} asof={rw.get('asof')}")
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_))
        checks.append(("PETOT LIVE via bcrp-peru",
                       rw.get("status") == "LIVE" and "bcrp" in str(rw.get("source"))))
        checks.append(("n_live >= 452", (doc.get("n_live") or 0) >= 452))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%), "
               f"PETOT {rw.get('value')} from BCRP ({rw.get('asof')})")


if __name__ == "__main__":
    main()
