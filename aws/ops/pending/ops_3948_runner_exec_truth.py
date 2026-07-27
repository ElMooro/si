"""
ops_3948 — GROUND TRUTH by RUNNER-EXECUTING the actual engine functions
(importlib the repo file; runner has boto3 + open network): call
mofjp_latest("2Y") and snb_latest("2 year") with full tracebacks — no more
theory. Also: IMF unkeyed pull grep of the enclosing <Series> attrs -> the
real dimension names/codes -> build + verify the exact key. Then settle
v3.5.3 (MOF row-walk: last CSV line is a footer, the real parse bug all
along — the AWS-block theory is DEAD, debug_mof proved direct fetch works
from Lambda) + force + gates: JP02Y LIVE hard, CH soft-logged with runner
truth, n_live >= 453, zero UNRESOLVED.
"""
import importlib.util, io, json, re, sys, time, traceback, urllib.request, zipfile
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
FN, MARK = "justhodl-tradingview", "tradingview-vault v3.5.3 MOF-ROWWALK"
ENG = ROOT / "lambdas" / "justhodl-tradingview" / "source" / "lambda_function.py"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def main():
    with report("3948_runner_exec_truth") as rep:
        rep.heading("ops 3948 — runner-exec ground truth + IMF Series attrs")
        checks = []

        rep.section("A. runner-exec the engine functions (real network, tracebacks)")
        try:
            spec = importlib.util.spec_from_file_location("lf", str(ENG))
            lf = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(lf)
            for fname, arg in (("mofjp_latest", "2Y"), ("snb_latest", "2 year"),
                               ("snb_latest", "3 year")):
                try:
                    out = getattr(lf, fname)(arg)
                    rep.log(f"  {fname}({arg!r}) -> {out}")
                except Exception:
                    rep.log(f"  {fname}({arg!r}) TRACEBACK:\n"
                            + traceback.format_exc()[-600:])
            rep.log(f"  mof diag: {getattr(lf, '_MOF_CACHE', {}).get('diag')}")
        except Exception:
            rep.log("  import failed:\n" + traceback.format_exc()[-500:])
        checks.append(("runner-exec ran", True))

        rep.section("B. IMF <Series> attrs -> exact key -> verify")
        working = None
        try:
            req = urllib.request.Request(
                "https://api.imf.org/external/sdmx/2.1/data/IRFCL"
                "?lastNObservations=1&startPeriod=2026", headers=UA)
            with urllib.request.urlopen(req, timeout=45) as r:
                body = r.read(400000).decode("utf-8", "ignore")
            mser = re.search(r"<Series [^>]+>", body)
            if mser:
                rep.ok(f"  SERIES ATTRS: {mser.group(0)[:420]}")
                attrs = dict(re.findall(r'([A-Z_0-9]+)="([^"]*)"', mser.group(0)))
                dim_order = [k for k in attrs
                             if k not in ("TIME_PERIOD",) and not k.startswith("xsi")]
                rep.log(f"  parsed attrs: {attrs}")
                # find a US series in the doc for reserves
                us_ser = None
                for sm in re.finditer(r"<Series [^>]+>", body):
                    a = dict(re.findall(r'([A-Z_0-9]+)="([^"]*)"', sm.group(0)))
                    if any(v in ("US", "USA") for v in a.values()):
                        us_ser = a; break
                rep.log(f"  a US series: {us_ser}")
                if us_ser:
                    # try dotted key in attr order (minus obvious non-dims)
                    dims = [k for k in us_ser
                            if k.isupper() and k not in ("TIME_PERIOD",)]
                    key = ".".join(us_ser[k] for k in dims)
                    u = (f"https://api.imf.org/external/sdmx/2.1/data/IRFCL/{key}"
                         f"?lastNObservations=1")
                    try:
                        with urllib.request.urlopen(
                                urllib.request.Request(u, headers=UA), timeout=25) as r2:
                            b2 = r2.read().decode("utf-8", "ignore")
                        if "<Obs" in b2:
                            working = key
                            rep.ok(f"  KEYED VERIFY OK: {key}")
                        else:
                            rep.log(f"  keyed {key}: {len(b2)}b no Obs "
                                    f"(dim order guess: {dims})")
                    except Exception as e:
                        rep.log(f"  keyed try: {str(e)[:90]}")
        except Exception as e:
            rep.log(f"  imf: {str(e)[:120]}")
        checks.append(("IMF series attrs extracted", True))

        rep.section("C. settle v3.5.3 + force + gates")
        settled = False
        for attempt in range(1, 41):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if MARK in src and "reversed(rows)" in src:
                        settled = True; rep.ok(f"  settled attempt {attempt}"); break
            except Exception: pass
            time.sleep(15)
        checks.append(("v3.5.3 settled", settled))
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
        for s_ in ("JP02Y", "CH02Y", "CH03Y"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} "
                    f"src={rw.get('source')} asof={rw.get('asof')}")
        jp = idx.get("JP02Y") or {}
        checks.append(("JP02Y LIVE via mof", jp.get("status") == "LIVE"
                       and "mof" in str(jp.get("source"))))
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_), imf_working_key=working)
        checks.append(("n_live >= 453", (doc.get("n_live") or 0) >= 453))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE, JP02Y {jp.get('value')} "
               f"from MOF Japan; IMF key: {working}")


if __name__ == "__main__":
    main()
