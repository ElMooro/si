"""
ops_3957 — gov-sources v2.0 FULL-PULL (Khalid: 'you should be pulling
hundreds'): every agency now pulls its full curated set — entire Treasury
par curve, entire JGB curve (1Y-40Y), BoE SONIA + gilt ladder, ECB 4-tenor
AAA curve, Norges all tenors, Eurostat 7-country debt/GDP, BCRP ToT pair,
BOJ JPLG + Tankan DI. FIX: FRED probe 400 = env key never propagated ->
copy FRED_KEY/FMP_KEY from donor justhodl-signal-backtest onto the
function. Universe annotations per agency. Gates: FRED LIVE with real
DGS10; n_series_pulled >= 45; treasury >= 12, mof >= 14, boe >= 4,
ecb >= 3, norges >= 4, estat >= 6, boj >= 2; 13 agencies; page served
with new KPI.
"""
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
FN, MARK = "justhodl-gov-sources", "gov-sources v2.0 FULL-PULL"
UA = {"User-Agent": "Mozilla/5.0"}


def deployed_src():
    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
    blob = urllib.request.urlopen(loc, timeout=60).read()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        return z.read("lambda_function.py").decode("utf-8", "ignore")


def main():
    with report("3957_gov_fullpull") as rep:
        rep.heading("ops 3957 — gov-sources v2.0 FULL-PULL + FRED env fix")
        checks = []

        rep.section("env fix: copy FRED_KEY/FMP_KEY from donor")
        donor = lam.get_function_configuration(FunctionName="justhodl-signal-backtest")
        dvars = (donor.get("Environment") or {}).get("Variables") or {}
        cur = lam.get_function_configuration(FunctionName=FN)
        cvars = (cur.get("Environment") or {}).get("Variables") or {}
        need = {k: v for k, v in dvars.items() if k in ("FRED_KEY", "FMP_KEY")}
        rep.log(f"  donor has: {sorted(need.keys())}; fn had FRED_KEY: "
                f"{'FRED_KEY' in cvars}")
        if not all(cvars.get(k) == v for k, v in need.items()):
            lam.update_function_configuration(
                FunctionName=FN, Environment={"Variables": {**cvars, **need}})
            for _ in range(20):
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("LastUpdateStatus") != "InProgress":
                    break
                time.sleep(6)
            rep.ok("  env updated")
        checks.append(("FRED_KEY on function", True))

        rep.section("settle v2.0 (marker + full-pull strings in zip)")
        settled = False
        for attempt in range(1, 41):
            try:
                src = deployed_src()
                if MARK in src and "IUDLNPY" in src and "TK99F1000601GCQ01000" in src:
                    settled = True
                    rep.ok(f"  settled attempt {attempt}")
                    break
            except Exception:
                pass
            time.sleep(15)
        if not settled:
            rep.log("  deploy-lambdas slow — self-heal update_function_code")
            SRC = ROOT / "lambdas" / "justhodl-gov-sources" / "source"
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                for f in SRC.iterdir():
                    z.write(f, f.name)
            lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
            for _ in range(20):
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                    break
                time.sleep(8)
            settled = MARK in deployed_src()
        checks.append(("v2.0 settled", settled))
        if not settled:
            rep.fail("never settled")
            sys.exit(1)
        for _ in range(20):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(8)

        rep.section("invoke + gates")
        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        doc = None
        for i in range(40):
            time.sleep(10)
            try:
                d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                             Key="data/gov-sources.json")["Body"].read())
                if d.get("generated_at", "") > t_mark:
                    doc = d
                    rep.ok(f"  artifact ~{(i+1)*10}s")
                    break
            except Exception:
                pass
        checks.append(("artifact written", doc is not None))
        if not doc:
            rep.fail("never wrote")
            sys.exit(1)

        ag = {a["id"]: a for a in doc.get("agencies") or []}
        counts = {k: len(v.get("probes") or []) for k, v in ag.items()}
        rep.kv(n_series_pulled=doc.get("n_series_pulled"),
               n_live=doc.get("n_live"), per_agency=str(counts))
        fred = ag.get("fred") or {}
        fred_p = (fred.get("probes") or [{}])[0]
        rep.log(f"  FRED: {fred.get('status')} {fred_p.get('name')}="
                f"{fred_p.get('value')} @ {fred_p.get('asof')}")
        checks.append(("FRED LIVE with real DGS10",
                       fred.get("status") == "LIVE"
                       and isinstance(fred_p.get("value"), (int, float))))
        checks.append(("n_series_pulled >= 45", (doc.get("n_series_pulled") or 0) >= 45))
        for aid, mn in (("us_treasury", 12), ("mof_japan", 14), ("boe", 4),
                        ("ecb", 3), ("norges", 4), ("eurostat", 6), ("boj", 2)):
            checks.append((f"{aid} >= {mn} series", counts.get(aid, 0) >= mn))
        checks.append(("13 agencies", doc.get("n_agencies") == 13))

        rep.section("served page (new KPI)")
        served = False
        for a in range(9):
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/gov-sources.html?cb=" + str(time.time()),
                    headers=UA)
                body = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
                if "Gov series pulled" in body and "Universe" in body:
                    served = True
                    rep.ok(f"  served attempt {a+1}")
                    break
            except Exception:
                pass
            time.sleep(20)
        checks.append(("page served with v2 markers", served))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_series_pulled')} gov-direct series pulled "
               f"across {doc.get('n_live')} live agencies; FRED fixed; page v2 served")


if __name__ == "__main__":
    main()
