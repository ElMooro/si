"""
ops_3958 — close the last red: (a) RUNNER-EXEC p_boj from the repo engine
with full traceback (the v2.0 Tankan add failed silently); (b) v2.0.1
hardened: fallback Tankan code + error captured into the probe note instead
of swallowed; settle + invoke + gate boj >= 2 ok probes and total
n_series_pulled reported.
"""
import importlib.util, io, json, sys, time, traceback, urllib.request, zipfile
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
FN, MARK = "justhodl-gov-sources", "gov-sources v2.0.1 TANKAN-HARDENED"
ENG = ROOT / "lambdas" / "justhodl-gov-sources" / "source" / "lambda_function.py"


def deployed_src():
    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
    blob = urllib.request.urlopen(loc, timeout=60).read()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        return z.read("lambda_function.py").decode("utf-8", "ignore")


def main():
    with report("3958_tankan_close") as rep:
        rep.heading("ops 3958 — Tankan close (runner-exec + hardened)")
        checks = []

        rep.section("runner-exec p_boj (ground truth)")
        try:
            spec = importlib.util.spec_from_file_location("gv", str(ENG))
            gv = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(gv)
            try:
                out = gv.p_boj()
                for r in out:
                    rep.log(f"  {r}")
            except Exception:
                rep.log("  p_boj TRACEBACK:\n" + traceback.format_exc()[-700:])
        except Exception:
            rep.log("  import failed:\n" + traceback.format_exc()[-400:])
        checks.append(("runner-exec ran", True))

        rep.section("settle v2.0.1")
        settled = False
        for attempt in range(1, 41):
            try:
                src = deployed_src()
                if MARK in src and "TK99F2000601GCQ01000" in src:
                    settled = True
                    rep.ok(f"  settled attempt {attempt}")
                    break
            except Exception:
                pass
            time.sleep(15)
        checks.append(("v2.0.1 settled", settled))
        if not settled:
            rep.fail("never settled")
            sys.exit(1)
        for _ in range(20):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(8)

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
        boj = next((a for a in doc.get("agencies") or [] if a["id"] == "boj"), {})
        for pr in boj.get("probes") or []:
            rep.log(f"  boj probe: {pr}")
        n_ok = sum(1 for pr in (boj.get("probes") or []) if pr.get("ok"))
        rep.kv(n_series_pulled=doc.get("n_series_pulled"), boj_ok=n_ok,
               n_live=doc.get("n_live"))
        checks.append(("boj >= 2 ok probes", n_ok >= 2))
        checks.append(("n_series_pulled >= 46", (doc.get("n_series_pulled") or 0) >= 46))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — BOJ {n_ok} live series; "
               f"{doc.get('n_series_pulled')} gov-direct series total")


if __name__ == "__main__":
    main()
