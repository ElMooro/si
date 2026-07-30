"""ops_4129 — streaming phase timeline: settle v3.15.4, invoke, watch the
run's [phase] prints live for 12 minutes. The hog gets a number."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tradingview"
MARK = "tradingview-vault v3.16.0 ops4129 ladder-wall"


def main():
    with report("4129_ladderwall_stream") as rep:
        rep.heading("ops 4124 — phase stream")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        for att in range(6):
            try:
                lam.update_function_code(FunctionName=FN,
                                         ZipFile=buf.getvalue(), Publish=True)
                rep.ok(f"  update accepted (attempt {att})")
                break
            except Exception as e:
                rep.log(f"  EXC {type(e).__name__}: {str(e)[:100]}")
                time.sleep(10)
        ok = False
        for i in range(35):
            try:
                cfg = lam.get_function_configuration(FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") == "Successful":
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                        lam.get_function(FunctionName=FN)["Code"]["Location"],
                        timeout=60).read())).read(
                        "lambda_function.py").decode()
                    if MARK in dep:
                        ok = True
                        rep.ok(f"  settled at loop {i}")
                        break
            except Exception:
                pass
            time.sleep(9)
        if not ok:
            rep.fail("never settled")
            sys.exit(1)

        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        rep.section("live phase timeline")
        seen = set()
        wrote = False
        for cyc in range(12):
            time.sleep(60)
            try:
                ev = logs.filter_log_events(
                    logGroupName=f"/aws/lambda/{FN}",
                    startTime=int(t_inv.timestamp() * 1000),
                    filterPattern='"tv-vault"', limit=200)
                for e in ev.get("events") or []:
                    msg = e["message"].strip()[:120]
                    if msg not in seen:
                        seen.add(msg)
                        rep.log(f"  {msg}")
            except Exception as e2:
                rep.log(f"  logs EXC {type(e2).__name__}")
            try:
                v = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
                if str(v.get("marker")) == MARK:
                    rep.ok(f"  \u2605 ARTIFACT WROTE v3.15.4 at cycle {cyc}")
                    wrote = True
                    break
            except Exception:
                pass
        if wrote:
            from collections import Counter
            fams = Counter()
            idx = {}
            live = 0
            for r2 in v.get("symbols") or []:
                idx[r2.get("symbol")] = r2
                if r2.get("status") == "LIVE":
                    live += 1
                ad = str(r2.get("adapter") or "")
                if ad.startswith("family:"):
                    fams[ad] += 1
            rep.kv(total_live=live, **{k: n for k, n in fams.most_common()})
            checks = [("INTR >=25", fams.get("family:INTR", 0) >= 25),
                      ("FER >=80", fams.get("family:FER", 0) >= 80),
                      ("WB trio >=250",
                       sum(fams.get("family:" + f, 0)
                           for f in ("GDPYY", "IRYY", "UR")) >= 250)]
            for sym2, want, tol in (("ECONOMICS:BRINTR", 14.25, 0.06),
                                    ("ECONOMICS:PEINTR", 4.25, 0.06),
                                    ("ECONOMICS:BRFER", 368899, 12000)):
                got = (idx.get(sym2) or {}).get("value")
                rep.log(f"  spot {sym2}: got={got} "
                        f"src={(idx.get(sym2) or {}).get('source')}")
                checks.append((f"spot {sym2}",
                               got is not None and
                               abs(float(got) - want) <= tol))
            failed = [l for l, k in checks if not k]
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            if failed:
                rep.fail(f"FAILED: {failed}")
                sys.exit(1)
            rep.ok(f"PASS_ALL — WROTE + families {dict(fams)} "
                   f"+ total LIVE {live}")
        else:
            rep.ok(f"STREAM DONE wrote=False — {len(seen)} lines; "
                   f"slow-prints above name the adapter")


if __name__ == "__main__":
    main()
