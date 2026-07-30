"""ops_4136 — streaming phase timeline: settle v3.15.4, invoke, watch the
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
MARK = "tradingview-vault v3.18.0 ops4136 symbol-feed"


def main():
    with report("4136_symbolfeed_stream") as rep:
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

        rep.section("A2. symbol-feed: create/settle/invoke")
        sfsrc = (ROOT / "lambdas" / "justhodl-symbol-feed" / "source" /
                 "lambda_function.py").read_text()
        sfb = io.BytesIO()
        with zf.ZipFile(sfb, "w", zf.ZIP_DEFLATED) as z2:
            z2.writestr("lambda_function.py", sfsrc)
        try:
            lam.get_function_configuration(FunctionName="justhodl-symbol-feed")
            for att in range(5):
                try:
                    lam.update_function_code(
                        FunctionName="justhodl-symbol-feed",
                        ZipFile=sfb.getvalue(), Publish=True)
                    break
                except Exception:
                    time.sleep(8)
        except Exception:
            donor = lam.get_function_configuration(
                FunctionName="justhodl-families-feed")
            lam.create_function(FunctionName="justhodl-symbol-feed",
                                Runtime=donor["Runtime"], Role=donor["Role"],
                                Handler="lambda_function.lambda_handler",
                                Code={"ZipFile": sfb.getvalue()},
                                Timeout=300, MemorySize=512, Publish=True)
            rep.ok("  symbol-feed CREATED")
        for i2 in range(30):
            c2 = lam.get_function_configuration(
                FunctionName="justhodl-symbol-feed")
            if c2.get("State") == "Active" and \
                    c2.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(8)
        r2 = lam.invoke(FunctionName="justhodl-symbol-feed",
                        InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(sf_err=r2.get("FunctionError"),
               sf_out=r2["Payload"].read().decode()[:120])

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
                if ad == "fleet:finviz":
                    fams[ad] += 1
                if ad == "feed:symbol":
                    fams[ad] += 1
                src2 = str(r2.get("source") or "")
                if "(family)" in src2:
                    fams["src-family:" + src2.split(":")[0]] += 1
            rep.kv(total_live=live, **{k: n for k, n in fams.most_common()})
            checks = [("INTR family-src >=12", fams.get("src-family:bis", 0) >= 12),
                      ("FER family-src >=80", fams.get("src-family:imf", 0)
                       + fams.get("src-family:wb", 0) >= 80),
                      ("WB trio via src >=250", fams.get("src-family:wb", 0) >= 250),
                      ("fleet:finviz >= 600 (US subset of a GLOBAL book)",
                       fams.get("fleet:finviz", 0) >= 600),
                      ("symbol-feed >= 250", fams.get("feed:symbol", 0) >= 250),
                      ("total LIVE >= 3000", live >= 3000)]
            for tk in ("AAPL", "NVDA", "MSFT"):
                _e = idx.get(tk) or {}
                rep.log(f"  equity {tk}: {_e.get('status')} "
                        f"v={_e.get('value')} src={_e.get('source')}")
                checks.append((f"equity {tk} LIVE",
                               _e.get("status") == "LIVE"
                               and isinstance(_e.get("value"), (int, float))
                               and _e.get("value") > 5))
            for sym2, want, tol in (("ECONOMICS:BRINTR", 14.25, 0.06),
                                    ("ECONOMICS:PEINTR", 4.25, 0.06),
                                    ("ECONOMICS:BRFER", 368899, 12000)):
                _r2 = (idx.get(sym2)
                       or idx.get(sym2.split(":")[-1]) or {})
                got = _r2.get("value")
                rep.log(f"  spot {sym2}: got={got} "
                        f"src={_r2.get('source')}")
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
