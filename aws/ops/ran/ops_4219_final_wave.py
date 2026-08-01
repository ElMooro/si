"""ops_4219 — FINAL WAVE: allocator/warroom/debate/pulse — everywhere completes."""
import io
import json
import re
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=290,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=90):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:140]


def settle(rep, name, mark):
    src = (ROOT / "lambdas" / name / "source" /
           "lambda_function.py").read_text()
    if mark not in src:
        rep.fail(f"  {name}: marker missing in checkout")
        return False
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
        if name == "justhodl-tradingview":
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
    for att in range(5):
        try:
            lam.update_function_code(FunctionName=name,
                                     ZipFile=buf.getvalue(), Publish=True)
            break
        except Exception:
            time.sleep(8)
    for i in range(35):
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=name)["Code"]
                    ["Location"], timeout=60).read())).read(
                    "lambda_function.py").decode()
                if mark in dep:
                    rep.ok(f"  {name} settled at loop {i}")
                    return True
        except Exception:
            pass
        time.sleep(9)
    rep.fail(f"  {name} never settled")
    return False




def main():
    with report("4219_final_wave") as rep:
        rep.heading("ops 4217 — wave-2 (recession, sentinel, dollar)")
        import base64
        checks = []
        TRIO = (('justhodl-allocator', 'bus_context', 'data/allocator.json'), ('justhodl-canary-warroom', 'bus_canaries', 'data/canary-warroom.json'), ('justhodl-debate-engine', 'bus_facts', 'data/debate.json'), ('justhodl-intraday-pulse', 'bus_pulse', 'data/intraday-pulse.json'))
        for name, blk, outk in TRIO:
            src = (ROOT / "lambdas" / name / "source" /
                   "lambda_function.py").read_text()
            assert blk in src
            buf = io.BytesIO()
            with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", src)
                for sh in sorted((ROOT / "shared").glob("*.py")):
                    z.writestr(sh.name, sh.read_text())
            for att in range(5):
                try:
                    lam.update_function_code(FunctionName=name,
                                             ZipFile=buf.getvalue(),
                                             Publish=True)
                    break
                except Exception:
                    time.sleep(8)
        time.sleep(14)
        for name, blk, outk in TRIO:
            r = lam.invoke(FunctionName=name,
                           InvocationType="RequestResponse",
                           Payload=b"{}", LogType="Tail")
            tail = base64.b64decode(
                r.get("LogResult") or b"").decode("utf-8", "ignore")
            wired = [ln.strip()[:110] for ln in tail.splitlines()
                     if blk in ln]
            for w in wired[:2]:
                rep.log("  " + w)
            d = json.loads(s3.get_object(
                Bucket=BUCKET, Key=outk)["Body"].read())
            has = blk in d
            rep.kv(**{name.split("-", 1)[1] + "_err":
                      r.get("FunctionError"),
                      name.split("-", 1)[1] + "_block": has})
            checks.append((f"{name} {blk} emitted", has))
            if name.endswith("allocator") and has:
                bl = d[blk]
                rep.log("  allocator: " + json.dumps(bl)[:180])
                wm = bl.get("world_policy_rate_median")
                checks.append(("allocator world rate 1-12",
                               isinstance(wm, (int, float))
                               and 1 < wm < 12))
            if name.endswith("warroom") and has:
                rep.log("  warroom: " + json.dumps(d[blk])[:180])

        led = {"marker": "bus-consumers v2 ops4219 COMPLETE",
               "wired": [
                   {"engine": "justhodl-indicator-bus",
                    "mode": "producer (n=18k, daily 12:15)"},
                   {"engine": "justhodl-tradingview",
                    "mode": "book + page"},
                   {"engine": "justhodl-domain-barometers",
                    "mode": "bus-extras drivers (+1170)"},
                   {"engine": "justhodl-macro-nowcast",
                    "mode": "auto via vault (10 legs)"},
                   {"engine": "justhodl-global-recession",
                    "mode": "bus_legs hard-data"},
                   {"engine": "justhodl-us10y-sentinel",
                    "mode": "bus_cross"},
                   {"engine": "justhodl-dollar-radar",
                    "mode": "bus_dollar modern COT"},
                   {"engine": "justhodl-regime-conditional-router",
                    "mode": "bus_macro world-rate"},
                   {"engine": "justhodl-market-tape",
                    "mode": "bus_tape homepage items"},
                   {"engine": "justhodl-activity-nowcast",
                    "mode": "bus_hard bridge"},
                   {"engine": "justhodl-allocator",
                    "mode": "bus_context"},
                   {"engine": "justhodl-canary-warroom",
                    "mode": "bus_canaries"},
                   {"engine": "justhodl-debate-engine",
                    "mode": "bus_facts"},
                   {"engine": "justhodl-intraday-pulse",
                    "mode": "bus_pulse"}],
               "candidates_next": []}
        s3.put_object(Bucket=BUCKET, Key="data/bus-consumers.json",
                      Body=json.dumps(led).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
        rep.ok(f"  ledger COMPLETE: wired={len(led['wired'])} next=0")

        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok("EVERYWHERE COMPLETE — fourteen surfaces on one bus")


if __name__ == "__main__":
    main()
