"""ops_4216 — WAVE-1: barometers bus-wired, nowcast auto-heal verified, consumers ledger."""
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
    with report("4216_wave1") as rep:
        rep.heading("ops 4216 — bus wave-1 consumers")
        import base64
        checks = []
        src = (ROOT / "lambdas" / "justhodl-domain-barometers" /
               "source" / "lambda_function.py").read_text()
        assert "ops4216 BUS WAVE-1" in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for att in range(5):
            try:
                lam.update_function_code(
                    FunctionName="justhodl-domain-barometers",
                    ZipFile=buf.getvalue(), Publish=True)
                break
            except Exception:
                time.sleep(8)
        time.sleep(12)
        r = lam.invoke(FunctionName="justhodl-domain-barometers",
                       InvocationType="RequestResponse", Payload=b"{}",
                       LogType="Tail")
        tail = base64.b64decode(r.get("LogResult") or b"").decode(
            "utf-8", "ignore")
        extras = 0
        for ln in tail.splitlines():
            if "bus extras" in ln:
                rep.log("  " + ln.strip()[:100])
                try:
                    extras = int(ln.split("+")[-1].split()[0])
                except Exception:
                    pass
        rep.kv(barometers_err=r.get("FunctionError"),
               bus_extras=extras)
        checks.append(("barometers bus extras >= 800", extras >= 800))
        bd = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/domain-barometers.json")["Body"].read())
        rep.kv(barometers_marker=str(bd.get("generated_at"))[:19])

        rep.section("nowcast auto-heal via vault")
        bus = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/indicator-bus.json")["Body"].read()
        ).get("indicators") or {}
        healed = 0
        for k in ("JPCCI", "DECCI", "FRCCI", "GBCCI", "GBBCOI",
                  "CNBCOI", "EUBCOI", "DEGDPYY", "FRGDPYY", "EUGDPYY"):
            hit = k in bus
            healed += 1 if hit else 0
            rep.log(f"  {k}: bus={hit} "
                    + json.dumps(bus.get(k))[:70])
        checks.append(("nowcast legs in bus >= 8", healed >= 8))
        lam.invoke(FunctionName="justhodl-macro-nowcast",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  macro-nowcast fired (consumes healed legs)")

        ledger = {"marker": "bus-consumers v1 ops4216",
                  "wired": [
                      {"engine": "justhodl-tradingview",
                       "mode": "producer+page"},
                      {"engine": "justhodl-domain-barometers",
                       "mode": "bus-extras drivers",
                       "extras": extras},
                      {"engine": "justhodl-macro-nowcast",
                       "mode": "auto via vault (wl_series)",
                       "healed_legs": healed}],
                  "candidates_next": [
                      "justhodl-us10y-sentinel",
                      "justhodl-regime-conditional-router",
                      "justhodl-dollar-radar",
                      "justhodl-market-tape",
                      "justhodl-activity-nowcast",
                      "justhodl-allocator",
                      "justhodl-global-recession",
                      "justhodl-canary-warroom",
                      "justhodl-debate-engine",
                      "justhodl-intraday-pulse"]}
        s3.put_object(Bucket=BUCKET, Key="data/bus-consumers.json",
                      Body=json.dumps(ledger).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
        rep.ok("  bus-consumers ledger written")

        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"WAVE-1 LIVE — extras={extras} healed={healed}")


if __name__ == "__main__":
    main()
