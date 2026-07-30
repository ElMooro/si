"""ops_4115 — deterministic: raw vault logs, then self-invoke + poll to
v3.15.0, then family gates. One op, one answer."""
import json
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=60, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
MARK = "tradingview-vault v3.15.0 ops4112 family-adapters"


def raw_tail(minutes, n):
    try:
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/justhodl-tradingview",
            startTime=int((datetime.now(timezone.utc) -
                           timedelta(minutes=minutes)).timestamp() * 1000),
            limit=200)
        return (ev.get("events") or [])[-n:]
    except Exception:
        return []


def main():
    with report("4115_family_deterministic") as rep:
        rep.heading("ops 4115 — deterministic family run")

        rep.section("A. raw vault log tail (last 75 min, unfiltered)")
        for e in raw_tail(75, 28):
            ts = datetime.fromtimestamp(e["timestamp"] / 1000,
                                        tz=timezone.utc).strftime("%H:%M:%S")
            rep.log(f"  [{ts}] {e['message'].strip()[:160]}")

        v = json.loads(s3.get_object(Bucket=BUCKET,
                                     Key="data/tradingview.json")["Body"].read())
        rep.kv(marker_before=str(v.get("marker"))[:56])

        if str(v.get("marker")) != MARK:
            rep.section("B. self-invoke + poll (11 min)")
            lam.invoke(FunctionName="justhodl-tradingview",
                       InvocationType="Event", Payload=b"{}")
            got = False
            for i in range(44):
                time.sleep(15)
                v = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
                if str(v.get("marker")) == MARK:
                    rep.ok(f"  v3.15.0 artifact after ~{(i+1)*15}s")
                    got = True
                    break
            if not got:
                rep.section("C. post-attempt raw tail")
                for e in raw_tail(15, 22):
                    ts = datetime.fromtimestamp(
                        e["timestamp"] / 1000,
                        tz=timezone.utc).strftime("%H:%M:%S")
                    rep.log(f"  [{ts}] {e['message'].strip()[:160]}")
                rep.fail("VAULT NEVER WROTE v3.15.0 — evidence above")
                sys.exit(1)

        fams = Counter()
        idx = {}
        live = 0
        for r in v.get("symbols") or []:
            idx[r.get("symbol")] = r
            if r.get("status") == "LIVE":
                live += 1
            ad = str(r.get("adapter") or "")
            if ad.startswith("family:"):
                fams[ad] += 1
        rep.kv(total_live=live, **{k: n for k, n in fams.most_common()})
        checks = [("INTR >=25", fams.get("family:INTR", 0) >= 25),
                  ("FER >=80", fams.get("family:FER", 0) >= 80),
                  ("WB trio >=250",
                   sum(fams.get("family:" + f, 0)
                       for f in ("GDPYY", "IRYY", "UR")) >= 250)]
        for sym, want, tol in (("ECONOMICS:BRINTR", 14.25, 0.06),
                               ("ECONOMICS:PEINTR", 4.25, 0.06),
                               ("ECONOMICS:BRFER", 368899, 12000)):
            got2 = (idx.get(sym) or {}).get("value")
            rep.log(f"  spot {sym}: got={got2} want~{want} "
                    f"src={(idx.get(sym) or {}).get('source')}")
            checks.append((f"spot {sym}",
                           got2 is not None and
                           abs(float(got2) - want) <= tol))
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — families {dict(fams)}, total LIVE {live}")


if __name__ == "__main__":
    main()
