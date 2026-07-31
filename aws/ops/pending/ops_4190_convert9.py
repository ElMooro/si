"""ops_4190 — convert: vault fire, class-wise LIVE ledger, realistic gates."""
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
                   config=Config(read_timeout=290,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"


def settle(rep, name, mark):
    src = (ROOT / "lambdas" / name / "source" /
           "lambda_function.py").read_text()
    assert mark in src
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
    with report("4190_convert9") as rep:
        rep.heading("ops 4165 — convert the chewed queue")
        from collections import Counter
        from datetime import datetime, timezone
        checksA = settle(rep, "justhodl-tradingview",
                         "tradingview-vault v3.26.0 ops4187 config-driven")
        if not checksA:
            sys.exit(1)
        t_op = datetime.now(timezone.utc)
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        v = None
        got = False
        for i in range(48):
            time.sleep(15)
            v = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
            try:
                if datetime.fromisoformat(
                        str(v.get("generated_at"))) > t_op:
                    rep.ok(f"  artifact after ~{(i+1)*15}s")
                    got = True
                    break
            except Exception:
                pass
        if not got:
            rep.fail("artifact never refreshed post-invoke")
            sys.exit(1)
        live = 0
        ad = Counter()
        st = Counter()
        for r2 in v.get("symbols") or []:
            st[r2.get("status")] += 1
            if r2.get("status") == "LIVE":
                live += 1
                ad[str(r2.get("adapter") or
                       r2.get("resolved_via") or "native")[:22]] += 1
        rep.kv(total_live=live)
        rep.log("  statuses: " + json.dumps(dict(st.most_common(6))))
        reasons = Counter()
        for r3 in v.get("symbols") or []:
            if r3.get("status") == "NO_FREE_SOURCE":
                reasons[str(r3.get("resolution_note"))[:44]] += 1
        rep.log("  NFS reasons: " + json.dumps(
            dict(reasons.most_common(6)))[:400])
        lab = sum(n for r5, n in reasons.items()
                  if "no free mirror" in r5)
        bm = sum(n for r5, n in reasons.items()
                 if "back-month" in r5)
        rep.kv(backmonth_labeled=bm)
        rep.kv(honest_label_rows=lab)
        if lab < 300:
            rep.fail(f"label wave inert: {lab}")
            sys.exit(1)
        rep.log("  LIVE by adapter: " + json.dumps(
            dict(ad.most_common(12)))[:400])
        if live < 4640:
            rep.fail(f"LIVE {live} < 4640")
            sys.exit(1)
        rep.ok(f"CONVERTED — LIVE {live}")


if __name__ == "__main__":
    main()
