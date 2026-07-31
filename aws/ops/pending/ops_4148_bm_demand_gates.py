"""ops_4148 — loud vault settle for v3.19.0 + async fire + gates on the
eight-family artifact."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from collections import Counter
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tradingview"
MARK = "tradingview-vault v3.20.0 ops4148 broad-money"


def main():
    with report("4148_bm_demand_gates") as rep:
        rep.heading("ops 4146 — v3.19.0 settle, fire, gate")
        rep.section("A. feed v1.3: settle + invoke + counts")
        fsrc = (ROOT / "lambdas" / "justhodl-families-feed" / "source" /
                "lambda_function.py").read_text()
        fb = io.BytesIO()
        with zf.ZipFile(fb, "w", zf.ZIP_DEFLATED) as z2:
            z2.writestr("lambda_function.py", fsrc)
        for att in range(5):
            try:
                lam.update_function_code(FunctionName="justhodl-families-feed",
                                         ZipFile=fb.getvalue(), Publish=True)
                break
            except Exception:
                time.sleep(8)
        for i2 in range(30):
            c2 = lam.get_function_configuration(
                FunctionName="justhodl-families-feed")
            if c2.get("State") == "Active" and \
                    c2.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(8)
        rf = lam.invoke(FunctionName="justhodl-families-feed",
                        InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(feed_err=rf.get("FunctionError"))
        fd = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/families.json")["Body"].read())
        rep.kv(**(fd.get("counts") or {}))

        src = (ROOT / "lambdas" / FN / "source" /
               "lambda_function.py").read_text()
        assert MARK in src and "LG|CBBS|M0" in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        for att in range(6):
            try:
                lam.update_function_code(FunctionName=FN,
                                         ZipFile=buf.getvalue(),
                                         Publish=True)
                rep.ok(f"  update accepted (attempt {att})")
                break
            except Exception as e:
                rep.log(f"  upd EXC: {type(e).__name__}: {str(e)[:130]}")
                time.sleep(10)
        ok = False
        for i in range(40):
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                st = (c.get("State"), c.get("LastUpdateStatus"),
                      str(c.get("LastUpdateStatusReason"))[:50])
                if i % 4 == 0:
                    rep.log(f"  [{i}] {st}")
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") == "Successful":
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                        lam.get_function(FunctionName=FN)["Code"]
                        ["Location"], timeout=60).read())).read(
                        "lambda_function.py").decode()
                    if MARK in dep:
                        ok = True
                        rep.ok(f"  settled at loop {i}")
                        break
                    rep.log(f"  [{i}] Successful but marker absent")
            except Exception as e:
                rep.log(f"  [{i}] cfg EXC {type(e).__name__}")
            time.sleep(9)
        if not ok:
            rep.fail("v3.19.0 never settled — refusals above")
            sys.exit(1)

        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        v = None
        for i in range(50):
            time.sleep(15)
            v = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
            if str(v.get("marker")) == MARK:
                rep.ok(f"  artifact v3.19.0 after ~{(i+1)*15}s")
                break
        else:
            rep.fail("artifact never moved to v3.19.0")
            sys.exit(1)

        idx = {}
        live = 0
        fam = Counter()
        for r in v.get("symbols") or []:
            idx[r.get("symbol")] = r
            if r.get("status") == "LIVE":
                live += 1
            src2 = str(r.get("source") or "")
            if "(family)" in src2:
                fam[src2.split(":")[0]] += 1
        rep.kv(total_live=live, **{("src-" + k): n
                                   for k, n in fam.most_common()})
        import re as _re
        checks = [("total LIVE >= 3300", live >= 3300),
                  ("BM feed >= 100",
                   (fd.get("counts") or {}).get("BM", 0) >= 100)]
        for famc in ("LG", "CBBS", "M0", "BM"):
            rx = _re.compile(r"^(?:ECONOMICS:)?([A-Z]{2})%s$" % famc)
            watched = [sy for sy in idx if rx.match(str(sy))]
            served = [sy for sy in watched
                      if (idx[sy] or {}).get("status") == "LIVE"]
            rep.log(f"  demand {famc}: watched={len(watched)} "
                    f"served={len(served)}")
            checks.append((f"{famc} demand-served >=50%",
                           not watched
                           or len(served) >= 0.5 * len(watched)))
        for sym, lo in (("BRLG", 1e12), ("BRCBBS", 1e11),
                        ("JPM0", 1e14), ("BRBM", 1e11)):
            r2 = idx.get(sym) or idx.get("ECONOMICS:" + sym) or {}
            val = r2.get("value")
            rep.log(f"  spot {sym}: {r2.get('status')} v={val} "
                    f"src={r2.get('source')}")
            checks.append((f"spot {sym} LIVE&plausible",
                           r2.get("status") == "LIVE"
                           and isinstance(val, (int, float))
                           and val > lo))
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — eight families in the artifact; LIVE {live}")


if __name__ == "__main__":
    main()
