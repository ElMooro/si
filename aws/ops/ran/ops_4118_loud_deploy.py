"""ops_4118 — deterministic deploy with NOTHING silent: current-zip truth,
pipeline-layout zip (source + shared), exceptions printed, settle with
state prints, then invoke+poll+family gates."""
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
                   config=Config(read_timeout=60, retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tradingview"
MARK = "tradingview-vault v3.15.2 ops4117 indent-true"


def deployed_text():
    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
    zb = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
    return zb, zb.read("lambda_function.py").decode()


def main():
    with report("4118_loud_deploy") as rep:
        rep.heading("ops 4118 — loud deterministic deploy")

        rep.section("A. what is deployed RIGHT NOW")
        zb, cur = deployed_text()
        import re as _re
        mm = _re.search(r'MARKER = "([^"]+)"', cur)
        rep.kv(current_marker=(mm.group(1) if mm else "?")[:60],
               zip_files=len(zb.namelist()))
        rep.log("  names: " + ", ".join(zb.namelist()[:12]))

        rep.section("B. build pipeline-layout zip + update, LOUDLY")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        rep.kv(new_zip_files=len(zf.ZipFile(io.BytesIO(buf.getvalue()))
                                 .namelist()))
        ok = False
        for i in range(50):
            try:
                lam.update_function_code(FunctionName=FN,
                                         ZipFile=buf.getvalue(), Publish=True)
                rep.log(f"  [{i}] update_function_code ACCEPTED")
            except Exception as e:
                rep.log(f"  [{i}] update EXC: {type(e).__name__}: "
                        f"{str(e)[:140]}")
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                st = (c.get("State"), c.get("LastUpdateStatus"),
                      str(c.get("LastUpdateStatusReason"))[:60])
                if i % 5 == 0:
                    rep.log(f"  [{i}] state={st}")
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") == "Successful":
                    _, dep = deployed_text()
                    if MARK in dep:
                        ok = True
                        rep.ok(f"  settled at loop {i}")
                        break
                    rep.log(f"  [{i}] Active/Successful but marker absent "
                            f"(deploy race?)")
            except Exception as e:
                rep.log(f"  [{i}] config EXC: {type(e).__name__}")
            time.sleep(9)
        if not ok:
            rep.fail("v3.15.2 never settled — every refusal printed above")
            sys.exit(1)

        rep.section("C. invoke + poll + gates")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        v = None
        for i in range(46):
            time.sleep(15)
            v = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
            if str(v.get("marker")) == MARK:
                rep.ok(f"  artifact v3.15.2 after ~{(i+1)*15}s")
                break
        else:
            rep.fail("artifact never moved to v3.15.2")
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
            got = (idx.get(sym) or {}).get("value")
            rep.log(f"  spot {sym}: got={got} "
                    f"src={(idx.get(sym) or {}).get('source')}")
            checks.append((f"spot {sym}",
                           got is not None and
                           abs(float(got) - want) <= tol))
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — families {dict(fams)}, total LIVE {live}")


if __name__ == "__main__":
    main()
