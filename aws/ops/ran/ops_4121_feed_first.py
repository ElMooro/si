"""ops_4121 — families-feed live + vault feed-first + 2048MB, full chain."""
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
                   config=Config(read_timeout=150, retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
VMARK = "tradingview-vault v3.15.3 ops4121 feed-first"
FMARK = "families-feed v1.0 ops4121"


def zip_src(name):
    src = (ROOT / "lambdas" / name / "source" / "lambda_function.py").read_text()
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
        for sh in sorted((ROOT / "shared").glob("*.py")):
            z.writestr(sh.name, sh.read_text())
    return src, buf.getvalue()


def settle_once(rep, name, mark, zb, create_env=False):
    try:
        lam.get_function_configuration(FunctionName=name)
        exists = True
    except Exception:
        exists = False
    if not exists:
        donor = lam.get_function_configuration(
            FunctionName="justhodl-tv-workbench")
        lam.create_function(FunctionName=name, Runtime=donor["Runtime"],
                            Role=donor["Role"],
                            Handler="lambda_function.lambda_handler",
                            Code={"ZipFile": zb}, Timeout=120,
                            MemorySize=256, Publish=True)
        rep.ok(f"  {name} CREATED (donor role/runtime)")
    else:
        for att in range(6):
            try:
                lam.update_function_code(FunctionName=name, ZipFile=zb,
                                         Publish=True)
                rep.ok(f"  {name} update accepted (attempt {att})")
                break
            except Exception as e:
                rep.log(f"  update EXC: {type(e).__name__}: {str(e)[:120]}")
                time.sleep(10)
    for i in range(40):
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=name)["Code"]["Location"],
                    timeout=60).read())).read("lambda_function.py").decode()
                if mark in dep:
                    rep.ok(f"  {name} settled at loop {i}")
                    return True
        except Exception as e:
            rep.log(f"  [{i}] {type(e).__name__}")
        time.sleep(9)
    rep.fail(f"  {name} never settled")
    return False


def main():
    with report("4121_feed_first") as rep:
        rep.heading("ops 4121 — families-feed + vault feed-first")
        checks = []

        rep.section("A. families-feed: create/settle, invoke, verify")
        _, fzb = zip_src("justhodl-families-feed")
        checks.append(("feed settled",
                       settle_once(rep, "justhodl-families-feed", FMARK, fzb)))
        r = lam.invoke(FunctionName="justhodl-families-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(feed_fnerr=r.get("FunctionError"),
               feed_counts=r["Payload"].read().decode()[:160])
        fd = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(**c, feed_elapsed_s=fd.get("elapsed_s"))
        checks += [("feed INTR >=25", (c.get("INTR") or 0) >= 25),
                   ("feed FER >=120", (c.get("FER") or 0) >= 120),
                   ("feed WB trio >=600",
                    sum(c.get(k, 0) for k in ("GDPYY", "IRYY", "UR")) >= 600)]

        rep.section("B. vault: settle v3.15.3 + memory 2048")
        _, vzb = zip_src("justhodl-tradingview")
        checks.append(("vault settled",
                       settle_once(rep, "justhodl-tradingview", VMARK, vzb)))
        try:
            lam.update_function_configuration(
                FunctionName="justhodl-tradingview", MemorySize=2048)
            rep.ok("  memory -> 2048 (2x CPU)")
        except Exception as e:
            rep.log(f"  mem EXC: {type(e).__name__}: {str(e)[:100]}")
        time.sleep(12)

        rep.section("C. vault run + gates")
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        v = None
        for i in range(50):
            time.sleep(15)
            v = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
            if str(v.get("marker")) == VMARK:
                rep.ok(f"  artifact v3.15.3 after ~{(i+1)*15}s")
                break
        else:
            rep.fail("vault artifact never moved to v3.15.3")
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            sys.exit(1)

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
        checks += [("INTR >=25", fams.get("family:INTR", 0) >= 25),
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
        rep.ok(f"PASS_ALL — feed {c} | families {dict(fams)} | LIVE {live}")


if __name__ == "__main__":
    main()
