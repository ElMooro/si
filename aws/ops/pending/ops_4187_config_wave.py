"""ops_4187 — config-driven wave: probe demand-true candidates, PUT
family-defs.json (data, not code), settle feed v2.0 + vault v3.26.0,
invoke, gate, fire."""
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


def settle(rep, name, mark, zb):
    for att in range(5):
        try:
            lam.update_function_code(FunctionName=name, ZipFile=zb,
                                     Publish=True)
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
    with report("4187_config_wave") as rep:
        rep.heading("ops 4187 — config-driven demand wave")
        checks = []
        defs = {}

        st, x = fetch("https://api.worldbank.org/v2/country/all/"
                      "indicator/BN.CAB.XOKA.GD.ZS?format=json&mrnev=1"
                      "&per_page=400", 60)
        try:
            n = sum(1 for r in json.loads(x)[1] or []
                    if r.get("value") is not None)
        except Exception:
            n = 0
        rep.log(f"  CAG wb: {n}")
        if n >= 100:
            defs["CAG"] = {"kind": "wb", "code": "BN.CAB.XOKA.GD.ZS",
                           "div": 1}

        MEIC = {"BCOI": [("BSCICP03", "STSA"), ("BSCICP03", "ST"),
                         ("BSCICP03", "IXNSA")],
                "CCI": [("CSCICP03", "STSA"), ("CSCICP03", "ST")],
                "IPRI": [("PRINTO01", "IXOBSA")],
                "LEI": [("LOLITOAA", "ST"), ("LOLITOAA", "IXNSA"),
                        ("LOLITONO", "ST")],
                "CU": [("BSCURT02", "ST"), ("BSCURT02", "STSA"),
                       ("BSCURT02", "IXOBSA")]}
        for fam, cands in MEIC.items():
            for subj, meas in cands:
                ok2 = 0
                for cc in ("JPN", "USA"):
                    st4, x4 = fetch(
                        "https://api.db.nomics.world/v22/series/OECD/"
                        f"MEI/{cc}.{subj}.{meas}.M?observations=1", 45)
                    try:
                        d4 = json.loads(x4)["series"]["docs"][0]
                        if [z for z in d4["value"] if z is not None]:
                            ok2 += 1
                    except Exception:
                        pass
                if ok2 == 2:
                    defs[fam] = {"kind": "mei", "subject": subj,
                                 "measure": meas}
                    rep.log(f"  MEI {fam}: {subj}.{meas} ✓")
                    break
            if fam not in defs:
                rep.log(f"  MEI {fam}: none answered")

        st2, x2 = fetch("https://api.imf.org/external/sdmx/2.1/data/"
                        "PPI/all?detail=serieskeysonly", 120)
        trs = set(re.findall(
            r'TYPE_OF_TRANSFORMATION="([A-Za-z0-9_]+)"', x2))
        m2 = re.search(r"<Series[^>]*>", x2)
        if "YOY_PCH_PA_PT" in trs and m2:
            order = list(dict(re.findall(
                r'([A-Za-z0-9_]+)="[^"]*"', m2.group(0))))
            parts = ["M" if k.upper() == "FREQUENCY"
                     else ("YOY_PCH_PA_PT"
                           if k == "TYPE_OF_TRANSFORMATION" else "")
                     for k in order]
            mask = ".".join(parts)
            st3, x3 = fetch("https://api.imf.org/external/sdmx/2.1/data/"
                            f"PPI/{mask}?lastNObservations=1", 90)
            nc3 = len(set(re.findall(r'COUNTRY="([A-Z]{3})"', x3)))
            rep.log(f"  PPI MPRYY countries: {nc3}")
            if nc3 >= 40:
                defs["MPRYY"] = {"kind": "imf_mask", "flow": "PPI",
                                 "mask": mask}
        rep.kv(defs=json.dumps(list(defs)))
        checks.append(("defs proven >= 3", len(defs) >= 3))
        if len(defs) < 3:
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            rep.fail("insufficient — not wiring")
            sys.exit(1)

        s3.put_object(Bucket=BUCKET, Key="data/family-defs.json",
                      Body=json.dumps({"marker": "family-defs v1 ops4187",
                                       "families": defs}).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
        rep.ok(f"  family-defs.json written ({len(defs)})")

        for name, mark in (("justhodl-families-feed",
                            "families-feed v2.0 ops4187 config-driven"),
                           ("justhodl-tradingview",
                            "tradingview-vault v3.26.0 ops4187 "
                            "config-driven")):
            src = (ROOT / "lambdas" / name / "source" /
                   "lambda_function.py").read_text()
            assert mark in src
            buf = io.BytesIO()
            with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", src)
                if name == "justhodl-tradingview":
                    for sh in sorted((ROOT / "shared").glob("*.py")):
                        z.writestr(sh.name, sh.read_text())
            checks.append((f"{name} settled",
                           settle(rep, name, mark, buf.getvalue())))

        lam.invoke(FunctionName="justhodl-families-feed",
                   InvocationType="RequestResponse", Payload=b"{}")
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(**{k: c.get(k) for k in defs})
        checks.append(("new families sum >= 120",
                       sum(c.get(k, 0) for k in defs) >= 120))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"CONFIG WAVE — {list(defs)}")


if __name__ == "__main__":
    main()
