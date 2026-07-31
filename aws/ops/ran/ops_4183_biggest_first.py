"""ops_4183 — BIGGEST-FIRST: joint PENDING+NFS econ census, candidate
probes (CPI extra masks, WB set, IMF PPI discovery), wire proven into
families v1.8 + vault v3.26.0, fire."""
import io
import json
import re
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


def wb_count(code):
    st, x = fetch("https://api.worldbank.org/v2/country/all/indicator/"
                  f"{code}?format=json&mrnev=1&per_page=400", 60)
    try:
        rows = [r for r in json.loads(x)[1] or []
                if r.get("value") is not None]
        return len(rows), (rows[0].get("date") if rows else "")
    except Exception:
        return 0, ""


def imf_countries(mask):
    st, x = fetch("https://api.imf.org/external/sdmx/2.1/data/CPI/"
                  f"{mask}?lastNObservations=1", 90)
    return len(set(re.findall(r'COUNTRY="([A-Z]{3})"', x)))


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


WB_CAND = {"EXP": ("NE.EXP.GNFS.CD", 1e6), "IMP": ("NE.IMP.GNFS.CD", 1e6),
           "EXTD": ("DT.DOD.DECT.CD", 1e6), "POP": ("SP.POP.TOTL", 1e6),
           "GDP": ("NY.GDP.MKTP.CD", 1e9),
           "GDPPC": ("NY.GDP.PCAP.CD", 1.0),
           "GNI": ("NY.GNP.MKTP.CD", 1e9),
           "MINR": ("", 1.0)}


def main():
    with report("4183_biggest_first") as rep:
        rep.heading("ops 4183 — biggest families first")
        checks = []

        rep.section("A. joint PENDING+NFS econ suffix census")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        sfx = Counter()
        for r in v.get("symbols") or []:
            if r.get("status") in ("PENDING_RESOLUTION",
                                   "NO_FREE_SOURCE"):
                m = re.match(r"^([A-Z]{2})([A-Z]{2,12})$",
                             str(r.get("symbol")))
                if m:
                    sfx[m.group(2)] += 1
        top = sfx.most_common(40)
        rep.log("  demand: " + json.dumps(dict(top))[:700])

        rep.section("B. probes for demanded candidates")
        proven_wb = {}
        for fam, (code, div) in WB_CAND.items():
            if sfx.get(fam, 0) < 8 or not code:
                continue
            n, yr = wb_count(code)
            rep.log(f"  WB {fam} {code}: {n} countries @{yr}")
            if n >= 100:
                proven_wb[fam] = (code, div)
        cpi_extra = {}
        if sfx.get("CPIMM", 0) >= 8 or sfx.get("INFMM", 0) >= 8:
            nm = imf_countries("..CP00.POP_PCH_PA_PT.M")
            rep.log(f"  CPI CPIMM mask countries: {nm}")
            if nm >= 60:
                cpi_extra["CPIMM"] = "..CP00.POP_PCH_PA_PT.M"
        ppi_flow = None
        st, xf = fetch("https://api.imf.org/external/sdmx/2.1/dataflow",
                       60)
        pids = sorted(set(re.findall(r'id="(PPI[A-Za-z0-9_]*)"', xf)))
        rep.log("  PPI-ish flows: " + json.dumps(pids[:6]))
        if pids and sfx.get("PPIYY", 0) >= 8:
            st2, x2 = fetch("https://api.imf.org/external/sdmx/2.1/data/"
                            f"{pids[0]}/all?detail=serieskeysonly", 120)
            trs = set(re.findall(
                r'TYPE_OF_TRANSFORMATION="([A-Za-z0-9_]+)"', x2))
            if "YOY_PCH_PA_PT" in trs:
                m2 = re.search(r"<Series[^>]*>", x2)
                order = list(dict(re.findall(
                    r'([A-Za-z0-9_]+)="[^"]*"', m2.group(0))))
                parts = ["M" if k.upper() == "FREQUENCY"
                         else ("YOY_PCH_PA_PT"
                               if k == "TYPE_OF_TRANSFORMATION" else "")
                         for k in order]
                mask = ".".join(parts)
                st3, x3 = fetch(
                    "https://api.imf.org/external/sdmx/2.1/data/"
                    f"{pids[0]}/{mask}?lastNObservations=1", 90)
                nc3 = len(set(re.findall(r'COUNTRY="([A-Z]{3})"', x3)))
                rep.log(f"  PPI {pids[0]} yoy mask countries: {nc3}")
                if nc3 >= 40:
                    ppi_flow = (pids[0], mask)
        rep.kv(wb_proven=json.dumps(list(proven_wb))[:100],
               cpi_extra=json.dumps(list(cpi_extra)),
               ppi=bool(ppi_flow))
        checks.append(("something proven",
                       bool(proven_wb or cpi_extra or ppi_flow)))
        if not (proven_wb or cpi_extra or ppi_flow):
            rep.fail("nothing proved — not wiring")
            sys.exit(1)

        rep.section("C. wire feed v1.8 + vault v3.26.0")
        fp = ROOT / "lambdas" / "justhodl-families-feed" / "source" / \
            "lambda_function.py"
        fs = fp.read_text()
        if "BIGWAVE ops4183" not in fs:
            anchor = '    out["FER"] = fer'
            assert fs.count(anchor) == 1
            block = "    # BIGWAVE ops4183\n"
            for fam, (code, div) in proven_wb.items():
                block += (
                    "    dW = {}\n"
                    "    tW = _fetch(\"https://api.worldbank.org/v2/"
                    "country/all/indicator/%s?format=json&mrnev=1"
                    "&per_page=400\")\n"
                    "    try:\n"
                    "        for rW in json.loads(tW)[1] or []:\n"
                    "            vW = rW.get(\"value\")\n"
                    "            cW = (rW.get(\"country\") or {})"
                    ".get(\"id\") or \"\"\n"
                    "            if vW is not None and len(cW) == 2:\n"
                    "                dW[cW.upper()] = [round(float(vW)"
                    " / %s, 2), \"wb:%%s\" %%\n"
                    "                                  rW.get(\"date\")]\n"
                    "    except Exception:\n"
                    "        pass\n"
                    "    out[%r] = dW\n" % (code, div, fam))
            for fam, mask in cpi_extra.items():
                block += (
                    "    d9 = {}\n"
                    "    t9 = _fetch(\"https://api.imf.org/external/"
                    "sdmx/2.1/data/CPI/%s?lastNObservations=1\")\n"
                    "    for b9 in re.split(r\"<Series[ >]\", t9)[1:]:\n"
                    "        a9 = re.search(r'COUNTRY=\"([A-Z]{3})\"', b9)\n"
                    "        v9 = re.findall(r'OBS_VALUE=\"([\\d\\.eE"
                    "\\+\\-]+)\"', b9)\n"
                    "        p9 = re.findall(r'TIME_PERIOD=\"([^\"]+)\"'"
                    ", b9)\n"
                    "        cm9 = inv3.get(a9.group(1)) if a9 else None\n"
                    "        if v9 and cm9:\n"
                    "            d9[cm9] = [round(float(v9[-1]), 2),\n"
                    "                       \"imf:\" + (p9[-1] if p9 "
                    "else \"m\")]\n"
                    "    out[%r] = d9\n" % (mask, fam))
            if ppi_flow:
                block += (
                    "    dP = {}\n"
                    "    tP = _fetch(\"https://api.imf.org/external/"
                    "sdmx/2.1/data/%s/%s?lastNObservations=1\")\n"
                    "    for bP in re.split(r\"<Series[ >]\", tP)[1:]:\n"
                    "        aP = re.search(r'COUNTRY=\"([A-Z]{3})\"', bP)\n"
                    "        vP = re.findall(r'OBS_VALUE=\"([\\d\\.eE"
                    "\\+\\-]+)\"', bP)\n"
                    "        pP = re.findall(r'TIME_PERIOD=\"([^\"]+)\"'"
                    ", bP)\n"
                    "        cmP = inv3.get(aP.group(1)) if aP else None\n"
                    "        if vP and cmP:\n"
                    "            dP[cmP] = [round(float(vP[-1]), 2),\n"
                    "                       \"imf:\" + (pP[-1] if pP "
                    "else \"m\")]\n"
                    "    out[\"PPIYY\"] = dP\n"
                    % ppi_flow)
            fs = fs.replace(anchor, block + anchor, 1)
            allnew = list(proven_wb) + list(cpi_extra) + \
                (["PPIYY"] if ppi_flow else [])
            fs = fs.replace('"GRES": {}',
                            '"GRES": {}' + "".join(
                                ', "%s": {}' % f for f in allnew), 1)
            fs = fs.replace(
                'MARKER = "families-feed v1.7 ops4181 gres"',
                'MARKER = "families-feed v1.8 ops4183 bigwave"', 1)
            import ast as _ast
            _ast.parse(fs)
            fp.write_text(fs)
        else:
            allnew = []
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", fs)
        checks.append(("feed v1.8 settled",
                       settle(rep, "justhodl-families-feed",
                              "families-feed v1.8 ops4183 bigwave",
                              buf.getvalue())))

        vp = ROOT / "lambdas" / "justhodl-tradingview" / "source" / \
            "lambda_function.py"
        vs = vp.read_text()
        if allnew and "|%s|" % allnew[0] not in vs:
            vs = vs.replace("|GRES|", "|GRES|" + "|".join(allnew) + "|", 1)
            addsrc = "".join('"%s": "wb-imf:%s", ' % (f, f)
                             for f in allnew)
            vs = vs.replace('src = {"GRES"',
                            'src = {' + addsrc + '"GRES"', 1)
            vs = vs.replace(
                'MARKER = "tradingview-vault v3.25.0 ops4181 '
                'gres-old-months"',
                'MARKER = "tradingview-vault v3.26.0 ops4183 bigwave"', 1)
            import ast as _ast
            _ast.parse(vs)
            vp.write_text(vs)
        vb = io.BytesIO()
        with zf.ZipFile(vb, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", vs)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        checks.append(("vault v3.26.0 settled",
                       settle(rep, "justhodl-tradingview",
                              "tradingview-vault v3.26.0 ops4183 bigwave",
                              vb.getvalue())))

        lam.invoke(FunctionName="justhodl-families-feed",
                   InvocationType="RequestResponse", Payload=b"{}")
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(**{k: c.get(k) for k in allnew})
        checks.append(("new families sum >= 150",
                       sum(c.get(k, 0) for k in allnew) >= 150))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"BIGWAVE WIRED — {allnew}")


if __name__ == "__main__":
    main()
