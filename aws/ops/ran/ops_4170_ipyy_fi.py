"""ops_4170 — IPYY (self-computed yoy) + FI (CPI COICOP self-discovery)
+ label activation: probe, wire feed v1.6 + vault v3.24.0, fire."""
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


def fetch(url, timeout=70):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:150]


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
    with report("4170_ipyy_fi") as rep:
        rep.heading("ops 4170 — IPYY + FI + label activation")
        checks = []

        rep.section("A. IPYY: MEI index obs=13, yoy self-computed")
        st, x = fetch("https://api.db.nomics.world/v22/series/OECD/MEI/"
                      "JPN.PRINTO01.IXOBSA.M?observations=13")
        yoy = None
        try:
            d0 = json.loads(x)["series"]["docs"][0]
            vv = [z for z in d0["value"] if z is not None]
            if len(vv) >= 13:
                yoy = round((vv[-1] / vv[-13] - 1) * 100, 2)
        except Exception:
            pass
        rep.kv(jpn_ip_yoy=yoy)
        checks.append(("IPYY computable", yoy is not None))

        rep.section("B. FI: CPI Series attribute self-discovery")
        st, x = fetch("https://api.imf.org/external/sdmx/2.1/data/CPI/all"
                      "?detail=serieskeysonly", timeout=120)
        m = re.search(r"<Series[^>]*>", x)
        attrs = dict(re.findall(r'([A-Za-z0-9_]+)="([^"]*)"',
                                m.group(0))) if m else {}
        rep.log("  sample attrs: " + json.dumps(attrs)[:340])
        food_dim = None
        food_val = None
        for k in attrs:
            vals = set(re.findall(r'%s="([A-Za-z0-9_\.]+)"' % k,
                                  x[:400000]))
            hit = next((v2 for v2 in vals
                        if v2 in ("CP01", "CP01.1") or "FOOD" in v2), None)
            if hit:
                food_dim, food_val = k, hit
                break
        tr_dim = next((k for k in attrs if "TRANSFORM" in k.upper()), None)
        tr_vals = set(re.findall(r'%s="([A-Za-z0-9_]+)"' % tr_dim,
                                 x[:400000])) if tr_dim else set()
        tr_pick = next((v2 for v2 in ("PC_CP_A_PT", "PCH_CP_A_PT", "GY",
                                      "PA") if v2 in tr_vals), None)
        rep.kv(food_dim=food_dim, food_val=food_val,
               transform_dim=tr_dim, transform=tr_pick)
        fi_mask = None
        if food_dim and tr_pick and m:
            order = list(attrs)
            parts = []
            for k in order:
                if k == food_dim:
                    parts.append(food_val)
                elif k == tr_dim:
                    parts.append(tr_pick)
                elif k.upper() in ("FREQ", "FREQUENCY"):
                    parts.append("M")
                else:
                    parts.append("")
            fi_mask = ".".join(parts)
            st2, x2 = fetch("https://api.imf.org/external/sdmx/2.1/data/"
                            f"CPI/{fi_mask}?lastNObservations=1",
                            timeout=90)
            nc = len(set(re.findall(r'COUNTRY="([A-Z]{3})"', x2)))
            rep.kv(fi_mask=fi_mask, fi_bulk_countries=nc)
            if nc < 40:
                fi_mask = None
        checks.append(("FI mask proven >=40", bool(fi_mask)))

        if not (yoy is not None and fi_mask):
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            rep.fail("probes incomplete — not wiring")
            sys.exit(1)

        rep.section("C. wire feed v1.6 + vault v3.24.0")
        fp = ROOT / "lambdas" / "justhodl-families-feed" / "source" / \
            "lambda_function.py"
        fs = fp.read_text()
        if "IPYY-FI ops4170" not in fs:
            anchor = '    out["FER"] = fer'
            assert fs.count(anchor) == 1
            block = (
                "    # IPYY-FI ops4170\n"
                "    d7 = {}\n"
                "    t7 = _fetch(\"https://api.db.nomics.world/v22/series/"
                "OECD/MEI/.PRINTO01.IXOBSA.M?observations=13&limit=500\")\n"
                "    try:\n"
                "        for doc in json.loads(t7)[\"series\"][\"docs\"]:\n"
                "            cc3 = str(doc.get(\"series_code\", \"\")"
                ").split(\".\")[0]\n"
                "            vv7 = [z for z in doc.get(\"value\") or [] "
                "if z is not None]\n"
                "            pp7 = doc.get(\"period\") or []\n"
                "            c2m = inv3.get(cc3)\n"
                "            if len(vv7) >= 13 and c2m:\n"
                "                d7[c2m] = [round((vv7[-1] / vv7[-13] - 1)"
                " * 100, 2),\n"
                "                           \"oecd:\" + (pp7[-1] if pp7 "
                "else \"m\")]\n"
                "    except Exception:\n"
                "        pass\n"
                "    out[\"IPYY\"] = d7\n"
                "    d8 = {}\n"
                "    t8 = _fetch(\"https://api.imf.org/external/sdmx/2.1/"
                "data/CPI/%s?lastNObservations=1\")\n"
                "    for blk8 in re.split(r\"<Series[ >]\", t8)[1:]:\n"
                "        a8 = re.search(r'COUNTRY=\"([A-Z]{3})\"', blk8)\n"
                "        v8 = re.findall(r'OBS_VALUE=\"([\\d\\.eE\\+\\-]+)\"'"
                ", blk8)\n"
                "        tp8 = re.findall(r'TIME_PERIOD=\"([^\"]+)\"', blk8)\n"
                "        c2m = inv3.get(a8.group(1)) if a8 else None\n"
                "        if v8 and c2m:\n"
                "            d8[c2m] = [round(float(v8[-1]), 2),\n"
                "                       \"imf:\" + (tp8[-1] if tp8 else "
                "\"m\")]\n"
                "    out[\"FI\"] = d8\n" % fi_mask)
            fs = fs.replace(anchor, block + anchor, 1)
            fs = fs.replace('"TOT": {}, "CA": {}',
                            '"TOT": {}, "CA": {}, "IPYY": {}, "FI": {}', 1)
            fs = fs.replace(
                'MARKER = "families-feed v1.5 ops4167 mei"',
                'MARKER = "families-feed v1.6 ops4170 ipyy-fi"', 1)
            import ast as _ast
            _ast.parse(fs)
            fp.write_text(fs)
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", fs)
        checks.append(("feed v1.6 settled",
                       settle(rep, "justhodl-families-feed",
                              "families-feed v1.6 ops4170 ipyy-fi",
                              buf.getvalue())))

        vp = ROOT / "lambdas" / "justhodl-tradingview" / "source" / \
            "lambda_function.py"
        vs = vp.read_text()
        if "|IPYY|FI|" not in vs:
            vs = vs.replace("|TOT|CA|RSYY", "|TOT|CA|IPYY|FI|RSYY", 1)
            vs = vs.replace('src = {"RSYY"',
                            'src = {"IPYY": "oecd:MEI PRINTO01 yoy", '
                            '"FI": "imf:CPI food", "RSYY"', 1)
            vs = vs.replace(
                'MARKER = "tradingview-vault v3.23.1 ops4168 '
                'honest-labels"',
                'MARKER = "tradingview-vault v3.24.0 ops4170 ipyy-fi"', 1)
            import ast as _ast
            _ast.parse(vs)
            vp.write_text(vs)
        vb = io.BytesIO()
        with zf.ZipFile(vb, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", vs)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        checks.append(("vault v3.24.0 settled",
                       settle(rep, "justhodl-tradingview",
                              "tradingview-vault v3.24.0 ops4170 ipyy-fi",
                              vb.getvalue())))

        rep.section("D. invokes: families, symbol (bare-misses), vault")
        lam.invoke(FunctionName="justhodl-families-feed",
                   InvocationType="RequestResponse", Payload=b"{}")
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(IPYY=c.get("IPYY"), FI=c.get("FI"))
        checks += [("IPYY >= 25", (c.get("IPYY") or 0) >= 25),
                   ("FI >= 60", (c.get("FI") or 0) >= 60)]
        lam.invoke(FunctionName="justhodl-symbol-feed",
                   InvocationType="Event", Payload=b"{}")
        time.sleep(20)
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired — 4171 converts")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"IPYY+FI WIRED — mask={fi_mask}")


if __name__ == "__main__":
    main()
