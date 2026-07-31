"""ops_4167 — MEI WAVE self-drive: DBnomics tuple probes, bulk-mask test,
IMF CPI flow-id retry for FI; wire families v1.5 + vault on proof."""
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
DB = "https://api.db.nomics.world/v22/series/OECD/MEI/"

CAND = {"IPYY": [("PRINTO01", "GY"), ("PRINTO01", "GP")],
        "RSYY": [("SLRTTO01", "GY"), ("SLRTCR03", "GY")],
        "CU": [("BSCURT02", "STSA"), ("BSCURT02", "ST")],
        "CIR": [("CPGRLE01", "GY")],
        "INBR": [("IR3TIB01", "ST")],
        "LEI": [("LOLITOAA", "IXOBSA"), ("LOLITONO", "IXOBSA")],
        "UP": [("LFHUTTTT", "STSA"), ("LFHUTTTT", "ST")],
        "MPRYY": [("PIEAMP01", "GY"), ("PITGND01", "GY")]}


def fetch(url, timeout=50):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:150]


def latest_val(txt):
    try:
        j = json.loads(txt)
        docs = j["series"]["docs"]
        if not docs:
            return None, None
        d0 = docs[0]
        vals = [x for x in d0.get("value") or [] if x is not None]
        per = d0.get("period") or []
        if vals:
            return float(vals[-1]), (per[-1] if per else "?")
    except Exception:
        pass
    return None, None


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
    with report("4167_mei_wave") as rep:
        rep.heading("ops 4167 — MEI wave: probe, prove, wire")
        checks = []

        rep.section("A. tuple probes (JPN + USA)")
        picked = {}
        for fam, cands in CAND.items():
            for subj, meas in cands:
                ok = 0
                sample = None
                for cc in ("JPN", "USA"):
                    st, x = fetch(f"{DB}{cc}.{subj}.{meas}.M"
                                  "?observations=1")
                    v2, p2 = latest_val(x)
                    if v2 is not None:
                        ok += 1
                        sample = (cc, v2, p2)
                if ok == 2:
                    picked[fam] = (subj, meas)
                    rep.log(f"  {fam}: {subj}.{meas} ✓ "
                            f"sample={sample}")
                    break
            if fam not in picked:
                rep.log(f"  {fam}: no candidate answered")
        checks.append(("MEI tuples >= 5", len(picked) >= 5))

        rep.section("B. bulk mask test (all countries, one call)")
        subj, meas = next(iter(picked.values()))
        st, x = fetch(f"{DB}.{subj}.{meas}.M?observations=1"
                      "&limit=400", timeout=90)
        n_series = x.count('"series_code"')
        rep.kv(bulk_status=st, bulk_series=n_series)
        bulk_ok = st == 200 and n_series >= 30
        checks.append(("bulk mask works >=30", bulk_ok))

        rep.section("C. IMF CPI flow retry (FI)")
        st, x = fetch("https://api.imf.org/external/sdmx/2.1/dataflow",
                      timeout=60)
        cids = re.findall(r'id="(CPI[A-Za-z0-9_]*)"', x)
        rep.log("  CPI-ish flows: " + json.dumps(sorted(set(cids))[:8]))
        fi_ok = False
        for fid in sorted(set(cids))[:3]:
            st2, x2 = fetch("https://api.imf.org/external/sdmx/2.1/data/"
                            f"{fid}/all?detail=serieskeysonly",
                            timeout=120)
            foods = re.findall(
                r'INDICATOR="([A-Za-z0-9_]*(?:FOOD|CP01)[A-Za-z0-9_]*)"',
                x2)[:3]
            rep.log(f"  {fid}: keys={x2.count('<Series')} "
                    f"food-ish={foods}")
            if foods:
                fi_ok = True
        rep.kv(fi_path_found=fi_ok)

        if not (len(picked) >= 5 and bulk_ok):
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            rep.fail("probes incomplete — not wiring")
            sys.exit(1)

        rep.section("D. wire families-feed v1.5 (+MEI) + vault")
        fp = ROOT / "lambdas" / "justhodl-families-feed" / "source" / \
            "lambda_function.py"
        fs = fp.read_text()
        if "MEI WAVE" not in fs:
            anchor = '    out["FER"] = fer'
            assert fs.count(anchor) == 1
            tuples = json.dumps(picked)
            block = (
                "    # MEI WAVE ops4167 — DBnomics OECD/MEI bulk per "
                "family\n"
                "    _MEI = json.loads(%r)\n"
                "    for _fam, (_sub, _mea) in _MEI.items():\n"
                "        d5 = {}\n"
                "        t5 = _fetch(\"https://api.db.nomics.world/v22/"
                "series/OECD/MEI/.\"\n"
                "                    + _sub + \".\" + _mea + \".M"
                "?observations=1&limit=500\")\n"
                "        try:\n"
                "            for doc in json.loads(t5)[\"series\"]"
                "[\"docs\"]:\n"
                "                cc3 = str(doc.get(\"series_code\", \"\")"
                ").split(\".\")[0]\n"
                "                vv5 = [z for z in doc.get(\"value\") or []"
                " if z is not None]\n"
                "                pp5 = doc.get(\"period\") or []\n"
                "                c2m = inv3.get(cc3)\n"
                "                if vv5 and c2m:\n"
                "                    d5[c2m] = [round(float(vv5[-1]), 2),\n"
                "                               \"oecd:\" + (pp5[-1] if pp5"
                " else \"m\")]\n"
                "        except Exception:\n"
                "            pass\n"
                "        out[_fam] = d5\n"
                % tuples)
            fs = fs.replace(anchor, block + anchor, 1)
            newfams = "".join(', "%s": {}' % f for f in picked)
            fs = fs.replace('"TOT": {}, "CA": {}}',
                            '"TOT": {}, "CA": {}' + newfams + "}", 1)
            fs = fs.replace('MARKER = "families-feed v1.4 ops4162 wb-wave"',
                            'MARKER = "families-feed v1.5 ops4167 mei"', 1)
            import ast as _ast
            _ast.parse(fs)
            fp.write_text(fs)
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", fs)
        checks.append(("feed v1.5 settled",
                       settle(rep, "justhodl-families-feed",
                              "families-feed v1.5 ops4167 mei",
                              buf.getvalue())))
        r = lam.invoke(FunctionName="justhodl-families-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(feed_err=r.get("FunctionError"))
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(**{k: c.get(k) for k in picked})
        checks.append(("MEI families sum >= 150",
                       sum(c.get(k, 0) for k in picked) >= 150))

        vp = ROOT / "lambdas" / "justhodl-tradingview" / "source" / \
            "lambda_function.py"
        vs = vp.read_text()
        alt = "|".join(picked)
        if alt.split("|")[0] not in vs.split("FAM_RX")[1][:400]:
            vs = vs.replace(
                "(INTR|FER|GDPYY|IRYY|UR|LG|CBBS|M0|BM|GDG|BOT|DIR|LIR"
                "|TOT|CA)",
                "(INTR|FER|GDPYY|IRYY|UR|LG|CBBS|M0|BM|GDG|BOT|DIR|LIR"
                "|TOT|CA|" + alt + ")", 1)
            srcadd = "".join('"%s": "oecd:MEI %s", ' % (f, sm[0])
                             for f, sm in picked.items())
            vs = vs.replace('src = {"GDG": "wb:GC.DOD.TOTL.GD.ZS",',
                            'src = {' + srcadd
                            + '"GDG": "wb:GC.DOD.TOTL.GD.ZS",', 1)
            vs = vs.replace(
                'MARKER = "tradingview-vault v3.22.0 ops4162 wb-wave"',
                'MARKER = "tradingview-vault v3.23.0 ops4167 mei"', 1)
            import ast as _ast
            _ast.parse(vs)
            vp.write_text(vs)
        vb = io.BytesIO()
        with zf.ZipFile(vb, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", vs)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        checks.append(("vault v3.23.0 settled",
                       settle(rep, "justhodl-tradingview",
                              "tradingview-vault v3.23.0 ops4167 mei",
                              vb.getvalue())))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired async — converter next cycle")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"MEI WIRED — {list(picked)} fi_path={fi_ok}")


if __name__ == "__main__":
    main()
