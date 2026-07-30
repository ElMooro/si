"""ops_4143 — MFS ROUND-2, discover->wire->deploy->gate in one pass.
XDC spots; DC private-credit code hunt; bulk-vs-perCountry decision;
families-feed v1.1 self-patch (LG/CBBS/M0); vault FAM_RX widen; gates."""
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
                   config=Config(read_timeout=150, retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
BASE = "https://api.imf.org/external/sdmx/2.1"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:150]


def series_ct(x):
    return x.count("<Series")


def settle(rep, name, mark, zb):
    for att in range(6):
        try:
            lam.update_function_code(FunctionName=name, ZipFile=zb,
                                     Publish=True)
            break
        except Exception as e:
            rep.log(f"  upd EXC {type(e).__name__}")
            time.sleep(8)
    for i in range(35):
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=name)["Code"]["Location"],
                    timeout=60).read())).read(
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
    with report("4143_round2_wire") as rep:
        rep.heading("ops 4143 — MFS round-2: discover, wire, gate")
        checks = []

        rep.section("A. XDC spots + DC code hunt")
        st, x = fetch(f"{BASE}/data/MFS_CBS/JPN.TA.XDC.M"
                      "?lastNObservations=2")
        ta_ok = series_ct(x) > 0
        v_ta = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', x)[:2]
        rep.log(f"  JPN.TA.XDC.M -> series={series_ct(x)} vals={v_ta}")
        st, x = fetch(f"{BASE}/data/MFS_CBS/JPN.MB.XDC.M"
                      "?lastNObservations=2")
        mb_ok = series_ct(x) > 0
        rep.log(f"  JPN.MB.XDC.M -> series={series_ct(x)} "
                f"vals={re.findall(chr(79)+'BS_VALUE=\"([0-9.eE+-]+)\"', x)[:2]}")
        checks.append(("CBS TA/MB XDC keys live", ta_ok and mb_ok))

        stS, xs = fetch(f"{BASE}/dataflow/IMF.STA/MFS_DC?references=all",
                        timeout=90)
        dc_codes = re.findall(
            r'<[A-Za-z]*:?Code\b[^>]*id="([A-Za-z0-9_\-]+)"'
            r'[\s\S]{0,400}?<[A-Za-z]*:?Name[^>]*>'
            r'([^<]*(?:Claims|Credit)[^<]*Private[^<]*\(DC\))<', xs)
        rep.log(f"  DC private candidates: {len(dc_codes)}")
        lg_code = None
        for cid, nm in dc_codes[:6]:
            st2, x2 = fetch(f"{BASE}/data/MFS_DC/JPN.{cid}.XDC.M"
                            "?lastNObservations=2")
            ok2 = series_ct(x2) > 0
            rep.log(f"    {cid} ({nm[:48]}) -> series={series_ct(x2)}")
            if ok2 and not lg_code:
                lg_code = cid
        checks.append(("DC private-credit code found", bool(lg_code)))

        rep.section("B. bulk wildcard test (blank COUNTRY)")
        stB, xb = fetch(f"{BASE}/data/MFS_CBS/.TA.XDC.M"
                        "?lastNObservations=1", timeout=90)
        nb = series_ct(xb)
        rep.kv(bulk_series=nb)
        bulk_ok = nb >= 40

        if not (ta_ok and mb_ok and lg_code):
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            rep.fail("grammar incomplete — not wiring")
            sys.exit(1)

        rep.section("C. families-feed v1.1 self-patch + deploy")
        fp = ROOT / "lambdas" / "justhodl-families-feed" / "source" / \
            "lambda_function.py"
        fs = fp.read_text()
        if "MFS ROUND2" not in fs:
            anchor = '    out["FER"] = fer'
            assert fs.count(anchor) == 1
            mode = "bulk" if bulk_ok else "percountry"
            block = (
                "    # MFS ROUND2 ops4143 — LG/CBBS/M0 via IMF.STA "
                "(COUNTRY.INDICATOR.XDC.M), mode=%s\n"
                "    inv3 = {v: k for k, v in iso23.items()}\n"
                "    def _mfs(flow, ind):\n"
                "        d = {}\n"
                "        if %r == \"bulk\":\n"
                "            t2 = _fetch(\"%s/data/\" + flow + \"/.\" + ind\n"
                "                        + \".XDC.M?lastNObservations=1\")\n"
                "            for blk in re.split(r\"<Series[ >]\", t2)[1:]:\n"
                "                a2 = re.search(r'COUNTRY=\"([A-Z]{3})\"', blk)\n"
                "                v2 = re.findall(r'OBS_VALUE=\"([\\d\\.eE\\+\\-]+)\"', blk)\n"
                "                tp2 = re.findall(r'TIME_PERIOD=\"([^\"]+)\"', blk)\n"
                "                if a2 and v2 and a2.group(1) in inv3:\n"
                "                    d[inv3[a2.group(1)]] = [round(float(v2[-1]), 1),\n"
                "                                            \"imf:\" + (tp2[-1] if tp2 else \"m\")]\n"
                "        else:\n"
                "            for c2, c3 in list(iso23.items()):\n"
                "                if time.time() - t0 > 220:\n"
                "                    break\n"
                "                t2 = _fetch(\"%s/data/\" + flow + \"/\" + c3 + \".\"\n"
                "                            + ind + \".XDC.M?lastNObservations=1\")\n"
                "                v2 = re.findall(r'OBS_VALUE=\"([\\d\\.eE\\+\\-]+)\"', t2)\n"
                "                tp2 = re.findall(r'TIME_PERIOD=\"([^\"]+)\"', t2)\n"
                "                if v2:\n"
                "                    d[c2] = [round(float(v2[-1]), 1),\n"
                "                             \"imf:\" + (tp2[-1] if tp2 else \"m\")]\n"
                "        return d\n"
                "    out[\"CBBS\"] = _mfs(\"MFS_CBS\", \"TA\")\n"
                "    out[\"M0\"] = _mfs(\"MFS_CBS\", \"MB\")\n"
                "    out[\"LG\"] = _mfs(\"MFS_DC\", %r)\n"
                % (mode, mode, BASE, BASE, lg_code))
            fs = fs.replace(anchor, block + anchor, 1)
            fs = fs.replace('out = {"INTR": {}, "FER": {}, "GDPYY": {}, '
                            '"IRYY": {}, "UR": {}}',
                            'out = {"INTR": {}, "FER": {}, "GDPYY": {}, '
                            '"IRYY": {}, "UR": {}, "LG": {}, "CBBS": {}, '
                            '"M0": {}}', 1)
            fs = fs.replace('MARKER = "families-feed v1.0 ops4121"',
                            'MARKER = "families-feed v1.1 ops4143 mfs"', 1)
            import ast as _ast
            _ast.parse(fs)
            fp.write_text(fs)
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", fs)
        try:
            lam.update_function_configuration(
                FunctionName="justhodl-families-feed", Timeout=300)
            time.sleep(6)
        except Exception:
            pass
        checks.append(("feed v1.1 settled",
                       settle(rep, "justhodl-families-feed",
                              "families-feed v1.1 ops4143 mfs",
                              buf.getvalue())))
        r = lam.invoke(FunctionName="justhodl-families-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(feed_err=r.get("FunctionError"))
        fd = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(**c)
        checks += [("LG >= 40", (c.get("LG") or 0) >= 40),
                   ("CBBS >= 40", (c.get("CBBS") or 0) >= 40),
                   ("M0 >= 40", (c.get("M0") or 0) >= 40)]

        rep.section("D. vault FAM_RX widen + settle + invoke")
        vp = ROOT / "lambdas" / "justhodl-tradingview" / "source" / \
            "lambda_function.py"
        vs = vp.read_text()
        if "LG|CBBS|M0" not in vs:
            vs = vs.replace("(INTR|FER|GDPYY|IRYY|UR)",
                            "(INTR|FER|GDPYY|IRYY|UR|LG|CBBS|M0)", 1)
            vs = vs.replace(
                'MARKER = "tradingview-vault v3.18.0 ops4136 symbol-feed"',
                'MARKER = "tradingview-vault v3.19.0 ops4143 mfs-families"', 1)
            vs = vs.replace('src = {"INTR": "bis:CBPOL",',
                            'src = {"LG": "imf:MFS_DC", "CBBS": '
                            '"imf:MFS_CBS TA", "M0": "imf:MFS_CBS MB",\n'
                            '           "INTR": "bis:CBPOL",', 1)
            import ast as _ast
            _ast.parse(vs)
            vp.write_text(vs)
        vb = io.BytesIO()
        with zf.ZipFile(vb, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", vs)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        checks.append(("vault v3.19.0 settled",
                       settle(rep, "justhodl-tradingview",
                              "tradingview-vault v3.19.0 ops4143 "
                              "mfs-families", vb.getvalue())))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired async — artifact lands ~t+610s; "
               "next op reads it")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"ROUND-2 WIRED — LG={c.get('LG')} CBBS={c.get('CBBS')} "
               f"M0={c.get('M0')} lg_code={lg_code} "
               f"mode={'bulk' if bulk_ok else 'percountry'}")


if __name__ == "__main__":
    main()
