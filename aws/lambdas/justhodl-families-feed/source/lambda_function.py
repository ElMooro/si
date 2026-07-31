"""justhodl-families-feed v1.0 ops4121 — the five family bulk pulls on
their own clock, decoupled from the vault's 900s ceiling.
INTR=BIS CBPOL · FER=IMF IRFCL RAF_USD (WB fallback, USD mn) ·
GDPYY/IRYY/UR=World Bank mrnev. Writes data/families.json."""
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

MARKER = "families-feed v1.4 ops4162 wb-wave"
S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"


def _fetch(url):
    urls = [url]
    if "api.imf.org" in url:
        urls.append("https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
                    + urllib.request.quote(url, safe=""))
    for u in urls:
        try:
            req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception:
            continue
    return ""


def _sdmx(t, alen):
    d = {}
    for blk in re.split(r"<Series[ >]", t)[1:]:
        a = re.search(r'(?:REF_AREA|COUNTRY)="([A-Z0-9]{%d})"' % alen, blk)
        v = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', blk)
        tp = re.findall(r'TIME_PERIOD="([^"]+)"', blk)
        if a and v:
            try:
                d[a.group(1)] = [float(v[-1]), tp[-1] if tp else "latest"]
            except Exception:
                pass
    return d


def lambda_handler(event, context):
    t0 = time.time()
    out = {"INTR": {}, "FER": {}, "GDPYY": {}, "IRYY": {}, "UR": {}, "LG": {}, "CBBS": {}, "M0": {}, "BM": {}, "GDG": {}, "BOT": {}, "DIR": {}, "LIR": {}, "TOT": {}, "CA": {}}
    iso23 = {}
    for fam, code in (("GDPYY", "NY.GDP.MKTP.KD.ZG"),
                      ("IRYY", "FP.CPI.TOTL.ZG"),
                      ("UR", "SL.UEM.TOTL.ZS"),
                      ("GDG", "GC.DOD.TOTL.GD.ZS"),
                      ("BOT", "BN.GSR.GNFS.CD"),
                      ("DIR", "FR.INR.DPST"),
                      ("LIR", "FR.INR.LEND"),
                      ("TOT", "TT.PRI.MRCH.XD.WD"),
                      ("CA", "BN.CAB.XOKA.CD"),
                      ("FERWB", "FI.RES.TOTL.CD")):
        t = _fetch("https://api.worldbank.org/v2/country/all/indicator/"
                   f"{code}?format=json&mrnev=1&per_page=400")
        try:
            rows = json.loads(t)[1] or []
        except Exception:
            rows = []
        d = {}
        for r in rows:
            v = r.get("value")
            c2 = (r.get("country") or {}).get("id") or ""
            if v is None or len(c2) != 2:
                continue
            vv = float(v)
            if fam == "FERWB":
                vv = round(vv / 1e6, 1)
            d[c2.upper()] = [round(vv, 2), "wb:%s" % r.get("date")]
            c3 = r.get("countryiso3code")
            if c3:
                iso23[c2.upper()] = c3
        if fam == "FERWB":
            ferwb = d
        else:
            out[fam] = d
    t = _fetch("https://stats.bis.org/api/v1/data/WS_CBPOL/D../all"
               "?lastNObservations=1")
    out["INTR"] = {k: [v[0], "bis:" + v[1]] for k, v in _sdmx(t, 2).items()}
    inv = {v: k for k, v in iso23.items()}
    t = _fetch("https://api.imf.org/external/sdmx/2.1/data/IRFCL/"
               "M..RAF_USD?lastNObservations=1")
    fer = {}
    for a3, (vv, tp) in _sdmx(t, 3).items():
        if a3 in inv:
            fer[inv[a3]] = [round(vv, 1), "imf:" + tp]
    for cc, rec in ferwb.items():
        fer.setdefault(cc, rec)
    # MFS ROUND2 ops4143 — LG/CBBS/M0 via IMF.STA (COUNTRY.INDICATOR.XDC.M), mode=bulk
    inv3 = {v: k for k, v in iso23.items()}
    def _mfs(flow, ind):
        d = {}
        if 'bulk' == "bulk":
            t2 = _fetch("https://api.imf.org/external/sdmx/2.1/data/" + flow + "/." + ind
                        + ".XDC.M?lastNObservations=1")
            for blk in re.split(r"<Series[ >]", t2)[1:]:
                a2 = re.search(r'COUNTRY="([A-Z]{3})"', blk)
                v2 = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', blk)
                tp2 = re.findall(r'TIME_PERIOD="([^"]+)"', blk)
                if a2 and v2 and a2.group(1) in inv3:
                    d[inv3[a2.group(1)]] = [round(float(v2[-1]), 1),
                                            "imf:" + (tp2[-1] if tp2 else "m")]
        else:
            for c2, c3 in list(iso23.items()):
                if time.time() - t0 > 220:
                    break
                t2 = _fetch("https://api.imf.org/external/sdmx/2.1/data/" + flow + "/" + c3 + "."
                            + ind + ".XDC.M?lastNObservations=1")
                v2 = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', t2)
                tp2 = re.findall(r'TIME_PERIOD="([^"]+)"', t2)
                if v2:
                    d[c2] = [round(float(v2[-1]), 1),
                             "imf:" + (tp2[-1] if tp2 else "m")]
        return d
    out["CBBS"] = _mfs("MFS_CBS", 'S121_A_TA_ASEC_CB1SR')
    out["M0"] = _mfs("MFS_CBS", 'S121_L_MB_CBS')
    out["BM"] = _mfs("MFS_DC", "DCORP_L_BM")
    out["LG"] = _mfs("MFS_DC", 'DCORP_A_ACO_PS')
    out["FER"] = fer
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER,
           "elapsed_s": round(time.time() - t0, 1),
           "counts": {k: len(v) for k, v in out.items()},
           "families": out}
    S3.put_object(Bucket=BUCKET, Key="data/families.json",
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print("[families-feed]", doc["counts"], doc["elapsed_s"], "s")
    return doc["counts"]
