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

MARKER = "families-feed v2.0 ops4187 config-driven"
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
    out = {"INTR": {}, "FER": {}, "GDPYY": {}, "IRYY": {}, "UR": {}, "LG": {}, "CBBS": {}, "M0": {}, "BM": {}, "GDG": {}, "BOT": {}, "DIR": {}, "LIR": {}, "TOT": {}, "CA": {}, "IPYY": {}, "FI": {}, "GRES": {}, "GRES": {}, "RSYY": {}, "CIR": {}, "INBR": {}, "UP": {}, "MPRYY": {}}
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
                      ("GRES", "FI.RES.XGLD.CD"),
                      ("GRES", "FI.RES.XGLD.CD"),
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
            if fam in ("FERWB", "GRES"):
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
    # MEI WAVE ops4167 — DBnomics OECD/MEI bulk per family
    _MEI = json.loads('{"RSYY": ["SLRTCR03", "GY"], "CIR": ["CPGRLE01", "GY"], "INBR": ["IR3TIB01", "ST"], "UP": ["LFHUTTTT", "STSA"], "MPRYY": ["PITGND01", "GY"]}')
    for _fam, (_sub, _mea) in _MEI.items():
        d5 = {}
        t5 = _fetch("https://api.db.nomics.world/v22/series/OECD/MEI/."
                    + _sub + "." + _mea + ".M?observations=1&limit=500")
        try:
            for doc in json.loads(t5)["series"]["docs"]:
                cc3 = str(doc.get("series_code", "")).split(".")[0]
                vv5 = [z for z in doc.get("value") or [] if z is not None]
                pp5 = doc.get("period") or []
                c2m = inv3.get(cc3)
                if vv5 and c2m:
                    d5[c2m] = [round(float(vv5[-1]), 2),
                               "oecd:" + (pp5[-1] if pp5 else "m")]
        except Exception:
            pass
        out[_fam] = d5
    # IPYY-FI ops4176
    d7 = {}
    t7 = _fetch("https://api.db.nomics.world/v22/series/OECD/MEI/.PRINTO01.IXOBSA.M?observations=1&limit=500")
    try:
        for doc in json.loads(t7)["series"]["docs"]:
            cc3 = str(doc.get("series_code", "")).split(".")[0]
            vv7 = [z for z in doc.get("value") or [] if z is not None]
            pp7 = doc.get("period") or []
            c2m = inv3.get(cc3)
            if len(vv7) >= 13 and c2m:
                d7[c2m] = [round((vv7[-1] / vv7[-13] - 1) * 100, 2),
                           "oecd:" + (pp7[-1] if pp7 else "m")]
    except Exception:
        pass
    out["IPYY"] = d7
    d8 = {}
    t8 = _fetch("https://api.imf.org/external/sdmx/2.1/data/CPI/..CP01.YOY_PCH_PA_PT.M?lastNObservations=1")
    for blk8 in re.split(r"<Series[ >]", t8)[1:]:
        a8 = re.search(r'COUNTRY="([A-Z]{3})"', blk8)
        v8 = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', blk8)
        tp8 = re.findall(r'TIME_PERIOD="([^"]+)"', blk8)
        c2m = inv3.get(a8.group(1)) if a8 else None
        if v8 and c2m:
            d8[c2m] = [round(float(v8[-1]), 2),
                       "imf:" + (tp8[-1] if tp8 else "m")]
    out["FI"] = d8
    # CONFIG-DRIVEN v2.0 ops4187 — extra families from S3 defs; adding a
    # family is now data, never code. kinds: wb | mei | imf_mask.
    try:
        _defs = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/family-defs.json")["Body"].read()
        ).get("families") or {}
    except Exception:
        _defs = {}
    for _fam, _dd in _defs.items():
        try:
            _kind = _dd.get("kind")
            dX = {}
            if _kind == "wb":
                tX = _fetch("https://api.worldbank.org/v2/country/all/"
                            "indicator/" + _dd["code"]
                            + "?format=json&mrnev=1&per_page=400")
                for rX in (json.loads(tX)[1] or []):
                    vX = rX.get("value")
                    cX = (rX.get("country") or {}).get("id") or ""
                    if vX is not None and len(cX) == 2:
                        dX[cX.upper()] = [
                            round(float(vX) / float(_dd.get("div", 1)), 2),
                            "wb:%s" % rX.get("date")]
            elif _kind == "mei":
                tX = _fetch("https://api.db.nomics.world/v22/series/OECD/"
                            "MEI/." + _dd["subject"] + "."
                            + _dd["measure"] + ".M?observations=1"
                            "&limit=500")
                for doc in json.loads(tX)["series"]["docs"]:
                    c3X = str(doc.get("series_code", "")).split(".")[0]
                    vXs = [z for z in doc.get("value") or []
                           if z is not None]
                    pXs = doc.get("period") or []
                    cmX = inv3.get(c3X)
                    if vXs and cmX:
                        dX[cmX] = [round(float(vXs[-1]), 2),
                                   "oecd:" + (pXs[-1] if pXs else "m")]
            elif _kind == "imf_mask":
                tX = _fetch("https://api.imf.org/external/sdmx/2.1/data/"
                            + _dd["flow"] + "/" + _dd["mask"]
                            + "?lastNObservations=1")
                for bX in re.split(r"<Series[ >]", tX)[1:]:
                    aX = re.search(r'COUNTRY="([A-Z]{3})"', bX)
                    vX2 = re.findall(
                        r'OBS_VALUE="([\d\.eE\+\-]+)"', bX)
                    pX2 = re.findall(r'TIME_PERIOD="([^"]+)"', bX)
                    cmX = inv3.get(aX.group(1)) if aX else None
                    if vX2 and cmX:
                        dX[cmX] = [round(float(vX2[-1]), 2),
                                   "imf:" + (pX2[-1] if pX2 else "m")]
            out[_fam] = dX
        except Exception:
            out.setdefault(_fam, {})
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
