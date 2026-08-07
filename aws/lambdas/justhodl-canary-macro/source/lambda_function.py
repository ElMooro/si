"""justhodl-canary-macro — Khalid's unemployment/LEI/canaries doc, complete
(ops 4501). Six FRED panels via keyless multi-series fredgraph CSV, BLS
labor block via keyed v2 batch, Cleveland yield-curve model xlsx, Atlanta
GDPNow tracking xlsx, DOL ar539 claims — warm archives + ONE hot
enveloped feed data/canary-macro.json with computed trigger flags
(Sahm >=0.50, 10y3m inversion, claims 4wk rising). Explicit failures."""
import gzip
import io
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None
try:
    from provenance import wrap as _lw, missing as _lm
except Exception:
    _lw = _lm = None


def wrap(v, **kw):
    if _lw:
        try:
            env = _lw(v, **kw)
        except TypeError:
            try:
                env = _lw(v, kw.get("source_url"))
            except TypeError:
                try:
                    env = _lw(v)
                except TypeError:
                    env = None
        if isinstance(env, dict):
            for k, x in kw.items():
                env.setdefault(k, x)
            return env
    return {"value": v, **kw}


def missing(reason, **kw):
    if _lm:
        try:
            env = _lm(reason, **kw)
        except TypeError:
            env = _lm(reason)
        if isinstance(env, dict):
            for k, x in kw.items():
                env.setdefault(k, x)
            return env
    return {"data_unavailable": True, "reason": reason, **kw}


def _fetch(u, headers=None, timeout=120, data=None):
    h = {"User-Agent": "JustHodl research admin@justhodl.ai"}
    h.update(headers or {})
    req = urllib.request.Request(u, headers=h, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    if b[:2] == b"\x1f\x8b":
        b = gzip.decompress(b)
    return b


PANELS = {
    "labor_claims": ["ICSA", "IC4WSA", "CCSA", "CC4WSA", "IURSA"],
    "leading": ["USSLIND", "USPHCI", "T10Y3M", "T10Y2Y", "T10YFF",
                "T5YFF", "AWHMAN", "NEWORDER", "ACOGNO", "PERMIT",
                "UMCSENT"],
    "recession_prob": ["SAHMREALTIME", "SAHMCURRENT", "RECPROUSM156N",
                       "USREC", "JHDUSRGDPBR", "USARECDM", "ANFCI"],
    "consumer_housing": ["MICH", "T5YIE", "T10YIE", "HOUST",
                         "MORTGAGE30US", "MSACSR", "HSN1F",
                         "EXHOSLUSM495S"],
    "credit_cycle": ["DRTSCILM", "TOTALSL", "REVOLSL", "DRCCLACBS",
                     "DRCLACBS", "BAMLH0A0HYM2", "NFCICREDIT",
                     "NFCILEVERAGE", "NFCINONFINLEVERAGE", "INDPRO"],
    "fed_liquidity": ["WALCL", "WTREGEN", "RRPONTSYD", "RESPPANWW",
                      "WLCFLPCL", "M2SL", "DTWEXBGS", "VIXCLS"],
}
BLS_EXTRA = ["LNS13327709", "LNS11300000", "LNS12300000",
             "LNS12032194", "LNS13023621", "LNS13008397",
             "CES3000000007", "CES0500000002", "CES0500000003",
             "JTS000000000000000QUL", "JTS000000000000000LDL"]


def _fred_panel(name, ids, hot, S):
    u = ("https://fred.stlouisfed.org/graph/fredgraph.csv?id="
         + ",".join(ids))
    try:
        try:
            raw = _fetch(u, timeout=60)
            if raw.startswith(b"PK"):
                # ops 4537 (Perplexity P0): mixed-frequency batches
                # return a ZIP, not CSV — the 40-dead-series bug. HARD
                # failure here routes into the per-id fallback below,
                # which is frequency-proof (one series per call).
                raise ValueError("mixed-frequency ZIP (PK magic)")
        except Exception:
            # per-id sequential fallback (frequency-proof)
            parts = {}
            for sid in ids:
                try:
                    r1 = _fetch("https://fred.stlouisfed.org/graph/"
                                "fredgraph.csv?id=" + sid, timeout=30)
                    parts[sid] = r1
                except Exception:
                    pass
            if not parts:
                raise
            first = next(iter(parts.values())).decode(
                "utf-8", "replace").splitlines()
            hdr = ["DATE"] + list(parts)
            rows = {}
            for sid, rb in parts.items():
                for ln in rb.decode("utf-8",
                                    "replace").splitlines()[1:]:
                    d0, _, v0 = ln.partition(",")
                    rows.setdefault(d0, {})[sid] = v0
            out = [",".join(hdr)]
            for d0 in sorted(rows):
                out.append(",".join([d0] + [rows[d0].get(s, ".")
                                            for s in parts]))
            raw = "\n".join(out).encode()
        rk = snapshot("fred", u, raw[:300000]) if snapshot else None
        s3.put_object(Bucket=BUCKET,
                      Key=f"data/warm/fred-canary/{name}.csv.gz",
                      Body=gzip.compress(raw),
                      ContentType="application/gzip")
        lines = raw.decode("utf-8", "replace").splitlines()
        hdr = [c.strip('"') for c in lines[0].split(",")]
        cols = {c: i for i, c in enumerate(hdr)}
        got = 0
        for sid in ids:
            ci = cols.get(sid)
            if ci is None:
                hot[sid] = missing("column absent", panel=name)
                continue
            latest = prev = None
            for ln in reversed(lines[1:]):
                p = ln.split(",")
                if len(p) <= ci:
                    continue
                v = p[ci].strip().strip('"')
                if v in ("", "."):
                    continue
                try:
                    fv = float(v)
                except ValueError:
                    continue
                if latest is None:
                    latest = (p[0], fv)
                elif prev is None:
                    prev = (p[0], fv)
                    break
            if latest:
                hot[sid] = wrap(latest[1], observed=latest[0],
                                prev=(prev[1] if prev else None),
                                prev_date=(prev[0] if prev else None),
                                panel=name, provider="fred",
                                source_url=u[:120],
                                raw_snapshot_key=rk)
                got += 1
            else:
                hot[sid] = missing("no numeric obs", panel=name)
        S[name] = {"ok": True, "ids": len(ids), "with_data": got}
    except Exception as e:
        S[name] = {"data_unavailable": True,
                   "reason": f"{type(e).__name__}: {str(e)[:60]}"}
        for sid in ids:
            hot.setdefault(sid, missing(S[name]["reason"], panel=name))


def _bls(hot, S):
    try:
        key = boto3.client("ssm", region_name="us-east-1").get_parameter(
            Name="/justhodl/bls-api-key",
            WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        key = None
    try:
        body = json.dumps({"seriesid": BLS_EXTRA, "startyear": "2006",
                           "endyear": str(datetime.now().year),
                           **({"registrationkey": key} if key else {})}
                          ).encode()
        raw = _fetch("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                     headers={"Content-Type": "application/json"},
                     data=body, timeout=90)
        d = json.loads(raw)
        n = 0
        for ser in d.get("Results", {}).get("series", []):
            sid = ser.get("seriesID")
            rows = ser.get("data") or []
            if rows:
                hot[sid] = wrap(float(rows[0]["value"]),
                                observed=f"{rows[0]['year']}-"
                                         f"{rows[0]['period']}",
                                prev=(float(rows[1]["value"])
                                      if len(rows) > 1 else None),
                                panel="bls_labor", provider="bls")
                n += 1
            else:
                hot[sid] = missing("empty", panel="bls_labor")
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/fred-canary/bls-labor.json.gz",
                      Body=gzip.compress(raw))
        S["bls_labor"] = {"ok": True, "with_data": n,
                          "keyed": bool(key)}
    except Exception as e:
        S["bls_labor"] = {"data_unavailable": True,
                          "reason": f"{type(e).__name__}: "
                                    f"{str(e)[:60]}"}


def _xlsx(name, urls, key, S):
    last = "none"
    for u in urls:
        try:
            raw = _fetch(u, timeout=120)
            if len(raw) < 8000:
                raise ValueError(f"small {len(raw)}b")
            if not raw.startswith(b"PK"):
                # ops 4535 (Perplexity P1): first-fetch assertion — an
                # xlsx MUST be a zip; a 404/HTML page never registers ok
                raise ValueError("content-type assertion: not xlsx")
            s3.put_object(Bucket=BUCKET, Key=key, Body=raw)
            S[name] = {"ok": True, "kb": round(len(raw) / 1024),
                       "source": u[:90]}
            return
        except Exception as e:
            last = f"{type(e).__name__}: {str(e)[:50]}"
    S[name] = {"data_unavailable": True, "reason": last}



def _budget(t0, S, name, cap=420):
    if time.time() - t0 > cap:
        S[name] = {"data_unavailable": True,
                   "reason": "time-budget: deferred to next run"}
        return False
    return True


def _finalize(hot, S):
    def _v(sid):
        e = hot.get(sid) or {}
        return e.get("value") if isinstance(e, dict) else None
    flags = {}
    # ops 4537 (Perplexity P0): a missing input must NEVER read as
    # all-clear. Tri-state: True / False / None(=UNKNOWN).
    sv = _v("SAHMREALTIME")
    flags["sahm_triggered"] = (None if sv is None else sv >= 0.50)
    flags["sahm_value"] = sv
    tv = _v("T10Y3M")
    flags["curve_10y3m_inverted"] = (None if tv is None else tv < 0)
    ic = _v("IC4WSA")
    icp = (hot.get("IC4WSA") or {}).get("prev")
    flags["claims_4wk_rising"] = (None if (ic is None or icp is None)
                                  else ic > icp)
    flags["status"] = ("UNKNOWN" if any(
        flags[k] is None for k in ("sahm_triggered",
                                   "curve_10y3m_inverted",
                                   "claims_4wk_rising"))
        else "COMPUTED")
    hot["flags"] = flags
    s3.put_object(Bucket=BUCKET, Key="data/canary-macro.json",
                  Body=json.dumps(hot, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/canary-macro-summary.json",
                  Body=json.dumps(S, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


def lambda_handler(event, context):
    _t0 = time.time()
    now = datetime.now(timezone.utc)
    S = {"as_of": now.isoformat(timespec="seconds")}
    hot = {"as_of": S["as_of"],
           "source": "canary-macro (FRED keyless CSV + BLS v2 + "
                     "Cleveland/Atlanta/DOL)"}
    for name, ids in PANELS.items():
        _fred_panel(name, ids, hot, S)
    _bls(hot, S)
    _fred_panel("nfci", ["NFCI", "ANFCI", "NFCIRISK", "NFCICREDIT",
                          "NFCILEVERAGE", "NFCINONFINLEVERAGE"],
                hot, S)  # ops 4535: chicagofed via FRED (xlsx dead)
    _finalize(hot, S)  # ops 4510: hot feed lands EARLY, tail can't kill it
    if _budget(_t0, S, "cleveland_model"):
        # ops 4535: Cleveland xlsx 404s (Perplexity-verified). Substitute
        # the model inputs from FRED: recession prob + 10y-3m slope.
        try:
            raw = _fetch("https://fred.stlouisfed.org/graph/"
                         "fredgraph.csv?id=RECPROUSM156N,T10Y3M",
                         timeout=45)
            s3.put_object(Bucket=BUCKET,
                          Key="data/warm/fred-canary/"
                              "cleveland-model.csv.gz",
                          Body=gzip.compress(raw),
                          ContentType="application/gzip")
            S["cleveland_model"] = {"ok": True, "via": "FRED",
                                    "bytes": len(raw)}
            try:
                _lines = raw.decode("utf-8", "replace").splitlines()
                _hdr = _lines[0].split(",")
                _ix = _hdr.index("RECPROUSM156N")
                for _ln in reversed(_lines[1:]):
                    _cs = _ln.split(",")
                    if len(_cs) > _ix and _cs[_ix] not in (".", ""):
                        hot["RECPROUSM156N"] = {"value": float(_cs[_ix]),
                                        "date": _cs[0]}
                        break
            except Exception:
                pass
        except Exception as e:
            S["cleveland_model"] = {"data_unavailable": True,
                                    "reason": f"{type(e).__name__}: "
                                              f"{str(e)[:60]}"}
    if _budget(_t0, S, "atlanta_gdpnow"):
        # ops 4536: Atlanta xlsx serves HTML (assertion caught it) — use
        # the official GDPNow series on FRED instead.
        try:
            raw = _fetch("https://fred.stlouisfed.org/graph/"
                         "fredgraph.csv?id=GDPNOW", timeout=45)
            s3.put_object(Bucket=BUCKET,
                          Key="data/warm/fred-canary/"
                              "atlanta-gdpnow.csv.gz",
                          Body=gzip.compress(raw),
                          ContentType="application/gzip")
            S["atlanta_gdpnow"] = {"ok": True, "via": "FRED",
                                   "bytes": len(raw)}
            try:
                _lines = raw.decode("utf-8", "replace").splitlines()
                _hdr = _lines[0].split(",")
                _ix = _hdr.index("GDPNOW")
                for _ln in reversed(_lines[1:]):
                    _cs = _ln.split(",")
                    if len(_cs) > _ix and _cs[_ix] not in (".", ""):
                        hot["GDPNOW"] = {"value": float(_cs[_ix]),
                                        "date": _cs[0]}
                        break
            except Exception:
                pass
        except Exception as e:
            S["atlanta_gdpnow"] = {"data_unavailable": True,
                                   "reason": f"{type(e).__name__}: "
                                             f"{str(e)[:60]}"}
    if _budget(_t0, S, "dol_ar539"):
     try:
        raw = _fetch("https://oui.doleta.gov/unemploy/csv/ar539.csv",
                     timeout=60)
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/fred-canary/dol-ar539.csv.gz",
                      Body=gzip.compress(raw))
        S["dol_ar539"] = {"ok": True,
                          "lines": raw.count(b"\n")}
     except Exception as e:
        S["dol_ar539"] = {"data_unavailable": True,
                          "reason": f"{type(e).__name__}: "
                                    f"{str(e)[:60]}"}

    _finalize(hot, S)  # refresh incl tail statuses
    flags = hot.get("flags") or {}
    n_hot = sum(1 for v in hot.values()
                if isinstance(v, dict) and "value" in v
                and v.get("value") is not None)
    res = {"ok": True, "hot_series": n_hot, "flags": flags,
           "panels": {k: v for k, v in S.items() if k != "as_of"}}
    print(json.dumps(res, default=str)[:600])
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
