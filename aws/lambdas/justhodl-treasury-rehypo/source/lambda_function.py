"""justhodl-treasury-rehypo v1.0 — Treasury collateral re-use (rehypothecation)
stress desk. ops 4302.

Rehypothecation itself is unobservable directly; this engine builds the
honest institutional PROXY stack (Singh-style velocity + chain-stress
tells), every leg from an official primary source, coverage-renormalized,
nothing invented:

  VELOCITY  dealer gross UST repo financing ÷ net UST positions
            (NY Fed FR2004 via markets.newyorkfed.org — keyids are
            DISCOVERED from the API's own /list/timeseries catalog by
            description match, never guessed)
  FAILS     dealer Treasury delivery fails z (same catalog discovery)
  SPECIAL   GCF−Triparty rate spread z (OFR STFM) — collateral scarcity
  FUNDING   SOFR−IORB z (FRED) — cash-collateral pressure
  DRAIN     ON-RRP 4w delta z (FRED) — collateral parked at the Fed

Composite REHYPO_STRESS 0-100 over PRESENT legs; each leg carries its
source, latest value, z, and lookback n. Missing legs are listed, not
filled. Artifact: data/treasury-rehypo.json (+ rolling history).
"""
import gzip
import io
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/treasury-rehypo.json"
FRED_KEY = os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY", "")
UA = {"User-Agent": "JustHodl-research/1.0 (github.com/ElMooro)"}
s3 = boto3.client("s3", region_name="us-east-1")
VERSION = "1.0"


def http_json(url, timeout=40, gz=False):
    req = urllib.request.Request(url, headers=UA)
    raw = urllib.request.urlopen(req, timeout=timeout).read()
    if gz or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def zscore(series, lookback=104):
    xs = [v for _, v in series[-lookback:] if v is not None]
    if len(xs) < 10:
        return None, len(xs)
    mu = sum(xs) / len(xs)
    sd = math.sqrt(sum((x - mu) ** 2 for x in xs) / len(xs)) or 1e-9
    return round((xs[-1] - mu) / sd, 2), len(xs)


# ── NY Fed FR2004: catalog-driven keyid discovery ─────────────────────
NYFED = "https://markets.newyorkfed.org/api/pd"


def nyfed_catalog():
    doc = http_json(f"{NYFED}/list/timeseries.json")
    rows = (doc.get("pd") or {}).get("timeseries") or doc.get(
        "timeseries") or []
    return [{"keyid": r.get("keyid"),
             "desc": (r.get("description") or "").upper()}
            for r in rows if r.get("keyid")]


def pick(cat, must, forbid=()):
    hits = [c for c in cat
            if all(m in c["desc"] for m in must)
            and not any(f in c["desc"] for f in forbid)]
    return hits


def nyfed_series(keyid, n=140):
    doc = http_json(f"{NYFED}/get/all/timeseries/{keyid}.json")
    rows = (doc.get("pd") or {}).get("timeseries") or []
    out = []
    for r in rows:
        try:
            out.append((r.get("asofdate"), float(r.get("value"))))
        except Exception:
            continue
    out.sort()
    return out[-n:]


def sum_series(list_of_series):
    acc = {}
    for s in list_of_series:
        for d, v in s:
            acc[d] = acc.get(d, 0.0) + v
    return sorted(acc.items())


# ── OFR STFM ──────────────────────────────────────────────────────────
OFR = ("https://data.financialresearch.gov/v1/series/timeseries"
       "?mnemonic={m}")
OFR_CANDIDATES = {
    "gcf_rate": ["REPO-GCF_AR_AG-P", "REPO-GCF_AR_OO-P"],
    "tri_rate": ["REPO-TRI_AR_OO-P", "REPO-TRI_AR_AG-P"],
    "dvp_vol": ["REPO-DVP_TV_OO-P", "REPO-DVP_TV_TOT-P"],
}


def ofr_series(mnemonic, n=180):
    doc = http_json(OFR.format(m=mnemonic), gz=True)
    rows = doc if isinstance(doc, list) else doc.get("timeseries") or \
        doc.get("data") or []
    out = []
    for r in rows:
        try:
            if isinstance(r, list) and len(r) >= 2:
                out.append((str(r[0]), float(r[1])))
            elif isinstance(r, dict):
                out.append((str(r.get("date") or r.get("as_of")),
                            float(r.get("value"))))
        except Exception:
            continue
    out.sort()
    return out[-n:]


def fred_series(sid, n=200):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
           "&sort_order=asc")
    doc = http_json(url)
    out = []
    for o in doc.get("observations", []):
        try:
            out.append((o["date"], float(o["value"])))
        except Exception:
            continue
    return out[-n:]


def lambda_handler(event=None, context=None):
    t0 = time.time()
    legs, missing, notes = {}, [], []

    # FR2004 discovery
    picked = {}
    try:
        cat = nyfed_catalog()
        notes.append(f"nyfed catalog: {len(cat)} keyids")
        fails_c = pick(cat, ("FAIL", "TREASUR"), ("MBS", "AGENCY",
                                                  "CORPORATE"))
        repo_in = pick(cat, ("SECURITIES IN", "TREASUR"),
                       ("MBS", "AGENCY"))
        repo_out = pick(cat, ("SECURITIES OUT", "TREASUR"),
                        ("MBS", "AGENCY"))
        netpos = pick(cat, ("NET", "POSITION", "TREASUR"),
                      ("MBS", "AGENCY", "FORWARD", "TIPS", "FRN"))
        picked = {"fails": [c["keyid"] for c in fails_c][:6],
                  "sec_in": [c["keyid"] for c in repo_in][:8],
                  "sec_out": [c["keyid"] for c in repo_out][:8],
                  "net_pos": [c["keyid"] for c in netpos][:8]}
        notes.append("picked: " + json.dumps(
            {k: len(v) for k, v in picked.items()}))

        def agg(ids):
            ss = []
            for kid in ids:
                try:
                    ss.append(nyfed_series(kid))
                except Exception:
                    continue
            return sum_series(ss) if ss else []

        fails_s = agg(picked["fails"])
        if fails_s:
            z, n = zscore(fails_s)
            legs["fails"] = {
                "source": "NYFed FR2004 (catalog-discovered)",
                "keyids": picked["fails"], "latest": fails_s[-1][1],
                "as_of": fails_s[-1][0], "z": z, "n": n}
        fin_in, fin_out = agg(picked["sec_in"]), agg(
            picked["sec_out"])
        pos = agg(picked["net_pos"])
        gross = sum_series([s for s in (fin_in, fin_out) if s])
        if gross and pos:
            pd_, pv = dict(pos), dict(gross)
            common = sorted(set(pd_) & set(pv))
            vel = [(d, pv[d] / abs(pd_[d]))
                   for d in common if abs(pd_[d]) > 1e-6]
            if vel:
                z, n = zscore(vel)
                legs["velocity"] = {
                    "source": "FR2004 gross UST financing / |net "
                              "positions| (Singh-style proxy)",
                    "latest": round(vel[-1][1], 2),
                    "as_of": vel[-1][0], "z": z, "n": n}
    except Exception as e:
        missing.append(f"nyfed: {str(e)[:90]}")

    # OFR specialness (GCF - Triparty)
    try:
        def first_ok(cands):
            for m in cands:
                try:
                    s = ofr_series(m)
                    if s:
                        return m, s
                except Exception:
                    continue
            return None, []
        mg, gcf = first_ok(OFR_CANDIDATES["gcf_rate"])
        mt, tri = first_ok(OFR_CANDIDATES["tri_rate"])
        if gcf and tri:
            gd, td = dict(gcf), dict(tri)
            common = sorted(set(gd) & set(td))
            spread = [(d, gd[d] - td[d]) for d in common]
            z, n = zscore(spread)
            legs["specialness"] = {
                "source": f"OFR STFM {mg} - {mt}",
                "latest_bps": round(spread[-1][1] * 100, 1),
                "as_of": spread[-1][0], "z": z, "n": n}
        else:
            missing.append("ofr: gcf/tri unresolved from candidates")
        md, dvp = first_ok(OFR_CANDIDATES["dvp_vol"])
        if dvp:
            z, n = zscore(dvp)
            legs["dvp_volume"] = {"source": f"OFR STFM {md}",
                                  "latest": dvp[-1][1],
                                  "as_of": dvp[-1][0], "z": z,
                                  "n": n}
    except Exception as e:
        missing.append(f"ofr: {str(e)[:90]}")

    # FRED legs
    try:
        sofr, iorb = fred_series("SOFR"), fred_series("IORB")
        sd, idd = dict(sofr), dict(iorb)
        common = sorted(set(sd) & set(idd))
        sp = [(d, (sd[d] - idd[d]) * 100) for d in common]
        if sp:
            z, n = zscore(sp)
            legs["sofr_iorb"] = {"source": "FRED SOFR-IORB (bps)",
                                 "latest_bps": round(sp[-1][1], 1),
                                 "as_of": sp[-1][0], "z": z, "n": n}
        rrp = fred_series("RRPONTSYD")
        if len(rrp) > 25:
            delta = [(rrp[i][0], rrp[i][1] - rrp[i - 20][1])
                     for i in range(20, len(rrp))]
            z, n = zscore(delta)
            legs["rrp_drain_4w"] = {"source": "FRED RRPONTSYD Δ4w "
                                              "($bn)",
                                    "latest": round(delta[-1][1], 1),
                                    "as_of": delta[-1][0], "z": z,
                                    "n": n}
    except Exception as e:
        missing.append(f"fred: {str(e)[:90]}")

    # Composite over present legs — stress-signed
    SIGN = {"fails": +1, "velocity": +1, "specialness": +1,
            "sofr_iorb": +1, "rrp_drain_4w": -1, "dvp_volume": 0}
    zs = [(SIGN.get(k, 0) * v["z"]) for k, v in legs.items()
          if v.get("z") is not None and SIGN.get(k, 0)]
    comp = None
    if zs:
        comp = round(min(100, max(0, 50 + 12.5 * (sum(zs) /
                                                  len(zs)))), 1)
    band = (None if comp is None else
            "CALM" if comp < 45 else
            "WATCH" if comp < 62 else
            "STRAINED" if comp < 78 else "SEIZING")

    out = {"engine": "justhodl-treasury-rehypo", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(
               timespec="seconds"),
           "composite": comp, "band": band,
           "legs": legs, "legs_missing": missing or None,
           "picked_keyids": picked or None,
           "methodology": ("Rehypothecation is unobservable; this is "
                           "the honest proxy stack: FR2004 "
                           "catalog-discovered fails + Singh-style "
                           "velocity, OFR GCF-Tri specialness, "
                           "SOFR-IORB, RRP drain. Composite over "
                           "PRESENT legs only."),
           "notes": notes,
           "elapsed_s": round(time.time() - t0, 1)}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    try:
        h = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/treasury-rehypo-history.json")["Body"].read())
    except Exception:
        h = {"rows": []}
    h["rows"] = (h.get("rows") or [])[-364:] + [{
        "t": out["generated_at"], "composite": comp, "band": band,
        "legs_z": {k: v.get("z") for k, v in legs.items()}}]
    s3.put_object(Bucket=BUCKET, Key="data/treasury-rehypo-history.json",
                  Body=json.dumps(h).encode(),
                  ContentType="application/json")
    print(f"[rehypo] composite={comp} {band} legs={list(legs)} "
          f"missing={len(missing)} {out['elapsed_s']}s")
    return {"ok": True, "composite": comp, "band": band,
            "legs": list(legs), "missing": missing}
