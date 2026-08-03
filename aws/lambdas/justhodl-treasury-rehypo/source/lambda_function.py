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
LONG_KEY = "data/treasury-rehypo-long.json"
ERA_ORDER = ["SBP2001", "SBP2013", "SBN2013", "SBN2015", "SBN2022",
             "SBN2024"]
FRED_KEY = os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY", "")
UA = {"User-Agent": "JustHodl-research/1.0 (github.com/ElMooro)"}
s3 = boto3.client("s3", region_name="us-east-1")
VERSION = "1.3"


def http_json(url, timeout=40, gz=False):  # gz kept for signature
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
             "sb": r.get("seriesbreak"),
             "desc": (r.get("description") or "").upper()}
            for r in rows if r.get("keyid")]


def pick(cat, must, forbid=()):
    hits = [c for c in cat
            if all(m in c["desc"] for m in must)
            and not any(f in c["desc"] for f in forbid)]
    return hits


def nyfed_series(keyid, sb, n=140):
    # ops 4304 truth: /get/all/ is an empty stub; each keyid's own
    # seriesbreak (catalog field) is the working segment.
    doc = http_json(f"{NYFED}/get/{sb}/timeseries/{keyid}.json")
    rows = (doc.get("pd") or {}).get("timeseries") or []
    out = []
    for r in rows:
        try:
            out.append((r.get("asofdate"), float(r.get("value"))))
        except Exception:
            continue
    out.sort()
    return out[-n:]


def stitch(per_break):
    """Concatenate era segments; on overlap dates the LATEST break
    wins (breaks are sequential FR2004 format regimes)."""
    acc = {}
    for sb in ERA_ORDER:
        for d, v in per_break.get(sb, []):
            acc[d] = v
    return sorted(acc.items())


def weekly_last(series):
    """Resample any cadence to weekly (ISO year-week, last obs)."""
    acc = {}
    for d, v in series:
        try:
            y, w, _ = datetime.strptime(str(d)[:10],
                                        "%Y-%m-%d").isocalendar()
        except Exception:
            continue
        acc[(y, w)] = (d, v)
    return [acc[k] for k in sorted(acc)]


def rolling_z(series, win=156):
    out = []
    vals = [v for _, v in series]
    for i, (d, v) in enumerate(series):
        lo = max(0, i - win + 1)
        xs = vals[lo:i + 1]
        if len(xs) < 26:
            out.append((d, None))
            continue
        mu = sum(xs) / len(xs)
        sd = math.sqrt(sum((x - mu) ** 2 for x in xs) / len(xs)) \
            or 1e-9
        out.append((d, round((v - mu) / sd, 2)))
    return out


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
    "gcf_rate": ["REPO-GCF_AR_AG-P", "REPO-GCF_AR_OO-P",
                 "REPO-GCF_AR_TOT-P"],
    "tri_rate": ["REPO-TRI_AR_OO-P", "REPO-TRI_AR_AG-P",
                 "REPO-TRI_AR_TOT-P"],
    "dvp_vol": ["REPO-DVP_TV_OO-P", "REPO-DVP_TV_TOT-P",
                "REPO-DVP_TV_AG-P"],
}
OFR_LIST = ("https://data.financialresearch.gov/v1/metadata/"
            "mnemonics")


def ofr_discover(notes):
    try:
        doc = http_json(OFR_LIST)  # plain JSON; auto-detect only
        rows = doc if isinstance(doc, list) else \
            doc.get("mnemonics") or []
        ms = [str(r if isinstance(r, str) else r.get("mnemonic"))
              for r in rows]
        repo = [m for m in ms if m and m.startswith("REPO-")]
        notes.append(f"ofr catalog: {len(repo)} REPO-* mnemonics; "
                     f"sample {repo[:8]}")
        return repo
    except Exception as e:
        notes.append(f"ofr discover: {str(e)[:60]}")
        return []


def ofr_series(mnemonic, n=180):
    doc = http_json(OFR.format(m=mnemonic))  # auto-detect gzip
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
        # FR2004 vocabulary: financing legs are (REVERSE) REPURCHASE
        # lines; positions are "NET OUTRIGHT" Treasury lines.
        repo_in = pick(cat, ("REVERSE REPURCHASE",),
                       ("MBS", "AGENCY", "CORPORATE"))
        repo_out = pick(cat, ("REPURCHASE",),
                        ("REVERSE", "MBS", "AGENCY", "CORPORATE"))
        netpos = pick(cat, ("TREASUR", "NET"),
                      ("MBS", "AGENCY", "FORWARD", "FAIL",
                       "REPURCHASE"))
        if not netpos:
            netpos = pick(cat, ("TREASUR", "OUTRIGHT"),
                          ("MBS", "AGENCY"))
        for lbl, lst in (("repo_in", repo_in), ("repo_out", repo_out),
                         ("netpos", netpos), ("fails", fails_c)):
            if not lst:
                samp = [c["desc"][:60] for c in cat
                        if "TREASUR" in c["desc"]][:3]
                notes.append(f"{lbl}=0; sample TREASUR descs: {samp}")
        picked = {"fails": [(c["keyid"], c["sb"])
                            for c in fails_c][:6],
                  "sec_in": [(c["keyid"], c["sb"])
                             for c in repo_in][:8],
                  "sec_out": [(c["keyid"], c["sb"])
                              for c in repo_out][:8],
                  "net_pos": [(c["keyid"], c["sb"])
                              for c in netpos][:8]}
        notes.append("picked: " + json.dumps(
            {k: len(v) for k, v in picked.items()}))

        def agg(pairs, lbl=""):
            ss = []
            for kid, sb in pairs:
                try:
                    s_ = nyfed_series(kid, sb or "SBN2024")
                    if s_:
                        ss.append(s_)
                    else:
                        notes.append(f"{lbl}/{kid}@{sb}: 0 rows")
                except Exception as e_:
                    notes.append(f"{lbl}/{kid}@{sb}: {str(e_)[:46]}")
            return sum_series(ss) if ss else []

        fails_s = agg(picked["fails"], "fails")
        if fails_s:
            z, n = zscore(fails_s)
            legs["fails"] = {
                "source": "NYFed FR2004 (catalog-discovered)",
                "keyids": [k for k, _ in picked["fails"]],
                "latest": fails_s[-1][1],
                "as_of": fails_s[-1][0], "z": z, "n": n}
        fin_in = agg(picked["sec_in"], "in")
        fin_out = agg(picked["sec_out"], "out")
        pos = agg(picked["net_pos"], "pos")
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
                "latest_bps": round(spread[-1][1] * 100, 2),
                "as_of": spread[-1][0], "z": z, "n": n}
        else:
            repo_all = ofr_discover(notes)
            def by_pat(sub):
                return [m for m in repo_all if sub in m and
                        "_AR_" in m][:3]
            mg2, gcf = first_ok(by_pat("GCF") or
                                OFR_CANDIDATES["gcf_rate"])
            mt2, tri = first_ok(by_pat("TRI") or
                                OFR_CANDIDATES["tri_rate"])
            if gcf and tri:
                gd, td = dict(gcf), dict(tri)
                common = sorted(set(gd) & set(td))
                spread = [(d, gd[d] - td[d]) for d in common]
                z, n = zscore(spread)
                legs["specialness"] = {
                    "source": f"OFR STFM {mg2} - {mt2} (discovered)",
                    "latest_bps": round(spread[-1][1] * 100, 2),
                    "as_of": spread[-1][0], "z": z, "n": n}
            else:
                missing.append("ofr: gcf/tri unresolved even after "
                               "catalog discovery")
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
                                 "latest_bps": round(sp[-1][1], 2),
                                 "as_of": sp[-1][0], "z": z, "n": n}
        rrp = fred_series("RRPONTSYD")
        if len(rrp) > 25:
            delta = [(rrp[i][0], rrp[i][1] - rrp[i - 20][1])
                     for i in range(20, len(rrp))]
            z, n = zscore(delta)
            legs["rrp_drain_4w"] = {"source": "FRED RRPONTSYD Δ4w "
                                              "($bn)",
                                    "latest": round(delta[-1][1], 2),
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
    # ── ops 4306: long history (target 1996; actual start disclosed)
    try:
        # 4307 truth: catalog carries ONLY modern (SBN2024) keyids;
        # older eras publish no entries of their own -- but modern
        # keyids fetch across old break segments. So: the proven
        # short-pass keyid sets x all six segments; latest break
        # wins on overlap.
        def agg_long(pairs, cap=5):
            per_break = {}
            kids = [k for k, _ in pairs][:cap]
            for sb in ERA_ORDER:
                ss = []
                for kid in kids:
                    try:
                        s_ = nyfed_series(kid, sb, n=3000)
                        if s_:
                            ss.append(s_)
                    except Exception:
                        continue
                if ss:
                    per_break[sb] = sum_series(ss)
            return stitch(per_break), {sb: len(v) for sb, v in
                                       per_break.items()}
        f_long, f_cov = agg_long(picked["fails"])
        in_long, i_cov = agg_long(picked["sec_in"])
        out_long, o_cov = agg_long(picked["sec_out"])
        pos_long, p_cov = agg_long(picked["net_pos"])
        gross_l = sum_series([x for x in (in_long, out_long) if x])
        vel_l = []
        if gross_l and pos_long:
            gd, pd_ = dict(gross_l), dict(pos_long)
            vel_l = [(d, gd[d] / abs(pd_[d]))
                     for d in sorted(set(gd) & set(pd_))
                     if abs(pd_[d]) > 1e-6]
        legs_long = {}
        if f_long:
            legs_long["fails"] = rolling_z(weekly_last(f_long))
        if vel_l:
            legs_long["velocity"] = rolling_z(weekly_last(vel_l))
        # modern legs, full length where the data exists
        try:
            sofr_f, iorb_f = fred_series("SOFR", 99999), \
                fred_series("IORB", 99999)
            sd2, id2 = dict(sofr_f), dict(iorb_f)
            sp2 = [(d, (sd2[d] - id2[d]) * 100)
                   for d in sorted(set(sd2) & set(id2))]
            if sp2:
                legs_long["sofr_iorb"] = rolling_z(weekly_last(sp2))
        except Exception:
            pass
        try:
            rrp_f = fred_series("RRPONTSYD", 99999)
            if len(rrp_f) > 25:
                dl = [(rrp_f[i][0], rrp_f[i][1] - rrp_f[i - 20][1])
                      for i in range(20, len(rrp_f))]
                legs_long["rrp_drain_4w"] = rolling_z(weekly_last(dl))
        except Exception:
            pass
        # weekly composite over PRESENT legs per date
        allz = {}
        for name, ts in legs_long.items():
            sgn = SIGN.get(name, 0)
            if not sgn:
                continue
            for d, z in ts:
                if z is None:
                    continue
                allz.setdefault(d, {})[name] = sgn * z
        weekly = []
        for d in sorted(allz):
            zz = list(allz[d].values())
            cmp_ = round(min(100, max(0, 50 + 12.5 *
                                      (sum(zz) / len(zz)))), 1)
            weekly.append({"d": d, "c": cmp_, "n": len(zz),
                           "z": {k: round(v, 2) for k, v in
                                 allz[d].items()}})
        weekly = weekly[-1600:]
        long_doc = {
            "engine": "justhodl-treasury-rehypo",
            "version": VERSION,
            "generated_at": out["generated_at"],
            "target_start": "1996-01-01",
            "actual_start": weekly[0]["d"] if weekly else None,
            "n_weekly": len(weekly),
            "era_coverage": {"fails": f_cov, "financing_in": i_cov,
                             "financing_out": o_cov,
                             "positions": p_cov},
            "legs_available": {k: {"from": v[0][0],
                                   "to": v[-1][0]}
                               for k, v in legs_long.items() if v},
            "weekly": weekly,
            "note": ("Composite per week over legs PRESENT that "
                     "week (era-renormalized rolling-156w z). "
                     "FR2004 stitched across seriesbreaks, latest "
                     "break wins on overlap; pre-API history does "
                     "not exist through this endpoint and is shown "
                     "as absent, never interpolated.")}
        s3.put_object(Bucket=BUCKET, Key=LONG_KEY,
                      Body=json.dumps(long_doc).encode(),
                      ContentType="application/json",
                      CacheControl="public, max-age=3600")
        out["long_history"] = {"actual_start": long_doc[
            "actual_start"], "n_weekly": len(weekly)}
        notes.append(f"long: {long_doc['actual_start']} -> "
                     f"{len(weekly)}w")
    except Exception as e:
        notes.append(f"long-history: {str(e)[:90]}")

    print(f"[rehypo] composite={comp} {band} legs={list(legs)} "
          f"missing={len(missing)} {out['elapsed_s']}s")
    return {"ok": True, "composite": comp, "band": band,
            "legs": list(legs), "missing": missing}
