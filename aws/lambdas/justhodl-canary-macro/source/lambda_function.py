"""justhodl-canary-macro â€” Khalid's unemployment/LEI/canaries doc, complete
(ops 4501). Six FRED panels via keyless multi-series fredgraph CSV, BLS
labor block via keyed v2 batch, Cleveland yield-curve model xlsx, Atlanta
GDPNow tracking xlsx, DOL ar539 claims â€” warm archives + ONE hot
enveloped feed data/canary-macro.json with computed trigger flags
(Sahm >=0.50, 10y3m inversion, claims 4wk rising). Explicit failures."""
import gzip
import io
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
from provenance import wrap as _lw, missing as _lm
from evidence_store import capture
from canary_measurements import DEFINITIONS, csv_observations, observation_quality, diagnostics


def wrap(v, **kw):
    """Explicit adapter: provider metadata cannot occupy the field-name slot."""
    field = kw.get("field") or kw.get("series") or "unidentified_measurement"
    env = _lw(v, field, unit=kw.get("unit"), source=kw.get("provider", "unknown"),
                    series_id=kw.get("series") or field, url=kw.get("source_url"),
                    as_of=kw.get("observed"), received_at=kw.get("fetched_at"),
                    raw_key=kw.get("raw_snapshot_key"), evidence=kw.get("evidence"),
                    confidence=kw.get("confidence"))
    for key, value in kw.items():
        env.setdefault(key, value)
    return env


def missing(reason, **kw):
    field = kw.get("field") or kw.get("series") or "unidentified_measurement"
    env = _lm(field, reason, unit=kw.get("unit"), source=kw.get("provider"))
    for key, value in kw.items():
        env.setdefault(key, value)
    return env


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
                     "DRCLACBS", "BAMLH0A0HYM2", "BAMLC0A0CM", "NFCICREDIT",
                     "NFCILEVERAGE", "NFCINONFINLEVERAGE", "INDPRO"],
    "fed_liquidity": ["WALCL", "WTREGEN", "RRPONTSYD", "RESPPANWW",
                      "WLCFLPCL", "M2SL", "DTWEXBGS", "VIXCLS"],
}
BLS_EXTRA = ["LNS13327709", "LNS11300000", "LNS12300000",
             "LNS12032194", "LNS13023621", "LNS13008397",
             "CES3000000007", "CES0500000002", "CES0500000003",
             "JTS000000000000000QUL", "JTS000000000000000LDL"]


def _fred_panel(name, ids, hot, S):
    base = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
    url = base + ",".join(ids)
    sources = {}
    failures = {}
    try:
        raw = _fetch(url, timeout=45)
        if raw.startswith(b"PK"):
            raise ValueError("mixed-frequency ZIP requires per-series responses")
        # Validate every column before accepting a panel. Each fallback preserves
        # its original provider response, never a reconstructed CSV as raw proof.
        for sid in ids:
            csv_observations(raw, sid)
        proof = capture(s3, BUCKET, "fred", url, raw)
        sources = {sid: (raw, url, proof) for sid in ids}
        s3.put_object(Bucket=BUCKET, Key=f"data/warm/fred-canary/{name}.csv.gz",
                      Body=gzip.compress(raw), ContentType="application/gzip")
    except Exception:
        def individual(sid):
            try:
                raw = _fetch(base + sid, timeout=20)
                csv_observations(raw, sid)
                proof = capture(s3, BUCKET, "fred", base + sid, raw)
                s3.put_object(Bucket=BUCKET, Key=f"data/warm/fred-canary/{name}/{sid}.csv.gz",
                              Body=gzip.compress(raw), ContentType="application/gzip")
                return sid, (raw, base + sid, proof), None
            except Exception as exc:
                return sid, None, type(exc).__name__ + ": " + str(exc)[:100]
        with ThreadPoolExecutor(max_workers=4) as pool:
            for future in as_completed([pool.submit(individual, sid) for sid in ids]):
                sid, result, error = future.result()
                if result: sources[sid] = result
                else: failures[sid] = error
    got = 0
    for sid in ids:
        try:
            raw, source_url, proof = sources[sid]
            observations = csv_observations(raw, sid)
            if not observations: raise ValueError("no numeric observations")
            latest = observations[0]
            previous = observations[1] if len(observations) > 1 else None
            window = [value for _, value in observations[:252]]
            differences = [window[i] - window[i+1] for i in range(min(60, len(window)-1))]
            mean = sum(window)/len(window)
            sd = (sum((x-mean)**2 for x in window)/len(window))**.5 if len(window)>20 else None
            dm = sum(differences)/len(differences) if differences else None
            ds = (sum((x-dm)**2 for x in differences)/len(differences))**.5 if len(differences)>10 else None
            definition = DEFINITIONS.get(sid)
            hot[sid] = wrap(latest[1], field=sid, series=sid, observed=latest[0],
                            unit=definition[0] if definition else None,
                            frequency=definition[1] if definition else None,
                            seasonal_adjustment=definition[3] if definition else None,
                            definition_url="https://fred.stlouisfed.org/series/"+sid,
                            d1=round(window[0]-window[1],5) if len(window)>1 else None,
                            dmean60=round(dm,5) if dm is not None else None,
                            dstd60=round(ds,5) if ds else None,
                            mean252=round(mean,4), std252=round(sd,4) if sd else None,
                            statistics_basis="trailing observations, not days", statistics_n=len(window),
                            prev=previous[1] if previous else None, prev_date=previous[0] if previous else None,
                            panel=name, provider="fred", source_url=source_url, evidence=proof,
                            fetched_at=proof["first_received_at"], raw_snapshot_key=proof["key"])
            hot[sid]["freshness"] = observation_quality(sid, latest[0])
            got += 1
        except Exception as exc:
            reason = failures.get(sid) or type(exc).__name__ + ": " + str(exc)[:100]
            hot[sid] = missing(reason, field=sid, series=sid, provider="fred", panel=name)
    S[name] = {"ok": got == len(ids), "ids": len(ids), "with_data": got,
               "evidence_basis": "original_provider_responses", "missing_series": [sid for sid in ids if hot[sid].get("data_unavailable")]}


def _bls(hot, S):
    try:
        key = boto3.client("ssm", region_name="us-east-1").get_parameter(
            Name="/justhodl/bls-api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        key = None
    try:
        # Keyed BLS supports at most 20 years; public requests at most 10.
        year = datetime.now(timezone.utc).year
        body = json.dumps({"seriesid": BLS_EXTRA, "startyear": str(year-(19 if key else 9)),
                           "endyear": str(year), **({"registrationkey": key} if key else {})}).encode()
        url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        raw = _fetch(url, headers={"Content-Type": "application/json"}, data=body, timeout=45)
        data = json.loads(raw)
        if data.get("status") != "REQUEST_SUCCEEDED": raise ValueError("BLS request did not succeed")
        proof = capture(s3, BUCKET, "bls", url, raw)
        available = {row.get("seriesID"): row for row in data.get("Results", {}).get("series", [])}
        n = 0
        for sid in BLS_EXTRA:
            rows = [row for row in available.get(sid, {}).get("data", []) if row.get("period") in {"M%02d" % m for m in range(1,13)}]
            rows.sort(key=lambda row: (row["year"], row["period"]), reverse=True)
            if not rows:
                hot[sid] = missing("monthly series absent", field=sid, series=sid, provider="bls", panel="bls_labor")
                continue
            latest = rows[0]
            # These extra-series units are not inferred from numeric magnitude.
            # Catalog migration will fill them; incomplete envelopes cannot vote.
            hot[sid] = wrap(float(latest["value"]), field=sid, series=sid,
                            observed=latest["year"]+"-"+latest["period"], unit=None,
                            prev=float(rows[1]["value"]) if len(rows)>1 else None,
                            prev_date=rows[1]["year"]+"-"+rows[1]["period"] if len(rows)>1 else None,
                            panel="bls_labor", provider="bls", source_url=url,
                            evidence=proof, raw_snapshot_key=proof["key"], fetched_at=proof["first_received_at"])
            n += 1
        s3.put_object(Bucket=BUCKET, Key="data/warm/fred-canary/bls-labor.json.gz", Body=gzip.compress(raw))
        S["bls_labor"] = {"ok": n == len(BLS_EXTRA), "with_data": n, "keyed": bool(key)}
    except Exception as exc:
        reason = type(exc).__name__+": "+str(exc)[:100]
        S["bls_labor"] = {"data_unavailable": True, "reason": reason}
        for sid in BLS_EXTRA: hot[sid] = missing(reason, field=sid, series=sid, provider="bls", panel="bls_labor")


def _xlsx(name, urls, key, S):
    last = "none"
    for u in urls:
        try:
            raw = _fetch(u, timeout=120)
            if len(raw) < 8000:
                raise ValueError(f"small {len(raw)}b")
            if not raw.startswith(b"PK"):
                # ops 4535 (Perplexity P1): first-fetch assertion â€” an
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
    hot["flags"] = diagnostics(hot)
    hot["generated_at"] = datetime.now(timezone.utc).isoformat()
    hot["schema_version"] = "2.0"
    hot["authority"] = "research_only"
    hot["sizing_eligible"] = False
    hot["calls_eligible"] = False
    hot["collection_status"] = S
    s3.put_object(Bucket=BUCKET, Key="data/canary-macro.json",
                  Body=json.dumps(hot, default=str, allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    s3.put_object(Bucket=BUCKET, Key="data/warm/canary-macro-summary.json",
                  Body=json.dumps(S, default=str).encode(), ContentType="application/json", CacheControl="no-cache")


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
    _fred_panel("floor_reserves",
                ["SOFR", "IORB", "EFFR", "DFII10", "DGS2",
                 "DGS10"], hot, S)  # ops 4543: Perplexity 23-series
    _fred_panel("reserves_w", ["WRESBAL", "STLFSI4", "KCFSI"],
                hot, S)  # weekly lane (4546)
    _fred_panel("macro_q", ["GDP"], hot, S)  # quarterly lane (4546)
    _fred_panel("nfci", ["NFCI", "ANFCI", "NFCIRISK", "NFCICREDIT",
                          "NFCILEVERAGE", "NFCINONFINLEVERAGE"],
                hot, S)  # ops 4535: chicagofed via FRED (xlsx dead)
    _finalize(hot, S)  # ops 4510: hot feed lands EARLY, tail can't kill it
    # RECPROUSM156N is Chauvet/Piger's smoothed recession probability,
    # not the Cleveland Fed's yield-curve model. Preserve it under its real ID.
    S["cleveland_model"] = {"data_unavailable": True,
                            "reason": "No verified Cleveland model source; RECPROUSM156N is a different model"}
    if _budget(_t0, S, "atlanta_gdpnow"):
        _fred_panel("atlanta_gdpnow", ["GDPNOW"], hot, S)
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
