"""justhodl-plumbing-panel — Khalid's plumbing map, the three true gaps
(ops 4496). Weekly Fri 14:05 ET-ish (18:05 UTC) + on demand.
 1) PD ONE-CALL (his recommended pattern): seriesbreaks.json -> newest
    break -> pd/latest/{BRK}.csv = the ENTIRE FR2004 weekly in one pull
    (PDFTD-*/PDFTR-* fails, PDPOS* positions, PDSOOS financing) ->
    warm; FTD/FTR row counts surfaced.
 2) OFR datasets mmf + nypd (completing the trio with repo).
 3) Chicago Fed NFCI 105-indicator weekly xlsx (his exact link).
F4 snapshots; explicit failures."""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None


def _fetch(u, timeout=120, browser=False):
    hdr = ({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/126 Safari/537.36",
            "Accept": "*/*",
            "Referer": "https://www.chicagofed.org/research/data/nfci/"
                       "current-data"} if browser else
           {"User-Agent": "JustHodl research admin@justhodl.ai"})
    req = urllib.request.Request(u, headers=hdr)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    if b[:2] == b"\x1f\x8b":
        b = gzip.decompress(b)
    return b


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    S = {"as_of": now.isoformat(timespec="seconds")}
    # 1) PD one-call
    try:
        sb = json.loads(_fetch("https://markets.newyorkfed.org/api/pd/"
                               "list/seriesbreaks.json"))
        breaks = (sb.get("pd", {}).get("seriesbreaks")
                  or sb.get("seriesbreaks") or [])
        # ops 4497: alphabetical picked SBP2013 (P>N) — the CURRENT
        # break is the one with the latest startDate / open endDate.
        def _start(b):
            return str(b.get("startDate") or b.get("startdate") or "")
        cur = sorted([b for b in breaks if isinstance(b, dict)],
                     key=_start)[-1] if breaks else {}
        brk = (cur.get("seriesbreak") or cur.get("id") or "SBN2024")
        u = f"https://markets.newyorkfed.org/api/pd/latest/{brk}.csv"
        raw = _fetch(u, timeout=180)
        rk = snapshot("nyfed", u, raw[:400000]) if snapshot else None
        lines = raw.decode("utf-8", "replace").splitlines()
        ftd = sum(1 for ln in lines if "PDFTD" in ln)
        ftr = sum(1 for ln in lines if "PDFTR" in ln)
        s3.put_object(Bucket=BUCKET,
                      Key=f"data/warm/nyfed-markets/pd-latest-{brk}"
                          f".csv.gz",
                      Body=gzip.compress(raw),
                      ContentType="application/gzip")
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/nyfed-markets/pd-latest.csv.gz",
                      Body=gzip.compress(raw),
                      ContentType="application/gzip")
        S["pd_one_call"] = {"ok": True, "break": brk,
                            "rows": len(lines) - 1,
                            "fails_to_deliver_rows": ftd,
                            "fails_to_receive_rows": ftr,
                            "kb": round(len(raw) / 1024),
                            "raw_snapshot_key": rk}
    except Exception as e:
        S["pd_one_call"] = {"data_unavailable": True,
                            "reason": f"{type(e).__name__}: "
                                      f"{str(e)[:70]}"}
    # 2) OFR mmf + nypd
    for ds in ("mmf", "nypd"):
        try:
            u = ("https://data.financialresearch.gov/v1/series/"
                 f"dataset?dataset={ds}")
            raw = _fetch(u, timeout=180)
            rk = snapshot("ofr", u, raw[:400000]) if snapshot else None
            d = json.loads(raw)
            n = (len(d.get("timeseries", d)) if isinstance(d, dict)
                 else len(d))
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/ofr/dataset-{ds}.json.gz",
                          Body=gzip.compress(json.dumps(
                              {"source_url": u,
                               "raw_snapshot_key": rk,
                               "as_of": S["as_of"], "n_series": n,
                               "payload": d}).encode()),
                          ContentType="application/gzip")
            S[f"ofr_{ds}"] = {"ok": True, "n_series": n,
                              "mb": round(len(raw) / 1e6, 1)}
        except Exception as e:
            S[f"ofr_{ds}"] = {"data_unavailable": True,
                              "reason": f"{type(e).__name__}: "
                                        f"{str(e)[:60]}"}
    # 3) Chicago NFCI xlsx
    try:
        last = "none"
        for u in ("https://www.chicagofed.org/-/media/publications/"
                  "nfci/nfci-data-series-xlsx.xlsx",
                  "https://www.chicagofed.org/-/media/publications/"
                  "nfci/nfci-data-series-xlsx.xlsx?la=en",
                  "https://www.chicagofed.org/~/media/publications/"
                  "nfci/nfci-data-series-xlsx.xlsx"):
            try:
                raw = _fetch(u, timeout=120, browser=True)
                break
            except Exception as e:
                last = f"{type(e).__name__}: {str(e)[:40]}"
        else:
            raise ValueError(last)
        if len(raw) < 50_000:
            raise ValueError(f"small {len(raw)}b")
        rk = snapshot("chicagofed", u, raw[:200000]) if snapshot else None
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/chicagofed/nfci-105.xlsx",
                      Body=raw)
        S["nfci_xlsx"] = {"ok": True, "kb": round(len(raw) / 1024),
                          "raw_snapshot_key": rk}
    except Exception as e:
        S["nfci_xlsx"] = {"data_unavailable": True,
                          "reason": f"{type(e).__name__}: "
                                    f"{str(e)[:60]}"}
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/plumbing-panel-summary.json",
                  Body=json.dumps(S, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(S, default=str)[:400])
    return {"statusCode": 200, "body": json.dumps(S, default=str)}
