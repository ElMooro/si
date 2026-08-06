"""justhodl-nyfed-repo-deep — Khalid's endpoint sheet, the two true gaps
(ops 4494). Recon: repo-monitor covers recent ops; fedliquidityapi has
IORB; repo-market has RRP award; liquidity has RRPONTSYD. MISSING was:
 1) FULL operation history — every ON RRP + SRF/TOMO repo op since
    2014-01-01 via rp/results/search (his exact path), the primary-source
    record behind FRED's daily aggregates.
 2) OFR one-call bulk: series/dataset?dataset=repo — the complete repo
    microstructure (DVP/GCF/tri-party rates+volumes) in one payload.
Daily 05:22 -> data/warm/nyfed-markets/rp-{op}-history.json.gz +
data/warm/ofr/dataset-repo.json.gz. F4 snapshots; explicit failures."""
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


def _fetch(u, timeout=120):
    req = urllib.request.Request(u, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    if b[:2] == b"\x1f\x8b":
        b = gzip.decompress(b)
    return b


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    S = {"as_of": now.isoformat(timespec="seconds")}
    for op in ("reverserepo", "repo"):
        u = (f"https://markets.newyorkfed.org/api/rp/results/search.json"
             f"?startDate=2014-01-01&endDate={today}&operationType={op}")
        cands = [u,
                 f"https://markets.newyorkfed.org/api/rp/{op}/all/"
                 f"results/search.json?startDate=2014-01-01"
                 f"&endDate={today}"]
        last = "none"
        for cu in cands:
            try:
                raw = _fetch(cu)
                rk = (snapshot("nyfed", cu, raw[:400000])
                      if snapshot else None)
                d = json.loads(raw)
                ops_all = (d.get("repo", {}).get("operations")
                           or d.get("operations") or [])
                # ops 4495: server ignored operationType — split
                # client-side; keep only this op's rows
                ops = [o for o in ops_all
                       if op.rstrip("s") in str(
                           o.get("operationType", "")).lower()
                       .replace(" ", "")]
                if not ops:
                    ops = ops_all  # shape drift: keep full, labelled
                if not ops:
                    raise ValueError("zero operations parsed")
                ops.sort(key=lambda o: o.get("operationDate", ""),
                         reverse=True)
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"data/warm/nyfed-markets/rp-{op}-history"
                        f".json.gz",
                    Body=gzip.compress(json.dumps(
                        {"operation": op, "source_url": cu,
                         "raw_snapshot_key": rk, "n_ops": len(ops),
                         "span": (f"{ops[-1].get('operationDate')}"
                                  f"..{ops[0].get('operationDate')}"),
                         "operations": ops}).encode()),
                    ContentType="application/gzip")
                latest = ops[0]
                S[f"rp_{op}"] = {
                    "ok": True, "n_ops": len(ops),
                    "latest_date": latest.get("operationDate"),
                    "latest_accepted": (latest.get("totalAmtAccepted")
                                        or latest.get("amtAccepted")
                                        or latest.get(
                                            "totalAccepted")),
                    "row_keys": sorted(latest.keys())[:12]}
                break
            except Exception as e:
                last = f"{cu[:70]} -> {type(e).__name__}: {str(e)[:50]}"
        else:
            S[f"rp_{op}"] = {"data_unavailable": True, "reason": last}
    try:
        u = ("https://data.financialresearch.gov/v1/series/dataset"
             "?dataset=repo")
        raw = _fetch(u, timeout=180)
        rk = snapshot("ofr", u, raw[:400000]) if snapshot else None
        d = json.loads(raw)
        n = (len(d) if isinstance(d, list) else
             len(d.get("timeseries", d)) if isinstance(d, dict) else 0)
        s3.put_object(Bucket=BUCKET,
                      Key="data/warm/ofr/dataset-repo.json.gz",
                      Body=gzip.compress(json.dumps(
                          {"source_url": u, "raw_snapshot_key": rk,
                           "as_of": S["as_of"], "n_series": n,
                           "payload": d}).encode()),
                      ContentType="application/gzip")
        S["ofr_dataset"] = {"ok": True, "n_series": n,
                            "mb": round(len(raw) / 1e6, 1)}
    except Exception as e:
        S["ofr_dataset"] = {"data_unavailable": True,
                            "reason": f"{type(e).__name__}: "
                                      f"{str(e)[:70]}"}
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/nyfed-markets/repo-deep-summary.json",
                  Body=json.dumps(S, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(S, default=str)[:400])
    return {"statusCode": 200, "body": json.dumps(S, default=str)}
