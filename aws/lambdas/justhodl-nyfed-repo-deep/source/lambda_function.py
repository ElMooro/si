"""justhodl-nyfed-repo-deep — Khalid's endpoint sheet, the two true gaps
(ops 4494). Recon: repo-monitor covers recent ops; fedliquidityapi has
IORB; repo-market has RRP award; liquidity has RRPONTSYD. MISSING was:
 1) FULL operation history — every ON RRP + SRF/TOMO repo op since
    2014-01-01 via rp/results/search (his exact path), the primary-source
    record behind FRED's daily aggregates.
 2) OFR one-call bulk: series/dataset?dataset=repo — the complete repo
    microstructure (DVP/GCF/tri-party rates+volumes) in one payload.
Daily 05:22 -> data/warm/nyfed-markets/rp-{op}-history.json.gz +
data/warm/ofr/dataset-repo.json.gz. F4 snapshots; explicit failures.

v2 (ops 4748): also self-extends the spec-driven family banks from ops
4742-4747 (fxs, ambs, tsy, seclending, soma-summary) -- 45-day overlap
window against each doc's own stored endpoint, merge-deduped, never
clobbering. Banking history and KEEPING it current are separate jobs
(the import-canary prune lesson). Also recomputes each doc's 'latest'
from operation-date fields only, fixing tsy's maturity-date artifact."""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

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
                def _match(o):
                    ot = str(o.get("operationType", "")).lower() \
                        .replace(" ", "")
                    return (ot.startswith("reverse")
                            if op == "reverserepo"
                            else ot.startswith("repo"))
                ops = [o for o in ops_all if _match(o)]
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
    # ── v2 (ops 4748): self-extend the spec-driven family banks ─────
    OP_DATE_FIELDS = ("operationDate", "auctionDate", "tradeDate",
                       "effectiveDate", "settlementDate", "asOfDate",
                       "asofdate", "date")

    def _op_latest(rows):
        best = None
        for r0 in rows:
            if isinstance(r0, dict):
                for f in OP_DATE_FIELDS:
                    v = r0.get(f)
                    if isinstance(v, str) and len(v) >= 10 \
                            and v[:4].isdigit():
                        if best is None or v[:10] > best:
                            best = v[:10]
                        break
        return best

    def _first_rows(d):
        if isinstance(d, list):
            return d
        if isinstance(d, dict):
            for v in d.values():
                if isinstance(v, list) and v:
                    return v
            for v in d.values():
                if isinstance(v, dict):
                    for v2 in v.values():
                        if isinstance(v2, list) and v2:
                            return v2
        return []

    def _refresh_family(fam, key, dated):
        try:
            raw0 = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            doc = json.loads(gzip.decompress(raw0))
            rows = doc.get("rows") or []
            ep = doc.get("endpoint")
            if not ep:
                raise ValueError("no endpoint stored in doc")
            if dated:
                start = (now - timedelta(days=45)).strftime("%Y-%m-%d")
                join = "&" if "?" in ep else "?"
                u = f"{ep}{join}startDate={start}&endDate={today}"
            else:
                u = ep
            new = _first_rows(json.loads(_fetch(u)))
            seen = {json.dumps(r0, sort_keys=True)
                    if isinstance(r0, dict) else str(r0) for r0 in rows}
            added = 0
            for r0 in new:
                k0 = (json.dumps(r0, sort_keys=True)
                      if isinstance(r0, dict) else str(r0))
                if k0 not in seen:
                    seen.add(k0)
                    rows.append(r0)
                    added += 1
            doc["rows"] = rows
            doc["n_rows"] = len(rows)
            lt = _op_latest(rows)
            if lt:
                doc["latest"] = lt
            doc["refreshed_at"] = S["as_of"]
            s3.put_object(Bucket=BUCKET, Key=key,
                          Body=gzip.compress(json.dumps(
                              doc, separators=(",", ":")).encode()),
                          ContentType="application/json",
                          ContentEncoding="gzip")
            S[fam] = {"ok": True, "n_rows": len(rows), "added": added,
                      "latest": doc.get("latest")}
        except Exception as e:
            S[fam] = {"data_unavailable": True,
                      "reason": f"{type(e).__name__}: {str(e)[:70]}"}

    for fam, key, dated in (
            ("fxs", "data/warm/nyfed-markets/fxs-history.json.gz", True),
            ("ambs", "data/warm/nyfed-markets/ambs-history.json.gz", True),
            ("tsy", "data/warm/nyfed-markets/tsy-history.json.gz", True),
            ("seclending",
             "data/warm/nyfed-markets/seclending-history.json.gz", True),
            ("soma_summary",
             "data/warm/nyfed-markets/soma-summary-history.json.gz",
             False)):
        _refresh_family(fam, key, dated)

    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/nyfed-markets/repo-deep-summary.json",
                  Body=json.dumps(S, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(S, default=str)[:400])
    return {"statusCode": 200, "body": json.dumps(S, default=str)}
