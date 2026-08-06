"""justhodl-nyfed-markets-full — registry worklist item 2 (ops 4463).

Beyond E8's five reference rates: the rest of the NY Fed Markets API that
the council unanimously flagged (0.8% share vs plumbing-platform thesis).
v1 pulls the bounded JSON endpoints — SOMA holdings summary, repo &
reverse-repo operation results, securities lending — and DISCOVERS the
Primary Dealer timeseries catalog (candidate-chain) storing it as the
cursor worklist for 100% PD convergence via hourly tranches. Explicit
failures; F4 snapshots; keyless."""
import gzip
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

BASE = "https://markets.newyorkfed.org/api"
BOUNDED = {
    "soma_summary": "/soma/summary.json",
    "repo_ops_latest": "/rp/all/all/results/latest.json",
    "seclending_latest": "/seclending/all/results/latest.json",
    "ambs_latest": "/ambs/all/results/latest.json",
}
PD_CATALOG_CANDS = ["/pd/list/timeseries.json",
                    "/pd/list/seriesbreaks.json"]
TRANCHE = int(os.environ.get("PD_TRANCHE", "20"))


def _fetch(path):
    req = urllib.request.Request(BASE + path, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return r.read()


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    summary = {"as_of": now, "bounded": {}, "pd": {}}
    for name, path in BOUNDED.items():
        try:
            raw = _fetch(path)
            rk = snapshot("nyfed", BASE + path, raw) if snapshot else None
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/nyfed-markets/{name}.json.gz",
                          Body=gzip.compress(json.dumps(
                              {"endpoint": path, "as_of": now,
                               "raw_snapshot_key": rk,
                               "payload": json.loads(raw)}).encode()),
                          ContentType="application/gzip")
            summary["bounded"][name] = {"ok": True,
                                        "bytes": len(raw)}
        except Exception as e:
            summary["bounded"][name] = {
                "data_unavailable": True,
                "reason": f"{type(e).__name__}: {str(e)[:70]}"}
    # PD catalog discovery + cursored tranche
    state_key = "data/warm/nyfed-markets/pd-state.json"
    try:
        state = json.loads(s3.get_object(
            Bucket=BUCKET, Key=state_key)["Body"].read())
    except Exception:
        state = {"catalog": None, "done": []}
    if not state.get("catalog"):
        for cand in PD_CATALOG_CANDS:
            try:
                d = json.loads(_fetch(cand))
                ts = (d.get("pd", {}).get("timeseries")
                      or d.get("timeseries") or [])
                keys = sorted({(x.get("keyid") or x.get("key")
                                or x.get("timeseries"))
                               for x in ts if isinstance(x, dict)} - {None})
                if len(keys) > 20:
                    state["catalog"] = keys
                    state["catalog_source"] = cand
                    break
            except Exception as e:
                state["catalog_note"] = (f"{cand}: {type(e).__name__}: "
                                         f"{str(e)[:60]}")
    got = 0
    if state.get("catalog"):
        todo = [k for k in state["catalog"]
                if k not in set(state["done"])][:TRANCHE]
        for k in todo:
            path = f"/pd/get/{k}.json"
            try:
                raw = _fetch(path)
                rk = (snapshot("nyfed", BASE + path, raw)
                      if snapshot else None)
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"data/warm/nyfed-markets/pd/{k}.json.gz",
                    Body=gzip.compress(json.dumps(
                        {"series": k, "as_of": now,
                         "raw_snapshot_key": rk,
                         "payload": json.loads(raw)}).encode()),
                    ContentType="application/gzip")
                state["done"].append(k)
                got += 1
            except Exception as e:
                state.setdefault("failures", {})[k] = \
                    f"{type(e).__name__}: {str(e)[:50]}"
            time.sleep(0.35)
        n = len(state["catalog"])
        nd = len(set(state["done"]))
        state["progress_pct"] = round(100 * nd / n, 1) if n else 0
        state["status"] = ("COMPLETE-maintaining" if nd >= n
                           else "converging")
    state["as_of"] = now
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    summary["pd"] = {"catalog": len(state.get("catalog") or []),
                     "catalog_source": state.get("catalog_source"),
                     "pulled_this_run": got,
                     "done_total": len(set(state.get("done", []))),
                     "progress_pct": state.get("progress_pct")}
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/nyfed-markets/latest-summary.json",
                  Body=json.dumps(summary, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True,
           "bounded_ok": [k for k, v in summary["bounded"].items()
                          if v.get("ok")],
           "bounded_failed": [k for k, v in summary["bounded"].items()
                              if v.get("data_unavailable")],
           "pd": summary["pd"]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
