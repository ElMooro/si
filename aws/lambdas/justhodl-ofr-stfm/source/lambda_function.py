"""justhodl-ofr-stfm — council unanimous #1 (ops 4462).

OFR Short-Term Funding Monitor: free, keyless, ~442 mnemonics across repo /
primary-dealer / MMF / rates / yields — THE primary dataset for a funding-
plumbing platform, at 0% share until today. 100%-of-provider pattern:
(1) discover the full mnemonic catalog (candidate-chain over metadata
endpoints — never assume shape), (2) pull FULL history for a bounded
tranche per run (cursor in the doc itself), (3) converge to every mnemonic
over runs, COMPLETE-and-maintain. Explicit failures; F4 snapshots."""
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

BASE = "https://data.financialresearch.gov/v1"
TRANCHE = int(os.environ.get("TRANCHE", "30"))
SEED = ["REPO-TRI_AR_OO-P", "REPO-TRI_TV_OO-P", "REPO-DVP_AR_OO-P",
        "REPO-GCF_AR_OO-P", "MMF-MMF_RP_TOT-M", "NYPD-PD_RP_TOT-W",
        "FNYR-SOFR-A", "FNYR-EFFR-A", "FNYR-BGCR-A", "FNYR-TGCR-A"]


def _fetch(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req, timeout=45) as r:
        return r.read()


def _catalog():
    """Candidate-chain discovery of the full mnemonic list."""
    cands = [f"{BASE}/metadata/mnemonics",
             f"{BASE}/metadata/query?output=by_mnemonic",
             f"{BASE}/metadata/search?query="]
    for cu in cands:
        try:
            raw = _fetch(cu)
            d = json.loads(raw)
            mns = []
            if isinstance(d, list):
                mns = [x if isinstance(x, str) else
                       (x.get("mnemonic") or x.get("id"))
                       for x in d]
            elif isinstance(d, dict):
                mns = list(d.keys()) or \
                    [x.get("mnemonic") for x in
                     (d.get("mnemonics") or d.get("results") or [])
                     if isinstance(x, dict)]
            mns = sorted({m for m in mns if m and isinstance(m, str)})
            if len(mns) > 50:
                return mns, cu, None
        except Exception as e:
            last = f"{cu} -> {type(e).__name__}: {str(e)[:60]}"
    return SEED, "seed-fallback", last


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state_key = "data/warm/ofr/state.json"
    try:
        state = json.loads(s3.get_object(
            Bucket=BUCKET, Key=state_key)["Body"].read())
    except Exception:
        state = {"done": [], "catalog": None}
    if not state.get("catalog"):
        mns, src, err = _catalog()
        state["catalog"] = mns
        state["catalog_source"] = src
        if err:
            state["catalog_note"] = ("discovery fell back to seed; "
                                     f"last err: {err}")
    todo = [m for m in state["catalog"]
            if m not in set(state["done"])][:TRANCHE]
    got = failed = 0
    for m in todo:
        url = f"{BASE}/series/full?mnemonic={m}"
        try:
            raw = _fetch(url)
            rk = snapshot("ofr", url, raw) if snapshot else None
            d = json.loads(raw)
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/ofr/series/{m}.json.gz",
                          Body=gzip.compress(json.dumps(
                              {"mnemonic": m, "source_url": url,
                               "raw_snapshot_key": rk, "as_of": now,
                               "payload": d}).encode()),
                          ContentType="application/gzip")
            state["done"].append(m)
            got += 1
        except Exception as e:
            failed += 1
            state.setdefault("failures", {})[m] = \
                f"{type(e).__name__}: {str(e)[:60]}"
        time.sleep(0.4)
    n_cat = len(state["catalog"])
    n_done = len(set(state["done"]))
    state["as_of"] = now
    state["progress_pct"] = round(100 * n_done / n_cat, 1) if n_cat else 0
    state["status"] = ("COMPLETE-maintaining" if n_done >= n_cat
                       else "converging")
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "catalog": n_cat,
           "catalog_source": state.get("catalog_source"),
           "pulled_this_run": got, "failed": failed,
           "done_total": n_done, "progress_pct": state["progress_pct"]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
